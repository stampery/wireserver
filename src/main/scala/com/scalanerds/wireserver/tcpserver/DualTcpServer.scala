package com.scalanerds.wireserver.tcpserver

import java.net.InetSocketAddress

import akka.actor.{Actor, ActorRef, PoisonPill, Props, Stash}
import akka.io.Tcp.Close
import akka.stream.TLSProtocol._
import akka.stream._
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Keep, Merge, Sink, Source, TLS, Tcp}
import akka.util.ByteString
import akka.{Done, NotUsed}

import scala.concurrent.Future

object DualTcpServer {
  def props(props: (InetSocketAddress, InetSocketAddress, SharedKillSwitch) => Props, address: String = "localhost", port: Int = 3001):
  Props =
    Props(classOf[DualTcpServer], props, address, port)
}

/** *
  * Tcp server that works with plain and SSL connections
  *
  * @param streamHandlerProps   the props of the actor that handles the ByteStream
  * @param address the server binding address
  * @param port    the server binding port
  *
  */
class DualTcpServer(streamHandlerProps: (InetSocketAddress, InetSocketAddress, SharedKillSwitch) => Props, address: String, port: Int) extends
  TcpServer(address, port) with TcpSSL with TcpFraming {
  private val serverSSL = TLS(sslContext("/server.keystore", "/truststore"),
    TLSProtocol.negotiateNewSession, TLSRole.server)

  override def handler: Sink[Tcp.IncomingConnection, Future[Done]] = Sink.foreach[Tcp.IncomingConnection] { conn =>
    var isSSL: Option[Boolean] = None
    println("Client connected from: " + conn.remoteAddress + " dual")


    val killSwitch: SharedKillSwitch = KillSwitches.shared("switch")
    val streamHandler: ActorRef = context.actorOf(streamHandlerProps(conn.remoteAddress, conn.localAddress, killSwitch))

    val getFlow: (String) => Flow[ByteString, ByteString, NotUsed] = (name: String) => {
      val forwarderActor: ActorRef = context.actorOf(Props(new ForwarderActor(streamHandler)), name + Math.random())
      val in: Sink[ByteString, NotUsed] = Flow[ByteString].to(Sink.actorRef(forwarderActor, PoisonPill))
      val out = Source.actorRef[ByteString](100, OverflowStrategy.fail)
        .mapMaterializedValue(forwarderActor ! _)

      Flow.fromSinkAndSourceMat(framing.to(in), out)(Keep.none)
    }

    val plainFlow = getFlow("plainActor")

    val ssl = Flow[SslTlsInbound]
      .collect[ByteString] { case SessionBytes(_, bytes) => bytes }
      .via(getFlow("sslActor"))
      .map[SslTlsOutbound](SendBytes)

    // handle ssl connections
    val sslFlow: Flow[ByteString, ByteString, NotUsed] = serverSSL.reversed.join(ssl).alsoTo(Sink.onComplete(_ => {
      println("Client disconnected")
      streamHandler ! PoisonPill
    }))



    // redirects the stream to sslFlow or plainFlow
    val router = Flow.fromGraph(GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      val src = b.add(Flow[ByteString].map(stream => {
        // checks if the first message is a handshake
        if (isSSL.isEmpty) isSSL = Some(stream(0) == 22 && stream.length != 22)
        stream
      }))


      val outbound = b.add(Flow[ByteString])

      val bcast = b.add(Broadcast[ByteString](2))
      val merge = b.add(Merge[ByteString](2))

      val plainFilter = Flow[ByteString].filter(_ => !isSSL.get)
      val sslFilter = Flow[ByteString].filter(_ => isSSL.get)

      src ~> bcast ~> plainFilter ~> plainFlow ~> merge  ~> outbound
             bcast ~> sslFilter   ~> sslFlow   ~> merge

      FlowShape(src.in, outbound.out)
    })

    conn handleWith router.via(killSwitch.flow)
  }
}

/** *
  * Actor that forwards the bytestrings after everything has been initialized
  *
  * this actor is required to avoid creating two actors that process the ByteStream, as one can't be shared
  * between the sink and the source of the connection because the flow in the router would fail in the merge
  *
  * @param streamHandler actor that will process the ByteStream
  */
class ForwarderActor(streamHandler: ActorRef) extends Actor with Stash {
  var src: Option[ActorRef] = None
  var gotBytes = false

  override def receive: Receive = uninitialized

  def uninitialized: Receive = {
    case ref: ActorRef if gotBytes =>
      streamHandler ! ref
      context.become(initialized)
      unstashAll()
    case ref: ActorRef =>
      src = Some(ref)
    case _: ByteString if src.nonEmpty =>
      stash()
      context.become(initialized)
      streamHandler ! src.get
      unstashAll()
    case _: ByteString =>
      stash()
      gotBytes = true
    case m => println(s"forwarder uninitialized got unknown message $m")
  }

  def initialized: Receive = {

    case bytes: ByteString =>
      streamHandler forward bytes

    case m => s"forwarder got unknown message $m"
  }
}


