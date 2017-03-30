package com.scalanerds.wireserver.tcpserver

import java.net.InetSocketAddress

import akka.actor.{ActorRef, PoisonPill, Props}
import akka.stream.TLSProtocol._
import akka.stream._
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Keep, Merge, Sink, Source, TLS, Tcp}
import akka.util.ByteString
import akka.{Done, NotUsed}

import scala.concurrent.Future

object DualTcpServer {
  def props(props: (InetSocketAddress, InetSocketAddress) => Props, address: String = "localhost", port: Int = 3001):
  Props =
    Props(classOf[DualTcpServer], props, address, port)

}

/***
  * Tcp server that works with plain and SSL connections
  * @param props  the props of the actor that will process the ByteStreams
  * @param address the server binding address
  * @param port the server binding port
  */
class DualTcpServer(props: (InetSocketAddress, InetSocketAddress) => Props, address: String, port: Int) extends
  TcpServer(address, port) with TcpSSL with TcpFraming {
  private val serverSSL = TLS(sslContext("/server.keystore", "/truststore"),
    TLSProtocol.negotiateNewSession, TLSRole.server)

  override def handler: Sink[Tcp.IncomingConnection, Future[Done]] = Sink.foreach[Tcp.IncomingConnection] { conn =>
    var isSSL: Option[Boolean] = None
    println("Client connected from: " + conn.remoteAddress)

    val getFlow = () => {
      val actor: ActorRef = context.actorOf(props(conn.remoteAddress, conn.localAddress))
      val in: Sink[ByteString, NotUsed] = Flow[ByteString].to(Sink.actorRef(actor, PoisonPill))
      val out: Source[ByteString, Unit] = Source.actorRef[ByteString](100, OverflowStrategy.fail)
        .mapMaterializedValue(actor ! _)
      Flow.fromSinkAndSourceMat(framing.to(in), out)(Keep.none)
    }

    // handle for plain connections
    val plainFlow = getFlow()

    val ssl = Flow[SslTlsInbound]
      .collect[ByteString] { case SessionBytes(_, bytes) => bytes }
      .via(getFlow())
      .map[SslTlsOutbound](SendBytes)

    // handle ssl connections
    val sslFlow = serverSSL.reversed.join(ssl).alsoTo(Sink.onComplete(_ => println("Client disconnected")))


    // redirects the stream to sslFlow or plainFlow
    val router = Flow.fromGraph(GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      val src = b.add(Flow[ByteString].map(e => {
        if (isSSL.isEmpty) {
          // checks if the first message is a handshake
          isSSL = Some(e(0) == 22 && e.length != 22)
        }
        e
      }))

      val outbound = b.add(Flow[ByteString])

      val bcast = b.add(Broadcast[ByteString](2))
      val merge = b.add(Merge[ByteString](2))

      val plainFilter = Flow[ByteString].filter(_ => !isSSL.get)
      val sslFilter = Flow[ByteString].filter(_ => isSSL.get)

      src ~> bcast ~> plainFilter ~> plainFlow ~> merge ~> outbound
             bcast ~> sslFilter ~> sslFlow ~> merge

      FlowShape(src.in, outbound.out)
    })

    conn handleWith router
  }
}


