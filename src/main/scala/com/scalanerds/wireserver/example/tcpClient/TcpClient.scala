package com.scalanerds.wireserver.example.tcpClient

import java.net.InetSocketAddress

import akka.actor.{Actor, ActorRef}
import akka.event.Logging
import akka.io.Tcp._
import akka.io.{IO, Tcp}
import akka.util.ByteString
import com.scalanerds.wireserver.messages.Ready
import com.scalanerds.wireserver.tcpserver.Packet

class TcpClient(listener: ActorRef, remote: InetSocketAddress) extends Actor {

  import context.system
  val log = Logging(context.system, this)
  log.debug("Eve constructor")
  override def preStart(): Unit = {
    log.debug("Eve is born")
    connect()
    super.preStart()
  }

  def receive: Receive = ready

  def ready: Receive = {
    case CommandFailed(_: Connect) =>
      log.debug("Eve connection failed.")
      context stop self

    case Connected(_, _) =>
      log.debug("Eve connection succeeded")
      val connection = sender()
      connection ! Register(self)
      listener ! Ready

      context become listening(connection)

    case msg: String => log.debug("wrong " + msg)
  }

  def listening(connection: ActorRef): Receive = {
    case Packet(_, data) =>
      connection ! Write(data)

    case data: ByteString =>
      listener ! Packet("mongod", data)

    case CommandFailed(_: Write) =>
      log.debug("client write failed")
      listener ! "write failed"

    case Received(data) =>
      listener ! Packet("mongod", data)

    case ConfirmedClose =>
      log.debug("client close")
      connection ! ConfirmedClose

    case ConfirmedClosed =>
      log.debug("client confirmed closed")
      listener ! ConfirmedClosed

    case "drop connection" =>
      log.debug("client drop connection")
      connection ! Close
      context become ready
      connect()

    case c: ConnectionClosed =>
      log.debug("client connectionClosed " + c.getErrorCause)
      listener ! "connection closed"
      context become ready
      connect()

    case PeerClosed =>
      log.debug("client peerClosed")
      context become ready
      connect()

    case msg => println(s"Something else is up.\n$msg")
  }

  def connect(): Unit = {
    println("Connecting client.")
    IO(Tcp) ! Connect(remote)
    println(s"Client connected to port ${remote.getPort}")
  }
}