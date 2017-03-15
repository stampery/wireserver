package com.scalanerds.wireserver.example.handler

import java.net.InetSocketAddress

import akka.actor.{ActorRef, PoisonPill, Props}
import akka.event.Logging
import akka.io.Tcp.{Close, Write}
import akka.util.ByteString
import com.scalanerds.wireserver.example.tcpClient.TcpClient
import com.scalanerds.wireserver.handlers.{HandlerProps, MsgHandler}
import com.scalanerds.wireserver.tcpserver.Packet
import com.scalanerds.wireserver.wire.opcodes._

object SnifferServerProps extends HandlerProps {
  def props(connection: ActorRef) = Props(classOf[SnifferServer], connection)
}

class SnifferServer(connection: ActorRef) extends MsgHandler(connection) {

//  val log = Logging(context.system, this)
  val tcpClient: ActorRef = context.actorOf(Props(new TcpClient(self, new InetSocketAddress("localhost", 27017))), "sniffer")

  override def received(data: ByteString): Unit = {
    log.debug("alice says: \n" + data.mkString(", "))
    parse(data)
    tcpClient ! Packet("mongocli", data)
  }

  override def received(packet: Packet): Unit = {
    log.debug("bob says: \n" + packet.data.mkString(", "))
    parse(packet.data)
    connection ! Write(packet.data)
  }

  override def received(str: String): Unit = {
    str match {
      case "close" => connection ! Close
      case m => log.debug(s"no match for message $m")
    }
  }
  override def peerClosed() {
    tcpClient ! PoisonPill
    connection ! Close
  }

  override def onOpReply(msg: OpReply): Unit = log.debug(s"OpReply\n$msg\n")

  override def onOpMsg(msg: OpMsg): Unit = log.debug(s"OpMsg\n$msg\n")

  override def onOpUpdate(msg: OpUpdate): Unit = log.debug(s"OpUpdate\n$msg\n")

  override def onOpInsert(msg: OpInsert): Unit = log.debug(s"OpInsert\n$msg\n")

  override def onOpQuery(msg: OpQuery): Unit = log.debug(s"OpQuery\n$msg\n")

  override def onOpGetMore(msg: OpGetMore): Unit = log.debug(s"OpGetMore\n$msg\n")

  override def onOpDelete(msg: OpDelete): Unit = log.debug(s"OpDelete\n$msg\n")

  override def onOpKillCursor(msg: OpKillCursor): Unit = log.debug(s"OpKillCursor\n$msg\n")

  override def onOpCommand(msg: OpCommand): Unit = log.debug(s"OpCommand\n$msg\n")

  override def onOpCommandReply(msg: OpCommandReply): Unit = log.debug(s"OpCommandReply\n$msg\n")

  override def onError(msg: Any): Unit = {
    log.debug("sniffer error")
    tcpClient ! "drop connection"
    tcpClient ! stop
    connection ! Close
    log.debug(s"Unknown message\n$msg\n")
  }
}
