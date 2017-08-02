package com.scalanerds.wireserver.example

import akka.actor.ActorSystem
import com.scalanerds.wireserver.example.handler.Sniffer
import com.scalanerds.wireserver.tcpserver.{DualTcpServer, PlainTcpServer, SSLTcpServer}

object Main extends App {

  val system = ActorSystem()

  // Tcp server
  system.actorOf(PlainTcpServer.props(Sniffer.props))
  // TLS enabled tcp server
  system.actorOf(SSLTcpServer.props(Sniffer.props))
  // Tcp server that works with plain and SSL connections
//  system.actorOf(DualTcpServer.props(Sniffer.props))
}
