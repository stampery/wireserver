package com.mongonerds.wire.opcodes

import java.nio.ByteOrder.LITTLE_ENDIAN

import akka.util.ByteString
import com.mongonerds.wire.{Message, MsgHeader}
import org.bson.{BSON, BSONObject}

object OpCommand {
  def apply(msgHeader: MsgHeader, content: Array[Byte]): OpCommand = ???
}

class OpCommand(val msgHeader: MsgHeader) extends Message {
  override def serialize: ByteString = ???
}
