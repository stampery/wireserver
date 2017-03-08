package com.scalanerds.wire.opcodes

import akka.util.ByteString
import com.scalanerds.utils.Utils._
import com.scalanerds.wire.conversions._
import com.scalanerds.wire.{Message, MsgHeader}
import org.bson.BsonDocument

object OpDelete {
  def apply(msgHeader: MsgHeader, content: Array[Byte]): OpDelete = {
    val it = content.iterator
    val reserved = it.getInt
    val fullCollectionName = it.getString
    val flags = OpDeleteFlags(it.getInt)
    val selector = it.getBson
    new OpDelete(msgHeader, fullCollectionName, flags, selector, reserved)
  }
}

class OpDelete(val msgHeader: MsgHeader,
               val fullCollectionName: String,
               val flags: OpDeleteFlags,
               val selector: BsonDocument,
               val reserved: Int = 0) extends Message {
  override def serialize: ByteString = {
    val content = msgHeader.serialize ++
      reserved.toByteArray ++
      fullCollectionName.toByteArray ++
      flags.serialize ++
      selector.toByteArray

    ByteString((content.length + 4).toByteArray ++ content)
  }

  override def toString: String = {
    s"""
       |$msgHeader
       |fullCollectionName: $fullCollectionName
       |flags: $flags
     """.stripMargin
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[OpDelete]

  override def equals(other: Any): Boolean = other match {
    case that: OpDelete =>
      (that canEqual this) &&
        msgHeader.opCode == that.msgHeader.opCode &&
        fullCollectionName == that.fullCollectionName &&
        flags == that.flags &&
        selector == that.selector
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(msgHeader.opCode, fullCollectionName, flags, selector)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}


object OpDeleteFlags {
  def apply(raw: Int): OpDeleteFlags = {
    val bytes = raw.toBooleanArray
    new OpDeleteFlags(
      bytes(0)
    )
  }
}

class OpDeleteFlags(val singleRemove: Boolean = false) {
  def serialize: Array[Byte] = {
    Array[Byte](singleRemove).binaryToInt.toByteArray
  }

  override def toString: String = {
    s"""
       |singleRemove: $singleRemove
     """.stripMargin
  }
}