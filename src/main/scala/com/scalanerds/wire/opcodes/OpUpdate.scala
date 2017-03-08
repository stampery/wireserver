package com.scalanerds.wire.opcodes

import akka.util.ByteString
import com.scalanerds.utils.Utils._
import com.scalanerds.wire.conversions._
import com.scalanerds.wire.{Message, MsgHeader}
import org.bson.BsonDocument

object OpUpdate {
  def apply(msgHeader: MsgHeader, content: Array[Byte]): OpUpdate = {
    val it = content.iterator
    val reserved = it.getInt
    val fullCollectionName = it.getString
    val flags = OpUpdateFlags(it.getInt)
    val bson = it.getBsonArray
    val selector = bson(0)
    val update = bson(1)
    new OpUpdate(msgHeader, fullCollectionName, flags, selector, update, reserved)
  }
}

class OpUpdate(val msgHeader: MsgHeader,
               val fullCollectionName: String,
               val flags: OpUpdateFlags,
               val selector: BsonDocument,
               val update: BsonDocument,
               val reserved: Int = 0) extends Message {

  override def serialize: ByteString = {
    val content = msgHeader.serialize ++
      reserved.toByteArray ++
      fullCollectionName.toByteArray ++
      flags.serialize ++
      selector.toByteArray ++
      update.toByteArray

    ByteString((content.length + 4).toByteArray ++ content)
  }

  override def toString: String = {
    s"""
       |$msgHeader
       |fullCollectionName: $fullCollectionName
       |flags: $flags
       |selector: $selector
       |update: $update
     """.stripMargin
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[OpUpdate]

  override def equals(other: Any): Boolean = other match {
    case that: OpUpdate =>
      (that canEqual this) &&
        msgHeader.opCode == that.msgHeader.opCode &&
        fullCollectionName == that.fullCollectionName &&
        flags == that.flags &&
        selector == that.selector &&
        update == that.update
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(msgHeader.opCode, fullCollectionName, flags, selector, update)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

object OpUpdateFlags {
  def apply(raw: Int): OpUpdateFlags = {
    val bytes = raw.toBooleanArray
    new OpUpdateFlags(
      upsert = bytes(0),
      multiUpdate = bytes(1)
    )
  }
}

class OpUpdateFlags(val upsert: Boolean = false,
                    val multiUpdate: Boolean = false) {
  def serialize: Array[Byte] = {
    Array[Byte](upsert, multiUpdate).binaryToInt.toByteArray
  }

  override def toString: String = {
    s"""
       |upsert: $upsert
       |multiUpdate: $multiUpdate
     """.stripMargin
  }
}