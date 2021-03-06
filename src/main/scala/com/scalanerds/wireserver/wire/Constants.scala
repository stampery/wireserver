package com.scalanerds.wireserver.wire

import scala.language.implicitConversions

package object conversions {
  implicit def byte2bool(b: Byte): Boolean = b.toInt != 0
  implicit def bool2byte(b: Boolean): Byte = (if (b) 1 else 0).toByte
}

object OPCODES {
  val opReply         = 1
  val opMsg           = 1000
  val opUpdate        = 2001
  val opInsert        = 2002
  val opQuery         = 2004
  val opGetMore       = 2005
  val opDelete        = 2006
  val opKillCursor    = 2007
  val opCommand       = 2010
  val opCommandReply  = 2011

  val clientCodes = Vector(opUpdate, opInsert, opQuery, opGetMore, opDelete, opKillCursor, opCommand)
  val serverCodes = Vector(opReply, opMsg, opCommandReply)
}

