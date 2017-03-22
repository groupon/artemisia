/*
 * Copyright (c) 2016, Groupon, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 *
 * Neither the name of GROUPON nor the names of its contributors may be
 * used to endorse or promote products derived from this software without
 * specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
 * IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package com.groupon.artemisia.util

/**
  * Created by chlr on 7/30/16.
  */

/**
  * An utility class for parsing memory literal to long
  * {{{
  *  scala> MemorySize("10MB").getBytes
  *  res5: Long = 10485760
  * }}}
  *
  * @param memory string literal of memory eg (75 kilobytes)
  */
class MemorySize(memory: String) {

  private val by = "b" :: "B" :: "byte" :: "bytes" :: Nil
  private val kb = "K" :: "k" :: "Ki" :: "KiB" :: "kB" :: "kilobyte" :: "kilobytes" :: Nil
  private val mb = "M" :: "m" :: "Mi" :: "MiB" :: "MB" :: "megabyte" :: "megabytes" :: Nil
  private val gb = "G" :: "g" :: "Gi" :: "GiB" :: "GB" :: "gigabyte" :: "gigabytes" :: Nil
  private val tb = "T" :: "t" :: "Ti" :: "TiB" :: "TB" :: "terabyte" :: "terabytes" :: Nil


  /**
    * get number of bytes in long
    * @return number of bytes in long
    */
  def toBytes: Long = {
    val rgx = (by ++ kb ++ mb ++ gb ++ tb) map { x => s"^([0-9]+)\\s*($x)$$".r }
    val matched = rgx map { _.findFirstMatchIn(memory) } collect {
      case Some(x) => x.group(1) -> x.group(2)
    }
    matched match {
      case head :: Nil => head match {
        case (x, y) if by contains y => x.toLong
        case (x, y) if kb contains y => x.toLong * 1024
        case (x, y) if mb contains y => x.toLong * 1024 * 1024
        case (x, y) if gb contains y => x.toLong * 1024 * 1024 * 1024
        case (x, y) if tb contains y => x.toLong * 1024 * 1024 * 1024 * 1024
        case _ => throw new RuntimeException(s"only bytes, kilobytes, megabytes, gigabytes, terabytes are supported")
      }
      case _ => throw new RuntimeException(s"$memory cannot be parsed. only bytes, kilobytes, megabytes, gigabytes, terabytes are supported")
    }
  }

}

object MemorySize {

  def apply(memory: String): MemorySize = new MemorySize(memory)

}
