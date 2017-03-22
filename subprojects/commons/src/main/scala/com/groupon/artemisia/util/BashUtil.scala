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

import java.io.ByteArrayOutputStream
import CommandUtil._
import com.groupon.artemisia.core.AppLogger._

/**
  * This class hosts utility methods that is guaranteed to work only in Bash shell.
  * It might or might not work in other Linux shells.
  */
object BashUtil {


  /**
    * expand path to a sequence of files
    * @param path
    */
  def listFiles(path: String): Seq[String] = {
    val byteStream = new ByteArrayOutputStream()
    executeShellCommand(s"ls -1 $path", stdout = byteStream)
    byteStream.toString.split(System.lineSeparator)
  }


  /**
    * return total size of the path in bytes.
    * @param path input path
    * @return
    */
  def pathSize(path: String): Long = {
    val byteStream = new ByteArrayOutputStream()
    executeShellCommand(s"du $path", stdout = byteStream)
    val size = byteStream.toString.split(System.lineSeparator).map(_.split("[\\s]+").head.toLong).sum
    debug(s"total size of $path is $size")
    size
  }


}
