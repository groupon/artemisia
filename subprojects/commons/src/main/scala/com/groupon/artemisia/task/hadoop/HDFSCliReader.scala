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

package com.groupon.artemisia.task.hadoop

import java.io.{ByteArrayOutputStream, InputStream, PipedInputStream, PipedOutputStream}
import java.net.URI
import com.groupon.artemisia.core.AppLogger._
import com.groupon.artemisia.util.CommandUtil._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
  * Created by chlr on 8/21/16.
  */

/**
  * A HDFS CLI Reader that uses the locally installed Hadoop shell utilities
  * to read from HDFS.
  *
  * @param cli
  */
class HDFSCLIReader(cli: String) {

  /**
    *
    * @param path hdfs path to be read
    * @return inputstream for the data to read
    */
  def readPath(path: String): InputStream = {
    val command = cli :: "dfs" :: "-text" :: path :: Nil
    val inputStream = new PipedInputStream(10485760)
    val outputStream = new PipedOutputStream(inputStream)
    Future {
      executeCmd(command, outputStream)
    } onComplete  {
      case Success(retCode) =>
        debug("reading from path $path completed successfully")
        outputStream.close()
        assert(retCode == 0, s"command ${command.mkString(" ")} failed with retcode $retCode")
      case Failure(th) =>
        outputStream.close()
        throw th;
    }
    inputStream
  }

  /**
    *
    * @param path hdfs path to be read
    * @return inputstream for the data to read
    */
  def readPath(path: URI): InputStream = {
    readPath(path.toString)
  }


  /**
    * get the total volume of data a given HDFS path holds.
    * @param path HDFS path to inspect
    * @return total size in bytes
    */
  def getPathSize(path: URI): Long = {
    getPathSize(path.toString)
  }


  /**
    * get the total volume of data a given HDFS path holds.
    * @param path HDFS path to inspect
    * @return total size in bytes
    */
  def getPathSize(path: String): Long = {
    val command = cli :: "dfs" :: "-du" :: path :: Nil
    val cmdResult = new ByteArrayOutputStream()
    val result = executeCmd(command, cmdResult)
    assert(result == 0, s"command ${command.mkString(" ")} failed with return code $result")
    val output = new String(cmdResult.toByteArray)
    val num = output.split(System.lineSeparator()).filterNot(_.startsWith("Found")) // tail is done to remove the first line which says 'Found n items'
      .map(_.split("\\s+").head.toLong).sum
    debug(s"the total size of path $path is $num bytes")
    num
  }

}
