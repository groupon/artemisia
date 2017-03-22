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

package com.groupon.artemisia.task.localhost.util

import java.io.{BufferedReader, File, InputStreamReader}
import scala.collection.JavaConversions._

/**
 * Created by chlr on 2/23/16.
 */
class ProcessRunner(val interpreter: String = "/bin/sh") {

  def executeInShell(cwd: String = System.getProperty("user.home"), env: Map[String, String] = Map())(body : String): (String,String,Int) = {
    val pb = new  ProcessBuilder()
    pb.directory(new File(cwd))
    pb.redirectOutput(ProcessBuilder.Redirect.PIPE)
    pb.redirectError(ProcessBuilder.Redirect.PIPE)
    val env_variables = pb.environment()
    env map { vars => env_variables.put(vars._1,vars._2) }
    pb.command(interpreter :: "-c" :: s""" " ${body.split(System.getProperty("line.separator")).filter(_.trim.length > 0).mkString(" ; ")} " """ :: Nil)
    this.execute(pb)
  }

  def executeFile(cwd: String = System.getProperty("user.home"), env: Map[String, String] = Map())(file: String): (String,String,Int) = {
    val pb = new  ProcessBuilder()
    pb.directory(new File(cwd))
    pb.redirectOutput(ProcessBuilder.Redirect.PIPE)
    pb.redirectError(ProcessBuilder.Redirect.PIPE)
    val env_variables = pb.environment()
    env map { vars => env_variables.put(vars._1,vars._2) }
    pb.command(interpreter :: file :: Nil)
    this.execute(pb)

  }

  private def execute(pb: ProcessBuilder) : (String,String,Int) = {
    val process = pb.start()
    val stdout_buffer = new BufferedReader( new InputStreamReader(process.getInputStream))
    val stderr_buffer = new BufferedReader( new InputStreamReader(process.getErrorStream))
    val stdout = Stream.continually(stdout_buffer.readLine()).takeWhile(_ != null).mkString(System.getProperty("line.separator"))
    val stderr = Stream.continually(stderr_buffer.readLine()).takeWhile(_ != null).mkString(System.getProperty("line.separator"))
    val return_code = process.waitFor()
    (stdout,stderr,return_code)
  }

}
