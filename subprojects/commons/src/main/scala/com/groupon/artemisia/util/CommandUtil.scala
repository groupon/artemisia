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

import java.io.{File, OutputStream}
import org.apache.commons.exec.{CommandLine, DefaultExecutor, PumpStreamHandler}
import scala.collection.JavaConverters._
import com.groupon.artemisia.core.AppLogger._
import scala.util.{Failure, Success, Try}

/**
  * Created by chlr on 8/3/16.
  */

object CommandUtil {


  /**
    *
    * Execute command
    * @param command arguments to the executable
    * @param stdout standard output stream
    * @param stderr standard error stream
    * @param env environment variables to be used in addition to existing variables
    * @param cwd current working directory
    * @param obfuscate this specific argument while logging the command esp passwords.
    * @return return code of the command
    */
  def executeCmd(command: Seq[String], stdout: OutputStream = System.out, stderr: OutputStream = System.err
                 ,env: Map[String, String] = Map(), cwd: Option[File] = None, obfuscate: Seq[Int] = Nil
                ,validExitValues: Array[Int] = Array(0)): Int = {
    val cmdLine = new CommandLine(command.head)
    command.tail foreach cmdLine.addArgument
    val executor = new DefaultExecutor()
    cwd foreach executor.setWorkingDirectory
    executor.setStreamHandler(new PumpStreamHandler(stdout, stderr))
    executor.setExitValues(validExitValues)
    debug(s"""executing command ${obfuscatedCommandString(command, obfuscate)}""")
    Try(executor.execute(cmdLine, (env ++ System.getenv().asScala).asJava)) match {
      case Success(x) => x
      case Failure(th) =>
        error(s"command failed: ${command.mkString(" ")}")
        throw th
    }
  }


  /**
    *
    * Execute command within a shell. the shell used in /bin/sh.
    * @param command arguments to the executable
    * @param stdout standard output stream
    * @param stderr standard error stream
    * @param env environment variables to be used in addition to existing variables
    * @param cwd current working directory
    * @param obfuscate this specific argument while logging the command esp passwords.
    * @return return code of the command
    */
  def executeShellCommand(command: String, stdout: OutputStream = System.out, stderr: OutputStream = System.err
                          ,env: Map[String, String] = Map(), cwd: Option[File] = None, obfuscate: Seq[Int] = Nil) = {
    val cmdLine = CommandLine.parse("/bin/sh")
    cmdLine.addArguments(Array("-c", command), false)
    val executor  = new DefaultExecutor()
    cwd foreach executor.setWorkingDirectory
    executor.setStreamHandler(new PumpStreamHandler(stdout, stderr))
    debug(s"executing command: $command")
    executor.execute(cmdLine, (env ++ System.getenv().asScala).asJava)
  }


  /**
    * obfuscate sections of command string
    * @param command command sequence string
    * @param sections parts of the command sequence string to obfuscate
    * @return
    */
  def obfuscatedCommandString(command: Seq[String], sections: Seq[Int]) = {
    val boolList = for (i <- 1 to command.length) yield { sections contains i }
    command zip boolList map {
      case (_, true) => "*" * 5
      case (x, false) => x
    } mkString " "
  }

  /**
    * get the path of the executable by searching in the path environment variable
    *
    * @param executable name of the executable
    * @return
    */
  def getExecutablePath(executable: String): Option[String] = {
    val separator = System.getProperty("path.separator")
    scala.util.Properties.envOrNone("PATH") match {
      case Some(x) =>
              x.split(separator).map(new File(_).listFiles()).filter(_ != null)
                .flatMap(x => x).find(_.getName == executable)
                .map(_.toString)
      case None => None
    }
  }

  /**
    * search for an executable in PATH and if not found throw an exception.
    * @param executable executable to search for
    * @return absolute path of executable
    */
  def getExecutableOrFail(executable: String): String = {
    getExecutablePath(executable) match {
      case Some(exe) => exe
      case None => throw new RuntimeException(s"$executable not found in PATH")
    }
  }

}
