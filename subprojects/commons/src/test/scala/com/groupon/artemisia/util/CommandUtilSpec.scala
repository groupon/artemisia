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

import java.io.{ByteArrayOutputStream, File}
import CommandUtil._
import com.groupon.artemisia.TestSpec

/**
  * Created by chlr on 8/7/16.
  */

class CommandUtilSpec extends TestSpec {

  "CommandUtilSpec" must "get executable from path" in {
     if (new File(System.getenv("PATH").split(File.pathSeparator).head).list != null) {
       val randomFileInPath = new File(System.getenv("PATH").split(File.pathSeparator).head).list.head
       CommandUtil.getExecutablePath(randomFileInPath) match {
         case Some(x) => x.split(File.separatorChar).last must be(randomFileInPath)
         case None => fail(s"$randomFileInPath was not found in path")
       }
     }
    }

  it must "must executed command" in {
    TestUtils.runOnPosix {
      val cmd: Seq[String] = "echo" :: "hello" :: "world" :: Nil
      val stream = new ByteArrayOutputStream()
      val result = CommandUtil.executeCmd(cmd, stdout = stream)
      result must be (0)
      new String(stream.toByteArray).trim must be ("hello world")
    }
  }

  it must "obsfucate commands when needed" in {
    val command = "binary" :: "-password" :: "bingo" :: Nil
    CommandUtil.obfuscatedCommandString(command, Seq(3)) must be ("binary -password *****")
  }

  it must "ignore non-zero return codes when requested" in {
    val path = TestUtils.getExecutable(this.getClass.getResource("/executable/script_that_fails.sh"))
    val command = Seq(path, "3")
    executeCmd(command, validExitValues = Array(3))
    val ex = intercept[org.apache.commons.exec.ExecuteException] {
      executeCmd(command)
    }
    ex.getMessage must be ("Process exited with an error: 3 (Exit value: 3)")
  }


}
