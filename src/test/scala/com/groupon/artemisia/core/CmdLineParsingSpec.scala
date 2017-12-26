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

package com.groupon.artemisia.core

import com.groupon.artemisia.TestSpec

/**
 * Created by chlr on 12/11/15.
 */
class CmdLineParsingSpec extends TestSpec {

  var arr: Array[String] = _

  override def beforeEach() = {
     arr  = Array("run","--location","/var/tmp","--run-id","5f40e8c3-59c2-4548-8e51-71980c657bc0",
      "--config","/home/user/config.yml","--context","k1=v1,k2=v2")
  }

  "CmdLineParser" must "parse cmd line arguments correctly" in {
    val cmd_line_args = Main.parseCmdLineArguments(arr)
    info("verifying cmd property")
    cmd_line_args.cmd.getOrElse("") must be ("run")
    info("verifying value property")
    cmd_line_args.value.getOrElse("") must be ("/var/tmp")
    info("verifying workflow_id property")
    cmd_line_args.runId.getOrElse("") must be ("5f40e8c3-59c2-4548-8e51-71980c657bc0")
    info("verifying config property")
    cmd_line_args.config.getOrElse("") must be ("/home/user/config.yml")
    info("verifying context param")
    cmd_line_args.context.getOrElse("") must be ("k1=v1,k2=v2")
  }

  it must "throw an IllegalArgumentException when location parameter is missing for run command" in {
    arr(1) = "" ; arr(2) = "" //removing location argument and its value
    intercept[IllegalArgumentException] {
      Main.show_usage_on_error = false
      Main.main(arr)
    }
  }

}
