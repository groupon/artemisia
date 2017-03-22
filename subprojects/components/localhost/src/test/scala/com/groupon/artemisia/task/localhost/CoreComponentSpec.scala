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

package com.groupon.artemisia.task.localhost

import com.typesafe.config.ConfigFactory
import com.groupon.artemisia.TestSpec

/**
 * Created by chlr on 6/18/16.
 */
class CoreComponentSpec extends TestSpec {

  val component = new CoreComponent("MySQL")

  "CoreComponent" must "dispatch ScriptTask when requested" in {
      val config = ConfigFactory parseString
        """
          |{
          |  script = "echo Hello World"
          |  interpreter = /usr/local/bin/sh
          |  cwd = /var/tmp
          |  env = { foo = bar, hello = world }
          |  parse_output = no
          |}
        """.stripMargin
      val task = component.dispatchTask(ScriptTask.taskName, "script", config)
      task mustBe a [ScriptTask]
  }

  it must "dispatch EmailTask when requested" in {
    val config = ConfigFactory parseString
      s"""
        |{
        |  connection = ${EmailTaskSpec.defaultConnectionConfig.root().render()}
        |  email = ${EmailTaskSpec.defaultEmailRequestConfig.root().render()}
        |}
      """.stripMargin
    val task = component.dispatchTask(EmailTask.taskName, "email", config)
    task mustBe a [EmailTask]
  }

  it must "dispatch RestAPITask when requested" in {
    val config = ConfigFactory parseString
      s"""
         | {
         |   request = {
         |      url = "http://api.examples.com/post/json"
         |      method = get
         |      headers = {
         |        foo = bar
         |      }
         |      payload-type = "json"
         |   }
         |   emit-output = yes
         |   allowed-status-codes = [105]
         | }
       """.stripMargin
    val task = component.dispatchTask(RestAPITask.taskName, "test", config)
    task mustBe a [RestAPITask]
  }

  it must "list all tasks in the component" in {
    component.tasks must contain only (RestAPITask, EmailTask, ScriptTask, SFTPTask)
  }
  
}
