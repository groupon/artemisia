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

import com.groupon.artemisia.TestSpec
import com.groupon.artemisia.util.HoconConfigUtil.Handler

/**
 * Created by chlr on 3/6/16.
 */
class ScriptTaskSpec extends TestSpec {

  "ScriptTask" must "must execute command and parse result" in {
    val value = "foo" -> "bar"
    val task = new ScriptTask(script = s"echo { ${value._1} = ${value._2} }",parseOutput = true)
    val result = task.execute
    result.as[String](value._1) must be (value._2)
  }

  it must "throw an exception when script fails" in {
    val exception = intercept[AssertionError] {
      val value = "foo" -> "bar"
      val task = new ScriptTask(script = s"echo1 { ${value._1} = ${value._2} }", parseOutput = false)
      task.execute
    }
    exception.getMessage must be ("assertion failed: Non Zero return code detected")
  }

  it must "apply environment variables correctly"  in {
    val env = Map("foo" -> "bar")
    val task = new ScriptTask(script = s"echo { key = $${foo} }", env = env, parseOutput = true)
    val result = task.execute
    result.as[String]("key") must be(env("foo"))
  }

  it must "execute multi-line script correctly" in {
    val cmd =
      """
        |echo {
        |echo foo = bar
        |echo }
      """.stripMargin
    val task = new ScriptTask(script = cmd,parseOutput = true)
    val result = task.execute
    result.as[String]("foo") must be ("bar")
  }

}
