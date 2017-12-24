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

package com.groupon.artemisia.task

import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import com.groupon.artemisia.TestSpec
import scala.util.{Failure, Success}

/**
  * Created by chlr on 6/2/16.
  */
class TaskHandlerSpec extends TestSpec {

  "TaskHandler" must "handle assertions failure appropriately" in {
      val task = new Task("dummy_task") {
        override def setup(): Unit = ()
        override def work(): Config = ConfigFactory parseString "foo = 100"
        override def teardown(): Unit = ()
      }
      val taskConfig = TaskConfig(assertion = Some((ConfigValueFactory.fromAnyRef("${foo} == 0"),"test")))
      val handler = new TaskHandler(taskConfig, task, ConfigFactory.empty())
      val ex = intercept[AssertionError] {
        handler.execute()
      }
      ex.getMessage must be ("assertion failed: test")
  }


  it must "task output must correctly appear in assertions" in {

    val task = new Task("dummy_task") {
      override def setup(): Unit = ()
      override def work(): Config = ConfigFactory parseString "foo = 100"
      override def teardown(): Unit = ()
    }
    val taskConfig = TaskConfig(assertion = Some((ConfigValueFactory.fromAnyRef("${foo} == 100"),"test")))
    val handler = new TaskHandler(taskConfig, task, ConfigFactory.empty())
    handler.execute() match {
      case Success(config) => config.getInt("foo") must be (100)
      case Failure(th) => throw th
    }
  }

}
