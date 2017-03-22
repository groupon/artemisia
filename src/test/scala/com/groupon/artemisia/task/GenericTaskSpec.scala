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

import com.groupon.artemisia.dag.Status
import com.typesafe.config.{Config, ConfigFactory}
import com.groupon.artemisia.TestSpec
import scala.concurrent.duration._
/**
 * Created by chlr on 1/23/16.
 */
class GenericTaskSpec extends TestSpec {


  "The Task" must "retry a failing task for configured number of times" in {
    val task_config = GenericTaskSpec.getDefaultTaskConfig

    val fail_task = new Task("testTask") {
      override def work(): Config = { ConfigFactory.empty() }
      override def setup(): Unit = { throw new Exception("fail") }
      override def teardown(): Unit = {}
    }
    val task = new TaskHandler(task_config,fail_task, ConfigFactory.empty())
    val result = task.execute()
    result.isFailure must be (true)
    result.failed.get mustBe an [Exception]
    task.getAttempts must be (3)
    task.getStatus must be (Status.FAILED)
  }

  it must "stop retrying if it succeeds before retry limit" in {

    val task_config = GenericTaskSpec.getDefaultTaskConfig
     val succeed_on_2_attempt_task = new Task("testTask") {
       var attempts = 1
       override def setup(): Unit = {}
       override def work(): Config = { if (this.attempts == 1) {  attempts += 1 ; throw new Exception("") } else { ConfigFactory.empty() } }
       override def teardown(): Unit = {}
     }
    val task = new TaskHandler(task_config,succeed_on_2_attempt_task, ConfigFactory.empty())
    task.execute()
    task.getAttempts must be (2)
  }

  it must "ignore failures in teardown phase" in {
    val task_config = GenericTaskSpec.getDefaultTaskConfig
    val fail_on_teardown_task = new Task("testTask") {
      override def setup(): Unit = {}
      override def work(): Config = {  ConfigFactory.empty }
      override def teardown(): Unit = { throw new Exception("my own exception") }
    }
    val task = new TaskHandler(task_config,fail_on_teardown_task, ConfigFactory.empty())
    val result = task.execute()
    result.get must equal (ConfigFactory.empty)
  }

  it must "skip execution if the task_option flag is set" in {
    val task_config = TaskConfig(retryLimit = 3, cooldown = 10 milliseconds, conditions = Some(false -> ""))
    val a_dummy_task = new Task("testTask") {
      override def setup(): Unit = { throw new Exception("my own exception") }
      override def work(): Config = {  throw new Exception("my own exception") }
      override def teardown(): Unit = { throw new Exception("my own exception") }
    }
    val task = new TaskHandler(task_config,a_dummy_task, ConfigFactory.empty())
    val result = task.execute()
    task.getStatus must be (Status.SKIPPED)
    task.getAttempts must be (0)
    result.get must equal (ConfigFactory.empty)
  }


}


object GenericTaskSpec {

  def getDefaultTaskConfig = {
     TaskConfig(retryLimit = 3, cooldown = 10 milliseconds)
  }

}
