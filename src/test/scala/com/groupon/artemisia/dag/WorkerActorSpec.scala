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

package com.groupon.artemisia.dag

import akka.actor.Props
import com.typesafe.config.{Config, ConfigFactory}
import com.groupon.artemisia.ActorTestSpec
import com.groupon.artemisia.core.Keywords
import com.groupon.artemisia.dag.Message.{TaskFailed, TaskStats, TaskSucceeded, TaskWrapper}
import com.groupon.artemisia.task.{GenericTaskSpec, Task, TaskHandler}
import scala.concurrent.duration._
import scala.language.postfixOps

/**
 * Created by chlr on 1/25/16.
 */
class WorkerActorSpec extends ActorTestSpec {

  "The worker" must "send TaskSuceeded message when the task completes successfully" in {

    val worker = system.actorOf(Props[Worker])
    val test_task = new Task("testTask") {
      override def setup(): Unit = {}
      override def work(): Config = {ConfigFactory.empty()}
      override def teardown(): Unit = {}
    }
    val task = new TaskHandler(GenericTaskSpec.getDefaultTaskConfig, test_task, ConfigFactory.empty())
    within(1 seconds) {
      worker ! TaskWrapper("test_task", task)
      expectMsgPF(1 second) {
        case TaskSucceeded("test_task",TaskStats(_,_,Status.SUCCEEDED,1,_)) => ()
      }
    }

  }

  it must "send TaskFailed message when the task fails" in {

    val worker = system.actorOf(Props[Worker])
    val test_task = new Task("testTask"){

      override def setup(): Unit = { throw new Exception("my known exception") }
      override def work(): Config = {ConfigFactory.empty()}
      override def teardown(): Unit = {}
    }
    val task = new TaskHandler(GenericTaskSpec.getDefaultTaskConfig,test_task, ConfigFactory.empty())

      within(1 seconds) {
      worker ! TaskWrapper("test_task", task)
      expectMsgPF(1 second) {
        case TaskFailed("test_task",TaskStats(_,_,Status.FAILED,3,_),_) => ()
      }
    }
  }


  it must s"set status to FAILURE_IGNORED when ${Keywords.Task.IGNORE_ERROR} is set" in {
    val worker = system.actorOf(Props[Worker])
    val test_task = new Task("testTask"){

      override def setup(): Unit = { throw new Exception("my known exception") }
      override def work(): Config = {ConfigFactory.empty()}
      override def teardown(): Unit = {}
    }
    val task = new TaskHandler(GenericTaskSpec.getDefaultTaskConfig.copy(ignoreFailure = true),test_task, ConfigFactory.empty())

    within(1 seconds) {
      worker ! TaskWrapper("test_task", task)
      expectMsgPF(1 second) {
        case TaskFailed("test_task",TaskStats(_,_,Status.FAILURE_IGNORED,3,_),_) => ()
      }
    }

  }


}
