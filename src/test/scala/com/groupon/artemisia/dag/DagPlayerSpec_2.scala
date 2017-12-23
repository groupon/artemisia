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

import akka.actor.{ActorRef, Props}
import akka.routing.RoundRobinPool
import com.groupon.artemisia.ActorTestSpec
import com.groupon.artemisia.core.{AppContext, AppSetting}
import com.groupon.artemisia.dag.Message._
import com.groupon.artemisia.task.{TaskHandler, TestAdderTask}
import com.groupon.artemisia.util.FileSystemUtil._
import com.groupon.artemisia.util.HoconConfigUtil.Handler

import scala.concurrent.duration._
import scala.language.postfixOps

/**
  * Created by chlr on 8/13/16.
  */
class DagPlayerSpec_2 extends ActorTestSpec {

  var workers: ActorRef = _
  var probe: DagProbe = _
  var app_settings: AppSetting = _
  var dag: Dag = _
  var dag_player: ActorRef = _
  var app_context: AppContext = _

  override def beforeEach() = {
    workers = system.actorOf(RoundRobinPool(1).props(Props[Worker]))
    probe = new DagProbe(system)
  }

  "DagPlayer" must "apply local variables" in {
    setUpArtifacts(this.getClass.getResource("/code/local_variables.conf").getFile)
    within(1000 millis) {
      dag_player ! Tick
      probe.validateAndRelay(workers) {
        case TaskWrapper("step1",task_handler: TaskHandler) => {
          task_handler.task mustBe a[TestAdderTask]
        }
      }
      probe.validateAndRelay(dag_player) {
        case TaskSucceeded("step1", stats: TaskStats) => {
          stats.status must be (Status.SUCCEEDED)
          stats.taskOutput.as[Int]("foo") must be (50)
        }
      }
    }
  }


  it must "apply defaults defined in settings.conf" in {
    setUpArtifacts(this.getClass.getResource("/code/apply_defaults.conf").getFile)
    within(1000 millis) {
      dag_player ! Tick
      probe.validateAndRelay(workers) {
        case TaskWrapper("step1",task_handler: TaskHandler) => {
          task_handler.task mustBe a[TestAdderTask]
        }
      }
      probe.validateAndRelay(dag_player) {
        case TaskSucceeded("step1", stats: TaskStats) => {
          stats.status must be (Status.SUCCEEDED)
          stats.taskOutput.as[Int]("foo") must be (50)
        }
      }
    }
  }


  it must "handle looping in dag" in {
    setUpArtifacts(this.getClass.getResource("/code/iteration_test.conf").getFile)
    within(20000 millis) {
      dag_player ! Tick
      probe.receiveN(2, 2 second) foreach {
         case msg @ TaskWrapper("step1", taskHandler) => workers.tell(msg, probe.ref)
         case msg @ TaskWrapper("step2", taskHandler) => workers.tell(msg, probe.ref)
       }
      probe.receiveN(2, 2 second) foreach {
        case msg @ TaskSucceeded("step1", taskStats) => {
          taskStats.taskOutput.as[Int]("tango1") must be (3)
          dag_player.tell(msg, probe.ref)
        }
        case msg @ TaskSucceeded("step2", taskStats) => {
          taskStats.taskOutput.as[Int]("tango2") must be (3)
          dag_player.tell(msg, probe.ref)
        }
      }
      dag_player ! Tick
      probe.receiveN(2, 2 second) foreach {
        case msg @ TaskWrapper("step3$1", taskHandler) => workers.tell(msg, probe.ref)
        case msg @ TaskWrapper("step3$2", taskHandler) => workers.tell(msg, probe.ref)
      }
      probe.receiveN(2, 2 second) foreach {
        case msg @ TaskSucceeded("step3$1", taskStats) => {
          taskStats.taskOutput.as[Int]("tango3a") must be (30)
          dag_player.tell(msg, probe.ref)
        }
        case msg @ TaskSucceeded("step3$2", taskStats) => {
          taskStats.taskOutput.as[Int]("tango3b") must be (300)
          dag_player.tell(msg, probe.ref)
        }
      }


      dag_player ! Tick
      probe.receiveN(1, 1 second) foreach {
        case msg @ TaskWrapper("step3$3", taskHandler) => workers.tell(msg, probe.ref)
      }
      probe.receiveN(1, 1 second) foreach {
        case msg @ TaskSucceeded("step3$3", taskStats) => {
          taskStats.taskOutput.as[Int]("tango3c") must be (70)
          dag_player.tell(msg, probe.ref)
        }
      }

      dag_player ! Tick
      probe.receiveN(1, 1 second) foreach {
        case msg @ TaskWrapper("step4", taskHandler) => workers.tell(msg, probe.ref)
      }

      probe.receiveN(1, 1 second) foreach {
        case msg @ TaskSucceeded("step4", taskStats) => {
          taskStats.taskOutput.as[Int]("tango4") must be (30)
          dag_player.tell(msg, probe.ref)
        }
      }
    }
  }


  it must "file import worklet" in {
    withTempFile(fileName = "worklet_file_import") {
      file =>
         file <<= TestUtils.worklet_file_import(this.getClass.getResource("/code/worklet_file.conf").getFile)
        setUpArtifacts(file.getAbsolutePath)
    }
    within(20000 millis) {
      dag_player ! Tick
      probe.validateAndRelay(workers) {
        case TaskWrapper("task1",task_handler: TaskHandler) => {
          task_handler.task mustBe a[TestAdderTask]
        }
      }
      probe.validateAndRelay(dag_player) {
        case TaskSucceeded("task1", stats: TaskStats) => {
          stats.status must be (Status.SUCCEEDED)
          stats.taskOutput.as[Int]("tango1") must be (3)
        }
      }

      dag_player ! Tick
      probe.validateAndRelay(workers) {
        case TaskWrapper("task2$step1",task_handler: TaskHandler) => {
          task_handler.task mustBe a[TestAdderTask]
        }
      }
      probe.validateAndRelay(dag_player) {
        case TaskSucceeded("task2$step1", stats: TaskStats) => {
          stats.status must be (Status.SUCCEEDED)
          stats.taskOutput.as[Int]("tango") must be (30)
        }
      }

      dag_player ! Tick
      probe.validateAndRelay(workers) {
        case TaskWrapper("task2$step2",task_handler: TaskHandler) => {
          task_handler.task mustBe a[TestAdderTask]
        }
      }
      probe.validateAndRelay(dag_player) {
        case TaskSucceeded("task2$step2", stats: TaskStats) => {
          stats.status must be (Status.SUCCEEDED)
          stats.taskOutput.as[Int]("beta") must be (60)
        }
      }

      dag_player ! Tick
      probe.validateAndRelay(workers) {
        case TaskWrapper("task3",task_handler: TaskHandler) => {
          task_handler.task mustBe a[TestAdderTask]
        }
      }
      probe.validateAndRelay(dag_player) {
        case TaskSucceeded("task3", stats: TaskStats) => {
          stats.status must be (Status.SUCCEEDED)
          stats.taskOutput.as[Int]("tango2") must be (102)
        }
      }

      dag_player ! Tick
      probe.expectNoMsg(2 second)
      // By sending this final tick message when no nodes are available to execute
      // the actor system is shutdown and no further tests can be executed in this spec class.

    }
  }


  def setUpArtifacts(code: String) = {
    app_settings = AppSetting(value = Some(code),skip_checkpoints = true)
    app_context = new AppContext(app_settings).init()
    dag = Dag(app_context)
    dag_player = system.actorOf(Props(new DagPlayer(dag,app_context,probe.ref)))
  }

}

