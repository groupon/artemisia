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
import com.groupon.artemisia.dag.Message.{TaskStats, _}
import com.groupon.artemisia.task.{TaskHandler, TestAdderTask, TestFailTask}
import com.groupon.artemisia.util.HoconConfigUtil.Handler
import scala.concurrent.duration._

/**
 * Created by chlr on 1/25/16.
 */

class DagPlayerSpec_1 extends ActorTestSpec {

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

  "DagPlayer" must "execute all tasks in the Dag with stats assertion support" in {

    setUpArtifacts(this.getClass.getResource("/code/multi_step_addition_job.conf").getFile)
    info("Sending tick 1")
    dag_player ! Tick
    probe.validateAndRelay(workers) {
      case TaskWrapper("step1",task_handler: TaskHandler) => {
        task_handler.task mustBe a[TestAdderTask]
      }
    }

    probe.validateAndRelay(dag_player) {
      case TaskSucceeded("step1", stats: TaskStats) => {
        stats.status must be(Status.SUCCEEDED)
        stats.taskOutput.as[Int]("tango") must be (30)
      }
    }

    info("Sending tick 1")
    dag_player ! Tick

    probe.validateAndRelay(workers) {
      case TaskWrapper("step2",task_handler: TaskHandler) => {
        task_handler.task mustBe a[TestAdderTask]
      }
    }

    probe.validateAndRelay(dag_player) {
      case TaskSucceeded("step2", stats: TaskStats) => {
        stats.status must be(Status.SUCCEEDED)
      }
    }
  }


  it must "handle error" in {

    setUpArtifacts(this.getClass.getResource("/code/multi_step_addition_with_failure.conf").getFile)
    info("Sending tick 1")
    dag_player ! Tick

    probe.validateAndRelay(workers) {
      case TaskWrapper("step1",task_handler: TaskHandler) => {
        task_handler.task mustBe a[TestAdderTask]
      }
    }

    probe.validateAndRelay(dag_player) {
      case TaskSucceeded("step1", stats: TaskStats) => {
        stats.status must be(Status.SUCCEEDED)
        stats.taskOutput.as[Int]("tango") must be (20)
      }
    }

    info("Sending tick 1")
    dag_player ! Tick

    probe.validateAndRelay(workers) {
      case TaskWrapper("step2",task_handler: TaskHandler) => {
        task_handler.task mustBe a[TestFailTask]
      }
    }

    probe.validateAndRelay(dag_player) {
      case TaskFailed("step2", stats: TaskStats,exception: Throwable) => {
        stats.status must be(Status.FAILED)
        exception.getMessage must be ("FailTask always fail")
      }
    }

  }


   it must "handle ignore-error" in {

    setUpArtifacts(this.getClass.getResource("/code/multi_step_with_failure_ignored.conf").getFile)
    info("Sending tick 1")
    dag_player ! Tick

    probe.validateAndRelay(workers) {
      case TaskWrapper("step1",task_handler: TaskHandler) => {
        task_handler.task mustBe a[TestFailTask]
      }
    }

    probe.validateAndRelay(dag_player) {
      case TaskFailed("step1", stats: TaskStats, exception: Throwable) => {
        stats.status must be(Status.FAILURE_IGNORED)
        exception.getMessage must be ("FailTask always fail")
      }
    }

    info("Sending tick 1")
    dag_player ! Tick

    probe.validateAndRelay(workers) {
      case TaskWrapper("step2",task_handler: TaskHandler) => {
        task_handler.task mustBe a[TestAdderTask]
      }
    }

    probe.validateAndRelay(dag_player) {
      case TaskSucceeded("step2", stats: TaskStats) => {
        stats.status must be(Status.SUCCEEDED)
      }
    }
  }


  it must "handle failures in task initialization" in {
    setUpArtifacts(this.getClass.getResource("/code/incorrect_config.conf").getFile)
    within(1000 millis) {
      dag_player ! Tick
      expectNoMsg
    }
    dag.getNodeStatus("step1") must be (Status.INIT_FAILED)
  }



  def setUpArtifacts(code: String) = {
    app_settings = AppSetting(value = Some(code),skip_checkpoints = true)
    app_context = new AppContext(app_settings).init()
    dag = Dag(app_context)
    dag_player = system.actorOf(Props(new DagPlayer(dag,app_context,probe.ref)))
  }

}
