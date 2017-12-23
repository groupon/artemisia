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
import com.groupon.artemisia.task.{TaskHandler, TestAdderTask, TestFailTask}

/**
  * Created by chlr on 12/30/16.
  */
class DagPlayerAllDependencies extends ActorTestSpec {

  var workers: ActorRef = _
  var probe: DagProbe = _
  var app_settings: AppSetting = _
  var dag: Dag = _
  var dag_player: ActorRef = _
  var app_context: AppContext = _


  override def beforeEach() = {
    workers = system.actorOf(RoundRobinPool(1).props(Props[Worker]))
    probe =  new DagProbe(system)
  }

  "DagPlayer" must "play the dag with all dependencies types" in {
    setUpArtifacts(this.getClass.getResource("/code/all_type_dependencies.conf").getFile)
    info("Sending tick 1")


    dag_player ! Tick
    probe.validateAndRelayMessages(2, workers) {
      case x@TaskWrapper("step1", handler1) :: TaskWrapper("step2", handler2) :: Nil =>
        handler1.task mustBe a[TestAdderTask]
        handler2.task mustBe a[TestFailTask]
      case x => fail(s"received unexpected messages")
    }

    probe.validateAndRelayMessages(2, dag_player) {
      case x@TaskSucceeded("step1", stats1) :: TaskFailed("step2", stats2, th) :: Nil =>
        stats1.status must be(Status.SUCCEEDED)
        stats1.taskOutput.getInt("tango") must be(30)
        stats2.status must be(Status.FAILED)
      case x => fail(s"received unexpected messages")
    }


    dag_player ! Tick
    probe.validateAndRelay(workers) {
      case TaskWrapper("step3", handler: TaskHandler) =>
        handler.task mustBe a[TestFailTask]
    }

    probe.validateAndRelay(dag_player) {
      case TaskFailed("step3", stats: TaskStats, th: Throwable) => {
        stats.status must be(Status.FAILED)
      }
    }


    dag_player ! Tick
    probe.validateAndRelayMessages(2, workers) {
      case x@TaskWrapper("step5", handler1) :: TaskWrapper("step6", handler2) :: Nil =>
        handler1.task mustBe a[TestAdderTask]
        handler2.task mustBe a[TestAdderTask]
        dag.lookupNodeByName("step4").status must be(Status.DEACTIVATE)
        dag.lookupNodeByName("step7").status must be(Status.DEACTIVATE)
      case x => fail(s"received unexpected messages")
    }

    probe.validateAndRelayMessages(2, dag_player) {
      case x@TaskSucceeded("step5", stats1) :: TaskSucceeded("step6", stats2) :: Nil =>
        stats1.status must be(Status.SUCCEEDED)
        stats1.taskOutput.getInt("tango") must be(30)
        stats2.status must be(Status.SUCCEEDED)
        stats2.taskOutput.getInt("tango") must be(30)
      case x => fail(s"received unexpected messages")
    }


    dag_player ! Tick
    probe.validateAndRelayMessages(2, workers) {
      case x@TaskWrapper("step10", handler1) :: TaskWrapper("step8", handler2) :: Nil =>
        handler1.task mustBe a[TestAdderTask]
        handler2.task mustBe a[TestAdderTask]
      case x => fail(s"received unexpected messages")
    }

    probe.validateAndRelayMessages(2, dag_player) {
      case x@TaskSucceeded("step10", stats1) :: TaskSucceeded("step8", stats2) :: Nil =>
        stats1.status must be(Status.SUCCEEDED)
        stats1.taskOutput.getInt("tango") must be(30)
        stats2.status must be(Status.SUCCEEDED)
        stats2.taskOutput.getInt("tango") must be(30)
      case x => fail(s"received unexpected messages")
    }

  }


  def setUpArtifacts(code: String) = {
    app_settings = AppSetting(value = Some(code), skip_checkpoints = true)
    app_context = new AppContext(app_settings).init()
    dag = Dag(app_context)
    dag_player = system.actorOf(Props(new DagPlayer(dag, app_context, probe.ref)))
  }

}
