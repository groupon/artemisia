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

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.routing.BalancingPool
import com.typesafe.config.{Config, ConfigFactory}
import com.groupon.artemisia.core.{AppContext, AppLogger, Keywords}

/**
 * Created by chlr on 1/18/16.
 */
class ActorSysManager(app_context: AppContext) {

  AppLogger debug "starting actor system"
  val system = ActorSystem.create(Keywords.APP,getActorConfig(app_context))

  protected[dag] def getActorConfig(app_context: AppContext): Config = {
    val actor_config =
      s"""
         | ${Keywords.ActorSys.CUSTOM_DISPATCHER} {
         |  type = Dispatcher
         |  executor = "thread-pool-executor"
         |  thread-pool-executor {
         |    core-pool-size-min = 3
         |    core-pool-size-factor = 3.0
         |    core-pool-size-max = ${ math.ceil(app_context.dagSetting.concurrency * 1.5) }
          |  }
          |  throughput = 1
          |}
          |
      """.stripMargin

    ConfigFactory parseString actor_config
  }

  def createWorker(dispatcher: String = "akka.actor.default-dispatcher") = {
    AppLogger debug s"creating worker pool with ${app_context.dagSetting.concurrency} worker(s)"
     system.actorOf(BalancingPool(app_context.dagSetting.concurrency).props(Props[Worker])
      .withDispatcher(dispatcher), "router")
  }

  def createPlayer(dag: Dag, workers: ActorRef ): ActorRef = {
    system.actorOf(Props(new DagPlayer(dag,app_context,workers)),"supervisor")
  }

}
