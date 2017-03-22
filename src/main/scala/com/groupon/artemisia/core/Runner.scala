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

import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.joran.JoranConfigurator
import com.groupon.artemisia.dag.ActorSysManager
import org.slf4j.LoggerFactory
import com.groupon.artemisia.dag.Dag

/**
  * A helper class that orchestrates the execution of the dag workflow
  */
object Runner {

  def run(appContext: AppContext) = {
    prepare(appContext)
    val dag = Dag(appContext)
    AppLogger debug "starting Actor System"
    val actor_sys_manager =  new ActorSysManager(appContext)
    val workers = actor_sys_manager.createWorker(Keywords.ActorSys.CUSTOM_DISPATCHER)
    val dag_player = actor_sys_manager.createPlayer(dag,workers)
    dag_player ! 'Play
  }

  private def prepare(appContext: AppContext) = {
    configureLogging(appContext)
    AppLogger debug s"workflow_id: ${appContext.runId}"
    AppLogger debug s"working directory: ${appContext.workingDir}"
    if (appContext.globalConfigFile.nonEmpty) {
      AppLogger debug s"global config file: ${appContext.globalConfigFile.get}"
    }
  }


  private def configureLogging(app_context: AppContext) = {
    val context = LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]
    val jc = new JoranConfigurator
    jc.setContext(context)
    context.reset()
    context.putProperty("log.console.level", app_context.logging.console_trace_level)
    context.putProperty("log.file.level", app_context.logging.file_trace_level)
    context.putProperty("env.working_dir", app_context.workingDir)
    context.putProperty("workflow_id", app_context.runId)
    jc.doConfigure(this.getClass.getResourceAsStream("/logback_config.xml"))
  }

}
