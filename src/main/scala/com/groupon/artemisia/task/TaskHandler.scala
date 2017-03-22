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

import com.groupon.artemisia.core.AppLogger._
import com.groupon.artemisia.core.{BooleanEvaluator, Keywords}
import com.groupon.artemisia.dag.Status
import com.groupon.artemisia.util.Util
import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}
import com.groupon.artemisia.util.HoconConfigUtil.{Handler, configToConfigEnhancer}
import scala.util.{Failure, Success, Try}

/**
  * Created by chlr on 1/7/16.
  */

class TaskHandler(val taskConfig: TaskConfig, val task: Task, contextConfig: Config) {

  private var attempts = 0
  private var status: Status.Value = Status.UNKNOWN

  final def execute(): Try[Config] = {

    taskConfig.conditions match {
      case Some((true, x)) => {
        info(s"executing task ${task.taskName} since expression $x succeeded")
        executeLifeCycles
      }
      case None => executeLifeCycles
      case Some((false, x)) => {
        info(s"skipping execution of ${task.taskName} since expression $x failed")
        status = Status.SKIPPED
        Success(ConfigFactory.empty())
      }
    }
  }

  def getAttempts = attempts

  def getStatus = status


  private def executeLifeCycles = {
    val result = lifecyles()
    result match {
      case Success(resultConfig) => runAssertions(resultConfig)
      case _ => ()
    }
    result
  }

  private def runAssertions(taskResult: Config) = {
    val effectiveConfig = taskResult withFallback contextConfig
    taskConfig.assertion match {
      case Some((configValue, desc)) => {
        debug(s"running assertions on ${task.taskName}")
        val resolvedConfig = effectiveConfig.withFallback(ConfigFactory.empty.withValue(s""""${task.taskName}"""",
          ConfigFactory.empty.withValue(Keywords.Task.ASSERTION, configValue).root())).hardResolve()
        val result = BooleanEvaluator.evalBooleanExpr(
          resolvedConfig.as[Config](s"""${task.taskName}""").as[String](Keywords.Task.ASSERTION)
        )
        assert(result, desc)
      }
      case None => ()
    }
  }

  private def lifecyles(): Try[Config] = {

    info(s"running task with total allowed attempts of ${taskConfig.retryLimit}")

    val result = run(maxAttempts = taskConfig.retryLimit) {
      debug("executing setup phase of the task")
      task.setup()
      debug("executing work phase of the task")
      val result = task.work()
      debug(s"emitting config: ${result.root().render(ConfigRenderOptions.concise())}")
      result
    }

    try {
      // teardown must run even if the task setup or work has failed
      debug("executing teardown phase of the task")
      task.teardown()
    } catch {
      case ex: Throwable => warn(s"teardown phase failed with exception ${ex.getClass.getCanonicalName} with message ${ex.getMessage}")
    }
    result
  }

  private def run(maxAttempts: Int)(body: => Config): Try[Config] = {
    try {
      attempts += 1
      val result = body
      this.status = Status.SUCCEEDED
      Success(result)
    } catch {
      case ex: Throwable => {
        info(s"attempt ${taskConfig.retryLimit - maxAttempts + 1} for task ${task.taskName}")
        error(Util.printStackTrace(ex))
        if (maxAttempts > 1) {
          run(maxAttempts - 1)(body)
        }
        else {
          status = Status.FAILED
          Failure(ex)
        }
      }
    }
  }

}

