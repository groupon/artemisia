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

import com.typesafe.config.{ConfigRenderOptions, Config, ConfigFactory}
import com.groupon.artemisia.core.Keywords
import com.groupon.artemisia.util.HoconConfigUtil
import HoconConfigUtil.Handler
import com.groupon.artemisia.task.TaskHandler

/**
 * Created by chlr on 1/7/16.
 */
object Message {

  sealed trait Messageable

  object Tick extends Messageable

  class TaskCompleted(val name: String, val taskStats: TaskStats) extends Messageable

  case class TaskFailed(override val name: String, override val taskStats: TaskStats, exception: Throwable)
    extends TaskCompleted(name, taskStats)

  case class TaskWrapper(name: String, task: TaskHandler) extends Messageable

  case class TaskSucceeded(override val name: String, override val taskStats: TaskStats) extends
    TaskCompleted(name, taskStats)

  case class TaskStats (
                       startTime: String,
                       endTime: String = null,
                       status: Status.Value,
                       attempts: Int = 1,
                       taskOutput: Config = ConfigFactory.empty()
                       ) extends Messageable {

    def toConfig(task_name: String) = {

      ConfigFactory parseString {
        s"""
          |"$task_name" = {
          |  ${Keywords.TaskStats.START_TIME} = "$startTime"
          |  ${Keywords.TaskStats.END_TIME} = "$endTime"
          |  ${Keywords.TaskStats.STATUS} = $status
          |  ${Keywords.TaskStats.ATTEMPT} = $attempts
          |  ${Keywords.TaskStats.TASK_OUTPUT} = ${taskOutput.root().render(ConfigRenderOptions.concise())}
          |}
        """.stripMargin
      }
    }
  }


  object TaskStats {

     def apply(content: Config): TaskStats = {

       TaskStats(content.as[String](Keywords.TaskStats.START_TIME),
                content.as[String](Keywords.TaskStats.END_TIME),
                Status.withName(content.as[String](Keywords.TaskStats.STATUS)),
                content.as[Int](Keywords.TaskStats.ATTEMPT),
                content.as[Config](Keywords.TaskStats.TASK_OUTPUT))
    }
  }

}
