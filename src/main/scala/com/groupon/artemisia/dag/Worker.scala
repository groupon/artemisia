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

import akka.actor.Actor
import com.typesafe.config.{Config, ConfigFactory}
import Message._
import com.groupon.artemisia.core.AppLogger
import com.groupon.artemisia.dag.Message.{TaskCompleted, TaskWrapper}
import com.groupon.artemisia.util.Util
import scala.util.Try

/**
 * Created by chlr on 1/7/16.
 */
class Worker extends Actor {

  override def receive: Receive = {
    case message: TaskWrapper => {
      Thread.currentThread().setName(message.name)
      AppLogger info s"task ${message.name} has been submitted for execution"
      val start_time = Util.currentTime
      val result: Try[TaskCompleted] = message.task.execute() map {
        result: Config => {
          TaskSucceeded(message.name,TaskStats(start_time,Util.currentTime,message.task.getStatus,message.task.getAttempts,result))
        }
      } recover {
          case th: Throwable => {
            TaskFailed(message.name,TaskStats(start_time, Util.currentTime,
              if(message.task.taskConfig.ignoreFailure) Status.FAILURE_IGNORED else Status.FAILED,
            message.task.getAttempts, ConfigFactory.empty()), th)
          }
      }
      sender ! result.get
    }
  }

}
