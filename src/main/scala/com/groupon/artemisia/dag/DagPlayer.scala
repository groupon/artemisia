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

import java.util.concurrent.TimeUnit
import akka.actor.{Actor, ActorRef}
import com.groupon.artemisia.core.{AppContext, AppLogger, Keywords}
import com.groupon.artemisia.dag.Message.{TaskFailed, TaskStats, TaskSucceeded, Tick, _}
import com.groupon.artemisia.core.AppLogger._
import com.groupon.artemisia.task.TaskHandler
import scala.collection.Seq
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}


/**
  * Created by chlr on 1/7/16.
  */


/**
  * This class drives the execution of Dag nodes. It identifies runnable nodes
  * and dispatches them for execution.
 *
  * @param dag
  * @param appContext Application Context
  * @param router
  */
class DagPlayer(val dag: Dag, appContext: AppContext, val router: ActorRef) extends Actor {


  def play: Receive = {
    case 'Play => {
      implicit val dispatcher = context.dispatcher
      val heartbeat_interval = Duration(appContext.dagSetting.heartbeat_cycle.toMillis, TimeUnit.MILLISECONDS)
      debug(s"scheduling heartbeat messages for $heartbeat_interval")
      context.system.scheduler.schedule(0 seconds, heartbeat_interval, self, Tick)
    }
  }

  override def receive: Receive = preReceive andThen (healthyDag orElse onTaskComplete orElse play)

  def healthyDag: Receive = {
    case Tick =>
      if (dag.hasCompleted) {
        info("all tasks completed. System shutting down")
        context.system.shutdown()
      }
      else {
        dag.getRunnableTasks(appContext) match {
          case Success(tasks) => processNodes(tasks)
          case Failure(th) =>
            th.printStackTrace(System.err)
            error("Dag processor has crashed due to an error", th)
            dag.nodesWithStatus(Status.READY).foreach(x => dag.setNodeStatus(x.name, Status.INIT_FAILED))
            context.become(preReceive andThen (woundedDag orElse onTaskComplete))
        }
      }
    }


  /**
    * process tasks successfully parsed from the payload
    * @param task
    */
  private def processNodes(task: Seq[(String, Try[TaskHandler])]) = {
      task foreach {
        case (taskName, Success(taskHandler)) =>
          router ! TaskWrapper(taskName, taskHandler)
          dag.setNodeStatus(taskName, Status.RUNNING)
        case (taskName, Failure(th)) =>
          th.printStackTrace(System.err)
          error(s"node $taskName failed to initialize due to following error", th)
          dag.setNodeStatus(taskName, Status.INIT_FAILED)
          context.become(preReceive andThen (woundedDag orElse onTaskComplete))
      }
  }

  def preReceive: PartialFunction[Any, Any] = {
    case x: Any => Thread.currentThread().setName(Keywords.APP); x
  }

  def woundedDag: Receive = {
    case Tick => {
      dag.runningNodes match {
        case Nil =>
          info("all nodes completed. System shutting down")
          context.system.shutdown()
          sys.exit(-1)
        case _ =>
          info(s"${dag.failedNodes.mkString(",")} failed. " +
            s"waiting for ${dag.runningNodes.mkString(",")} to complete")
      }
    }
  }

  /**
    * commit checkpoint and change DagPlayer behaviour if required.
    * @return
    */
  private def commitCheckpoint: PartialFunction[TaskCompleted,Unit] = {
    case message: TaskCompleted =>
      checkpoint(message.name, message.taskStats)
      info(s"checkpoint completed successfully for ${message.name}")
      message.taskStats.status match {
      case Status.FAILED =>
        context.become(preReceive andThen (woundedDag orElse onTaskComplete))
      case _ => ()
    }
  }

  /**
    * receive and transform TaskCompletion message if required
    * @return
    */
  private def handleCompletionMsg: PartialFunction[Any,TaskCompleted] = {
    case message: TaskSucceeded =>
      info(s"node ${message.name} execution completed successfully")
      message
    case message: TaskFailed =>
      info(s"node ${message.name} execution failed")
      dag.isRecoverableNode(message.name) match {
        case false => message
        case true =>
          message.copy(taskStats = message.taskStats.copy(status = Status.FAILED_RECOVERED))
      }
  }


  def onTaskComplete: Receive = handleCompletionMsg andThen commitCheckpoint

  /**
    *
    * @param name       name of the node
    * @param task_stats task stats info
    */
  def checkpoint(name: String, task_stats: TaskStats) = {
    AppLogger debug s"running checkpoint for $name"
    dag.setNodeStatus(name, task_stats.status)
    appContext.commitCheckpoint(name, task_stats)
    dag.updateNodePayloads(appContext.payload)
  }

}



