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

import com.typesafe.config.{ConfigFactory, Config}
import com.groupon.artemisia.dag.Message.TaskStats

/**
 * Created by chlr on 5/23/16.
 */

/**
  * Checkpoint Manager that persists checkpoint data in-memory.
  * This checkpoint manager doesn't parse existing checkpoints
  * and all new checkpoints are kept in memory.
  *
  */
class BasicCheckpointManager {

  protected var adhocPayload: Config = ConfigFactory.empty()
  protected var taskStatRepo: Map[String,TaskStats] = Map()

  private[core] def save(taskName: String, taskStat: TaskStats) = {
    adhocPayload = taskStat.taskOutput withFallback adhocPayload
    taskStatRepo = taskStatRepo + (taskName -> taskStat)
  }

  private[core] def checkpoints = {
    BasicCheckpointManager.CheckpointData(adhocPayload, taskStatRepo)
  }

}


object BasicCheckpointManager {

  case class CheckpointData(adhocPayload: Config = ConfigFactory.empty(), taskStatRepo: Map[String, TaskStats] = Map())

}
