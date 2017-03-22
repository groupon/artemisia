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

import com.groupon.artemisia.util.FileSystemUtil

/**
 * Created by chlr on 1/1/16.
 */
object Keywords {

  val APP = "Artemisia"

  object ActorSys  {
    val CUSTOM_DISPATCHER = "balancing-pool-router-dispatcher"
  }

  object Config {
    val GLOBAL_FILE_REF_VAR = "ARTEMISIA_CONFIG"
    val SETTINGS_SECTION = "__settings__"
    val CONNECTION_SECTION = "__connections__"
    val USER_DEFAULT_CONFIG_FILE = FileSystemUtil.joinPath(System.getProperty("user.home"), "artemisia.conf")
    val CHECKPOINT_FILE = "checkpoint.conf"
    val DEFAULTS = "__defaults__"
    val WORKLET = "__worklet__"
    val SYSTEM_DEFAULT_CONFIG_FILE_JVM_PARAM = "setting.file"
  }

  object DagEditor {
    val Component = "DagEditor"
    val Task = "Import"
  }

  object Connection {
    val HOSTNAME = "host"
    val USERNAME = "username"
    val PASSWORD = "password"
    val DATABASE = "database"
    val PORT = "port"
  }

  object Task {
    val COMPONENT = "Component"
    val TASK = "Task"
    val DEPENDENCY = "dependencies"
    val SUCCESS_DEPENDENCY = "success"
    val FAIL_DEPENDENCY = "fail"
    val COMPLETE_DEPENDENCY = "complete"
    val IGNORE_ERROR = "ignore-error"
    val COOLDOWN = "cooldown"
    val ATTEMPT = "attempts"
    val PARAMS = "params"
    val CONDITION = "when"
    val VARIABLES = "define"
    val ASSERTION = "assert"
    val ITERATE = "for-all"
  }


  object TaskStats {
    val STATS = "__stats__"
    val QUEUE_TIME = "queue-time"
    val START_TIME = "start-time"
    val END_TIME = "end-time"
    val STATUS = "status"
    val DURATION = "duration"
    val ATTEMPT = "attempts"
    val TASK_OUTPUT = "task_output"
  }

  object Checkpoint {
    val TASK_STATES = "__taskstates__"
    val PAYLOAD = "__payload__"
  }
}
