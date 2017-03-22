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

package com.groupon.artemisia.task.settings

import com.typesafe.config.{ConfigFactory, ConfigValueType, ConfigValue, Config}
import com.groupon.artemisia.core.Keywords
import com.groupon.artemisia.task.TaskContext
import com.groupon.artemisia.util.HoconConfigUtil.Handler

/**
 * Created by chlr on 6/16/16.
 */


trait ConnectionHelper {

  type T

  def apply(config: Config): T

  def apply(connectionName: String): T = {
    this.apply(TaskContext.payload.as[Config](s"${Keywords.Config.CONNECTION_SECTION}.$connectionName"))
  }

  /**
   *
   * @param config input config that has a node dsn
   * @return
   */
  def parseConnectionProfile(config: ConfigValue) = {
    config.valueType() match {
      case ConfigValueType.STRING => this.apply(config.unwrapped().asInstanceOf[String])
      case ConfigValueType.OBJECT => this.apply(ConfigFactory.empty withFallback config)
      case x @ _ => throw new IllegalArgumentException(s"connection value must either be an object or string name}")
    }
  }

}