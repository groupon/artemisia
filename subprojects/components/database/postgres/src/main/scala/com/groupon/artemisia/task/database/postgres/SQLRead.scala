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

package com.groupon.artemisia.task.database.postgres

import com.typesafe.config.{Config, ConfigFactory}
import com.groupon.artemisia.task.database.DBInterface
import com.groupon.artemisia.task.settings.DBConnection
import com.groupon.artemisia.task.{TaskLike, database}
import com.groupon.artemisia.util.Util

/**
  * Created by chlr on 6/11/16.
  */

class SQLRead(name: String = Util.getUUID, sql: String, connectionProfile: DBConnection)
  extends database.SQLRead(name, sql, connectionProfile) {

  override val dbInterface: DBInterface = DbInterfaceFactory.getInstance(connectionProfile)

}

object SQLRead extends TaskLike {

  override val taskName = database.SQLRead.taskName

  override val defaultConfig = ConfigFactory.empty()

  override val info = database.SQLRead.info

  override def apply(name: String, config: Config, reference: Config) = database.SQLRead
    .create[SQLRead](name, config, reference)

  override val desc: String = database.SQLRead.desc

  override val paramConfigDoc = database.SQLRead.paramConfigDoc(5432)

  override val fieldDefinition = database.SQLRead.fieldDefinition

  override val outputConfig: Option[Config] = database.SQLRead.outputConfig

  override val outputConfigDesc: String = database.SQLRead.outputConfigDesc

}