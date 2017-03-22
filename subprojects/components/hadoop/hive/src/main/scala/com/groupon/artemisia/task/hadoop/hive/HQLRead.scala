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

package com.groupon.artemisia.task.hadoop.hive

import com.typesafe.config.{Config, ConfigFactory, ConfigValue}
import com.groupon.artemisia.task.database.DBInterface
import com.groupon.artemisia.task.settings.DBConnection
import com.groupon.artemisia.task.{TaskLike, database}
import com.groupon.artemisia.util.CommandUtil._
import com.groupon.artemisia.util.HoconConfigUtil.Handler

/**
  * Created by chlr on 8/2/16.
  */
class HQLRead(taskName: String, sql: String, connectionProfile: Option[DBConnection]) extends
                        database.SQLRead(taskName, sql, connectionProfile.getOrElse(DBConnection.getDummyConnection)) {


  protected lazy val hiveCli = getExecutablePath("hive") map {
    x => new HiveCLIInterface(x)
  } match {
    case Some(x) => x
    case None => throw new RuntimeException(s"hive executable not found in path")
  }

  override lazy val dbInterface: DBInterface = connectionProfile match {
    case Some(profile) => new HiveServerDBInterface(profile)
    case None => throw new RuntimeException("HiveServer2 interface being accessed when it is not defined")
  }


  override def work() = {
    connectionProfile match {
      case Some(profile) => super.work()
      case None => {
        hiveCli.queryOne(sql, taskName)
      }
    }
  }


  override def teardown() = {
    connectionProfile match {
      case Some(profile) => super.teardown()
      case _ => ()
    }
  }

}

object HQLRead extends TaskLike {

  override def taskName: String = "HQLRead"

  override val defaultConfig: Config = ConfigFactory.empty()

  override def apply(name: String, config: Config) = {
    val sql = config.asInlineOrFile("sql")
    val connection = config.getAs[ConfigValue]("dsn") map DBConnection.parseConnectionProfile
    new HQLRead(name, sql, connection)
  }

  override val info = database.SQLRead.info

  override val desc: String =
    s"""
      |The $taskName task lets you a run a SELECT query which returns a single row. This single row is
      |processed back and converted to a JSON/HOCON map object and merged with job context so that values
      |are available in the downstream task.
    """.stripMargin

  override val paramConfigDoc = database.SQLRead.paramConfigDoc(10000)

  override val fieldDefinition = database.SQLRead.fieldDefinition +
    ("dsn" ->
      """either a name of the dsn or a config-object with username/password and other credentials.
        |This field is optional field and if not provided then task would use the local Hive CLI installation to execute the query
        |""".stripMargin)

  override val outputConfig: Option[Config] = database.SQLRead.outputConfig

  override val outputConfigDesc: String = database.SQLRead.outputConfigDesc

}
