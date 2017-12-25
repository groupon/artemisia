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

import com.groupon.artemisia.inventory.exceptions.InvalidSettingException
import com.typesafe.config.{Config, ConfigFactory, ConfigValue, ConfigValueFactory}
import com.groupon.artemisia.task.database.DBInterface
import com.groupon.artemisia.task.settings.DBConnection
import com.groupon.artemisia.task.{TaskLike, database}
import com.groupon.artemisia.util.CommandUtil._
import com.groupon.artemisia.util.HoconConfigUtil.Handler

/**
  * Created by chlr on 8/2/16.
  */
class HQLRead(taskName: String,
              sql: String,
              mode: Mode,
              connectionProfile: Option[DBConnection])
  extends database.SQLRead(taskName, sql, connectionProfile.getOrElse(DBConnection.getDummyConnection)) {


  protected lazy val hiveCli: HiveCLIInterface = getExecutablePath("hive") map {
    x => new HiveCLIInterface(x)
  } match {
    case Some(x) => x
    case None => throw new RuntimeException(s"hive executable not found in path")
  }

  override lazy val dbInterface: DBInterface = connectionProfile match {
    case Some(profile) => new HiveServerDBInterface(profile)
    case None => throw new RuntimeException("HiveServer2 interface being accessed when it is not defined")
  }

  protected lazy val beeLineCli: BeeLineInterface = (getExecutablePath("beeline"), connectionProfile) match {
    case (Some(path), Some(connection)) =>  new BeeLineInterface(path, connection)
    case (_, None) => throw new RuntimeException(s"connection field is required for beeline execution")
    case (None, _) => throw new RuntimeException("beeline tool is not found in the PATH env variable")
  }

  override def work(): Config = {
    (mode, connectionProfile) match {
      case (HiveServer2, Some(_)) => super.work()
      case (HiveServer2, None) => throw new InvalidSettingException("HiveServer2 mode requires dsn setting defined")
      case (Beeline, Some(_)) => beeLineCli.queryOne(sql, taskName)
      case (Beeline, None) => throw new InvalidSettingException("Beeline mode requires dsn setting defined")
      case (HiveCLI, _) => hiveCli.queryOne(sql, taskName)
    }
  }


  override def teardown(): Unit = {
    connectionProfile match {
      case Some(_) => super.teardown()
      case _ => ()
    }
  }

}

object HQLRead extends TaskLike {

  override val taskName: String = "HQLRead"

  override val defaultConfig: Config = ConfigFactory.empty.withValue("mode", ConfigValueFactory.fromAnyRef("cli"))

  override def apply(name: String, config: Config, reference: Config): HQLRead = {
    val sql = config.asInlineOrFile("sql", reference)
    val connection = config.getAs[ConfigValue]("dsn") map DBConnection.parseConnectionProfile
    new HQLRead(name, sql, Mode(config.as[String]("mode")), connection)
  }

  override val info: String = database.SQLRead.info

  override val desc: String =
    s"""
      |The $taskName task lets you a run a SELECT query which returns a single row. This single row is
      |processed back and converted to a JSON/HOCON map object and merged with job context so that values
      |are available in the downstream task.
      |
      |The queries can be executed in three modes set using the **mode** property.
      | 1. HiveServer2: This uses direct hiveserver2 jdbc connection to execute queries
      | 2. Beeline: This uses the Beeline client installed locally to execute queries
      | 3. CLI: This uses local hive cli installation to execute queries.
      |
    """.stripMargin

  override val paramConfigDoc: Config = database.SQLRead.paramConfigDoc(10000)
    .withValue("""mode""", ConfigValueFactory.fromAnyRef("beeline @default(cli) @allowed(cli, beeline, hiveserver2)") )

  override val fieldDefinition: Map[String, String] = database.SQLRead.fieldDefinition +
    ("dsn" ->
      """either a name of the dsn or a config-object with username/password and other credentials.
        |This field is optional field and if not provided then task would use the local Hive CLI installation to execute the query
        |""".stripMargin)

  override val outputConfig: Option[Config] = database.SQLRead.outputConfig

  override val outputConfigDesc: String = database.SQLRead.outputConfigDesc

}
