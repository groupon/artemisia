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
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import com.groupon.artemisia.task.database.DBInterface
import com.groupon.artemisia.task.settings.DBConnection
import com.groupon.artemisia.task.{TaskLike, database}
import com.groupon.artemisia.util.CommandUtil._
import com.groupon.artemisia.util.HoconConfigUtil.Handler

/**
  * Created by chlr on 8/1/16.
  */

class HQLExecute(override val taskName: String,
                 override val sql: String,
                 mode: Mode,
                 connectionProfile: Option[DBConnection])
    extends database.SQLExecute(taskName, sql, connectionProfile.getOrElse(DBConnection.getDummyConnection)) {

  protected lazy val hiveCli: HiveCLIInterface = getExecutablePath("hive") map {
    x => new HiveCLIInterface(x)
  } match {
    case Some(x) => x
    case None => throw new RuntimeException(s"hive executable not found in path")
  }

  protected lazy val beeLineCli: BeeLineInterface = (getExecutablePath("beeline"), connectionProfile) match {
      case (Some(path), Some(connection)) =>  new BeeLineInterface(path, connection)
      case (_, None) => throw new RuntimeException(s"connection field is required for beeline execution")
      case (None, _) => throw new RuntimeException("beeline tool is not found in the PATH env variable")
  }

  override protected[task] def setup(): Unit = {}

  override lazy val dbInterface: DBInterface = connectionProfile match {
    case Some(profile) => new HiveServerDBInterface(profile)
    case None => throw new RuntimeException("HiveServer2 interface being accessed when it is not defined")
  }

  override def work(): Config = {
    (mode, connectionProfile) match {
      case (HiveServer2, Some(_)) => super.work()
      case (HiveServer2, None) => throw new InvalidSettingException("HiveServer2 mode requires dsn setting defined")
      case (Beeline, Some(_)) => wrapAsStats(ConfigFactory.empty.withValue("rows-effected", beeLineCli.execute(sql, taskName).root))
      case (Beeline, None) => throw new InvalidSettingException("Beeline mode requires dsn setting defined")
      case (HiveCLI, _) => wrapAsStats(ConfigFactory.empty().withValue("rows-effected", hiveCli.execute(sql, taskName).root))
    }
  }

  override def teardown(): Unit = {
    connectionProfile match {
      case Some(_) => super.teardown()
      case _ => ()
    }
  }

}

object HQLExecute extends TaskLike {

  override val taskName: String = "HQLExecute"

  override def paramConfigDoc: Config =  database.SQLExecute.paramConfigDoc(10000)
                                                .withValue(""""dsn_[1]"""",ConfigValueFactory.fromAnyRef("connection-name @optional"))
                                                .withValue(""""dsn_[2]"""",DBConnection.structure(10000).root())
                                                .withValue("""mode""", ConfigValueFactory.fromAnyRef("beeline @default(cli) @allowed(cli, beeline, hiveserver2)") )

  override def defaultConfig: Config =  ConfigFactory.empty().withValue("mode", ConfigValueFactory.fromAnyRef("cli"))

  override def fieldDefinition: Map[String, AnyRef] = database.SQLExecute.fieldDefinition + ("dsn" ->
    """
      |either a name of the dsn or a config-object with username/password and other credentials.
      |This field is optional field and if not provided then task would use the local Hive CLI installation to execute the query
    """.stripMargin,
  "mode" -> "mode of execution of HQL. three modes are allowed hiveserver2, cli, beeline")

  override def apply(name: String, config:  Config) = {
    val sql = config.asInlineOrFile("sql")
    val connection = config.hasPath("dsn") match {
      case true => Some(DBConnection.parseConnectionProfile(config.getValue("dsn")))
      case false => None
    }
    new HQLExecute(name, sql, Mode(config.as[String]("mode")) , connection)
  }

  override val info: String = "Execute Hive HQL queries"

  override val desc: String =
    s"""
      | $taskName is used to execute Hive DML/DDL queries (INSERT/CREATE etc).
      | The queries can be executed in three modes set using the **mode** property.
      |
      |  1. HiveServer2: This uses direct hiveserver2 jdbc connection to execute queries
      |  2. Beeline: This uses the Beeline client installed locally to execute queries
      |  3. CLI: This uses local hive cli installation to execute queries.
      |
      | Hiveserver2 and Beeline modes requires a connection object passed via a **dsn** field.
      | CLI mode doesnt require any dsn connection object.
    """.stripMargin

  override val outputConfig: Some[Config] = {
   val config = ConfigFactory parseString
      s"""
       |taskname = {
       |	__stats__ = {
       |		rows-effected = {
       |			tablename_1 = 52
       |      tablename_2 = 100
       |		}
       |	}
       |}
     """.stripMargin
    Some(config)
    }


  override val outputConfigDesc: String =
    """
      | Here the hypothetical task has two insert statements that updates two tables *tablename_1* and *tablename_2*.
      | *tablename_1* has modified 52 rows and *tablename_2* has modified 100 rows.
    """.stripMargin
}
