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

import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import com.groupon.artemisia.task.database.DBInterface
import com.groupon.artemisia.task.settings.DBConnection
import com.groupon.artemisia.task.{TaskLike, database}
import com.groupon.artemisia.util.CommandUtil._
import com.groupon.artemisia.util.HoconConfigUtil.Handler

/**
  * Created by chlr on 8/1/16.
  */

class HQLExecute(override val taskName: String, override val sql: String, connectionProfile: Option[DBConnection])
    extends database.SQLExecute(taskName, sql, connectionProfile.getOrElse(DBConnection.getDummyConnection)) {

  protected lazy val hiveCli = getExecutablePath("hive") map {
    x => new HiveCLIInterface(x)
  } match {
    case Some(x) => x
    case None => throw new RuntimeException(s"hive executable not found in path")
  }

  override protected[task] def setup(): Unit = {}

  override lazy val dbInterface: DBInterface = connectionProfile match {
    case Some(profile) => new HiveServerDBInterface(profile)
    case None => throw new RuntimeException("HiveServer2 interface being accessed when it is not defined")
  }

  override def work() = {
    connectionProfile match {
      case Some(profile) => super.work()
      case None => {
        wrapAsStats {
          val result = hiveCli.execute(sql, taskName)
          ConfigFactory.empty()
                  .withValue("loaded", result.root())
        }
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

object HQLExecute extends TaskLike {

  override val taskName: String = "HQLExecute"

  override def paramConfigDoc: Config =  database.SQLExecute.paramConfigDoc(10000)
                                                .withValue(""""dsn_[1]"""",ConfigValueFactory.fromAnyRef("connection-name @optional"))
                                                .withValue(""""dsn_[2]"""",DBConnection.structure(10000).root())

  override def defaultConfig: Config =  ConfigFactory.empty()

  override def fieldDefinition: Map[String, AnyRef] = database.SQLExecute.fieldDefinition + ("dsn" ->
    """
      |either a name of the dsn or a config-object with username/password and other credentials.
      |This field is optional field and if not provided then task would use the local Hive CLI installation to execute the query
    """.stripMargin)

  override def apply(name: String, config:  Config) = {
    val sql = config.asInlineOrFile("sql")
    val connection = config.hasPath("dsn") match {
      case true => Some(DBConnection.parseConnectionProfile(config.getValue("dsn")))
      case false => None
    }
    new HQLExecute(name, sql, connection)
  }

  override val info: String = "Execute Hive HQL queries"

  override val desc: String =
    s"""
      | $taskName is used to execute Hive DML/DDL queries (INSERT/CREATE etc). This task can take a HiveServer2 connection
      | as input param and execute the query by connecting to the HiveServer2 service. If no connection is provided
      | it would use local Hive CLI to execute the query.
    """.stripMargin

  override val outputConfig = {
   val config = ConfigFactory parseString
      s"""
       |taskname = {
       |	__stats__ = {
       |		loaded = {
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
