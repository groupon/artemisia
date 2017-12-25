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

package com.groupon.artemisia.task.database

import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}
import com.groupon.artemisia.core.AppLogger
import com.groupon.artemisia.task.Task
import com.groupon.artemisia.task.settings.DBConnection
import com.groupon.artemisia.util.HoconConfigUtil.Handler
import com.groupon.artemisia.util.Util
import scala.reflect.ClassTag

/**
 * Created by chlr on 4/22/16.
 */

/**
 *
 * @param name name of the task
 * @param sql query to be executed
 * @param connectionProfile connection profile to use
 */
abstract class SQLRead(name: String = Util.getUUID, val sql: String, val connectionProfile: DBConnection)
  extends Task(name) {

  val dbInterface: DBInterface

  override def setup(): Unit = {}

  /**
   * execute query and parse as config file.
   * considers only the first row of the query
    *
    * @return config file object
   */
  override def work(): Config = {
    val result = dbInterface.queryOne(sql)
    AppLogger debug s"query result ${result.root().render(ConfigRenderOptions.concise())}"
    result
  }

  override def teardown(): Unit = {
    AppLogger debug s"closing database connection"
    dbInterface.terminate()
  }

}

object SQLRead {

  val taskName = "SQLRead"

  val info = "execute select queries and wraps the results in config"

  val desc =
    s"""
      |$taskName task runs a select query and parse the first row as a Hocon Config.
      |The query must be a SELECT query and not any DML or DDL statements.
      |The configuration object is shown below.
    """.stripMargin

  def paramConfigDoc(defaultPort: Int) = {
    val config = ConfigFactory parseString  s"""
       |{
       |  sql = "SELECT count(*) as cnt from table @optional(either this or sqlfile key is required)"
       |  sqlfile =  "/var/tmp/sqlfile.sql @optional(either this or sql key is required)"
       |}
  """.stripMargin
    config.withValue("dsn", DBConnection.structure(defaultPort).root())
  }


  val fieldDefinition = Map(
    "dsn" -> "either a name of the dsn or a config-object with username/password and other credentials",
    "sql" -> "select query to be run",
    "sqlfile" -> "the file containing the query"
  )


  def create[T <: SQLRead: ClassTag](name: String, config: Config, reference: Config) = {
    val connectionProfile = DBConnection.parseConnectionProfile(config.getValue("dsn"))
    val sql = config.asInlineOrFile("sql", reference)
    implicitly[ClassTag[T]].runtimeClass.asSubclass(classOf[SQLRead]).getConstructor(classOf[String], classOf[String]
    , classOf[DBConnection]).newInstance(name, sql, connectionProfile)
  }


  val outputConfig: Option[Config] = {
    val config = ConfigFactory parseString
      s"""
         | {
         |   cnt =  100
         |   foo = bar
         | }
       """.stripMargin
    Some(config)
  }

  val outputConfigDesc =
    s"""
       | The first row of the result is retrieved and parsed to generate a config object.
       | The column-names are turned as keys and the values of the first row of the result-set
       | are turned into corresponding values. If there are more records in the result-set only
       | the first record is considered and the rest are ignored. As for the above config example
       | the input query would look like this.
       |
       |     SELECT count(*) as cnt, 'bar' as foo FROM database.tablename
       |
     """.stripMargin


}
