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

import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import com.groupon.artemisia.core.AppLogger
import com.groupon.artemisia.task.Task
import com.groupon.artemisia.task.settings.DBConnection
import com.groupon.artemisia.util.HoconConfigUtil.Handler

import scala.reflect.ClassTag

/**
 * Created by chlr on 5/21/16.
 */

/**
 * An abstract task to execute a query
  *
  * @param name a name for this task
 * @param sql query to be executed
 * @param connectionProfile connection detail for the database
 */
abstract class SQLExecute(name: String, val sql: String, val connectionProfile: DBConnection) extends Task(name) {

  val dbInterface: DBInterface

  override def setup(): Unit

  /**
   * The query is executed in this phase.
   * returns number of rows updated as a status node in config object
    *
    * @return any output of the work phase be encoded as a HOCON Config object.
   */
  override def work(): Config = {
    val updatedRows = dbInterface.execute(sql)
    AppLogger debug s"$updatedRows rows updated"
    wrapAsStats {
      ConfigFactory.empty.withValue("rows-effected", ConfigValueFactory.fromAnyRef(updatedRows)).root
    }
  }

  override def teardown(): Unit = {
    AppLogger debug s"closing database connection"
    dbInterface.terminate()
  }

}

object SQLExecute {

  val taskName = "SQLExecute"

  val info = "executes DML statements such as Insert/Update/Delete"

  val desc = s"$taskName task is used execute arbitrary DML/DDL statements against a database"

  def paramConfigDoc(defaultPort: Int) = {
    val config = ConfigFactory parseString
    s"""
       |{
       |  "dsn_[1]" = connection-name
       |  sql = "DELETE FROM TABLENAME @optional(either this or sqlfile key is required)"
       |  sql-file =  "/var/tmp/sqlfile.sql @optional(either this or sql key is required)"
       |}
     """.stripMargin
    config
        .withValue(""""dsn_[2]"""",DBConnection.structure(defaultPort).root())
  }

  val fieldDefinition = Map(
    "dsn" -> "either a name of the dsn or a config-object with username/password and other credentials",
    "sql" -> "select query to be run",
    "sql-file" -> "the file containing the query"
  )


  def create[T <: SQLExecute: ClassTag](name: String, config: Config) = {
    val sql = config.asInlineOrFile("sql")
    val connectionProfile = DBConnection.parseConnectionProfile(config.getValue("dsn"))
      implicitly[ClassTag[T]].runtimeClass.asSubclass(classOf[SQLExecute]).getConstructor(classOf[String],
        classOf[String], classOf[DBConnection]).newInstance(name, sql, connectionProfile)
  }

  val outputConfig = {
    val config = ConfigFactory parseString
      s"""
         | taskname = {
         | __stats__ = {
         |   rows-effected = 52
         |   }
         | }
       """.stripMargin
    Some(config)
  }

  val outputConfigDesc =
    """
      | **taskname.__stats__.row-effected** key has the total number of row effected (inserted/deleted/updated)
      | by the sql query.
    """.stripMargin


}


