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

import java.io.{InputStream, OutputStream}
import java.net.URI
import java.sql.{Connection, ResultSet}
import com.typesafe.config.Config
import com.groupon.artemisia.core.AppLogger._
import com.groupon.artemisia.task.settings.{ExportSetting, LoadSetting}
import com.groupon.artemisia.util.Util


/**
 * Created by chlr on 4/13/16.
 */


/**
 * A standard database interface for common operations such as
 *   - query result
 *   - execute DML statements
 *   - query and parse and store results HoconConfig objects
 *   - close connection
 */
trait DBInterface {

  self: DBImporter with DBExporter =>


  /**
   *  JDBC connection object
   */
  final lazy val connection: Connection = getNewConnection

  /**
    * creates a new connection object
    *
    * @return JDBC connection object
    */
  def getNewConnection: Connection

  /**
   *
   * @param sql Select query to be executed
   * @return result object
   */
  def query(sql: String, printSQL: Boolean = true): ResultSet = {
    if(printSQL)
        info(Util.prettyPrintAsciiBanner(sql,"query"))
    val stmt = connection.prepareStatement(sql)
    stmt.executeQuery()
  }

  /**
   *
   * @param sql DML query to be executed
   * @return number of records updated/deleted/inserted
   */
  def execute(sql: String, printSQL: Boolean = true): Long = {
    if (printSQL) {
      info("executing query")
      info(Util.prettyPrintAsciiBanner(sql, "query"))
    }
    val stmt = connection.prepareStatement(sql)
    val recordCnt = stmt.executeUpdate()
    stmt.close()
    recordCnt
  }

  /**
   *
   * @param sql Select query to be executed
   * @return Hocon Config object of the first record
   */
  def queryOne(sql: String, printSQL: Boolean = true): Config = {
    if (printSQL) {
      info("executing query")
      info(Util.prettyPrintAsciiBanner(sql, "query"))
    }
    val stmt = connection.prepareStatement(sql)
    val rs = stmt.executeQuery()
    val result = DBUtil.resultSetToConfig(rs)
    try {
      rs.close()
      stmt.close()
    }
    catch {
      case e: Throwable => {
        warn(e.getMessage)
      }
    }
    result
  }

  /**
    * export query result to file
    *
    * @param sql query
    * @param exportSetting export settings
    * @return no of records exported
    */
  def exportSQL(sql: String, target: Either[OutputStream, URI], exportSetting: ExportSetting): Long = {
    target match {
      case Left(outputStream) => self.export(sql, outputStream, exportSetting)
      case Right(location) => self.export(sql, location, exportSetting)
    }
  }

  /**
   * Load data to a table typically from a file.
   *
   * @param tableName destination table
   * @param loadSettings load settings
   * @return tuple of total records in source and number of records rejected
   */
  def loadTable(tableName: String, source: Either[InputStream,URI] , loadSettings: LoadSetting) = {
      info(s"running table load with below setting")
      info(loadSettings.setting)
      val (total, rejected) = source match {
        case Left(inputStream) => self.load(tableName, inputStream, loadSettings)
        case Right(location) => self.load(tableName, location, loadSettings)
      }
      loadSettings.errorTolerance foreach {
        val errorPct = (rejected.asInstanceOf[Float] / total) * 100
        x => assert( errorPct < x , s"Load Error % ${"%3.2f".format(errorPct)} greater than defined limit: ${x * 100}")
      }
    total -> rejected
  }

  /**
   * close the database connection
   */
  def terminate(): Unit = {
    connection.close()
  }

  /**
   *
   * @param databaseName databasename
   * @param tableName tablename
   * @return Iterable of Tuple of name and type of the column
   */
  def getTableMetadata(databaseName: Option[String] ,tableName: String): Iterable[(String,Int)] = {
    val effectiveTableName = (databaseName map {x => s"$x.$tableName"}).getOrElse(tableName)
    val sql = s"SELECT * FROM $effectiveTableName"
    val rs = this.query(sql, printSQL = false)
    val metadata = rs.getMetaData
    val result = for (i <- 1 to metadata.getColumnCount) yield {
      metadata.getColumnName(i) -> metadata.getColumnType(i)
    }
    rs.close()
    result
  }

}








