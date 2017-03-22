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

import java.sql.{Connection, PreparedStatement, ResultSet}
import com.typesafe.config.{Config, ConfigFactory}
import com.groupon.artemisia.task.settings.DBConnection
import scala.util.{Failure, Success, Try}

/**
  * Created by chlr on 4/15/16.
  */

object DBUtil {


  /**
   * takes a tablename literal and parses the optional databasename part and the tablename part
    *
    * @param tableName input table of format <databasename>.<tablename>
   * @return
   */
  def parseTableName(tableName: String): (Option[String],String) = {
      val result  = tableName.split("""\.""").toList
      result.length match {
        case 1 => None -> result.head
        case 2 => Some(result.head) -> result(1)
        case _ => throw new IllegalArgumentException(s"$tableName is a valid table identifier")
      }
  }


  /**
    *
    * @todo convert java.sql.Time to Duration type in Hocon
    * @param rs input ResultSet
    * @return config object parsed from first row
    */
  def resultSetToConfig(rs: ResultSet) = {
    if(rs.next()) {
      val result = for(i <- 1 to rs.getMetaData.getColumnCount) yield {
        rs.getMetaData.getColumnName(i) -> rs.getObject(i)
      }
      result.foldLeft[Config](ConfigFactory.empty()) {
        (config: Config, data: (String, Object)) => {
          val parsed = data match {
            case (x, y: String) =>  s" { $x = $y }"
            case (x, y: java.lang.Number) => s" { $x = ${y.toString} }"
            case (x, y: java.lang.Boolean) => s"{ $x = ${if (y) "yes" else "no"} }"
            case (x, y: java.util.Date) => s"""{ $x = "${y.toString}" }"""
            case _ => throw new UnsupportedOperationException(s"Type ${data._2.getClass.getCanonicalName} is not supported")
          }
          ConfigFactory parseString parsed withFallback config
        }
      }
    }
    else {
      ConfigFactory.empty()
    }
  }


  /**
    * execute DML statements and returns either scala.util.Success with number of records updated or
    * scala.util.Failure with the throwable object
    *
    * @param sql query to be executed
    * @param connection implicit connection object
    * @return Try monad with rows updated or exception object
    */
  def executeUpdateQuery(sql: String)(implicit connection: Connection): Try[Int] = {
    var stmt: PreparedStatement = null
    try {
       stmt = connection.prepareStatement(sql)
      Success(stmt.executeUpdate())
    } catch {
      case th: Throwable =>  Failure(th)
    } finally {
      try { stmt.close() } catch { case th: Throwable => () }
    }
  }


  /**
    *
    * @param dbType type of database connection. eg: mysql, postgres, teradata
    * @param connectionProfile DB Connection Profile case class.
    * @return
    */
  def dbConnectionUrl(dbType: String, connectionProfile: DBConnection) = {
      dbType match {
        case "mysql" =>
          Some("com.mysql.jdbc.Driver" ->
          s"jdbc:mysql://${connectionProfile.hostname}:${connectionProfile.port}/${connectionProfile.default_database}?zeroDateTimeBehavior=convertToNull")
        case "postgres" =>
          Some("org.postgresql.Driver" ->
          s"jdbc:postgresql://${connectionProfile.hostname}:${connectionProfile.port}/${connectionProfile.default_database}")
        case "teradata" =>
          Some("com.teradata.jdbc.TeraDriver" ->
          s"jdbc:teradata://${connectionProfile.hostname}:${connectionProfile.port}/${connectionProfile.default_database}")
        case _ => None
      }
  }


  /**
    * convert a ResultSet object to list of type T.
    * if two or more columns are to be extracted type T to be Tuple (recommended)
    * the result object is automatically closed
    *
    * @param resultSet
    * @tparam T
    */
  abstract class ResultSetIterator[T](val resultSet: ResultSet) extends Iterator[T] {

    override def hasNext: Boolean = {
      resultSet.next() match{
        case true => true
        case false => resultSet.close(); false
      }
    }

    override def next(): T = generateRow

    /**
      * this method is to be implemented by the concrete class to generate each row of the iterator
      *
      * @return
      */
    def generateRow: T

  }

 }
