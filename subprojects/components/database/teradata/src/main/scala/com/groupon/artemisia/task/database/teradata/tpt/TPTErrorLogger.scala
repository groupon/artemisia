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

package com.groupon.artemisia.task.database.teradata.tpt

import java.io.File
import com.groupon.artemisia.task.database.DBInterface
import com.groupon.artemisia.task.database.DBUtil.ResultSetIterator
import com.groupon.artemisia.core.AppLogger._
import com.groupon.artemisia.util.Util
import scala.io.Source
import scala.util.{Failure, Success, Try}


/**
  * Created by chlr on 9/24/16.
  */


/**
  * abstract TPTErrorLogger class
  */
trait TPTErrorLogger {

  /**
    * target table name. the error table names are derieved from the target table name.
    */
  protected val tableName: String

  /**
    * location of the error file
    */
  protected val errorFile: String

  /**
    * log error
    */
  def log(): Unit


  /**
    * db-interface
    */
  protected val dbInterface: DBInterface


  /**
    * error file content.
    */
  protected lazy val errorFileContent = {
    val file = new File(errorFile)
    file.exists() match {
      case true => Source.fromFile(errorFile).getLines.take(10).toSeq
      case false => Nil
    }
  }

}

object TPTErrorLogger {


  def createErrorLogger(tableName: String,
                        errorFile: String,
                        dbInterface: DBInterface,
                        mode: String) = {
    mode match {
      case "default" => new StreamOperErrLogger(tableName, errorFile, dbInterface)
      case "fastload" => new LoadOperErrLogger(tableName, errorFile, dbInterface)
      case x => throw new RuntimeException(s"mode $x is not supported.")
    }
  }

  /**
    * Error Info logger for load operator
    *
    * @param tableName
    * @param errorFile
    */
  class LoadOperErrLogger(override protected val tableName: String
                          , override protected val errorFile: String
                          , override protected val dbInterface: DBInterface)
    extends TPTErrorLogger {

    protected val errorSql =
      s"""|LOCKING ROW FOR ACCESS
          |SELECT
          |ErrorFieldName as Fields,
          |count(*) as cnt,
          |ErrorText as ErrorMessage
          |FROM ${tableName}_ET t0
          |INNER JOIN dbc.errormsgs t1
          |ON t0.ErrorCode = t1.ErrorCode
          |GROUP BY 1,3;""".stripMargin

    protected lazy val etTableContent: Seq[(String, String, String)] = {
      Seq(("FieldName", "Rowcount", "ErrorMessage")) ++ fetchData(errorSql)
    }

    final def fetchData(query: String) = {
      Try(dbInterface.query(query, printSQL = false)) match {
        case Failure(th) => Seq[(String,String, String)]()
        case Success(rs) =>
          val resultSetIterator = new ResultSetIterator[(String, String, String)](rs) {
            override def generateRow: (String, String, String) = {
              (resultSet.getString(1), resultSet.getString(2), resultSet.getString(3))
            }
          }
          resultSetIterator.toSeq
      }
    }


    /**
      * log error
      */
    override def log(): Unit = {
      if (etTableContent.length > 1) {
        info("printing _ET table content")
        val table = Util.prettyPrintAsciiTable(etTableContent.map(x => Array(x._1, x._2, x._3)).toArray)
        info(s"\n\n${table.mkString(System.lineSeparator())}\n")
      }
      if (errorFileContent.nonEmpty) {
        val message = ("printing first 10 lines in $errorFile" +: errorFileContent.toSeq)
          .mkString(System.lineSeparator())
        info(s"$message\n")
      }
    }

  }


  class StreamOperErrLogger(override protected val tableName: String
                            ,override protected val errorFile: String
                            ,override protected val dbInterface: DBInterface)
    extends TPTErrorLogger {

    protected val errorSql = s"""|SELECT ErrorMsg,count(*) AS cnt
                                 |FROM ${tableName}_ET
                                 |GROUP BY 1
                                 |ORDER BY 2 desc;""".stripMargin

    protected lazy val etTableContent: Seq[(String, String)] = {
      Seq(("ErrorMessage", "Rowcount")) ++ fetchData(errorSql)
    }

    final def fetchData(query: String) = {
      Try(dbInterface.query(query, printSQL = false)) match {
        case Failure(th) => Seq[(String, String)]()
        case Success(rs) =>
          val resultSetIterator = new ResultSetIterator[(String, String)](rs) {
            override def generateRow: (String, String) = {
              (resultSet.getString(1), resultSet.getString(2))
            }
          }
          resultSetIterator.toSeq
      }
    }

    /**
      * log error
      */
    override def log(): Unit = {
      if (etTableContent.length > 1) {
        info("printing _ET table content")
        val table = Util.prettyPrintAsciiTable(etTableContent.map(x => Array(x._1, x._2)).toArray)
        info(s"\n\n${table.mkString(System.lineSeparator())}\n")
      }
      if (errorFileContent.nonEmpty) {
        val message = ("printing first 10 lines in $errorFile" +: errorFileContent.toSeq)
          .mkString(System.lineSeparator())
        info(s"$message\n")
      }
    }
  }

}
