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

package com.groupon.artemisia.task.database.teradata

import java.sql.SQLException
import com.groupon.artemisia.core.AppLogger._
import com.groupon.artemisia.task.database.{DBInterface, DBUtil}
import scala.util.{Failure, Success, Try}

/**
  * Created by chlr on 7/31/16.
  */

/**
  * A collection of utility methods
  */
object TeraUtils {

  /**
    * utility method to drop and recreate a table
    *
    * @param tableName name of the table
    * @param dbInterface implicit dbinterface object
    */
  def dropRecreateTable(tableName: String)(implicit dbInterface: DBInterface): Unit = {
    info("dropping and recreating the table")
    val rs = dbInterface.query(s"SHOW TABLE $tableName", printSQL = false)
    val ct_stmt = { rs.next() ;val res = rs.getString(1); rs.close(); res }
    dbInterface.execute(s"DROP TABLE $tableName", printSQL = false)
    dbInterface.execute(ct_stmt, printSQL = false)
  }


  /**
    * truncate the table and if truncate fails due to fastload lock drop and re-create the table.
    *
    * @param tableName name of the table.
    * @param dBInterface
    */
  def truncateElseDrop(tableName: String)(implicit dBInterface: DBInterface): Unit = {
    info("attempting to truncate table")
    Try(dBInterface.execute(s"DELETE FROM $tableName")) match {
      case Success(_) => ()
      case Failure(th: SQLException)
        if th.getErrorCode == 2652  => dropRecreateTable(tableName)
      case Failure(x) => throw x
    }
  }


  /**
    *
    * @param loadSize size of data in bytes
    * @param loadSetting input load setting
    * @return customized final load setting
    */
  def autoTuneLoadSettings[T <: BaseTeraLoadSetting](loadSize: Long, loadSetting: T) : T = {
    def transform(x: T) = {
     if (loadSize > x.bulkLoadThreshold) {
       debug(s"target load size of $loadSize is greater than the threshold of ${x.bulkLoadThreshold}")
       x.create(batchSize = 80000, mode = "fastload")
     }
     else {
       debug(s"target load size of $loadSize is lesser than the threshold of ${x.bulkLoadThreshold}")
       x.create(batchSize = 4000, mode = "default")
     }
    }
    if (loadSetting.mode == "auto") {
      transform(loadSetting).asInstanceOf[T]
    } else {
      loadSetting
    }
  }


  /**
    * fetch table column metadata details
    *
    * @param databaseName databasename
    * @param tableName target table name
    * @param dBInterface Teradata DBInterface instance
    * @return
    */
  def tableMetadata(databaseName: String, tableName: String)(implicit dBInterface: DBInterface) = {
    val sql =
      s"""
        |SELECT
        | TRIM(ColumnName)
        |,ColumnType
        |,(CASE
        |  WHEN Columntype = 'DA' THEN 15
        |  WHEN ColumnType = 'D' THEN (decimaltotaldigits + 5)
        |  WHEN ColumnType IN ('BV','PM','SZ') THEN ColumnLength
        |  WHEN ColumnType IN ('TS','TZ') THEN 35
        |  WHEN ColumnType IN ('I ','I2','I1','I8','F') THEN 25 ELSE ColumnLength END ) AS byteLength
        |,Nullable
        |FROM DBC.Columns c WHERE DatabaseName = '$databaseName' AND TableName = '$tableName'  ORDER BY ColumnId
      """.stripMargin
    val rs = dBInterface.query(sql = sql, printSQL = false)
    val resultSetIterator = new DBUtil.ResultSetIterator[(String, String, Short, String)](rs) {
      override def generateRow: (String, String, Short, String) = {
        (resultSet.getString(1), resultSet.getString(2), resultSet.getShort(3), resultSet.getString(4))
      }
    }.toSeq
    resultSetIterator zip (1 to resultSetIterator.size) map {
      case (row, index) => (row._1,row._2,row._3,s"${row._1.take(23)}_${index}_",row._4)
    }
  }

}
