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

import java.io.FileOutputStream
import java.sql.Types
import com.groupon.artemisia.inventory.io.CSVFileWriter
import com.groupon.artemisia.task.TaskContext
import com.groupon.artemisia.task.settings.LoadSetting

/**
  * Created by chlr on 6/26/16.
  */

/**
  * An abstract BatchDBWriter that can be extended by each database components to customize implementations.
  *
  * @param tableName name of the table
  * @param loadSettings load settings
  * @param dbInterface database interface object
  */





abstract class BaseDBBatchWriter(tableName: String, loadSettings: LoadSetting, dbInterface: DBInterface)  {


  protected val tableMetadata = {
    val parsedTableName = DBUtil.parseTableName(tableName)
    dbInterface.getTableMetadata(parsedTableName._1, parsedTableName._2).toVector
  }

  protected val stmt = {
    val insertSQL =
      s"""|INSERT INTO $tableName (${tableMetadata.map({_._1}).mkString(",")})
          |VALUES (${tableMetadata.map({ x => "?" }).mkString(",")})
       """.stripMargin
    dbInterface.connection.prepareStatement(insertSQL)
  }

  protected val errorWriter = new CSVFileWriter(new FileOutputStream(TaskContext.getTaskFile("error.txt")),
    BasicExportSetting(header = false, delimiter = '\u0001',quoting = false))

  def processBatch(batch: Array[Array[String]])

  def close() = {
    stmt.close()
    errorWriter.close()
  }

  def getErrRowCount = {
    errorWriter.totalRows
  }

  protected def composeStmt(row: Array[String]) = {
    for (i <- 1 to tableMetadata.length) {
      tableMetadata(i-1)._2 match {
        case Types.BIGINT => row(i-1) match {
          case "" => stmt.setNull(i,Types.BIGINT)
          case _ => stmt.setLong(i, java.lang.Long.parseLong(row(i-1)))
        }
        case Types.BIT => row(i-1) match {
          case "" => stmt.setNull(i,Types.BIT)
          case _ => stmt.setBoolean(i, java.lang.Boolean.parseBoolean(row(i-1)))
        }
        case Types.BOOLEAN => row(i-1) match {
          case "" => stmt.setNull(i,Types.BOOLEAN)
          case _ => stmt.setBoolean(i, java.lang.Boolean.parseBoolean(row(i-1)))
        }
        case Types.DATE => row(i-1) match {
          case "" => stmt.setNull(i,Types.DATE)
          case _ => stmt.setDate(i, java.sql.Date.valueOf(row(i-1)))
        }
        case Types.DECIMAL => row(i-1) match {
          case "" => stmt.setNull(i,Types.DECIMAL)
          case _ => stmt.setBigDecimal(i, new java.math.BigDecimal(row(i-1)))
        }
        case Types.DOUBLE => row(i-1) match {
          case "" => stmt.setNull(i,Types.DOUBLE)
          case _ => stmt.setDouble(i, java.lang.Double.parseDouble(row(i-1)))
        }
        case Types.FLOAT => row(i-1) match {
          case "" => stmt.setNull(i,Types.FLOAT)
          case _ => stmt.setFloat(i, java.lang.Float.parseFloat(row(i-1)))
        }
        case Types.INTEGER => row(i-1) match {
          case "" => stmt.setNull(i,Types.INTEGER)
          case _ => stmt.setInt(i, java.lang.Integer.parseInt(row(i-1)))
        }
        case Types.NUMERIC => row(i-1) match {
          case "" => stmt.setNull(i,Types.NUMERIC)
          case _ => stmt.setBigDecimal(i, new java.math.BigDecimal(row(i-1)))
        }
        case Types.REAL => row(i-1) match {
          case "" => stmt.setNull(i,Types.REAL)
          case _ => stmt.setFloat(i, java.lang.Float.parseFloat(row(i-1)))
        }
        case Types.SMALLINT => row(i-1) match {
          case "" => stmt.setNull(i,Types.SMALLINT)
          case _ => stmt.setShort(i, java.lang.Short.parseShort(row(i-1)))
        }
        case Types.TIME => row(i-1) match {
          case "" => stmt.setNull(i,Types.TIME)
          case _ => stmt.setTime(i, java.sql.Time.valueOf(row(i-1)))
        }
        case Types.TIMESTAMP => row(i-1) match {
          case "" => stmt.setNull(i,Types.TIMESTAMP)
          case _ => stmt.setTimestamp(i, java.sql.Timestamp.valueOf(row(i-1)))
        }
        case Types.TINYINT => row(i-1) match {
          case "" => stmt.setNull(i,Types.TINYINT)
          case _ => stmt.setByte(i, java.lang.Byte.parseByte(row(i-1)))
        }
        case _ => row(i-1) match {
          case "" => stmt.setNull(i,tableMetadata(i-1)._2)
          case _ => stmt.setString(i,row(i-1))
        }
      }
    }
  }
}
