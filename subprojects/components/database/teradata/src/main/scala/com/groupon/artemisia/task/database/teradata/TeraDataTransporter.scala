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

import java.sql.{SQLException, BatchUpdateException}
import scala.collection.JavaConverters._
import com.groupon.artemisia.core.AppLogger._
import com.groupon.artemisia.task.database.{DefaultDBExporter, BatchDBImporter, BaseDBBatchWriter, DBInterface}
import com.groupon.artemisia.task.settings.LoadSetting

/**
  * Created by chlr on 7/2/16.
  */

trait TeraDataTransporter extends BatchDBImporter with DefaultDBExporter {

  self: DBInterface =>

  def getBatchWriter(tableName: String, loadSetting: LoadSetting) = {
    new TeraDataTransporter.FastLoadDBBatchWriter(tableName, loadSetting, this)
  }

}

object TeraDataTransporter {

  /**
   * This DBWriter instance differs from the default DBWriter on how BatchUpdateException is handled
   *
   * @param tableName    name of the table
   * @param loadSettings load settings
   * @param dBInterface  database interface object
   */
  class FastLoadDBBatchWriter(tableName: String, loadSettings: LoadSetting, dBInterface: DBInterface)
    extends BaseDBBatchWriter(tableName, loadSettings, dBInterface) {

    val errorRecordHandler = new FastLoadErrorRecordHandler(tableName)
    private var errorCounter = 0
    dBInterface.connection.setAutoCommit(false)

    val ignoreErrorCodes = Seq(1145, 1156, 1154, 1248, 1148, 1159, 1162, 1160, 1147)
    val errorRecordCode = 1160 // error code available only during commit operation
    val displayErrorCode = Seq(1159) // error code available only during commit operation

    def processBatch(batch: Array[Array[String]]) = {
      try {
        for (row <- batch) {
          try{ stmt.clearParameters(); composeStmt(row); stmt.addBatch() } catch {
            case th: Throwable => errorWriter.writeRow(row)
          }
        }
        stmt.executeBatch()
      } catch {
        case th: BatchUpdateException => {
          errorCounter += th.getUpdateCounts.count(_ < 0)
          th.iterator.asScala foreach {
            case x: SQLException if ignoreErrorCodes contains x.getErrorCode => ()
            case x: SQLException => { error(x.getMessage); throw x }
            case x: Throwable => true
          }
        }
        case th: Throwable => {
          error(th.getMessage)
          throw th
        }
      }
      debug(s"processed batch of size ${batch.length}")
    }

    override  def close() = {
      try {
        dBInterface.connection.commit()
        dBInterface.connection.setAutoCommit(true)
      } catch {
        case th: SQLException => {
          th.iterator.asScala foreach {
            case x: SQLException => {
              if (displayErrorCode contains x.getErrorCode) {
                warn(x.getMessage)
              }
              if (x.getErrorCode == errorRecordCode) {
                errorRecordHandler.parseException(x)
              }
              if (!(ignoreErrorCodes contains x.getErrorCode)) {
                error(x.getMessage)
                throw x
              }
            }
            case x: Throwable => {
              error(x.getMessage)
              throw x
            }
          }
        }
        case th: Throwable => {
          error(th.getMessage)
          throw th
        }
      } finally {
        stmt.close()
        errorWriter.close()
        errorRecordHandler.close()
      }
    }

  }
}
