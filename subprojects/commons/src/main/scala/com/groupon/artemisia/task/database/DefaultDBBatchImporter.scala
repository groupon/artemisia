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

import java.sql.{BatchUpdateException, SQLException}
import com.groupon.artemisia.task.settings.LoadSetting


/**
  * The Default implementation of BaseBatchDBWriter writer.
  *
  */
trait DefaultDBBatchImporter extends BatchDBImporter {

  self: DBInterface =>

  def getBatchWriter(tableName: String, loadSetting: LoadSetting) = {
    new DefaultDBBatchImporter.DefaultDBBatchImporter(tableName, loadSetting, this)
  }


}

object DefaultDBBatchImporter {

  class DefaultDBBatchImporter(tableName: String, loadSettings: LoadSetting, dbInterface: DBInterface)
    extends BaseDBBatchWriter(tableName, loadSettings, dbInterface) {

    def processBatch(batch: Array[Array[String]]) = {
      val (validRows: Array[Array[String]], invalidRows: Array[Array[String]]) = batch partition { x => x.length == tableMetadata.length }
      invalidRows foreach {  errorWriter.writeRow }

      try {
        for (row <- validRows) {
          try { composeStmt(row); stmt.addBatch() } catch { case th: Throwable => errorWriter.writeRow(row) }
        }
        stmt.executeBatch()
      }
      catch {
        case e: BatchUpdateException => {
          val results = e.getUpdateCounts
          val retryRecords = results zip validRows filter { x => x._1 < 0 } map { _._2 }
          stmt.clearBatch()
          for(record <- retryRecords) {
            try { composeStmt(record); stmt.execute() }
            catch {
              case e: SQLException => errorWriter.writeRow(record)
            }
          }
        }
        case th: Throwable => ()
      }
      finally {
        stmt.clearParameters()
        stmt.clearBatch()
      }
    }
  }
}
