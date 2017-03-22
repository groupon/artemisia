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

package com.groupon.artemisia.task.database.postgres

import java.io._
import java.net.URI
import org.postgresql.PGConnection
import com.groupon.artemisia.core.AppLogger
import com.groupon.artemisia.task.database.{DBExporter, DBImporter, DBInterface}
import com.groupon.artemisia.task.settings.{ExportSetting, LoadSetting}
import com.groupon.artemisia.util.Util

/**
  * Created by chlr on 6/11/16.
  */

trait PGDataTransporter extends DBExporter with DBImporter {

  self: DBInterface =>

  override def load(tableName: String, location: URI ,loadSettings: LoadSetting) = {
    val copyMgr = self.connection.asInstanceOf[PGConnection].getCopyAPI
    val reader = new BufferedReader(new FileReader(new File(location)))
    AppLogger info Util.prettyPrintAsciiBanner(PGDataTransporter.getLoadCmd(tableName, loadSettings), heading = "query")
     val result = copyMgr.copyIn(PGDataTransporter.getLoadCmd(tableName, loadSettings), reader)
    reader.close()
    result -> -1L
  }

  override def load(tableName: String, inputStream: InputStream, loadSetting: LoadSetting) = {
    throw new UnsupportedOperationException("load operation is not supported in this mode")
  }

  override def export(sql: String, location: URI, exportSetting: ExportSetting) = {
    val copyMgr = self.connection.asInstanceOf[PGConnection].getCopyAPI
    val writer = new BufferedWriter(new FileWriter(new File(location)))
    AppLogger info Util.prettyPrintAsciiBanner(PGDataTransporter.getExportCmd(sql, exportSetting), heading = "query")
    val rowCount = copyMgr.copyOut(PGDataTransporter.getExportCmd(sql, exportSetting), writer)
    writer.close()
    rowCount
  }

  override def export(sql: String, outputStream: OutputStream, exportSetting: ExportSetting) = {
    throw new UnsupportedOperationException("export operation is not supported in this mode")
  }

}

object PGDataTransporter {

  private[postgres] def getLoadCmd(tableName: String, loadSettings: LoadSetting) = {
    assert(loadSettings.skipRows <= 1, "this task can skip either 0 or 1 row only")
    s"""
       | COPY $tableName FROM STDIN
       | WITH (DELIMITER '${loadSettings.delimiter}',
       | FORMAT csv,
       | ${if (loadSettings.quoting) s"QUOTE '${loadSettings.quotechar}'" },
       | ESCAPE '${loadSettings.escapechar}',
       | HEADER ${if (loadSettings.skipRows == 1) "ON" else "OFF"}
       | )
    """.stripMargin
  }

  private[postgres] def getExportCmd(sql: String, exportSetting: ExportSetting) = {
    s"""
       |COPY ($sql) TO STDOUT
       |WITH (DELIMITER '${exportSetting.delimiter}',
       |FORMAT csv,
       |FORCE_QUOTE *,
       |${if (exportSetting.quoting) s"QUOTE '${exportSetting.quotechar}'" },
       |ESCAPE '${exportSetting.escapechar}',
       |HEADER ${if (exportSetting.header) "ON" else "OFF"}
       |)
       """.stripMargin
  }

}
