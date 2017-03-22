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

package com.groupon.artemisia.task.database.mysql

import java.io.{InputStream, OutputStream}
import java.net.URI
import com.groupon.artemisia.core.AppLogger
import com.groupon.artemisia.task.database.{DBExporter, DBImporter, DBInterface}
import com.groupon.artemisia.task.settings.{ExportSetting, LoadSetting}


/**
 * Created by chlr on 5/1/16.
 */

trait MySQLDataTransporter extends DBExporter with DBImporter {

  self: DBInterface =>

  override def load(tableName: String, location: URI, loadSettings: LoadSetting) = {
    AppLogger debug "error file is ignored in this mode"
    this.execute(MySQLDataTransporter.getLoadSQL(tableName, location, loadSettings)) -> 0L
  }

  override def load(sql: String, inputStream: InputStream, loadSetting: LoadSetting) = {
    throw new UnsupportedOperationException("bulk load utility is not supported")
  }

  override def export(sql: String, outputStream: OutputStream, exportSetting: ExportSetting) = {
    throw new UnsupportedOperationException("bulk export utility is not supported")
  }

  override def export(sql: String, location: URI, exportSetting: ExportSetting) = {
    throw new UnsupportedOperationException("bulk export utility is not supported")
  }
}

object MySQLDataTransporter {

  def getLoadSQL(tableName: String, location: URI ,loadSettings: LoadSetting) = {
    s"""
       | LOAD DATA LOCAL INFILE '${location.getPath}'
       | INTO TABLE $tableName FIELDS TERMINATED BY '${loadSettings.delimiter}' ${if (loadSettings.quoting) s"OPTIONALLY ENCLOSED BY '${loadSettings.quotechar}'"  else ""}
       | ESCAPED BY '${if (loadSettings.escapechar == '\\') "\\\\" else loadSettings.escapechar }'
       | IGNORE ${loadSettings.skipRows} LINES
     """.stripMargin
  }

}
