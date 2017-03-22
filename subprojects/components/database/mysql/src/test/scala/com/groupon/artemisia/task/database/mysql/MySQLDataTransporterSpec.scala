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

import java.nio.file.Paths
import java.sql.Connection
import com.groupon.artemisia.TestSpec
import com.groupon.artemisia.task.database.{BasicExportSetting, BasicLoadSetting, DBInterface}

  /**
 * Created by chlr on 6/14/16.
 */
class MySQLDataTransporterSpec extends TestSpec {

  "MYSQLDataTransporterSpec" must "throw unsupported exception for mysql bulk export" in {
    val sql = "select * from table"
    val exportSetting = BasicExportSetting()
    val transporter = new DBInterface with MySQLDataTransporter {
      override def getNewConnection: Connection = ???
    }
    val ex = intercept[UnsupportedOperationException]{
      transporter.export(sql, Paths.get("dummy_file").toUri, exportSetting)
    }
    ex.getMessage must be ("bulk export utility is not supported")
  }

  it must "generate load command" in {
    val loadSetting = BasicLoadSetting(skipRows = 1, delimiter = '\u0001')
    val tableName = "dbname.tablename"
    val location = Paths.get("dummy_file").toUri
    var command = MySQLDataTransporter.getLoadSQL(tableName, location, loadSetting)
    command = command.replace("\n","").replace("\r", "").replaceAll("[ ]+"," ")
    command must include (s"LOAD DATA LOCAL INFILE '${location.getPath}' INTO TABLE $tableName")
    command must include (s"FIELDS TERMINATED BY '${loadSetting.delimiter}'")
  }

}
