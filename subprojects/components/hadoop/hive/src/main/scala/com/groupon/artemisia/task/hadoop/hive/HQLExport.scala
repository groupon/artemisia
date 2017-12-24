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

package com.groupon.artemisia.task.hadoop.hive

import java.io.{File, FileOutputStream, OutputStream}
import java.net.URI
import com.typesafe.config.Config
import com.groupon.artemisia.task.database.{BasicExportSetting, DBInterface, ExportTaskHelper}
import com.groupon.artemisia.task.settings.DBConnection
import com.groupon.artemisia.task.{Task, database}

/**
  * Created by chlr on 8/2/16.
  */
class HQLExport(taskName: String, sql: String, location: URI, connectionProfile: DBConnection, exportSetting: BasicExportSetting)
    extends database.ExportToFile(taskName, sql, location, connectionProfile, exportSetting) {

  override val dbInterface: DBInterface = new HiveServerDBInterface(connectionProfile)

  override val supportedModes: Seq[String] = "default" :: Nil

  override val target: Either[OutputStream, URI] = Left(new FileOutputStream(new File(location)))

}

object HQLExport extends ExportTaskHelper {

  override val taskName = "HQLExport"

  override val desc =
    s"""
      |$taskName task is used to export SQL query results to a file.
      |The typical task $taskName configuration is as shown below.
      |Unlike ${HQLExecute.taskName} this task requires a HiveServer2 connection and cannot leverage local CLI installation.
    """.stripMargin

  override def supportedModes: Seq[String] = "default" :: Nil

  override val defaultPort: Int = 10000

  override def apply(name: String, config: Config): Task = {
    database.ExportTaskHelper.create[HQLExport](name, config)
  }
}

