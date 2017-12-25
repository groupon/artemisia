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

import com.typesafe.config.Config
import com.groupon.artemisia.task.database.{DBInterface, ExportTaskHelper}
import com.groupon.artemisia.task.hadoop.{ExportToHDFSHelper, HDFSWriteSetting}
import com.groupon.artemisia.task.settings.{DBConnection, ExportSetting}
import com.groupon.artemisia.task.{Task, hadoop}

/**
  * Created by chlr on 7/22/16.
  */

class ExportToHDFS(override val taskName: String, override val sql: String, override  val hdfsWriteSetting: HDFSWriteSetting,
                   override val connectionProfile: DBConnection, override val exportSetting: ExportSetting)
    extends hadoop.ExportToHDFS(taskName, sql, hdfsWriteSetting, connectionProfile, exportSetting) {

   override val dbInterface: DBInterface = DBInterfaceFactory.getInstance(connectionProfile, exportSetting.mode)

   override val supportedModes: Seq[String] = ExportToHDFS.supportedModes
}

object ExportToHDFS extends ExportToHDFSHelper {

  override def apply(name: String, config: Config, reference: Config): Task = ExportTaskHelper
    .create[ExportToHDFS](name, config, reference)

  override def supportedModes: Seq[String] = "default" :: "fastexport" :: Nil

  override val defaultPort: Int = 1025

}
