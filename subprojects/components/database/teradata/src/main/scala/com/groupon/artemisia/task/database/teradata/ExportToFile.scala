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


import java.io.{File, FileOutputStream, OutputStream}
import java.net.URI

import com.groupon.artemisia.task.database
import com.groupon.artemisia.task.database.{BasicExportSetting, DBInterface, ExportTaskHelper}
import com.groupon.artemisia.task.settings.DBConnection
import com.typesafe.config.Config

/**
  * Created by chlr on 6/10/16.
  */


class ExportToFile(override val name: String, override val sql: String, location: URI, override val connectionProfile: DBConnection
                   ,override val exportSetting: BasicExportSetting)
  extends database.ExportToFile(name: String, sql: String, location, connectionProfile: DBConnection ,exportSetting) {

  override val dbInterface: DBInterface = DBInterfaceFactory.getInstance(connectionProfile, mode = exportSetting.mode)

  override val target: Either[OutputStream, URI] = Left(new FileOutputStream(new File(location)))

  override def setup(): Unit = {
    assert(location.getScheme == "file", "LocalFileSystem is the only supported destination")
  }

  override val supportedModes: Seq[String] = ExportToFile.supportedModes

}


object ExportToFile extends ExportTaskHelper {

  override def apply(name: String,config: Config, reference: Config): database.ExportToFile = ExportTaskHelper
    .create[ExportToFile](name, config, reference)

  override val defaultPort: Int = 1025

  override def supportedModes: Seq[String] = "default" :: "fastexport" :: Nil

}



