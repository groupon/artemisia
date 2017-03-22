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

package com.groupon.artemisia.task.hadoop

import com.typesafe.config.Config
import com.groupon.artemisia.task.database.{BasicExportSetting, ExportTaskHelper}
import com.groupon.artemisia.task.settings.{DBConnection, ExportSetting}
import com.groupon.artemisia.util.HoconConfigUtil.Handler
import scala.reflect.ClassTag

/**
  * Created by chlr on 7/27/16.
  */

trait ExportToHDFSHelper extends ExportTaskHelper {

  override val taskName: String = "ExportToHDFS"

  override def paramConfigDoc = super.paramConfigDoc
                      .withValue("hdfs", HDFSWriteSetting.structure.root())
                      .withoutPath("location")

  override def defaultConfig: Config = super.defaultConfig.withValue("hdfs", HDFSWriteSetting.defaultConfig.root())

  override def fieldDefinition: Map[String, AnyRef] = super.fieldDefinition - "location" +
                                            ("hdfs" -> HDFSWriteSetting.fieldDescription)

  override val info: String = "Export database resultset to HDFS"

  override val desc: String = s"$taskName is used to export SQL query results to a HDFS file"

}

object ExportToHDFSHelper {

  def create[T <: ExportToHDFS: ClassTag](name: String, config: Config): T = {
    val exportSettings = BasicExportSetting(config.as[Config]("export"))
    val connectionProfile = DBConnection.parseConnectionProfile(config.getValue("dsn"))
    val hdfs = HDFSWriteSetting(config.as[Config]("hdfs"))
    val sql: String = config.asInlineOrFile("sql")
    implicitly[ClassTag[T]].runtimeClass.getConstructor(classOf[String], classOf[String], classOf[HDFSWriteSetting],
      classOf[DBConnection], classOf[ExportSetting])
      .newInstance(name, sql, hdfs, connectionProfile, exportSettings).asInstanceOf[T]
  }

}