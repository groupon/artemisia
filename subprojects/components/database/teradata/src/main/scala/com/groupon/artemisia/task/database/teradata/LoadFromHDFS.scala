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

import java.io.InputStream
import java.net.URI
import com.typesafe.config.Config
import com.groupon.artemisia.task.database.DBInterface
import com.groupon.artemisia.task.hadoop.{HDFSReadSetting, LoadFromHDFSHelper}
import com.groupon.artemisia.task.settings.DBConnection
import com.groupon.artemisia.task.{Task, hadoop}
import com.groupon.artemisia.util.HoconConfigUtil.Handler


/**
  * Task to load a teradata table from hdfs. This is abstract class which cannot be directly constructed.
  * use the apply method to construct the task.
  *
  * @param taskName task name
  * @param tableName target table
  * @param hdfsReadSetting HDFS read setting
  * @param connectionProfile database connection profile
  * @param loadSetting load setting
  */
abstract class  LoadFromHDFS(override val taskName: String, override val tableName: String, override val hdfsReadSetting: HDFSReadSetting,
                   override val connectionProfile: DBConnection, override val loadSetting: TeraLoadSetting) extends
  hadoop.LoadFromHDFS(taskName, tableName, hdfsReadSetting, connectionProfile, loadSetting) {

  override implicit val dbInterface: DBInterface = DBInterfaceFactory.getInstance(connectionProfile, loadSetting.mode)

  override val supportedModes: Seq[String] = LoadFromHDFS.supportedModes

  override val source: Either[InputStream, URI]

  /**
    * No operations are done in this phase
    */
  override def setup(): Unit = {
    if (loadSetting.truncate) {
      TeraUtils.truncateElseDrop(tableName)
    }
  }

}

object LoadFromHDFS extends LoadFromHDFSHelper {

  override def defaultPort: Int = 1025

  override def apply(name: String, config: Config, reference: Config): Task = {
    val loadSetting = TeraLoadSetting(config.as[Config]("load"))
    val connectionProfile = DBConnection.parseConnectionProfile(config.getValue("dsn"))
    val tableName = config.as[String]("destination-table")
    val hdfsReadSetting = HDFSReadSetting(config.as[Config]("hdfs"))
    LoadFromHDFS(name, tableName, hdfsReadSetting, connectionProfile, loadSetting)
  }

  def apply(taskName: String, tableName: String, hdfsReadSetting: HDFSReadSetting,connectionProfile: DBConnection,
  loadSetting: TeraLoadSetting) = {
    lazy val (hdfsStream: InputStream, loadSize: Long) = hadoop.LoadFromHDFS.getPathForLoad(hdfsReadSetting)
   val normalizedLoadSetting = TeraUtils.autoTuneLoadSettings(loadSize, loadSetting)
    new LoadFromHDFS(taskName, tableName, hdfsReadSetting, connectionProfile, normalizedLoadSetting) {
      override lazy val source = Left(hdfsStream)
    }
  }

  override def paramConfigDoc: Config = super.paramConfigDoc
                                    .withValue("load", TeraLoadSetting.structure.root())

  override val defaultConfig: Config = super.defaultConfig
                                    .withValue("load", TeraLoadSetting.defaultConfig.root())

  override val fieldDefinition: Map[String, AnyRef] = super.fieldDefinition +
                                    ("load" -> TeraLoadSetting.fieldDescription)

  override def supportedModes = "default" :: "fastload" :: "auto" :: Nil

}
