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

import java.net.URI
import java.nio.file.Paths
import com.typesafe.config.{Config, ConfigFactory}
import com.groupon.artemisia.task.database
import com.groupon.artemisia.task.database.{DBInterface, LoadTaskHelper}
import com.groupon.artemisia.task.settings.DBConnection
import com.groupon.artemisia.util.HoconConfigUtil.Handler
import com.groupon.artemisia.util.{FileSystemUtil, Util}

/**
  * Created by chlr on 6/26/16.
  */

/**
  * Task to load a teradata table from local filesystem. This is abstract class which cannot be directly constructed.
  * use the apply method to construct the task.
  *
  * @param taskName taskname
  * @param tableName destination table to be loaded
  * @param location path to load from
  * @param connectionProfile database connection profile
  * @param loadSetting load setting details
  */
abstract class LoadFromFile(override val taskName: String, override val tableName: String,
                            location: URI, override val connectionProfile: DBConnection, override val loadSetting: TeraLoadSetting)
  extends database.LoadFromFile(taskName, tableName, location, connectionProfile, loadSetting) {

  override val supportedModes = LoadFromFile.supportedModes

  override implicit val dbInterface: DBInterface = DBInterfaceFactory.getInstance(connectionProfile, loadSetting.mode)

  /**
    * No operations are done in this phase
    */
  override def setup(): Unit = {
    if (loadSetting.truncate) {
      TeraUtils.truncateElseDrop(tableName)
    }
  }

  /**
    * No operations are done in this phase
    */
  override def teardown(): Unit = {}

}

object LoadFromFile extends LoadTaskHelper {


  override val defaultConfig = ConfigFactory.empty().withValue("load", TeraLoadSetting.defaultConfig.root())

  override def apply(name: String, config: Config, reference: Config) = {
    val connectionProfile = DBConnection.parseConnectionProfile(config.getValue("dsn"))
    val destinationTable = config.as[String]("destination-table")
    val loadSettings = TeraLoadSetting(config.as[Config]("load"))
    val location = new URI(config.as[String]("location"))
    LoadFromFile(name, destinationTable, location ,connectionProfile, loadSettings)
  }

  def apply(taskName: String = Util.getUUID, tableName: String, location: URI, connectionProfile: DBConnection,
            loadSetting: TeraLoadSetting) = {
    lazy val (inputStream, loadSize) = FileSystemUtil.getPathForLoad(Paths.get(location.getPath))
    val normalizedLoadSettings = TeraUtils.autoTuneLoadSettings(loadSize,loadSetting)
    new LoadFromFile(taskName, tableName, location ,connectionProfile, normalizedLoadSettings) {
      override lazy val source = Left(inputStream)
    }
  }

  override def defaultPort = 1025

  override val paramConfigDoc =  super.paramConfigDoc.withValue("load",TeraLoadSetting.structure.root())

  override val fieldDefinition = super.fieldDefinition ++ Map("load" -> TeraLoadSetting.fieldDescription )

  override def supportedModes: Seq[String] = "fastload" :: "default" :: "auto" :: Nil

}

