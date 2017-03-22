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
import com.groupon.artemisia.task.database.{BasicLoadSetting, LoadTaskHelper}
import com.groupon.artemisia.task.settings.{DBConnection, LoadSetting}
import com.groupon.artemisia.util.HoconConfigUtil.Handler
import scala.reflect.ClassTag

/**
  * Created by chlr on 7/27/16.
  */


trait LoadFromHDFSHelper extends LoadTaskHelper {

  override val taskName: String = "LoadFromHDFS"

  def defaultPort: Int

  override def paramConfigDoc = super.paramConfigDoc.withValue("hdfs",HDFSReadSetting.structure.root())

  override def defaultConfig =  super.defaultConfig
                     .withValue("hdfs", HDFSReadSetting.defaultConfig.root())

  override def fieldDefinition: Map[String, AnyRef] = super.fieldDefinition - "location" + ("hdfs" -> HDFSReadSetting.fieldDescription)

  override val info: String = "Load Table from HDFS"

  override val desc: String = s"$taskName can be used to load file(s) from a HDFS path"

}

object LoadFromHDFSHelper {

  def create[T <: LoadFromHDFS: ClassTag](name: String, config: Config): T = {
    val loadSettings = BasicLoadSetting(config.as[Config]("load"))
    val connectionProfile = DBConnection.parseConnectionProfile(config.getValue("dsn"))
    val tableName = config.as[String]("destination-table")
    val hdfsReadSetting = HDFSReadSetting(config.as[Config]("hdfs"))
    implicitly[ClassTag[T]].runtimeClass.getConstructor(classOf[String], classOf[String], classOf[HDFSReadSetting], classOf[DBConnection],
      classOf[LoadSetting]).newInstance(name, tableName, hdfsReadSetting, connectionProfile, loadSettings).asInstanceOf[T]
  }

}
