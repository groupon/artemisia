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

package com.groupon.artemisia.task.database.teradata.tpt

import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import com.groupon.artemisia.task.TaskLike
import com.groupon.artemisia.task.settings.DBConnection

/**
 * Created by chlr on 9/12/16.
 */


trait TPTTaskHelper extends TaskLike {

  override def paramConfigDoc =  ConfigFactory.empty()
    .withValue("load",TPTLoadSetting.structure.root())
    .withValue("destination-table",ConfigValueFactory.fromAnyRef("target_table"))
    .withValue(""""dsn_[1]"""",ConfigValueFactory.fromAnyRef("my_conn @info(dsn name defined in connection node)"))
    .withValue(""""dsn_[2]"""",DBConnection.structure(1025).root())

  override def defaultConfig: Config = ConfigFactory.empty()
    .withValue("load", TPTLoadSetting.defaultConfig.root())


  override def fieldDefinition = Map(
    "dsn" -> "either a name of the dsn or a config-object with username/password and other credentials",
    "destination-table" -> "destination table to load",
    "location" -> "path pointing to the source file",
    "load" -> TPTLoadSetting.fieldDescription
  )

}

object TPTTaskHelper {

  def supportedModes: Seq[String] = Seq("fastload", "default", "auto")

}