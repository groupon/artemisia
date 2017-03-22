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

package com.groupon.artemisia.task.database

import java.io.File
import java.net.URI
import com.typesafe.config.{Config, ConfigFactory}
import com.groupon.artemisia.task.settings.DBConnection
import com.groupon.artemisia.task.{Task, TaskLike}
import com.groupon.artemisia.util.HoconConfigUtil.Handler
import scala.reflect.ClassTag

/**
  * Created by chlr on 7/27/16.
  */

trait LoadTaskHelper extends TaskLike {

  /**
    * task name
    */
  val taskName = "SQLLoad"

  /**
    * brief description of the task
    */
  val info = "load a file into a table"

  /**
    * brief description of task
    */
  val desc: String =
    s"""
       |$taskName task is used to load content into a table typically from a file.
       |the configuration object for this task is as shown below.
    """.stripMargin

  /**
    * undefined default port
    */
  def defaultPort: Int

  def supportedModes: Seq[String]

  override def fieldDefinition = Map(
    "dsn" -> "either a name of the dsn or a config-object with username/password and other credentials",
    "destination-table" -> "destination table to load",
    "location" -> "path pointing to the source file",
    "load" -> BasicLoadSetting.fieldDescription
  )

  override def defaultConfig: Config = ConfigFactory.empty()
                                  .withValue("load", BasicLoadSetting.defaultConfig.root())

  override def apply(name: String, config: Config): Task = ???

  override def paramConfigDoc: Config = {
    val config = ConfigFactory parseString
      s"""
         | "dsn_[1]" = connection-name
         |  destination-table = "dummy_table @required"
         |  location = /var/tmp/file.txt
     """.stripMargin
    config
      .withValue("load",BasicLoadSetting.structure.root())
      .withValue(""""dsn_[2]"""",DBConnection.structure(defaultPort).root())
  }

  override val outputConfig: Option[Config] = {
    val config = ConfigFactory parseString
      """
        | taskname = {
        |  __stats__ = {
        |    loaded = 100
        |    rejected = 2
        |  }
        |}
      """.stripMargin
    Some(config)
  }


  override val outputConfigDesc =
    """
      | **taskname.__stats__.loaded** and **taskname.__stats__.rejected** keys have the numbers of records
      | loaded and the number of records rejected respectively.
    """.stripMargin

}

object LoadTaskHelper {

  /**
    * factory method to build task objects that are sub-types of LoadTaskHelper
    *
    * @param name name of the task
    * @param config configuration node with task settings.
    * @tparam T concrete type of LoadTaskHelper to be constructed
    * @return instance of type T.
    */
  def create[T <: LoadFromFile : ClassTag](name: String, config: Config): LoadFromFile = {
    val connectionProfile = DBConnection.parseConnectionProfile(config.getValue("dsn"))
    val destinationTable = config.as[String]("destination-table")
    val loadSettings = BasicLoadSetting(config.as[Config]("load"))
    val location = new File(config.as[String]("location")).toURI
    implicitly[ClassTag[T]].runtimeClass.asSubclass(classOf[LoadFromFile]).getConstructor(classOf[String],
      classOf[String], classOf[URI], classOf[DBConnection], classOf[BasicLoadSetting]).newInstance(name, destinationTable,
      location ,connectionProfile, loadSettings)
  }

}
