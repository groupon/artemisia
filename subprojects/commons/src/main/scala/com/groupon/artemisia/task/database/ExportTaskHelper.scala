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
import com.groupon.artemisia.task.TaskLike
import com.groupon.artemisia.task.settings.DBConnection
import com.groupon.artemisia.util.HoconConfigUtil.Handler
import scala.reflect.ClassTag

/**
  * Created by chlr on 7/27/16.
  */


trait ExportTaskHelper extends TaskLike {

  val taskName = "SQLExport"

  val info = "export query results to a file"

  val desc: String =
    s"""
       |$taskName task is used to export SQL query results to a file.
       |The typical task $taskName configuration is as shown below
     """.stripMargin

  def supportedModes: Seq[String]

  override def defaultConfig: Config = ConfigFactory.empty().withValue("export",BasicExportSetting.defaultConfig.root())

  val defaultPort: Int

  override def paramConfigDoc = {
    val config = ConfigFactory parseString
      s"""
         |{
         |   "dsn_[1]" = connection-name
         |   sql = "SELECT * FROM TABLE @optional(either sql or sqlfile key is required)"
         |   sql-file = "run_queries.sql @info(path to the file) @optional(either sql or sqlfile key is required)"
         |   location = "/var/tmp/file.txt"
         |}
     """.stripMargin
    config
      .withValue(""""dsn_[2]"""",DBConnection.structure(defaultPort).root())
      .withValue("export",BasicExportSetting.structure.root())
  }

  override def fieldDefinition: Map[String, AnyRef] = Map(
    "dsn" -> "either a name of the dsn or a config-object with username/password and other credentials",
    "export" -> BasicExportSetting.fieldDescription.+("mode" -> ("modes of export. supported modes are" -> supportedModes)),
    "location" -> "path to the target file"
  )

  override val outputConfig: Option[Config] = {
    val config = ConfigFactory parseString
      s"""
         |
         | taskname = {
         |   __stats__ = {
         |      rows = 100
         |    }
         |  }
       """.stripMargin
    Some(config)
  }

  override val outputConfigDesc: String =
    """
      | **taskname.__stats__.rows__** node has the number of rows exported by the task.
      | Here it is assumed *taskname* is the name of the hypothetical export task.
    """.stripMargin


}

object ExportTaskHelper {

  /**
    *
    * @param name task name
    * @param config task configuration
    */
  def create[T <: ExportToFile : ClassTag](name: String, config: Config): ExportToFile = {
    val exportSettings = BasicExportSetting(config.as[Config]("export"))
    val connectionProfile = DBConnection.parseConnectionProfile(config.getValue("dsn"))
    val location = new File(config.as[String]("location")).toURI
    val sql = config.asInlineOrFile("sql")
    implicitly[ClassTag[T]].runtimeClass.getConstructor(classOf[String], classOf[String], classOf[URI], classOf[DBConnection],
      classOf[BasicExportSetting]).newInstance(name, sql, location, connectionProfile, exportSettings).asInstanceOf[ExportToFile]
  }

}
