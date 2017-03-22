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

package com.groupon.artemisia.task.database.teradata.tdch

import java.io.File
import java.nio.file.Paths
import com.typesafe.config.{Config, ConfigFactory}
import com.groupon.artemisia.task.ConfigurationNode
import com.groupon.artemisia.task.settings.DBConnection
import com.groupon.artemisia.util.HoconConfigUtil.Handler
import com.groupon.artemisia.util.{CommandUtil, FileSystemUtil, Util}


/**
  * Created by chlr on 8/27/16.
  */



case class TDCHSetting(tdchJar: String, hadoop: Option[File] = None, hive: Option[File] = None, numMapper: Int = 10,
                       queueName: String = "default", format: String = "textfile", textSettings: TDCHTextSetting = TDCHTextSetting(),
                       libJars: Seq[String] = Nil, miscOptions: Map[String, String] = Map()) {

  import TDCHSetting._

  hadoop foreach { x => require(x.exists(), s"hadoop binary ${x.toString} doesn't exists") }
  hive foreach { x => require(x.exists(), s"hive binary ${x.toString} doesn't exists") }
  require(new File(tdchJar).exists(), s"TDCH jar $tdchJar doesn't exists")
  require(allowedFormats contains format, s"$format is not supported. ${allowedFormats.mkString(",")} are the only format supported")

  lazy val hadoopBin = hadoop.map(_.toString).getOrElse(CommandUtil.getExecutableOrFail("hadoop"))

  lazy val hiveBin = hive.map(_.toString).getOrElse(CommandUtil.getExecutableOrFail("hive"))

  def commandArgs(export: Boolean, connection: DBConnection, otherSettings: Map[String, String]) = {

    val mainArguments = Map(
      "-url" -> s"jdbc:teradata://${connection.hostname}/database=${connection.default_database}",
      "-username" -> connection.username,
      "-password" -> connection.password,
      "-nummappers" -> numMapper.toString,
      "-fileformat" -> format
    ) ++ getLibJars ++ getTextSettings ++ otherSettings

    val env = libJars match {
      case Nil => Map[String,String]()
      case x => Map("HADOOP_CLASSPATH" -> libJars.mkString(":"))
    }

    val command = hadoopBin :: "jar" :: tdchJar ::
      (if (export) "com.teradata.connector.common.tool.ConnectorImportTool" else "com.teradata.connector.common.tool.ConnectorExportTool") ::
    s"-Dmapred.job.queue.name=$queueName" :: Nil ++ (mainArguments flatMap { case (x,y) => x :: y :: Nil })

    command -> env

  }

  private val getTextSettings = {
   format match {
      case "textfile" if textSettings.quoting => Map(
            "-separator" -> Util.unicodeCode(textSettings.delimiter),
            "-escapedby" -> textSettings.escapedBy.toString,
            "-enclosedby" -> textSettings.quoteChar.toString
          ) ++
        textSettings.nullString.map(x => Map("-nullstring" -> x)).getOrElse(Map[String, String]())
      case "textfile" if !textSettings.quoting => Map(
        "-separator" -> Util.unicodeCode(textSettings.delimiter)
      ) ++ textSettings.nullString.map(x => Map("-nullstring" -> x)).getOrElse(Map[String, String]())
      case _ => Map[String, String]()
    }
  }

  private def getLibJars = {
    libJars match {
      case Nil => Map[String, String]()
      case x =>  Map("-libjars" -> libJars.mkString(","))
    }
  }

}


object TDCHSetting extends ConfigurationNode[TDCHSetting] {

  private val allowedFormats = Seq("textfile", "avrofile", "rcfile", "orcfile", "sequencefile", "parquet")


  override val defaultConfig: Config = {
    val config = ConfigFactory parseString
      s"""
       | {
       |   num-mappers = 10
       |   queue-name = default
       |   format = textfile
       |   lib-jars = []
       |   misc-options = {}
       | }
     """.stripMargin
    config.withValue("text-setting", TDCHTextSetting.defaultConfig.root())
    }


  override def apply(config: Config): TDCHSetting= {
    TDCHSetting(
      config.as[String]("tdch-jar"),
      config.getAs[String]("hadoop").map(x => new File(x)),
      config.getAs[String]("hive").map(x => new File(x)),
      config.as[Int]("num-mappers"),
      config.as[String]("queue-name"),
      config.as[String]("format"),
      TDCHTextSetting(config.as[Config]("text-setting")),
      processLibJarsField(config.as[List[String]]("lib-jars")),
      config.asMap[String]("misc-options")
    )
  }

  override val structure: Config = {
    val config = ConfigFactory parseString
      s"""
       | {
       |   tdch-jar = "/path/teradata-connector.jar"
       |   hadoop = "/usr/local/bin/hadoop @optional"
       |   hive = "/usr/local/bin/hive @optional"
       |   num-mappers = "5 @default(10)"
       |   queue-name = "public @default(default)"
       |   format = "avrofile @default(default)"
       |   libjars = [
       |     "/path/hive/conf"
       |     "/path/hive/libs/*.jars"
       |   ]
       |   misc-options = {
       |     foo1 = bar1
       |     foo2 = bar2
       |   }
       | }
     """.stripMargin
    config.withValue("text-setting", TDCHTextSetting.structure.root())
    }


  override val fieldDescription: Map[String, Any] = Map(
    "tdch-jar" -> "path to tdch jar file",
    "hadoop" -> "optional path to the hadoop binary. If not specified the binary will be searched in the PATH variable",
    "hive" -> "optional path to the hive binary. If not specified the binary will be searched in the PATH variable",
    "num-mappers" -> "number of mappers to be used in the MR job",
    "queue-name" -> "scheduler queue where the MR job is submitted",
    "format" -> ("format of the file. Following are the allowed values" -> allowedFormats),
    "lib-jars" -> ("list of files and directories that will be added to libjars argument and set in HADOOP_CLASSPATH environment variable." +
      "Usually the hive conf and hive lib jars are added here. The path accept java glob pattern"),
    "text-setting" -> TDCHTextSetting.fieldDescription,
    "misc-options" -> "other TDHC arguments to be appended must be defined in this Config object"
  )

  /**
    * expand list of paths which uses java type glob pattern.
    *
    * @param paths
    * @return
    */
  private def processLibJarsField(paths: Seq[String]): Seq[String] = {
    paths.map(Paths.get(_))
      .flatMap(x => FileSystemUtil.expandPathToFiles(x))
      .map(_.toString)
  }
}


