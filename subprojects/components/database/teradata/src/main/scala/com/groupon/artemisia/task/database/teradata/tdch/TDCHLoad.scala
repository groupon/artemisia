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

import java.sql.SQLException
import com.typesafe.config.{Config, ConfigFactory, ConfigValue, ConfigValueFactory}
import com.groupon.artemisia.task.database.teradata._
import com.groupon.artemisia.task.database.{DBInterface, DBUtil}
import com.groupon.artemisia.task.settings.DBConnection
import com.groupon.artemisia.task.{Task, TaskLike}
import com.groupon.artemisia.util.CommandUtil
import com.groupon.artemisia.util.HoconConfigUtil.Handler

/**
  * Created by chlr on 8/26/16.
  */

class TDCHLoad(override val taskName: String, val dBConnection: DBConnection, val sourceType: String = "hdfs",
               val source: String, val targetTable: String, val method: String = "batch.insert",
               val truncate: Boolean = false, val tdchHadoopSetting: TDCHSetting)
  extends Task(taskName) {

  val dbInterface: DBInterface = DBInterfaceFactory.getInstance(dBConnection)

  protected val logStream = new TDCHLogParser(System.out)


  private val supportedMethods = Seq("batch.insert", "internal.fastload")

  private val supportedSourceTypes = Seq("hive", "hdfs")

  require(supportedMethods contains method, s"${supportedMethods.mkString(",")} ")

  require(supportedSourceTypes contains sourceType, s"source-type $sourceType is not supported. " +
    s"supported types are (${supportedSourceTypes.mkString(",")})")


  /**
    * @todo detect error table overrides with errortablename argument
    */
  override def setup(): Unit = {
    if (method == "batch.insert" && truncate) {
      dbInterface.execute(s"DELETE FROM $targetTable", printSQL = false)
    }

    else if(method == "internal.fastload") {
      try { dbInterface.execute(s"DROP TABLE ${targetTable}_ERR_1", printSQL = false) } catch { case th: SQLException => () }
      try { dbInterface.execute(s"DROP TABLE ${targetTable}_ERR_2", printSQL = false) } catch { case th: SQLException => () }
      TeraUtils.dropRecreateTable(targetTable)(dbInterface)
    }
  }

  override def work(): Config = {
    val sourceTypeConfigMap = Map (
      "hive" -> "sourcetable",
      "hdfs" -> "sourcepaths"
    )
    val settings: Map[String, String] = Map("-targettable" -> targetTable, "-method" -> method) ++ TDCHLoad.generateSourceParams(source, sourceType)
    val (command, env) = tdchHadoopSetting.commandArgs(export = false, connection = dBConnection, settings)
    CommandUtil.executeCmd(command = command, env = env, stderr = logStream, obfuscate = Seq(command.indexOf("-password")+2))
    wrapAsStats {
      ConfigFactory.empty().withValue("rows", ConfigValueFactory.fromAnyRef(logStream.rowsLoaded.toString)).root()
    }
  }

  override def teardown(): Unit = {}

}

object TDCHLoad extends TaskLike {

  override val taskName: String = "TDCHLoad"

  def generateSourceParams(source: String, sourceType: String) = {

    def hiveTableCommand(tableName: String) = {
      DBUtil.parseTableName(tableName) match {
        case (Some(database), table) => Map("-sourcedatabase" -> database, "-sourcetable" -> table)
        case (None, table) =>  Map("-sourcetable" -> table)
      }
    }

    sourceType match {
      case "hdfs" => Map("-jobtype" -> "hdfs", "-sourcepaths" -> source)
      case "hive" => Map("-jobtype" -> "hive") ++ hiveTableCommand(source)
    }
  }

  override def paramConfigDoc: Config = {
    val config = ConfigFactory parseString
      """
       |{
       |  "dsn_[1]" = connection-name
       |  source-type = "hive @defualt(hdfs) @allowed(hive, hdfs)"
       |	source =  "@required @info(hdfs path or hive table)"
       |	target-table = "database.tablename @info(teradata tablename)"
       |  truncate = "yes @default(no)"
       |	method = "@allowed(batch.insert, internal.fastload) @default(batch.insert)"
       |}
     """.
        stripMargin
  config
    .withValue(""""dsn_[2]"""",DBConnection.structure(1025).root())
    .withValue("tdch-setting",TDCHSetting.structure.root())

  }


  override def defaultConfig: Config = {
    val config = ConfigFactory parseString
      """
       |{
       |  source-type = hdfs
       |	method = "batch.insert"
       |  truncate = no
       |}
     """.stripMargin
      config.withValue("tdch-setting", TDCHSetting.defaultConfig.root())
    }

  override def fieldDefinition: Map[String, AnyRef] = Map(
    "dsn" -> "either a name of the dsn or a config-object with username/password and other credentials",
    "source-type" -> "type of the source. currently hive and hdfs are the allowed values",
    "source" -> "hdfs path or hive tablename depending on the job-type defined",
    "target-table" -> "teradata tablename",
    "method" -> "defines whether to use fastload or normal jdbc insert for loading data to teradata",
    "truncate" -> "truncate target table before load",
    "tdch-setting" -> TDCHSetting.fieldDescription
  )

  /**
    * config based constructor for task
    *
    * @param name   a name for the task
    * @param config param config node
    */
  override def apply(name: String, config: Config): Task = {
    new TDCHLoad(name,
      DBConnection.parseConnectionProfile(config.as[ConfigValue]("dsn")),
      config.as[String]("source-type"),
      config.as[String]("source"),
      config.as[String]("target-table"),
      config.as[String]("method"),
      config.as[Boolean]("truncate"),
      TDCHSetting(config.as[Config]("tdch-setting"))
    )
  }

  override val info: String = "Loads data from HDFS/Hive  into Teradata"

  override val desc: String =
    """
      | This task is used to load data to Teradata from HDFS/Hive. The hadoop task nodes directly connect to Teradata nodes (AMPs)
      | and the data from hadoop is loaded to Teradata with map-reduce jobs processing the data in hadoop and transferring
      | them over to Teradata. Preferred method of transferring large volume of data between Hadoop and Teradata.
      |
      | This requires TDCH library to be installed on the local machine. The **source-type** can be either *hive* or *hdfs.*
      | The data can loaded into Teradata in two modes.
      |
      |
      |###### batch.insert:
      |  Data is loaded via normal connections. No loader slots in Terdata are taken. This is ideal for loading few million
      |  rows of data. The major disadvantage of this mode is that this mode has zero tolerance for rejects. so even if
      |  a single record is rejected the entire job fails.
      |
      |###### fastload:
      |  Data is loaded via fastload protocol. This is ideal for loading several million records but these job occupy
      |  loader slots. This load is tolerant of some kind of rejects and certain rejects are persisted via the
      |  fastload error table such as _ET and _UV tables.
      |
      |To use hive as a target the field **tdch-settings.libjars** must be set with all the
      |
      | * Hive conf dir
      | * Hive library jars (jars in lib directory of hive)
      |
      | The **tdch-settings.libjars** field supports java style glob pattern. so for eg if hive lib path is located at
      |  `/var/path/hive/lib` and to add all the jars in the lib directory to the **tdch-settings.libjars** field one can
      |  use java style glob patterns such as `/var/path/hive/lib/*.jar`. so the most common value for **tdch-settings.libjars**
      |  will be like `libjars = ["/var/path/hive/conf", "/var/path/hive/lib/*.jar"]`.
      |
      |
      | If you want to set any specific TDCH command line argument that is not available in this task param such as
      | `targettimestampformat`, `usexviews` etc, you can use the  **tdch-settings.misc-options** field to defined these
      | arguments and values. for eg the below config object would effectively result in arguments `--foo bar --hello world`
      | added to the TDCH CLI command.
      |
      |
      |           misc-options = {
      |              foo = bar,
      |              hello = world
      |           }
      |
    """.stripMargin

  override val outputConfig = {
    val config = ConfigFactory parseString
      """
        |taskname = {
        |  __stats__ = {
        |    rows = 100
        |  }
        |}
      """.stripMargin
    Some(config)
  }


  override val outputConfigDesc =
    """
      | **taskname.__stats__.rows__** node has the number of rows loaded by the task.
      | Here it is assumed *taskname* is the name of the hypothetical export task.
    """.stripMargin

}
