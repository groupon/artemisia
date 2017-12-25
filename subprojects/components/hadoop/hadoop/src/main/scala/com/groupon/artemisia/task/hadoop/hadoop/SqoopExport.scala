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

package com.groupon.artemisia.task.hadoop.hadoop

import com.typesafe.config.{Config, ConfigFactory, ConfigList, ConfigValueFactory}
import com.groupon.artemisia.task.{Task, TaskLike}
import com.groupon.artemisia.task.database.DBUtil
import com.groupon.artemisia.task.settings.DBConnection
import com.groupon.artemisia.util.CommandUtil._
import scala.collection.JavaConverters._
import com.groupon.artemisia.util.HoconConfigUtil.Handler

/**
  * Created by chlr on 10/12/16.
  */

/**
  *
  * @param taskName name of the task
  * @param connection a tuple of database type and instance of DBConnection
  * @param sqoopBin optional path to the sqoop binary file. eg Some(/usr/local/bin/sqoop)
  * @param hdfsBin optional path to the sqoop binary file. eg Some(/usr/local/bin/hdfs)
  * @param sqoopExportSetting export settings for the sqoop job
  * @param sqoopOption sqoop settings object
  * @param miscSqoopOptions  miscellaneous sqoop options
  */
class SqoopExport(override val taskName: String,
                  val connection: (String, DBConnection),
                  val sqoopBin: Option[String],
                  val hdfsBin: Option[String],
                  val sqoopExportSetting: SqoopExportSetting,
                  val sqoopOption: SqoopSetting,
                  val miscSqoopOptions: Seq[Either[(String, String), String]]
                 ) extends Task(taskName) {

  /**
    * override this to implement the setup phase
    *
    * This method should have setup phase of the task, which may include actions like
    * - creating database connections
    * - generating relevant files
    */
  override def setup(): Unit = {}


  protected val logParser = new SqoopExportLogParser(System.out)


  /**
    * command to remove target hdfs path
    *
    * @return
    */
  protected def hdfsTruncateCmd = (hdfsBin ++ getExecutablePath("hdfs") ++ getExecutablePath("hadoop")).headOption match {
    case Some(x) => Seq(x, "dfs", "-rm", "-f", "-r", sqoopOption.target)
    case None => throw new RuntimeException("hdfs/hadoop binary not found in path. " +
      "please specify location in hdfs-bin field")
  }

  /**
    * override this to implement the work phase
    *
    * this is where the actual work of the task is done, such as
    * - executing query
    * - launching subprocess
    *
    * @return any output of the work phase be encoded as a HOCON Config object.
    */
  override def work(): Config = {
    if (sqoopOption.truncate && sqoopOption.targetType == "hdfs") {
     executeCmd(hdfsTruncateCmd)
    }
    val executable = sqoopBin match {
      case Some(x) => x
      case None => getExecutableOrFail("sqoop")
    }
    executeCmd(sqoopCommand(executable), stderr = logParser)
    logParser.close()
    wrapAsStats {
      ConfigFactory.empty
        .withValue("input-rows", ConfigValueFactory.fromAnyRef(logParser.inputRecords))
        .withValue("output-rows", ConfigValueFactory.fromAnyRef(logParser.outputRecords))
        .root()
    }
  }


  private[hadoop] def sqoopCommand(executable: String): Seq[String] = {
   val (kwargs, args)   =  ( Seq(Right(s"-Dmapreduce.job.name=$taskName"))
     ++ connectionArgs ++ sqoopExportSetting.args ++ sqoopOption.args) partition {
      case Left((x,y)) => true
      case Right(x) => false
    }

    val mergedMap = kwargs.collect{case Left((x,y)) => x -> y}.toMap ++
      miscSqoopOptions.collect{case Left((x,y)) => x -> y}.toMap

    val sortedList = (mergedMap.toSeq ++ (args ++ miscSqoopOptions).collect{case Right(x) => x})
        .map{
          case (x: (String, String) @unchecked) if x._1.matches("^-[\\w]{2,}") => x -> 0
          case (x: String) if x.startsWith("-D") || x.matches("^-[\\w]{2,}") => x -> 0
          case x => x -> 1
        }.sortWith((x, y) => x._2 < y._2)
         .map(x => x._1)

       Seq(executable, "import") ++ sortedList.flatMap{
          case (x: String, y: String) => x :: y :: Nil
          case x: String => x :: Nil
        }
  }


  def connectionArgs = {
    val (dbType, profile) = connection
    DBUtil.dbConnectionUrl(dbType, profile)
        .map{ case (driver, connect) => Seq(Left("--driver" -> driver), Left("--connect" -> connect)) }
        .getOrElse(Nil) ++
      Seq(Left("--username" -> profile.username), Left("--password" -> profile.password))
  }

  /**
    * override this to implement the teardown phase
    *
    * this is where you deallocate any resource you have acquired in setup phase.
    */
  override def teardown(): Unit = {}

}

object SqoopExport extends TaskLike {

  override val taskName  = "SqoopExport"

  /**
    * Sequence of config keys and their associated values
    */
  override def paramConfigDoc = ConfigFactory.empty()
    .withValue("connection.type", ConfigValueFactory.fromAnyRef(""))
    .withValue(""""connection.dsn_[1]"""", ConfigValueFactory.fromAnyRef("connection-name"))
    .withValue(""""connection.dsn_[2]"""", DBConnection.structure(-1).withoutPath("port")
      .withValue("port", ConfigValueFactory.fromAnyRef("100 @required")).root)
    .withValue("sqoop-bin", ConfigValueFactory.fromAnyRef("/var/path/bin/sqoop @optional"))
    .withValue("hdfs-bin", ConfigValueFactory.fromAnyRef("/var/path/bin/hdfs @optional"))
    .withValue("export", SqoopExportSetting.structure.root)
    .withValue("sqoop-setting", SqoopSetting.structure.root)
    .withValue("misc-setting", ConfigValueFactory.fromIterable(
      Seq("-Dmapreduce.map.speculative=true",Map("direct-split-size" -> "10000").asJava).asJava))

  /**
    *
    */
  override def defaultConfig: Config = ConfigFactory.empty()
      .withValue("sqoop-setting", SqoopSetting.defaultConfig.root())
      .withValue("export", SqoopExportSetting.defaultConfig.root())
      .withValue("misc-setting", ConfigValueFactory.fromIterable(Nil.asJava))

  /**
    * definition of the fields in task param config
    */
   override def fieldDefinition: Map[String, AnyRef] = Map(
    "connection" -> ("connection info for the source database is provided here. it has two fields" -> Seq(
        "type" -> "type of the database source. for eg: mysql, postgres etc.",
        "dsn" -> "connection information to connect to the database. DSN name or inline connection object"
      )),
    "sqoop-bin" -> "optional path to the Sqoop binary. This field is optional and not required if the sqoop binary is already available in the PATH variable",
    "hdfs-bin" -> "hdfs binary to use. This field is optional and not required if the hdfs or hadoop binary is already available in the PATH variable.",
    "export" -> SqoopExportSetting.fieldDescription ,
    "sqoop-setting" -> SqoopSetting.fieldDescription ,
    "misc-setting" -> "any miscellenous options to be applied to the sqoop command."
   )

  /**
    * config based constructor for task
    *
    * @param name   a name for the task
    * @param config param config node
    */
  override def apply(name: String, config: Config, reference: Config): Task = {
    new SqoopExport(
      name,
      config.as[String]("connection.type").toLowerCase ->
        DBConnection.parseConnectionProfile(config.getValue("connection.dsn")),
      config.getAs[String]("sqoop-bin"),
      config.getAs[String]("hdfs-bin"),
      SqoopExportSetting(config.as[Config]("export")),
      SqoopSetting(config.as[Config]("sqoop-setting")),
      parseMiscOptions(config.getList("misc-setting"))
    )
  }


  /**
    * parse misc-setting field. this field is a list field.
    * see Sqoop task documentation regarding how this field has to be parsed.
    * @param list
    * @return
    */
  private[SqoopExport] def parseMiscOptions(list: ConfigList) = {
    list.unwrapped().asScala map {
      case x: java.util.Map[AnyRef, AnyRef] @unchecked =>
        x.asScala.head match {
          case (key: AnyRef, value: AnyRef) if key.toString.startsWith("-") => Left(key.toString -> value.toString)
          case (key: AnyRef, value: AnyRef) => Left(s"--${key.toString}" -> value.toString)
        }
      case x: AnyRef if x.toString.startsWith("-") => Right(x.toString)
      case x: AnyRef => Right(s"--$x")
    }
  }

  override val outputConfig = {
    Some {
      ConfigFactory parseString
        s"""
           |taskname = {
           |	__stats__ = {
           |			input-rows = 100
           |      output-rows = 100
           |	}
           |}
     """.stripMargin
    }
  }

  override val info: String = "Launch Sqoop job to export data from database to HDFS filesystem."

  override val desc: String =
    """
      | This task helps you to export data from any RDBMS systems to HDFS using Sqoop. This task expects the Sqoop
      | application to be installed locally your machine. This task composes the sqoop command to launch using the
      | params config. You can optionally specify  **sqoop-bin** field to provide the path to the sqoop binary file.
      | make sure this is plain *sqoop* file and not *sqoop-import* file. This Sqoop binary file can be inferred from the
      | PATH system variable if found. This field is required only if the Sqoop file is missing in the PATH variable.
      | similarly to **sqoop-bin**, **hdfs-bin** field can be used to refer to the hdfs binary file. **hdfs-bin** field
      | is optional as well and required only if the *hdfs* file is not found in the PATH variable. The path is searched
      | for two files hdfs or hadoop and uses either of them.
      |
      | The Sqoop export supports quoting via **export.quoting** field. but this field is applicable only when the
      | format mode is set to *text*. The field **misc-setting** lets you set any miscellaneous arguments you want the
      | Sqoop job to take, but was not possible to set with other provided arguments. for eg if you had to set both
      | *direct-split-size* and *append* arguments to the Sqoop job you there is no direct way to set it since such fields
      | are not supported by this task. One way this could be achieved is by using the **misc-setting**. The **misc-setting**
      | field takes a list of values. Each item in the list can either be a string or an object with a single key value.
      | so as said earlier if we wanted to add *direct-split-size* and *append* argument to the sqoop job our misc-options
      | field would look like.
      |
      |        misc-setting = [ { direct-split-size = 1000 }, append ]
      |
      | The above config would translate to `--direct-split-size 1000 --append` arguments being added to the command line.
      | the misc setting can be used to override setting generated by other fields. for eg **sqoop-setting.num-mappers** field
      | would translate to --num-mappers command. so now with the below config the effective num-mappers will be 10 and not 5.
      |
      |             sqoop-setting = {
      |               num-mappers = 5
      |             }
      |             misc-setting = [
      |               { num-mappers = 10 },
      |               -Djob.mapred.name=my_custom_job_name
      |             ]
      |
      | Please note that you can only override key value arguments like *--num-mappers 10* with new value. you cannot remove
      | any arguments that was generated by other settings like *sqoop-setting*, *connection*, *export* etc using *misc-setting*.
      | Make sure minus D arguments (-D) are represented as a single argument as shown above without any space between D and
      | the argument name. If any of the arguments had leading dash(es) already defined then the arguments are taken as is.
      | For eg in the above config object the map-reduce name argument has a leading dash so no more dashses are added to the
      | above argument whereas the num-mappers will have a double dash prepended as **--num-mappers**
    """.stripMargin

  override val outputConfigDesc =
    """
      |**taskname.__stats__.input-rows** is the total no of rows read from the database (Map Output rows) and
      |**taskname.__stats__.output-rows** is the total no of rows written to the output file (Map Input rows).
      |Here we assume the taskname is the name of your task.
    """.stripMargin

}
