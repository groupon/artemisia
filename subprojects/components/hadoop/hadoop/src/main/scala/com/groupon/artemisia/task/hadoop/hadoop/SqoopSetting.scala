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

import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import com.groupon.artemisia.task.ConfigurationNode
import com.groupon.artemisia.util.HoconConfigUtil.Handler

/**
  * Created by chlr on 10/14/16.
  */


/**
  * Sqoop specific settings
  *
  * @param targetType target type. allowed targets are hive and hdfs.
  * @param truncate truncate target.
  * @param target target. this can be either a hdfs path or hive table.
  * @param srcTable source table to import.
  * @param whereClause optional where clause to apply on the source table.
  * @param sql source sql to import.
  * @param splitBy the split by column to use when sql field is used instead of table.
  */
case class SqoopSetting(targetType: String = "hdfs"
                        ,truncate: Boolean = false
                        ,target: String
                        ,srcTable: Option[String] = None
                        ,whereClause: Option[String] = None
                        ,sql: Option[String] = None
                        ,splitBy: Option[String] = None
                        ,numMappers: Int = 1
                        ,queueName: String = "default"
                        ) {

  val allowedTargetTypes = Seq("hdfs", "hive")

  def args: Seq[Either[(String, String), String]] = tgtTypeArgs ++ srcArgs ++
    Seq(Right(s"-Dmapred.job.queue.name=$queueName"), Left("--num-mappers" -> numMappers.toString))

  private def tgtTypeArgs = {
    targetType match {
      case "hdfs" => Seq(Left("--target-dir" -> target))
      case "hive" if truncate => Seq(Right("--hive-import"), Right("--hive-overwrite"), Left("--hive-table" -> target))
      case "hive" => Seq(Right("--hive-import"), Left("--hive-table" -> target))
      case _ => throw new IllegalArgumentException(s"unsupported target-type $targetType. " +
        s"allowed target types are ${allowedTargetTypes.mkString(",")}")
    }
  }

  private[hadoop] def srcArgs = {
    (srcTable, sql, splitBy) match {
      case (_, Some(x), Some(y)) =>  Seq(Left("--query" -> s"'$x'"), Left("--split-by" -> y))
      case (_, Some(x), None) =>
        throw new IllegalArgumentException(s"split by column is mandatory when free sql-export is set")
      case (Some(x), _, y) => Seq(Left("--table" -> x)) ++ {
        whereClause.map(wx => Seq(Left("--where" -> s"'$wx'"))).getOrElse(Nil) ++
        y.map(yps => Seq(Left("--split-by" -> yps))).getOrElse(Nil)
      }
      case _ => throw new IllegalArgumentException("either source sql or src-table field has to be defined")
    }
  }

}

object SqoopSetting extends ConfigurationNode[SqoopSetting] {

  override val defaultConfig = ConfigFactory parseString
     """
       |{
       |  target-type = hdfs
       |  truncate = no
       |  queue-name = default
       |  num-mappers = 1
       |}
     """.stripMargin

  override def apply(config: Config): SqoopSetting = {
    SqoopSetting(
      config.as[String]("target-type"),
      config.as[Boolean]("truncate"),
      config.as[String]("target"),
      config.getAs[String]("src-table"),
      config.getAs[String]("where-clause"),
      config.getAs[String]("sql"),
      config.getAs[String]("split-by"),
      config.as[Int]("num-mappers"),
      config.as[String]("queue-name")
    )
  }

  override val structure = ConfigFactory.empty()
    .withValue("target-type", ConfigValueFactory.fromAnyRef("hive @default(hdfs)"))
    .withValue("truncate", ConfigValueFactory.fromAnyRef("yes @default(no)"))
    .withValue("target", ConfigValueFactory.fromAnyRef("/user/artemisia/file.txt"))
    .withValue("src-table", ConfigValueFactory.fromAnyRef("database.tablename @optional"))
    .withValue("where-clause", ConfigValueFactory.fromAnyRef("id > 100 @optional"))
    .withValue("sql", ConfigValueFactory.fromAnyRef("SELECT * FROM table @optional"))
    .withValue("split-by", ConfigValueFactory.fromAnyRef("id @optional"))
    .withValue("queue-name", ConfigValueFactory.fromAnyRef("queue-name @default(default)"))

  override val fieldDescription = Map(
    "target-type" -> ("the target type. the supported target types are" -> Seq("hdfs", "hive")),
    "truncate" -> "truncate the target hdfs directory or target hive table",
    "target" -> "this is either hdfs path or hive tablename depending on target-type set",
    "src-table" -> "source table to be exported. either this field or the *sql* field must be provided",
    "where-clause" -> "where clause to be applied on *src-table*. applicable only when *src-table* is set",
    "sql" -> "source sql to be exported. either this field or the *src-table* field must be provided",
    "split-by" -> "column to be used split",
    "queue-name" -> "name of the queue where the MR job will be submitted"
  )

}



