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

import com.typesafe.config._
import com.groupon.artemisia.inventory.exceptions.InvalidSettingException
import com.groupon.artemisia.task.database.BasicLoadSetting
import com.groupon.artemisia.task.database.teradata.BaseTeraLoadSetting
import com.groupon.artemisia.task.{ConfigurationNode, TaskContext}
import com.groupon.artemisia.util.HoconConfigUtil.Handler
import com.groupon.artemisia.util.MemorySize

import scala.collection.JavaConverters._


/**
  * case class for TPT Load Setting
  * @param skipRows no of rows to skip in load file
  * @param delimiter delimiter of the load file
  * @param quoting is file quoted
  * @param quotechar quote char used to enclose fields.
  * @param escapechar escape character to escape special symbols
  * @param truncate truncate target table
  * @param batchSize size of batch insert
  * @param errorTolerance tolerance factor records rejection during load
  * @param mode mode of operation.
  * @param bulkLoadThreshold threshold load size for fastload
  * @param nullString replace occurences of these strings as NULL
  * @param errorLimit fail this job if no of rejected records exceeds this number.
  * @param errorFile location of the file where rejected records are written.
  * @param loadOperatorAttrs
  * @param dataConnectorAttrs
  */
case class TPTLoadSetting(override val skipRows: Int = 0,
                          override val delimiter: Char = ',',
                          override val quoting: Boolean = false,
                          override val quotechar: Char = '"',
                          override val escapechar: Char = '\\',
                          override val truncate: Boolean = false,
                          override val batchSize: Int = 100,
                          override val errorTolerance: Option[Double] = None,
                          override val mode: String = "default",
                          override val bulkLoadThreshold: Long = 104857600L,
                          nullString: Option[String] =  None,
                          errorLimit: Int = 2000,
                          errorFile: String = TaskContext.getTaskFile("error.txt").toString,
                          loadOperatorAttrs: Map[String,(String,String)] = Map(),
                          dataConnectorAttrs: Map[String,(String,String)] = Map()) extends
  BaseTeraLoadSetting(skipRows, delimiter, quoting, quotechar, escapechar, truncate, mode ,batchSize, errorTolerance, bulkLoadThreshold) {

  require(TPTLoadSetting.supportedModes contains mode,
    s"$mode is not supported. supported modes are ${TPTLoadSetting.supportedModes.mkString(",")}")

  override def setting: String = ???

  /**
    *
    * @param batchSize batch size
    * @param mode      load mode
    * @return
    */
  override def create(batchSize: Int, mode: String): BaseTeraLoadSetting = {
    copy(batchSize = batchSize, mode = mode)
  }

}

object TPTLoadSetting extends ConfigurationNode[TPTLoadSetting] {

  val supportedModes = Seq("default", "fastload", "auto")

  override val structure = BasicLoadSetting.structure
    .withValue("error-limit", ConfigValueFactory.fromAnyRef("1000 @default(2000)"))
    .withValue("bulk-threshold", ConfigValueFactory.fromAnyRef("100M @info()"))
    .withValue("error-file", ConfigValueFactory.fromAnyRef("/var/path/error.txt @optional"))
    .withValue("null-string", ConfigValueFactory.fromAnyRef("\\N @optional @info(marker string for null)"))

  override val fieldDescription = BasicLoadSetting.fieldDescription ++
    Map(
      "null-string" -> "marker string for null. default value is blank string",
      "error-limit" -> "maximum number of records allowed in error table",
      "error-file" -> "location of the reject file",
      "load-attrs" -> "miscellaneous load operator attributes",
      "dtconn-attrs" -> "miscellaneous data-connector operator attributes",
        "bulk-threshold" -> "size of the source file(s) above which fastload mode will be selected if auto mode is enabled"
    ) -- Seq("batch-size")

  override val defaultConfig = BasicLoadSetting.defaultConfig
    .withValue("error-limit", ConfigValueFactory.fromAnyRef(2000))
    .withValue("bulk-threshold", ConfigValueFactory.fromAnyRef("100M"))
    .withValue("load-attrs", ConfigFactory.empty().root())
    .withValue("dtconn-attrs", ConfigFactory.empty().root())



  override def apply(config: Config): TPTLoadSetting = {
    val loadSetting = BasicLoadSetting(config)
    TPTLoadSetting(
      skipRows = if (config.as[Int]("skip-lines") == 0)
        if (config.as[Boolean]("header")) 1 else 0 else config.as[Int]("skip-lines"),
      loadSetting.delimiter,
      loadSetting.quoting,
      loadSetting.quotechar,
      loadSetting.escapechar,
      loadSetting.truncate,
      loadSetting.batchSize,
      loadSetting.errorTolerance,
      loadSetting.mode,
      config.as[MemorySize]("bulk-threshold").toBytes,
      config.getAs[String]("null-string"),
      config.as[Int]("error-limit"),
      config.getAs[String]("error-file").getOrElse(TaskContext.getTaskFile("error.txt").toString),
      parseAttributeNodes(config.as[Config]("load-attrs")),
      parseAttributeNodes(config.as[Config]("dtconn-attrs"))
    )
  }


  def parseAttributeNodes(node: Config) = {
    val map = node.root.keySet().asScala map {
      x => node.as[ConfigValue](x).valueType() match {
        case ConfigValueType.STRING => x -> ("VARCHAR", node.as[String](x))
        case ConfigValueType.OBJECT =>
          val valueNode = node.as[Config](x)
          x -> (valueNode.as[String]("type") -> valueNode.as[String]("value"))
        case _ => throw new InvalidSettingException(s"operator attributes $x can only either be a string or config object")
      }
    }
    map.toMap
  }

}




