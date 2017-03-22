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

import com.typesafe.config.{Config, ConfigValueFactory}
import com.groupon.artemisia.task.ConfigurationNode
import com.groupon.artemisia.task.database.BasicExportSetting
import com.groupon.artemisia.task.settings.ExportSetting
import com.groupon.artemisia.util.HoconConfigUtil.Handler

/**
  * Created by chlr on 10/13/16.
  */

/**
  *
  * @param format export data format
  * @param delimiter delimiter to be used for text format
  * @param quoting enable/disable quoting for text format
  * @param quotechar quotechar to be used for text format
  * @param escapechar escapechar to be used for
  */
case class SqoopExportSetting(format: String = "text"
                              ,override val delimiter: Char = ','
                              ,override val quoting: Boolean = false
                              ,override val quotechar: Char = '"'
                              ,override val escapechar: Char = '\\')
  extends ExportSetting(false, delimiter, quoting, quotechar, escapechar, "default") {

  import SqoopExportSetting._


  def args: Seq[Either[(String, String), String]] = {
    Seq (
        Left("--fields-terminated-by" -> String.format("'\\0x%03x'", new Integer(delimiter))),
        Right(getTargetFormat)
      ) ++ quotingArgs
  }

  private def quotingArgs = {
    quoting match {
      case true if format == "text" => Seq(
        Left("--enclosed-by" -> String.format("'\\0x%03x'", new Integer(quotechar))),
        Left("--escaped-by" -> String.format("'\\0x%03x'", new Integer(escapechar)))
      )
      case _ => Nil
    }
  }

  private def getTargetFormat = {
    format match {
      case "text" => "--as-textfile"
      case "sequence" => "--as-sequencefile"
      case "avro" => "--as-avrodatafile"
      case _ => throw new IllegalArgumentException(s"format $format is not supported." +
        s"supported formats are ${supportedFormats.mkString(",")}")
    }
  }

}

object SqoopExportSetting extends ConfigurationNode[SqoopExportSetting] {

  val supportedFormats = Seq("text", "sequence", "avro")

  override val defaultConfig = BasicExportSetting.defaultConfig
    .withValue("format", ConfigValueFactory.fromAnyRef("text"))
    .withValue("delimiter", ConfigValueFactory.fromAnyRef(","))
    .withValue("quoting", ConfigValueFactory.fromAnyRef(false))

  override def apply(config: Config): SqoopExportSetting = {
    val basicExportSetting = BasicExportSetting {
      config
        .withValue("header", ConfigValueFactory.fromAnyRef(false))
        .withValue("mode", ConfigValueFactory.fromAnyRef("default"))
    }
    SqoopExportSetting(
      config.as[String]("format"),
      basicExportSetting.delimiter,
      basicExportSetting.quoting,
      basicExportSetting.quotechar,
      basicExportSetting.escapechar
    )
  }

  override val structure: Config = BasicExportSetting.structure
    .withValue("format", ConfigValueFactory.fromAnyRef("text"))
    .withoutPath("header")
    .withoutPath("mode")

  override val fieldDescription: Map[String, Any] = BasicExportSetting.fieldDescription -- Seq("header", "mode") +
    ("format" -> s"output data format. allowed values are ${supportedFormats.mkString(",")}")

}