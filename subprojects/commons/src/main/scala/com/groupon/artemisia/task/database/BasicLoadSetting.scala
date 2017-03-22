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

import com.typesafe.config.{Config, ConfigFactory}
import com.groupon.artemisia.task.ConfigurationNode
import com.groupon.artemisia.task.settings.LoadSetting
import com.groupon.artemisia.util.HoconConfigUtil.Handler

/**
 * Created by chlr on 4/30/16.
 */

/**
 * Load settings definition
 */
case class BasicLoadSetting(override val skipRows: Int = 0, override val delimiter: Char = ',',
                            override val quoting: Boolean = false, override val quotechar: Char = '"', override val escapechar: Char = '\\',
                            override val truncate: Boolean = false, override val mode: String = "default", override val batchSize: Int = 100,
                            override val errorTolerance: Option[Double] = None)
 extends LoadSetting(skipRows, delimiter, quoting, quotechar, escapechar,truncate ,mode, batchSize, errorTolerance) {

  override def setting: String = s"skip-rows: $skipRows, delimiter: $delimiter, quoting: $quoting, quotechar: $quotechar," +
    s" escapechar: $escapechar, truncate: $truncate, mode: $mode," +
    s" batch-size: $batchSize ${errorTolerance.map(x => s"error-tolerance: $x").getOrElse("")}"

}

object BasicLoadSetting extends ConfigurationNode[BasicLoadSetting] {

  val structure = ConfigFactory parseString
 raw"""|{
     | header = "no @default(false) @type(boolean)"
     | skip-lines = "0 @default(0) @type(int)"
     | delimiter = "'|' @default(',') @type(char)"
     | quoting = "no @default(false) @type(boolean)"
     | quotechar = "\" @default('\"') @type(char)"
     | escapechar = "\" @default(\\) @type(char)"
     | truncate = "yes @type(boolean)"
     | mode = "default @default(default) @type(string)"
     | batch-size = "200 @default(100)"
     | error-tolerence = "0.57 @default(2) @type(double,0,1)"
     |}""".stripMargin

  val fieldDescription = Map(
     "header" -> "boolean field to enable/disable headers",
     "skip-lines" -> "number of lines to skip",
     "delimiter" -> "delimiter of the file",
     "quoting" -> "boolean field to indicate if the file is quoted.",
     "quotechar" -> "character to be used for quoting",
     "escapechar" -> "escape character used in the file",
     "truncate" -> "truncate the target table before loading data",
     "mode" -> "mode of loading the table",
     "batch-size" -> "loads into table will be grouped into batches of this size.",
     "error-tolerance" -> "% of data that is allowable to get rejected value ranges from (0.00 to 1.00)"
  )


  val defaultConfig = ConfigFactory parseString
    """
      |{
      |	  header =  no
      |	  skip-lines = 0
      |	  delimiter = ","
      |	  quoting = no
      |	  quotechar = "\""
      |   escapechar = "\\"
      |   truncate = false
      |   batch-size = 100
      |   mode = default
      |}
    """.stripMargin

  def apply(config: Config): BasicLoadSetting = {
    BasicLoadSetting (
    skipRows = if (config.as[Int]("skip-lines") == 0) if (config.as[Boolean]("header")) 1 else 0 else config.as[Int]("skip-lines"),
    delimiter = config.as[Char]("delimiter"),
    quoting = config.as[Boolean]("quoting"),
    quotechar = config.as[Char]("quotechar"),
    escapechar = config.as[Char]("escapechar"),
    mode = config.as[String]("mode").toLowerCase,
    truncate = config.as[Boolean]("truncate"),
    errorTolerance = config.getAs[Double]("error-tolerence"),
    batchSize = config.as[Int]("batch-size")
    )
  }

}
