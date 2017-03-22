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
import com.groupon.artemisia.task.settings.ExportSetting
import com.groupon.artemisia.util.HoconConfigUtil.Handler

/**
  * Created by chlr on 4/13/16.
  */

/**
  * A case class for storing Export settings
  *
  * @param header     include header in file
  * @param delimiter  delimiter of the file
  * @param quoting    enable/disable quoting fields
  * @param quotechar  character to be used for quoting if enabled
  * @param escapechar escape character to be used for escaping special characters
  */
case class BasicExportSetting(override val header: Boolean = false, override val delimiter: Char = ',',
                              override val quoting: Boolean = false, override val quotechar: Char = '"',
                              override val escapechar: Char = '\\', override val mode: String = "default")
  extends ExportSetting(header, delimiter, quoting, quotechar, escapechar, mode)

object BasicExportSetting extends ConfigurationNode[BasicExportSetting] {

  override val structure = ConfigFactory parseString
    raw"""|{
          |  header =  "yes @default(false) @type(boolean)"
          |  delimiter = "| @default(,) @type(char)"
          |  quoting = "yes @default(false) @type(boolean)"
          |  quotechar = "'\"' @default(\") @type(char)"
          |  escapechar = "'\\' @default(\\) @type(char)"
          |  mode = "default @default(default)"
          |}""".stripMargin

  override val fieldDescription = Map[String, Any](
    "header" -> "boolean literal to enable/disable header",
    "delimiter" -> "character to be used for delimiter",
    "quoting" -> "boolean literal to enable/disable quoting of fields.",
    "quotechar" -> "quotechar to use if quoting is enabled.",
    "escapechar" -> "escape character use for instance to escape delimiter values in field",
    "mode" -> ("modes of export. supported modes are" -> Seq("default", "bulk")),
    "sqlfile" -> "used in place of sql key to pass the file containing the SQL"
  )


  override val defaultConfig = ConfigFactory parseString
    """
      | {
      |	  header = false
      |	  delimiter = ","
      |	  quoting = no,
      |	  quotechar = "\""
      |   escapechar = "\\"
      |   mode = "default"
      |	}
    """.stripMargin

  def apply(config: Config): BasicExportSetting = {
    BasicExportSetting(
      header = config.as[Boolean]("header"),
      delimiter = config.as[Char]("delimiter"),
      quoting = config.as[Boolean]("quoting"),
      escapechar = config.as[Char]("escapechar"),
      quotechar = config.as[Char]("quotechar"),
      mode = config.as[String]("mode")
    )
  }
}
