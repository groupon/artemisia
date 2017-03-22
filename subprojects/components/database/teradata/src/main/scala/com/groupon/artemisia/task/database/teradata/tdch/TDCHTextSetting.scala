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

import com.typesafe.config.{Config, ConfigFactory}
import com.groupon.artemisia.task.ConfigurationNode
import com.groupon.artemisia.util.HoconConfigUtil.Handler
import com.groupon.artemisia.util.Util

/**
  * Created by chlr on 8/28/16.
  */

/**
  *
  * @param delimiter delimiter for fields
  * @param quoting boolean field to enabled/disable quoting
  * @param quoteChar quoting character that encloses the fields
  * @param nullString string to represent null values
  * @param escapedBy escape characters to be used.
  */
case class TDCHTextSetting(delimiter: Char = ',', quoting: Boolean = false, quoteChar: Char= '"', nullString: Option[String] = None,
                           escapedBy: Char = '\\') {


  val commandArgs = "-separator" :: Util.unicodeCode(delimiter) :: Nil ++
    (if(quoting) "-enclosedby" :: quoteChar :: "-escapedby" :: escapedBy :: Nil else Nil) ++
    (if(nullString.isDefined) "-nullstring" :: nullString.get :: Nil else Nil)


}

object TDCHTextSetting extends ConfigurationNode[TDCHTextSetting] {

  override val defaultConfig: Config = ConfigFactory parseString
   """
     |{
     |  delimiter = ","
     |  quoting = no
     |  quote-char = "\""
     |  escape-char = "\\"
     |}
   """.stripMargin

  override def apply(config: Config): TDCHTextSetting = {
    TDCHTextSetting(
      config.as[Char]("delimiter"),
      config.as[Boolean]("quoting"),
      config.as[Char]("quote-char"),
      config.getAs[String]("null-string"),
      config.as[Char]("escape-char")
    )
  }

  override val structure: Config = ConfigFactory parseString
     """
       |{
       |  delimiter = "| @default(,)"
       |  quoting = "no @type(boolean)"
       |  quote-char = "\""
       |  escape-char = "\\"
       |}
       |
     """.stripMargin

  override val fieldDescription: Map[String, Any] = Map(
    "delimiter" -> "delimiter of the textfile",
    "quoting" -> "enable or disable quoting. both quote-char and escape-char fields are considered only when quoting is enabled",
    "quote-char" -> "character used for quoting",
    "escape-char" -> "escape character to be used. forward slash by default",
    "null-string" -> "string to represent null values"
  )
}
