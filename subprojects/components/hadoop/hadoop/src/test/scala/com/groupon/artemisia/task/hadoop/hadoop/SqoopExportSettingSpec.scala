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

import com.typesafe.config.ConfigFactory
import com.groupon.artemisia.TestSpec

/**
  * Created by chlr on 10/20/16.
  */
class SqoopExportSettingSpec extends TestSpec {

  "SqoopExportSetting" must "construct itself from config" in {

    val config = ConfigFactory parseString
      """
        | {
        |   format = text
        |   delimiter = ","
        |   quoting = yes
        |   quotechar = "~"
        |   escapechar = "|"
        | }
      """.stripMargin

    val setting = SqoopExportSetting(config)
    setting.escapechar must be ('|')
    setting.quotechar must be ('~')
    setting.quoting mustBe true
    setting.delimiter must be (',')
    setting.format must be ("text")

  }

  it must "fail when incorrect format is specified" in {
    val config = ConfigFactory parseString
      """
        | {
        |   format = unknown_format
        |   delimiter = ","
        |   quoting = yes
        |   quotechar = "~"
        |   escapechar = "|"
        | }
      """.stripMargin
    val setting = SqoopExportSetting(config)
    val ex = intercept[IllegalArgumentException](setting.args)
    ex.getMessage must be ("format unknown_format is not supported.supported formats are text,sequence,avro")
  }


  it must "generate correct arguments when quoting is enabled for text" in {
    val config = ConfigFactory parseString
      """
        | {
        |   format = text
        |   delimiter = ","
        |   quoting = yes
        |   quotechar = "~"
        |   escapechar = "|"
        | }
      """.stripMargin
    val setting = SqoopExportSetting(config)
    setting.args must contain only {
      Seq(
        Left(("--fields-terminated-by","'\\0x02c'")),
        Right("--as-textfile"),
        Left(("--enclosed-by", "'\\0x07e'")),
        Left(("--escaped-by", "'\\0x07c'"))
      ) : _*
    }
  }


  it must "generate correct arguments when non-text output is selected" in {
    val config = ConfigFactory parseString
      """
        | {
        |   format = avro
        |   delimiter = ","
        |   quoting = yes
        |   quotechar = "~"
        |   escapechar = "|"
        | }
      """.stripMargin
    val setting = SqoopExportSetting(config)
    setting.args must contain only {
      Seq(
        Left(("--fields-terminated-by","'\\0x02c'")),
        Right("--as-avrodatafile")
      ) : _*
    }
  }



}
