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

import com.typesafe.config.ConfigFactory
import com.groupon.artemisia.TestSpec

/**
  * Created by chlr on 9/14/16.
  */
class TPTLoadSettingSpec extends TestSpec {


  "TPTLoadSetting" must "construct itself from config object" in {

    val config = ConfigFactory parseString
       """
         | {
         |    delimiter = ","
         |    quoting = yes
         |    quotechar = "'"
         |    escapechar = "\\"
         |    mode = default
         |    batch-size = 10000
         |    error-limit = 200
         |    truncate = yes
         |    header = yes
         |    bulk-threshold = 1K
         |    load-attrs = {
         |        OPENMODE = WRITE
         |        BUFFERSIZE = {
         |           type = "INTEGER"
         |           value = "9876543"
         |        }
         |    }
         |    dtconn-attrs = {
         |        OPENMODE = WRITE
         |        BUFFERSIZE = {
         |           type = "INTEGER"
         |           value = "9876543"
         |         }
         |      }
         |    skip-lines = 10
         | }
         |
       """.stripMargin

    val setting = TPTLoadSetting(config)
    setting.delimiter must be (',')
    setting.quoting mustBe true
    setting.quotechar must be ('\'')
    setting.escapechar must be ('\\')
    setting.mode must be ("default")
    setting.bulkLoadThreshold must be (1024)
    setting.batchSize must be (10000)
    setting.errorLimit must be (200)
    setting.truncate mustBe true
    setting.loadOperatorAttrs must be (Map("OPENMODE" -> ("VARCHAR","WRITE"), "BUFFERSIZE" -> ("INTEGER","9876543")))
    setting.dataConnectorAttrs must be (Map("OPENMODE" -> ("VARCHAR","WRITE"), "BUFFERSIZE" -> ("INTEGER","9876543")))
  }


  it must "throw an exception when an unknown mode is set" in {
    val exception = intercept[IllegalArgumentException] {
      TPTLoadSetting(mode = "unknow_mode")
    }
    exception.getMessage must be ("requirement failed: unknow_mode is not supported. supported modes are default,fastload,auto")
  }

}
