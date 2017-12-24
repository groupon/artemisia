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

import java.io.{File, FileInputStream}
import java.net.URI
import com.typesafe.config.ConfigRenderOptions
import com.groupon.artemisia.TestSpec
import com.groupon.artemisia.task.settings.DBConnection
import com.groupon.artemisia.util.FileSystemUtil._
import com.groupon.artemisia.util.HoconConfigUtil.Handler

/**
 * Created by chlr on 5/18/16.
 */
class LoadToTableSpec extends TestSpec {

  "LoadToTable" must "load a file into the given table" in {
    val tableName = "LoadToTableSpec"
    withTempFile(fileName = s"${tableName}_1") {
      file => {
        // row id 106 will be rejected since quicksilver is more than the target column width (10)
        file <<=
          """|102\u0001magneto\u0001true\u0001100\u000110000000\u000187.3\u000112:30:00\u00011945-05-09\u00011945-05-09 12:30:00
             |103\u0001xavier\u0001true\u0001100\u000110000000\u000187.3\u000112:30:00\u00011945-05-09\u00011945-05-09 12:30:00
             |104\u0001wolverine\u0001true\u0001100\u000110000000\u000187.3\u000112:30:00\u00011945-05-09\u00011945-05-09 12:30:00
             |105\u0001mystique\u0001true\u0001100\u000110000000\u000187.3\u000112:30:00\u00011945-05-09\u00011945-05-09 12:30:00
             |106\u0001quicksilver\u0001true\u0001100\u000110000000\u000187.3\u000112:30:00\u00011945-05-09\u00011945-05-09 12:30:00|""".stripMargin
        val loadSettings = BasicLoadSetting(delimiter = '\u0001', batchSize = 1)
        val loader = LoadToTableSpec.loader("LoadToTableSpec1",tableName, file.toURI, TestDBInterFactory.stubbedConnectionProfile,loadSettings)
        val config = loader.execute()
        info(loader.dbInterface.queryOne(s"select count(*) as cnt from $tableName").root().render(ConfigRenderOptions.concise()))
        config.as[Int]("test_task.__stats__.loaded") must be (4)
        config.as[Int]("test_task.__stats__.rejected") must be (1)
      }
    }
  }
}



object LoadToTableSpec {

  def loader(name: String, tableName: String, location: URI ,connectionProfile: DBConnection, loadSettings: BasicLoadSetting) =

    new LoadFromFile("test_task",tableName, location ,connectionProfile, loadSettings) {
    override val supportedModes = "default" :: "bulk" :: Nil
    override val dbInterface: DBInterface = TestDBInterFactory.withDefaultDataLoader(tableName)
    override val source = Left(new FileInputStream(new File(location)))
    override def setup(): Unit = {}
    override def teardown(): Unit = {}
  }

}


