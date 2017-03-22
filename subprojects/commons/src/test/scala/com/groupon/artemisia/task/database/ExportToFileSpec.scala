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

import java.io.{File, FileOutputStream}
import com.groupon.artemisia.TestSpec
import com.groupon.artemisia.core.Keywords
import com.groupon.artemisia.task.settings.DBConnection
import com.groupon.artemisia.util.HoconConfigUtil.Handler

/**
 * Created by chlr on 4/28/16.
 */
class ExportToFileSpec extends TestSpec {

  val table = "export_to_file"
  val testDbInterface = TestDBInterFactory.withDefaultDataLoader(table)
  val connectionProfile = DBConnection("","","","default", 1000)
  val file = new File(this.getClass.getResource("/exports/ExportToFile.txt").getFile)
  val exportSettings = BasicExportSetting(delimiter = 0x1, header = true)

  "ExportToFile" must "export query result to file" in {
    val exportToFile = new ExportToFile(name = "ExportToFileTest",
    sql = s"select * from $table",
    file.toURI,
    connectionProfile,
    exportSettings
    ) {
      override val supportedModes = "default" :: "bulk" :: Nil
      override val dbInterface: DBInterface = testDbInterface
      override val target = Left(new FileOutputStream(new File(location)))
    }
    val config = exportToFile.execute()
    config.as[Int](s"ExportToFileTest.${Keywords.TaskStats.STATS}.rows") must be (2)
    scala.io.Source.fromFile(file).getLines().toList(2) must be ("2\u0001bar\u0001FALSE\u0001100\u000110000000\u00018723.38\u000112:30:00\u00011945-05-09\u00011945-05-09 12:30:00.0")
  }


}
