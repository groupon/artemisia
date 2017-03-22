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

import java.io.File
import java.nio.file.Paths
import com.typesafe.config.ConfigFactory
import com.groupon.artemisia.TestSpec
import com.groupon.artemisia.util.FileSystemUtil._

/**
  * Created by chlr on 9/1/16.
  */
class TDCHSettingSpec extends TestSpec {

  "TDCHSettingSpec" must "throw exception if tdchjar doesn't exist" in {
    val ex = intercept[java.lang.IllegalArgumentException] {
      TDCHSetting("/path/jar_file_that_doesn't_exist.jar")
    }
    ex.getMessage must be ("requirement failed: TDCH jar /path/jar_file_that_doesn't_exist.jar doesn't exists")
  }

  it must "throw exception when unsupported format is specified" in {
    val file = this.getClass.getResource("/samplefiles/file.txt").getFile
    val ex = intercept[java.lang.IllegalArgumentException] {
      TDCHSetting(file, format = "some-unsupported-format")
    }
    ex.getMessage must be ("requirement failed: some-unsupported-format is not supported. " +
      "textfile,avrofile,rcfile,orcfile,sequencefile,parquet are the only format supported")
  }


  it must "throw an exception when the hadoop binary doesn't exists" in {
    val file = this.getClass.getResource("/samplefiles/file.txt").getFile
    val ex = intercept[java.lang.IllegalArgumentException] {
      TDCHSetting(file, hadoop = Some(new File("non-existant-hadoop")))
    }
    ex.getMessage must be ("requirement failed: hadoop binary non-existant-hadoop doesn't exists")
  }


  it must "construct the object properly" in {
    val tdchJar = this.getClass.getResource("/samplefiles/file.txt").getFile
    val hadoop = this.getClass.getResource("/executables/tdch_load_from_hdfs.sh").getFile
    val dir = this.getClass.getResource("/samplefiles").getFile
    val config = ConfigFactory parseString
    s"""
      |  {
      |   tdch-jar = $tdchJar
      |   hadoop = $hadoop
      |   num-mappers = 5
      |   queue-name = test
      |   format = textfile
      |   lib-jars = [
      |     ${joinPath(dir, "file.txt")}
      |     "${joinPath(dir, "dir2" , "file*.txt")}"
      |   ]
      |   misc-options = {
      |     foo1 = bar1
      |     foo2 = bar2
      |   }
      |   text-setting = {
      |    delimiter = ","
      |    quoting = no
      |    quote-char = "'"
      |    escape-char = "|"
      |    null-string = ""
      |   }
      | }
    """.stripMargin
    val setting = TDCHSetting(config)

    setting.tdchJar must be (tdchJar)
    setting.hadoop.get.toString must be (hadoop)
    setting.numMapper must be (5)
    setting.queueName must be ("test")
    setting.format must be ("textfile")
    info((setting.libJars map { x => Paths.get(x).getFileName.toString }).mkString(","))
    (setting.libJars map { x => Paths.get(x).getFileName.toString }) must contain only ("file.txt", "file1.txt", "file2.txt")
    setting.miscOptions must contain allOf ("foo1" -> "bar1", "foo2" -> "bar2")
    setting.textSettings.delimiter must be (',')
    setting.textSettings.quoting mustBe false
    setting.textSettings.quoteChar must be ('\'')
    setting.textSettings.escapedBy must be ('|')
    setting.textSettings.nullString.get must be ("")
  }

}
