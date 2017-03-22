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
import org.apache.commons.io.output.NullOutputStream
import com.groupon.artemisia.TestSpec
import com.groupon.artemisia.task.settings.DBConnection
import com.groupon.artemisia.util.TestUtils

/**
  * Created by chlr on 10/20/16.
  */
  class SqoopExportSpec extends TestSpec {

  "SqoopExport" must "construct itself from config" in {

    val config = ConfigFactory parseString {
      """
        |{
        |  connection = {
        |    type = mysql
        |    dsn = {
        |      host = server
        |      username = optimus
        |      password = prime
        |      port = 100
        |      database = cybertron
        |    }
        |  }
        |  sqoop-bin = /usr/local/bin/sqoop
        |  hdfs-bin = /usr/local/bin/hdfs
        |  export = {
        |     format = text
        |     delimiter = ","
        |     quoting = yes
        |     escapechar = "~"
        |     quotechar = "|"
        |  }
        |  sqoop-setting = {
        |     truncate = yes
        |     target-type = hdfs
        |     target = /var/path/file.txt
        |     sql = "SELECT col1,col2 FROM table"
        |     split-by = col2
        |     num-mappers = 10
        |     queue-name = public
        |  }
        |  misc-setting = [
        |    { direct-split-size = 1000 },
        |    append,
        |    "-Dmapreduce.map.maxattempts=10",
        |     "--verbose",
        |     { "--hadoop-home" = "/var/path" }
        |  ]
        |}
      """.stripMargin
    }

    val task = SqoopExport("task", config).asInstanceOf[SqoopExport]
    task.sqoopOption.queueName must be ("public")
    task.sqoopOption.splitBy must be (Some("col2"))
    task.sqoopOption.numMappers must be (10)
    task.miscSqoopOptions must contain only (
      Left("--direct-split-size" -> "1000"),
      Left("--hadoop-home", "/var/path"),
      Right("--append"),
      Right("-Dmapreduce.map.maxattempts=10"),
      Right("--verbose")
    )
    task.sqoopExportSetting.escapechar must be ('~')
    task.sqoopExportSetting.quotechar must be ('|')
    task.sqoopExportSetting.format must be ("text")

    task.sqoopBin.get must be ("/usr/local/bin/sqoop")
    task.hdfsBin.get must be ("/usr/local/bin/hdfs")

    task.connection._1 must be ("mysql")
    task.connection._2.hostname must be ("server")
    task.connection._2.username must be ("optimus")
    task.connection._2.password must be ("prime")
    task.connection._2.default_database must be ("cybertron")

    task.miscSqoopOptions must contain only (
      Left(("--direct-split-size","1000")),
      Right("--append"),Right("-Dmapreduce.map.maxattempts=10"),
      Right("--verbose"),
      Left(("--hadoop-home","/var/path"))
      )

    task.sqoopCommand(task.sqoopBin.get) must contain only (
      "/usr/local/bin/sqoop",
      "import",
      "-Dmapreduce.job.name=task",
      "-Dmapred.job.queue.name=public",
      "-Dmapreduce.map.maxattempts=10",
      "--username",
      "optimus",
      "--password",
      "prime",
      "--query",
      "'SELECT col1,col2 FROM table'",
      "--num-mappers",
      "10",
      "--direct-split-size",
      "1000",
      "--connect",
      "jdbc:mysql://server:100/cybertron?zeroDateTimeBehavior=convertToNull",
      "--split-by",
      "col2",
      "--fields-terminated-by",
      "'\\0x02c'",
      "--driver",
      "com.mysql.jdbc.Driver",
      "--escaped-by",
      "'\\0x07e'",
      "--target-dir",
      "/var/path/file.txt",
      "--enclosed-by",
      "'\\0x07c'",
      "--hadoop-home",
      "/var/path",
      "--as-textfile",
      "--append",
      "--verbose"
      )

    task.sqoopCommand(task.sqoopBin.get) must contain inOrder ("--username","optimus")
    task.sqoopCommand(task.sqoopBin.get) must contain inOrder ("--password","prime")
    task.sqoopCommand(task.sqoopBin.get) must contain inOrder ("--connect",
      "jdbc:mysql://server:100/cybertron?zeroDateTimeBehavior=convertToNull")
  }


  it must "execute as required" in {
    val task = new SqoopExport(
      "test",
      "mysql" -> DBConnection("servername", "username", "password", "database", 1000),
      Some(TestUtils.getExecutable(this.getClass.getResource("/sqoop.sh"))),
      Some(TestUtils.getExecutable(this.getClass.getResource("/hdfs.sh"))),
      new SqoopExportSetting(),
      SqoopSetting(
        truncate = true,
        target = "/var/path/hdfs",
        sql = Some("SELECT col1,col2 FROM TABLE"),
        splitBy = Some("col2")
      ),
      Nil
    ) {
      hdfsTruncateCmd must contain only (this.getClass.getResource("/hdfs.sh").getFile,
        "dfs", "-rm", "-r", "-f", "/var/path/hdfs")
      override val logParser = new SqoopExportLogParser(new NullOutputStream)
    }
    val result = task.execute()
    result.getInt("test.__stats__.input-rows") must be (8)
    result.getInt("test.__stats__.output-rows") must be (8)
  }

}
