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

class SqoopSettingSpec extends TestSpec {

  "SqoopSetting" must "construct itself from config" in {

    val config =  ConfigFactory parseString
      s"""
         |{
         |  target-type = hdfs
         |  target = /hdfs/path/file.txt
         |  src-table = my_src_table
         |  where-clause = "where col_id > 1000"
         |  sql = "SELECT col1, col2 FROM database.table"
         |  split-by = col1
         |  num-mappers = 10
         |  truncate = yes
         |  queue-name = "test_queue"
         |}
       """.stripMargin

    val setting = SqoopSetting(config)
    setting.numMappers must be (10)
    setting.truncate must be (true)
    setting.targetType must be ("hdfs")
    setting.target must be ("/hdfs/path/file.txt")
    setting.srcTable.getOrElse("") must be ("my_src_table")
    setting.whereClause.getOrElse("") must be ("where col_id > 1000")
    setting.sql.getOrElse("") must be ("SELECT col1, col2 FROM database.table")
    setting.splitBy.getOrElse("") must be ("col1")
    setting.queueName must be ("test_queue")
  }


  it must "ensure either sql or src-table field is specified" in {
    val config =  ConfigFactory parseString
      s"""
         |{
         |  target-type = hdfs
         |  target = /hdfs/path/file.txt
         |  where-clause = "where col_id > 1000"
         |  split-by = col1
         |  num-mappers = 10
         |  truncate = yes
         |  queue-name = "test_queue"
         |}
       """.stripMargin
    val setting = SqoopSetting(config)
    val ex = intercept[IllegalArgumentException]{
      setting.args
    }
    ex.getMessage must be ("either source sql or src-table field has to be defined")
  }


  it must "generate rights commands with free form sql mode" in {
    val config =  ConfigFactory parseString
      s"""
         |{
         |  target-type = hdfs
         |  target = /hdfs/path/file.txt
         |  sql = "SELECT col1, col2 FROM database.table"
         |  split-by = col1
         |  num-mappers = 10
         |  truncate = yes
         |  queue-name = "test_queue"
         |}
       """.stripMargin
    val setting = SqoopSetting(config)
    setting.args must contain (Left(("--target-dir","/hdfs/path/file.txt")))
    setting.args must contain (Left(("--query","'SELECT col1, col2 FROM database.table'")))
    setting.args must contain (Left(("--split-by","col1")))
    setting.args must contain (Right("-Dmapred.job.queue.name=test_queue"))
    setting.args must contain (Left(("--num-mappers","10")))

    setting.args must contain only { List(
      Left(("--target-dir","/hdfs/path/file.txt")),
      Left(("--query","'SELECT col1, col2 FROM database.table'")),
      Left(("--split-by","col1")),
      Right("-Dmapred.job.queue.name=test_queue"),
      Left(("--num-mappers","10"))
    ): _*
    }
  }


  it must "generate rights commands with src-table" in {

    val config =  ConfigFactory parseString
      s"""
         |{
         |  target-type = hdfs
         |  target = /hdfs/path/file.txt
         |  src-table = my_src_table
         |  where-clause = "where col_id > 1000"
         |  split-by = col1
         |  num-mappers = 10
         |  truncate = yes
         |  queue-name = "test_queue"
         |}
       """.stripMargin
    val setting = SqoopSetting(config)
    setting.args must contain only {
      Seq(
      Left(("--target-dir", "/hdfs/path/file.txt")),
      Left(("--table", "my_src_table")),
      Left(("--where", "'where col_id > 1000'")),
      Left(("--split-by", "col1")),
      Right("-Dmapred.job.queue.name=test_queue"),
      Left(("--num-mappers","10"))
      ): _*
    }

  }




}
