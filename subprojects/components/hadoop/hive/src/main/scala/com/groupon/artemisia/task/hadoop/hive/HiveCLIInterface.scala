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

package com.groupon.artemisia.task.hadoop.hive

import java.io.OutputStream
import com.groupon.artemisia.core.AppLogger._
import com.groupon.artemisia.task.TaskContext
import com.groupon.artemisia.util.CommandUtil._
import com.groupon.artemisia.util.FileSystemUtil._
import com.groupon.artemisia.util.Util

/**
  * Created by chlr on 8/3/16.
  */

/**
  * A DBInterface like class that supports functionality like queryOne and execute
  * but doesnt extends DBInterace. The Local Hive CLI installation is used to
  * submit queries. This interface can be used in the absence of a hiveserver service
  * not being available.
  */
class HiveCLIInterface(val hive: String, stdout: OutputStream = System.out, stderr: OutputStream = System.err) {

  /**
    * execute select query that returns a single row and parse the single row as Hocon config object
    *
    * @param hql SELECT query to be executed
    * @param taskName name to be set for the hive mapred job
    * @return resultset of the query with header and first row as Hocon config object
    */
  def queryOne(hql: String, taskName: String) = {
    info(Util.prettyPrintAsciiBanner(hql,"query"))
    val effectiveHQL =
      s"""set mapred.job.name = $taskName;
         |set hive.cli.print.header=true;
         |$hql
       """.stripMargin
    val cmd = makeHiveCommand(effectiveHQL)
    val parser = new HQLReadParser(stdout)
    executeCmd(cmd, stdout = parser, stderr = stderr)
    parser.close()
    parser.getData
  }


  /**
    * execute DML/DDL HQL queries
    *
    * @param hql hql query to be executed
    * @param taskName name to be set for the hive mapred job
    * @return config with stats on rows loaded.
    */
  def execute(hql: String, taskName: String, printSQL: Boolean = true) = {
    if (printSQL)
      info(Util.prettyPrintAsciiBanner(hql, "query"))
    val effectiveHQL = s"set mapred.job.name = $taskName;\n" + hql
    val cmd = makeHiveCommand(effectiveHQL)
    val logParser = new HQLExecuteParser(stderr)
    val retCode = executeCmd(cmd, stdout = stdout ,stderr = logParser)
    logParser.close()
    assert(retCode == 0, s"query execution failed with ret code $retCode")
    Util.mapToConfig(logParser.rowsLoaded.toMap)
  }


  /**
    *
    * @param hql hql query to be executed.
    * @return command to execute the hive query
    */
  private[hive] def makeHiveCommand(hql: String) = {
    val file = TaskContext.getTaskFile("query.hql")
    file <<= hql
    hive :: "-f" :: file.toPath.toString :: Nil
  }

}
