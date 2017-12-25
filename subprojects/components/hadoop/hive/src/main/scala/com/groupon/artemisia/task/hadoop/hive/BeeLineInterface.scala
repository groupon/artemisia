package com.groupon.artemisia.task.hadoop.hive

import java.io.{File, OutputStream}

import com.groupon.artemisia.util.FileSystemUtil._
import com.groupon.artemisia.core.AppLogger.info
import com.groupon.artemisia.task.TaskContext
import com.groupon.artemisia.task.settings.DBConnection
import com.groupon.artemisia.util.CommandUtil.executeCmd
import com.groupon.artemisia.util.Util
import com.typesafe.config.Config

/**
  * Created by chlr on 12/11/17.
  */
class BeeLineInterface(beeline: String,
                       connectionProfile: DBConnection,
                       stdout: OutputStream = System.out,
                       stderr: OutputStream = System.err) {


  def queryOne(hql: String, taskName: String): Config = {
    info(Util.prettyPrintAsciiBanner(hql,"query"))
    val effectiveHQL =
      s"""set mapred.job.name = $taskName;
         |$hql
       """.stripMargin
    val cmd = makeBeelineCommand(effectiveHQL, taskName) :+ "--silent=true"
    val parser = new BeeLineReadParser(stdout)
    executeCmd(cmd, stdout = parser, stderr = stderr, obfuscate = Seq(7))
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
  def execute(hql: String, taskName: String, printSQL: Boolean = true): Config = {
    if (printSQL)
      info(Util.prettyPrintAsciiBanner(hql, "query"))
    val effectiveHQL = s"set mapred.job.name = $taskName;\n" + hql
    val cmd = makeBeelineCommand(effectiveHQL, taskName)
    val logParser = new BeeLineExecuteParser(stderr)
    val retCode = executeCmd(cmd, stdout = stdout ,stderr = logParser, obfuscate = Seq(7))
    logParser.close()
    assert(retCode == 0, s"query execution failed with ret code $retCode")
    logParser.getData
  }

  /**
    * beeline execute command
    * @param file
    * @return
    */
  protected def command(file: File): Seq[String] = {
    Seq(beeline, "-u", s""""${HiveServerDBInterface.makeUrl(connectionProfile)}"""",
      "-n", s""""${connectionProfile.username}"""", "-p", s""""${connectionProfile.password}"""" ,"-f",
      file.toPath.toString)
  }

  /**
    *
    * @param hql hql query to be executed.
    * @return command to execute the hive query
    */
  private[hive] def makeBeelineCommand(hql: String, taskName: String) = {
    val file = TaskContext.getTaskFile("query.hql", taskName=Some(taskName))
    file <<= hql
    command(file)
  }

}
