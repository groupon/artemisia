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

import java.io.{ByteArrayOutputStream, File}
import java.net.URI
import com.typesafe.config.Config
import org.apache.commons.io.FileUtils
import com.groupon.artemisia.core.AppLogger._
import com.groupon.artemisia.task.database.teradata._
import com.groupon.artemisia.task.database.{DBInterface, DBUtil}
import com.groupon.artemisia.task.settings.DBConnection
import com.groupon.artemisia.task.{Task, TaskContext}
import com.groupon.artemisia.util.CommandUtil._
import com.groupon.artemisia.util.FileSystemUtil._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, Promise}


/**
  * Created by chlr on 9/9/16.
  */

/**
 *
 * @param taskName name of the task
 * @param tableName target table name
 * @param location location of the file(s) to load.
 * @param connectionProfile database connection profile
 * @param loadSetting load settings
 */
abstract class TPTLoad(override val taskName: String
                     ,val tableName: String
                     ,val location: URI
                     ,val connectionProfile: DBConnection
                     ,val loadSetting: TPTLoadSetting) extends Task(taskName) {


  protected val loadDataSize: Long

  protected val scriptGenerator: BaseTPTLoadScriptGen

  protected val readerFuture: Future[Unit]

  implicit protected val dbInterface: DBInterface = DBInterfaceFactory.getInstance(connectionProfile)

  lazy protected val tbuildBin = getExecutableOrFail("tbuild")

  lazy protected val twbKillBin = getExecutableOrFail("twbkill")

  lazy protected val twbStat = getExecutableOrFail("twbstat")

  protected val dataPipe = joinPath(TaskContext.workingDir.toString, "input.pipe")

  protected val tptScriptFile = this.getFileHandle("load.scr")

  protected val logParser = TPTLoad.logParser(loadSetting.mode)

  protected val errorLogger: TPTErrorLogger =
    TPTErrorLogger.createErrorLogger(s"$tableName", loadSetting.errorFile, dbInterface, loadSetting.mode)

  private val tptCheckpointDir = {
    val dir = new File(joinPath(TaskContext.workingDir.toString, "tpt_checkpoint"))
    dir.mkdirs()
    FileUtils.cleanDirectory(dir)
    dir.toString
  }

  protected val tptLoadConfig =  {
    val (database, table) = DBUtil.parseTableName(tableName) match {
      case (Some(x),y) => x -> y
      case (None, y) => connectionProfile.default_database -> y
    }
    TPTLoadConfig(database, table, TaskContext.workingDir.toString, "input.pipe")
  }

  /**
   * writer future. this is a Future of type Unit that launches the TPT script on a separate thread.
   */
  protected lazy val writerFuture = {
    val tptCommand = Seq(tbuildBin, "-f", tptScriptFile.toString, "-h", "128M", "-j", tableName,"-r",
      TaskContext.workingDir.toString, "-r", tptCheckpointDir, "-R", "0", "-z", "0", "-o")
    Future {
      val ret = executeCmd(tptCommand, stdout = logParser, validExitValues = Array(0,4))
      assert(ret <= 4, s"command ${tptCommand.mkString(" ")} failed with return code $ret")
      if (ret == 4) warn(s"TPT job completed with warning")
    }
  }

  override protected[task] def setup(): Unit = {
    if (loadDataSize > 0) {
      assert(detectTPTRun(tableName) == Nil, s"detected TPT job(s) already running for the table " +
        s"${detectTPTRun(tableName).mkString(",")}. try again after sometime")
      if (loadSetting.truncate) {
        TeraUtils.truncateElseDrop(tableName)
      }
      createNamedPipe(dataPipe)
      tptScriptFile <<= scriptGenerator.tptScript
    }
  }

  override protected[task] def work(): Config = {
    if (loadDataSize > 0) {
      val combinedFuture = TPTLoad.monitor(readerFuture, writerFuture)
      Await.result(combinedFuture, Duration.Inf)
    }
      wrapAsStats {
        logParser match {
          case x: TPTLoadLogParser => x.toConfig
          case x: TPTStreamLogParser =>
            x.updateErrorTableCount(tableName)
            x.toConfig
        }
      }
  }

  override protected[task] def teardown(): Unit = {
    if (loadDataSize > 0) {
      errorLogger.log()
      if (logParser.jobId != null) {
        detectTPTRun(logParser.jobId) match {
          case jobId :: Nil =>
            debug(s"attempting to kill tpt job ${logParser.jobId}")
            killTPTJob(logParser.jobId)
          case _ => ()
        }
      }
      if (logParser.jobLogFile != null)
        debug(s"run tlogview -l ${logParser.jobLogFile} to view job output")
      new File(dataPipe).delete()
    }
  }

  /**
    *
    * @param jobName
    */
  def killTPTJob(jobName: String) = {
    val cmd = Seq(twbKillBin, jobName)
    executeCmd(cmd)
  }

  /**
    *
    * This is implemented by running twbstat command and parsing the output.
    *
    * @param jobName tpt job name
    * @return
    */
  def detectTPTRun(jobName: String): Seq[String] = {
    val stream = new ByteArrayOutputStream()
    assert(executeCmd(Seq(twbStat), stdout =stream) == 0, "twbstat command failed. ensure TPT is properly installed")
    val content = new String(stream.toByteArray)
    val rgx = s"$jobName-[\\d]+".r
    content.split(System.lineSeparator())
      .map(_.trim)
      .filter(rgx.findFirstMatchIn(_).isDefined)
  }


}

object TPTLoad {

  /**
   * takes in reader Future and writer Future and provides a new Future that holds the tuple
   * of the reader and writer Future. this combined future fails fast. ie if either the reader
   * or the writer future fails the resultant future also fails immediately and doesnt wait for the
   * other future to resolve.
   *
   * @param readerFuture
   * @param writerFuture
   * @return
   */
  def monitor(readerFuture: Future[Unit], writerFuture: Future[Unit]): Future[(Unit,Unit)] = {
    val promise = Promise[(Unit, Unit)]()
    readerFuture onFailure { case th if !promise.isCompleted =>  promise.failure(th) }
    writerFuture onFailure { case th if !promise.isCompleted => promise.failure(th) }
    val res = readerFuture zip writerFuture
    promise.completeWith(res).future
  }

  /**
    * get appropriate log-parser
    *
    * @param mode
    * @return
    */
  def logParser(mode: String) = {
    mode match {
      case "fastload" => new TPTLoadLogParser(System.out)
      case "default" => new TPTStreamLogParser(System.out)
      case x => throw new RuntimeException(s"mode $x is not supported")
    }
  }


   val outputConfig =  None

   val outputConfigDesc =
    """
      |The output config can be two types depending on the mode of operation.
      |
      |**default**:
      |
      |  The output of the default mode is shown below.
      |
      |       taskname = {
      |          __stats__ = {
      |             loaded = 90
      |             error-file = 5
      |             error-table = 5
      |             rejected = 10
      |             source = 100
      |          }
      |        }
      |
      |  * *loaded*: number of records inserted
      |  * *error-file*: number of records sent to error file.
      |  * *error-table*: number of records sent to error table.
      |  * *rejected*: total number of records rejected. This is a derived field with formula error-table + error-file.
      |  * *source*: total number of records in source. This is a derived field with formula loaded + error-table + error-file.
      |
      |
      |**fastload**:
      |
      | The output of the fastload mode is
      |
      |      taskname = {
      |        __stats__ = {
      |            sent = 90
      |            loaded = 78
      |            err_table1 = 5
      |            err_table2 = 5
      |            duplicate = 2
      |            err_file = 10
      |            source = 100
      |            rejected = 22
      |        }
      |      }
      |
      |  * *sent*: number of records sent to the database. i.e. (source - err_file)
      |  * *loaded*: number of records inserted in the target table
      |  * *err_table1*: number of records sent to the error table 1
      |  * *err_table2*: number of records sent to the error table 2
      |  * *duplicate*: number of records ignored for being duplicates
      |  * *err_file*: number of records sent to error-file
      |  * *source*: number of rows in the source file(s). This is a derived field with formula (sent + err_file)
      |  * *rejected*: number of records rejected. This is a derived field with formula (err_table1 + err_table2 + duplicate + err_file)
      |
    """.stripMargin


}


