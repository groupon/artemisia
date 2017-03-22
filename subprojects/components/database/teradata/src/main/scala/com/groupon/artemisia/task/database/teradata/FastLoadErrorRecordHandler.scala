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

package com.groupon.artemisia.task.database.teradata

import java.io.{BufferedWriter, FileWriter}
import javax.xml.bind.DatatypeConverter
import com.groupon.artemisia.core.AppLogger
import com.groupon.artemisia.task.TaskContext
import com.groupon.artemisia.task.database.DBUtil
import com.groupon.artemisia.util.Util
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

/**
  * Created by chlr on 7/6/16.
  */
class FastLoadErrorRecordHandler(tableName: String) {

  val etErrorFileWriter = new BufferedWriter(new FileWriter(TaskContext.getTaskFile("error_et.txt")))
  val uvErrorFileWriter = new BufferedWriter(new FileWriter(TaskContext.getTaskFile("error_uv.txt")))
  var etRowsStats = mutable.Map[(String, Int), Int]().withDefault(x => 0)
  var uvRowCounter = 0

  def parseException(th: Throwable) = {
    val message = th.getMessage.split("\\r?\\n")
    val parsedTableName = DBUtil.parseTableName(tableName)._2
    if (message.head.contains(s"""${parsedTableName}_ERR_1""")) {
      processET(message)
    }
    else if (message.head.contains(s"""${parsedTableName}_ERR_2""")) {
      processUV(message)
    }
  }

  private def processET(message: Seq[String]) = {

    val errorCodeRgx = "ErrorCode=(.+)".r
    val errorFieldRgx = "ErrorFieldName=(.+)".r
    val dataParcelLenRgx = "ActualDataParcelLength=(.+)".r

    val errorCode = Try(errorCodeRgx.findFirstMatchIn(message(1)).map(_.group(1).toInt).getOrElse(-1)) match {
      case Success(x) => x
      case Failure(th) => -1
    }
    val errorField = Try(errorFieldRgx.findFirstMatchIn(message(2)).map(_.group(1)).getOrElse("")) match {
      case Success(x) => x
      case Failure(th) => ""
    }
    val dataParcel = Try(dataParcelLenRgx.findFirstMatchIn(message(3)).map(_.group(1).toInt).getOrElse(-1)) match {
      case Success(x) if x > 0 => {
        val rgx = s"[0-9]{5}\\s+(.{${x * 3}})".r
        Try(rgx.findFirstMatchIn(message(5)).map(x => DatatypeConverter.parseHexBinary(x.group(1).replace(" ",""))).map(new String(_)).getOrElse("")) match {
          case Success(y) => y
          case _ => ""
        }
      }
      case _ => println(s"data parcel length 0") ; ""
    }
    etRowsStats(errorField -> errorCode) = etRowsStats(errorField -> errorCode) + 1
    etErrorFileWriter.write(s"$errorField,$errorCode,$dataParcel${System.lineSeparator()}")
  }

  private def processUV(message: Seq[String]) = {
    uvRowCounter += 1
    uvErrorFileWriter.write(message.tail.mkString(",")+System.lineSeparator())
  }

  private def displayETRecords() = {
    if (etRowsStats.nonEmpty) {
      val content = Array("Errorfield", "Errorcode", "Rowcount") +:
        etRowsStats.toArray.map { case (key, value) => Array(key._1, key._2.toString, value.toString) }
      AppLogger info
        s"""
         /Summary of ET records
         /${Util.prettyPrintAsciiTable(content).mkString(System.lineSeparator())}
       """.stripMargin('/')
      }
  }

  def close() = {
    etErrorFileWriter.close()
    uvErrorFileWriter.close()
    displayETRecords()
  }
}
