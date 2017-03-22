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

import java.sql.SQLException
import javax.xml.bind.DatatypeConverter
import com.groupon.artemisia.TestSpec
import com.groupon.artemisia.task.TaskContext

/**
  * Created by chlr on 7/12/16.
  */
class FastLoadErrorRecordHandlerSpec extends TestSpec {

  "FastLoadErrorRecordHandler" must "parse Exceptions" in {
    val tableName = s"fastload_error_table"
    val errorHandler = new FastLoadErrorRecordHandler(tableName)
    val msg1 =
      s"""|Row 1 in FastLoad table "sandbox"."${tableName}_ERR_1" contains the following data:
         |ErrorCode=5317
         |ErrorFieldName=F_col1
         |ActualDataParcelLength=15
         |DataParcel: byte array length 15 (0xf), offset 0 (0x0), dump length 15 (0xf)
         |00000   00 00 00 00 3c 00 04 79 79 79 79 00 11 65 85     |....<..yyyy..e.| """.stripMargin
    val exception1 = new SQLException(msg1)
    errorHandler.parseException(exception1)

    val msg2 =
      s"""|[Teradata JDBC Driver] [TeraJDBC 15.10.00.14] [Error 1160] [SQLState HY000] Row 1 in FastLoad table "sandbox"."${tableName}_ERR_2" contains the following data:
         |col1=4
         |col2=xxxx
         |col3=2014-01-01""".stripMargin
    val exception2 = new SQLException(msg2)
    errorHandler.parseException(exception2)

    val file_et = TaskContext.getTaskFile("error_et.txt")
    val file_uv = TaskContext.getTaskFile("error_uv.txt")

    errorHandler.close()

    scala.io.Source.fromFile(file_et).mkString("") must be (s"F_col1,5317,${new String(DatatypeConverter.parseHexBinary("000000003c00047979797900116585"))}\n")
    scala.io.Source.fromFile(file_uv).mkString("") must be ("col1=4,col2=xxxx,col3=2014-01-01\n")

  }

}
