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

import com.groupon.artemisia.TestSpec
import TPTErrorLogger._
import com.groupon.artemisia.task.database.TestDBInterFactory
/**
  * Created by chlr on 9/26/16.
  */
class ErrLoggerSpec extends TestSpec {

  "LoadOperErrLogger" must "handle load operator error logger" in {
    val dummy_table = "target_table"
    val dbInterface = TestDBInterFactory.withDefaultDataLoader(dummy_table)
    val errorFile = this.getClass.getResource("/errorfile.txt").getFile
    val errlogger = new LoadOperErrLogger(tableName=dummy_table, errorFile = errorFile, dbInterface = dbInterface) {
      override val errorSql =  s"SELECT col2, col1, col2 FROM $dummy_table"
      etTableContent must have length 3
      etTableContent.head must be ("FieldName","Rowcount","ErrorMessage")
      etTableContent(1) must be ("foo", "1" ,"foo")
      etTableContent(2) must be ("bar", "2" ,"bar")
      errorFileContent.head must be ("col1,data1")
      errorFileContent(1) must be ("col2,data2")
      errorFileContent(2) must be ("col3,data3")
    }
    errlogger.log()
  }

  "StreamOperErrLogger" must "handle stream operator error logger" in {

    val dummy_table = "target_table"
    val dbInterface = TestDBInterFactory.withDefaultDataLoader(dummy_table)
    val errorFile = this.getClass.getResource("/errorfile.txt").getFile
    val errlogger = new StreamOperErrLogger(tableName=dummy_table, errorFile = errorFile, dbInterface = dbInterface) {
      override val errorSql =  s"SELECT col2, col1 FROM $dummy_table"
      etTableContent must have length 3
      etTableContent.head must be ("ErrorMessage", "Rowcount")
      etTableContent(1) must be ("foo", "1")
      etTableContent(2) must be ("bar", "2")
      errorFileContent.head must be ("col1,data1")
      errorFileContent(1) must be ("col2,data2")
      errorFileContent(2) must be ("col3,data3")
    }
    errlogger.log()
  }

}
