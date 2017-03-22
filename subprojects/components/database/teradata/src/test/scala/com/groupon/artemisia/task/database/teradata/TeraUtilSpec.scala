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

import com.groupon.artemisia.TestSpec
import com.groupon.artemisia.task.database.TestDBInterFactory

/**
  * Created by chlr on 9/16/16.
  */
class TeraUtilSpec extends TestSpec {

  "TeraUtil" must "retrieve table column metadata" in {
    implicit val dbInterface = TestDBInterFactory.withDefaultDataLoader("Columns", database = "dbc", createTestTable = false)
    dbInterface.execute("CREATE SCHEMA DBC;")
    dbInterface.execute(
      """
        |CREATE TABLE dbc.Columns
        |(
        | ColumnId INT,
        | ColumnName varchar(30),
        | Tablename varchar(30),
        | Databasename varchar(30),
        | ColumnType varchar(5),
        | decimaltotaldigits INT,
        | ColumnLength int,
        | Nullable char(1)
        |);
      """.stripMargin)
    dbInterface.execute("INSERT INTO dbc.Columns VALUES (1, 'col1', 'tablename', 'databasename', 'DA', 10, 21,'Y')")
    dbInterface.execute("INSERT INTO dbc.Columns VALUES (2, 'col2', 'tablename', 'databasename', 'DA', 10, 21,'Y')")
    val metadata = TeraUtils.tableMetadata("databasename", "tablename")
    metadata must contain theSameElementsInOrderAs Seq(("col1", "DA", 15, "col1_1_", "Y"),("col2","DA",15,"col2_2_","Y"))
  }

}

