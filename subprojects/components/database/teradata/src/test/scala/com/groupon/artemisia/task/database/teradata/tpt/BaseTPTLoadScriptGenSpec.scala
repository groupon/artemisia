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
import com.groupon.artemisia.task.settings.DBConnection

/**
  * Created by chlr on 9/14/16.
  */

class BaseTPTLoadScriptGenSpec extends TestSpec {


  "LoadScriptGenerator" must "generate load operator parameters" in {
    new BaseTPTLoadScriptGen {
      override protected val loadSetting = TPTLoadSetting(errorLimit = 1000)
      override protected val tptLoadConfig: TPTLoadConfig = TPTLoadConfig("database", "table", "/var/path", "input.pipe")
      override protected val dbConnection: DBConnection = DBConnection.getDummyConnection
      override protected val targetAttributes: Map[String, (String, String)] = Map(
        "ERRORTABLE" -> ("VARCHAR", "database.table_ET"),
        "ERRORLIMIT" -> ("INTEGER", "1000"),
        "TARGETTABLE" -> ("VARCHAR", "database.table")
      )
      override protected val loadType: String = "STREAM"
      override protected val preExecuteSQLs: Seq[String] = Nil
      targetAttributes("ERRORTABLE") must be("VARCHAR", "database.table_ET")
      targetAttributes("TARGETTABLE") must be("VARCHAR", "database.table")
      targetAttributes("ERRORLIMIT") must be("INTEGER", "1000")
    }
  }


  it must "generate dataconnector operator parameters with no quoting" in {
    new BaseTPTLoadScriptGen {
      override protected val loadSetting = TPTLoadSetting(errorLimit = 1000, delimiter = '\t')
      override protected val tptLoadConfig: TPTLoadConfig = TPTLoadConfig("database", "table", "/var/path", "input.pipe")
      override protected val dbConnection: DBConnection = DBConnection.getDummyConnection
      override protected val targetAttributes: Map[String, (String, String)] = Map()
      override protected val loadType: String = "STREAM"
      override protected val preExecuteSQLs: Seq[String] = Nil
      sourceAttributes("VALIDUTF8") must be("VARCHAR", "UTF8")
      sourceAttributes("NAMEDPIPETIMEOUT") must be("INTEGER", "120")
      sourceAttributes("REPLACEMENTUTF8CHAR") must be("VARCHAR", " ")
      sourceAttributes("FILENAME") must be("VARCHAR", "input.pipe")
      sourceAttributes("TEXTDELIMITERHEX") must be("VARCHAR", "09")
      sourceAttributes("INDICATORMODE") must be("VARCHAR", "N")
      sourceAttributes("OPENMODE") must be("VARCHAR", "Read")
      sourceAttributes("FORMAT") must be("VARCHAR", "DELIMITED")
      sourceAttributes("DIRECTORYPATH") must be("VARCHAR", "/var/path")
      sourceAttributes("SKIPROWS") must be("INTEGER", "0")
      sourceAttributes("BUFFERSIZE") must be("INTEGER", "524288")
      sourceAttributes must not contain "QUOTEDDATA"
      sourceAttributes must not contain "ESCAPEQUOTEDELIMITER"
      sourceAttributes must not contain "OPENQUOTEMARK"
    }
  }

  it must "generate dataconnector operator parameters with quoting" in {
    new BaseTPTLoadScriptGen {
      override protected val loadSetting = TPTLoadSetting(errorLimit = 1000, delimiter = '|',
        quoting = true, quotechar = '~', escapechar = '-')
      override protected val tptLoadConfig: TPTLoadConfig = TPTLoadConfig("database", "table", "/var/path", "input.pipe")
      override protected val dbConnection: DBConnection = DBConnection.getDummyConnection
      override protected val targetAttributes: Map[String, (String, String)] = Map()
      override protected val loadType: String = "STREAM"
      override protected val preExecuteSQLs: Seq[String] = Nil
      sourceAttributes("QUOTEDDATA") must be("VARCHAR", "Optional")
      sourceAttributes("ESCAPEQUOTEDELIMITER") must be("VARCHAR", "-")
      sourceAttributes("OPENQUOTEMARK") must be("VARCHAR", "~")
      sourceAttributes("TEXTDELIMITERHEX") must be("VARCHAR", "7c")
    }
  }


  it must "generate schema" in {
    new BaseTPTLoadScriptGen {
      override protected val tptLoadConfig = TPTLoadConfig("database", "table", "/var/path", "input.pipe")
      override protected val dbConnection: DBConnection = DBConnection.getDummyConnection
      override protected val loadSetting = TPTLoadSetting()
      override protected val targetAttributes: Map[String, (String, String)] = Map()
      override protected val loadType: String = "STREAM"
      override protected val preExecuteSQLs: Seq[String] = Nil
      override protected lazy val tableMetadata = Seq(
        ("col1", "I1", 25: Short, "col1_1", "Y"),
        ("col2", "I1", 25: Short, "col2_2", "Y")
      )
      this.schemaDefinition.split(System.lineSeparator) must contain theSameElementsInOrderAs
        Seq("\"col1_1\" VARCHAR(25)",",\"col2_2\" VARCHAR(25)")
    }
  }

  it must "generate insert column list" in {
    new BaseTPTLoadScriptGen {
      override protected val tptLoadConfig = TPTLoadConfig("database", "table", "/var/path", "input.pipe")
      override protected val dbConnection = DBConnection.getDummyConnection
      override protected val loadSetting = TPTLoadSetting()
      override protected val targetAttributes: Map[String, (String, String)] = Map()
      override protected val loadType: String = "STREAM"
      override protected val preExecuteSQLs: Seq[String] = Nil
      override protected lazy val tableMetadata = Seq(
        ("col1", "I1", 25: Short, "col1_1", "Y"),
        ("col2", "I1", 25: Short, "col2_2", "Y")
      )
      insertColumnList must be (
        """"col1"
          |,"col2"""".stripMargin)
    }
  }

  it must "generate value column list" in {
    new BaseTPTLoadScriptGen {
      override protected val tptLoadConfig = TPTLoadConfig("database", "table", "/var/path", "input.pipe")
      override protected val dbConnection = DBConnection.getDummyConnection
      override protected val loadSetting = TPTLoadSetting()
      override protected val targetAttributes: Map[String, (String, String)] = Map()
      override protected val loadType: String = "STREAM"
      override protected val preExecuteSQLs: Seq[String] = Nil
      override protected lazy val tableMetadata = Seq(
        ("col1", "I1", 25: Short, "col1_1", "Y"),
        ("col2", "I1", 25: Short, "col2_2", "Y")
      )
      valueColumnList must be (
        """:col1_1
          |,:col2_2""".stripMargin
      )
    }
  }

  it must "generate select list" in {
    new BaseTPTLoadScriptGen {
      override protected val tptLoadConfig = TPTLoadConfig("database", "table", "/var/path", "input.pipe")
      override protected val dbConnection = DBConnection.getDummyConnection
      override protected val loadSetting = TPTLoadSetting()
      override protected val targetAttributes: Map[String, (String, String)] = Map()
      override protected val loadType: String = "STREAM"
      override protected val preExecuteSQLs: Seq[String] = Nil
      override protected lazy val tableMetadata = Seq(
        ("col1", "I1", 25: Short, "col1_1", "N"),
        ("col2", "I1", 25: Short, "col2_2", "Y")
      )
      selectColumnList must be (""""col1_1" as "col1_1",
                                  |"col2_2" as "col2_2"""".stripMargin)
    }
  }


  it must "generate select list with custom null string" in {
    new BaseTPTLoadScriptGen {
      override protected val tptLoadConfig = TPTLoadConfig("database", "table", "/var/path", "input.pipe")
      override protected val dbConnection = DBConnection.getDummyConnection
      override protected val loadSetting = TPTLoadSetting(nullString= Some("\\T"))
      override protected val targetAttributes: Map[String, (String, String)] = Map()
      override protected val loadType: String = "STREAM"
      override protected val preExecuteSQLs: Seq[String] = Nil
      override protected lazy val tableMetadata = Seq(
        ("col1", "I1", 25: Short, "col1_1", "N"),
        ("col2", "I1", 25: Short, "col2_2", "Y")
      )
      selectColumnList must be (""""col1_1" as "col1_1",
                                  |CASE WHEN "col2_2" ='\T' THEN NULL ELSE "col2_2" END as "col2_2"""".stripMargin)
    }
  }

}
