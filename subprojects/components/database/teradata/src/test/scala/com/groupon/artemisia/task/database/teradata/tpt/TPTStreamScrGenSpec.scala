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
  * Created by chlr on 9/27/16.
  */
class TPTStreamScrGenSpec extends TestSpec {

  "TPTStreamScrGen" must "set correct target attributes (without quoting)" in {
    new TPTStreamScrGen(
      TPTLoadConfig("database", "table", "/var/path/dir","file"),
      TPTLoadSetting(
        skipRows = 5,
        delimiter = '\t',
        errorFile = "/var/error/file.txt"),
      DBConnection.getDummyConnection
    ) {
        sourceAttributes("VALIDUTF8") must be ("VARCHAR" -> "UTF8")
        sourceAttributes("NAMEDPIPETIMEOUT") must be ("INTEGER" -> "120")
        sourceAttributes("FILENAME") must be ("VARCHAR" -> "file")
        sourceAttributes("TEXTDELIMITERHEX") must be ("VARCHAR" -> "09")
        sourceAttributes("INDICATORMODE") must be ("VARCHAR" -> "N")
        sourceAttributes("OPENMODE") must be ("VARCHAR" -> "Read")
        sourceAttributes("FORMAT") must be ("VARCHAR" -> "DELIMITED")
        sourceAttributes("DIRECTORYPATH") must be ("VARCHAR" -> "/var/path/dir")
        sourceAttributes("ROWERRFILENAME") must be ("VARCHAR" -> "/var/error/file.txt")
        sourceAttributes("SKIPROWS") must be ("INTEGER" -> "5")
        sourceAttributes("BUFFERSIZE") must be ("INTEGER" -> "524288")
        sourceAttributes.keys must not contain "QUOTEDDATA"
        sourceAttributes.keys must not contain "ESCAPEQUOTEDELIMITER"
        sourceAttributes.keys must not contain "OPENQUOTEMARK"
    }
  }

  it must "set correct target attributes with quoting" in {
    new TPTStreamScrGen(
      TPTLoadConfig("database", "table", "/var/path/dir","file"),
      TPTLoadSetting(
        quoting = true,
        skipRows = 5,
        delimiter = '\t',
        errorFile = "/var/error/file.txt"),
      DBConnection.getDummyConnection
    ) {
        sourceAttributes("VALIDUTF8") must be ("VARCHAR" -> "UTF8")
        sourceAttributes("NAMEDPIPETIMEOUT") must be ("INTEGER" -> "120")
        sourceAttributes("FILENAME") must be ("VARCHAR" -> "file")
        sourceAttributes("TEXTDELIMITERHEX") must be ("VARCHAR" -> "09")
        sourceAttributes("INDICATORMODE") must be ("VARCHAR" -> "N")
        sourceAttributes("OPENMODE") must be ("VARCHAR" -> "Read")
        sourceAttributes("FORMAT") must be ("VARCHAR" -> "DELIMITED")
        sourceAttributes("DIRECTORYPATH") must be ("VARCHAR" -> "/var/path/dir")
        sourceAttributes("ROWERRFILENAME") must be ("VARCHAR" -> "/var/error/file.txt")
        sourceAttributes("SKIPROWS") must be ("INTEGER" -> "5")
        sourceAttributes("BUFFERSIZE") must be ("INTEGER" -> "524288")
        sourceAttributes("QUOTEDDATA") must be ("VARCHAR", "Optional")
        sourceAttributes("ESCAPEQUOTEDELIMITER") must be ("VARCHAR", "\\")
        sourceAttributes("OPENQUOTEMARK") must be ("VARCHAR", "\"")
    }
  }


  it must "set correctly set target attributes" in {
    new TPTStreamScrGen(
      TPTLoadConfig("database", "table", "/var/path/dir","file"),
      TPTLoadSetting(),
      DBConnection.getDummyConnection
    ) {
        info(targetAttributes.mkString(System.lineSeparator()))
        targetAttributes("ERRORTABLE") must be ("VARCHAR" -> "database.table_ET")
        targetAttributes("DropErrorTable") must be ("VARCHAR" -> "Yes")
        targetAttributes("ArraySupport") must be ("VARCHAR" -> "On")
        targetAttributes("PackMaximum") must be ("VARCHAR" -> "Yes")
    }
  }

  it must "generate schema" in {
    new TPTStreamScrGen(
      TPTLoadConfig("database", "table", "/var/path/dir","file"),
      TPTLoadSetting(),
      DBConnection.getDummyConnection
    ) {
      override protected lazy val tableMetadata = Seq(
        ("col1", "I1", 25: Short, "col1_1", "Y"),
        ("col2", "I1", 25: Short, "col2_2", "Y")
      )
      this.schemaDefinition.split(System.lineSeparator) must contain theSameElementsInOrderAs
        Seq("\"col1_1\" VARCHAR(25)",",\"col2_2\" VARCHAR(25)")
    }
  }


  it must "must generate insert column list" in {
    new TPTStreamScrGen (
      TPTLoadConfig("database", "table", "/var/path/dir","file"),
      TPTLoadSetting(),
      DBConnection.getDummyConnection) {
      override protected lazy val tableMetadata = Seq(
        ("col1", "I1", 25: Short, "col1_1", "Y"),
        ("col2", "I1", 25: Short, "col2_2", "Y")
      )
      insertColumnList must be (
        """"col1"
          |,"col2"""".stripMargin)
    }
  }

  it must "must generate value column list" in {
    new TPTStreamScrGen (
      TPTLoadConfig("database", "table", "/var/path/dir","file"),
      TPTLoadSetting(),
      DBConnection.getDummyConnection) {
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
    new TPTStreamScrGen (
      TPTLoadConfig("database", "table", "/var/path/dir","file"),
      TPTLoadSetting(),
      DBConnection.getDummyConnection) {
      override protected lazy val tableMetadata = Seq(
        ("col1", "I1", 25: Short, "col1_1", "Y"),
        ("col2", "I1", 25: Short, "col2_2", "Y")
      )
      selectColumnList must be (""""col1_1" as "col1_1",
                                  |"col2_2" as "col2_2"""".stripMargin)
    }
  }


  it must "generate select list with custom null string" in {
    new TPTStreamScrGen (
      TPTLoadConfig("database", "table", "/var/path/dir","file"),
      TPTLoadSetting(nullString = Some("\\T")),
      DBConnection.getDummyConnection) {
      override protected lazy val tableMetadata = Seq(
        ("col1", "I1", 25: Short, "col1_1", "N"),
        ("col2", "I1", 25: Short, "col2_2", "Y")
      )
      info(selectColumnList)
      selectColumnList must be (""""col1_1" as "col1_1",
                                  |CASE WHEN "col2_2" ='\T' THEN NULL ELSE "col2_2" END as "col2_2"""".stripMargin)
    }
  }


}
