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

import com.groupon.artemisia.task.database.teradata.{DBInterfaceFactory, TeraUtils}
import com.groupon.artemisia.task.settings.DBConnection
import com.groupon.artemisia.util.DocStringProcessor.StringUtil

/**
 * Created by chlr on 9/13/16.
 */

/**
 * TPT Script generator interface.
 *
 */
trait BaseTPTLoadScriptGen {

  protected val tptLoadConfig: TPTLoadConfig

  protected val loadSetting: TPTLoadSetting

  protected val dbConnection: DBConnection

  protected implicit lazy val dbInterface = DBInterfaceFactory.getInstance(dbConnection)

  protected lazy val tableMetadata = TeraUtils.tableMetadata(tptLoadConfig.databaseName, tptLoadConfig.tableName)

  final lazy val baseTargetAttributes = Map(
    "TRACELEVEL" -> ("VARCHAR" -> "None"),
    "PACK" -> ("INTEGER" -> "2000"),
    "PACKMAXIMUM" -> ("VARCHAR" -> "No"),
    "ERRORLIMIT" -> ("INTEGER" -> loadSetting.errorLimit.toString),
    "TARGETTABLE" -> ("VARCHAR",s"${tptLoadConfig.databaseName}.${tptLoadConfig.tableName}"),
    "LOGTABLE" -> ("VARCHAR",s"${tptLoadConfig.databaseName}.${tptLoadConfig.tableName}_LG")
  )


  /**
    * customized target attributes
    */
  protected val targetAttributes: Map[String,(String, String)]


  /**
    * final computed target attribute
    */
  final lazy val computedTargetAttr = (baseTargetAttributes ++ targetAttributes).map(x => x._1.toUpperCase -> x._2)


  protected val loadType: String

  /**
    * pre job sqls goes here.
    * sqls to drop work, error, log tables goes here.
 *
    * @return
    */
  protected val preExecuteSQLs: Seq[String]


  final def ddlStepSQLs = {
    require(preExecuteSQLs.nonEmpty, "DDL step sql cannot be empty. a minimum of one sql is required")
    preExecuteSQLs map { x => s"'$x'" } mkString s",${System.lineSeparator}"
  }

  /**
    * Map of source data-connector attributes
 *
    * @return
    */
  protected def sourceAttributes = {
    val quoteSettings = loadSetting.quoting match {
      case false => Map[String, (String, String)]()
      case true => Map (
        "ESCAPEQUOTEDELIMITER" -> ("VARCHAR", loadSetting.escapechar.toString),
        "OPENQUOTEMARK" -> ("VARCHAR", loadSetting.quotechar.toString),
        "QUOTEDDATA" -> ("VARCHAR", "Optional")
      )
    }
    quoteSettings ++ Map(
      "SKIPROWS" -> ("INTEGER",loadSetting.skipRows.toString),
      "OPENMODE" -> ("VARCHAR", "Read"),
      "TEXTDELIMITERHEX" -> ("VARCHAR" -> String.format("%02x", new Integer(loadSetting.delimiter.toInt))),
      "DIRECTORYPATH" -> ("VARCHAR" -> tptLoadConfig.directory),
      "NAMEDPIPETIMEOUT" -> ("INTEGER" -> "120"),
      "FILENAME" -> ("VARCHAR" -> tptLoadConfig.fileName),
      "REPLACEMENTUTF8CHAR" -> ("VARCHAR" -> " "),
      "INDICATORMODE" -> ("VARCHAR","N"),
      "FORMAT" -> ("VARCHAR", "DELIMITED"),
      "VALIDUTF8" -> ("VARCHAR", "UTF8"),
      "BUFFERSIZE" -> ("INTEGER", "524288"),
      "ROWERRFILENAME" -> ("VARCHAR", loadSetting.errorFile)
    )
  }

  /**
    * render target and source attributes in TPT format.
    * {{{
    *   <TYPE> <ATTRIBUTE_NAME> = <ATTRIBUTE_VALUE>
    * }}}
    *
    * @param attrs
    * @return
    */
  protected def renderAttributes(attrs: Map[String,(String, String)]) = {
    attrs map {
      case (attrName, (attrType, attrValue))
        if attrType.toUpperCase.trim == "VARCHAR" => s"VARCHAR $attrName = '$attrValue'"
      case (attrName, (attrType, attrValue)) => s"$attrType $attrName = $attrValue"
    } mkString s",${System.lineSeparator}"
  }

  protected def schemaDefinition = tableMetadata map { x => s""""${x._4}" VARCHAR(${x._3})""" } mkString s"${System.lineSeparator},"

  protected def insertColumnList = tableMetadata map { x => s""""${x._1}""""} mkString s"${System.lineSeparator},"

  protected def valueColumnList = tableMetadata map { x => s":${x._4}"} mkString s"${System.lineSeparator},"

  protected def selectColumnList = {
    tableMetadata map { x => x._5.trim.toUpperCase flatMap {
      case 'N' => s""""${x._4}" as "${x._4}""""
      case 'Y' if loadSetting.nullString.isEmpty => s""""${x._4}" as "${x._4}""""
      case _ =>
        s"""CASE WHEN "${x._4}" ='${loadSetting.nullString.get}' THEN NULL ELSE "${x._4}" END as "${x._4}""""
    }
    } mkString s",${System.lineSeparator()}"
  }



  /**
   * tpt script
   */
   final def tptScript =
    s"""
       |USING CHARACTER SET UTF8
       |DEFINE JOB load_${tptLoadConfig.databaseName}_${tptLoadConfig.tableName} (
       |    DEFINE OPERATOR tpt_writer
       |    TYPE $loadType
       |    SCHEMA *
       |    ATTRIBUTES
       |    (
       |        VARCHAR UserName,
       |        VARCHAR UserPassword,
       |        VARCHAR TdpId,
       |        ${renderAttributes(computedTargetAttr).ident(8)}
       |    );
       |    DEFINE SCHEMA W_0_sc_load_${tptLoadConfig.databaseName}_${tptLoadConfig.tableName}_
       |    (
       |        ${schemaDefinition.ident(8)}
       |    );
       |    DEFINE OPERATOR tpt_reader
       |    TYPE DATACONNECTOR PRODUCER
       |    SCHEMA W_0_sc_load_${tptLoadConfig.databaseName}_${tptLoadConfig.tableName}_
       |    ATTRIBUTES
       |    (
       |        ${renderAttributes(sourceAttributes).ident(8)}
       |    );
       |    DEFINE OPERATOR DDL_OPERATOR ()
       |    DESCRIPTION 'DDL Operator'
       |    TYPE DDL
       |    ATTRIBUTES
       |    (
       |        VARCHAR UserName = '${dbConnection.username}',
       |        VARCHAR UserPassword = '${dbConnection.password}',
       |        VARCHAR ARRAY ErrorList = ['2580','3807','3916'],
       |        VARCHAR TdpId = '${dbConnection.hostname}:${dbConnection.port}'
       |    );
       |    Step DROP_TABLE
       |    (
       |        APPLY
       |        ${ddlStepSQLs.ident(8)}
       |        TO OPERATOR (DDL_OPERATOR);
       |    );
       |    Step LOAD_TABLE
       |    (
       |        APPLY
       |        (
       |            'INSERT INTO ${tptLoadConfig.databaseName}.${tptLoadConfig.tableName} (
       |                   ${insertColumnList.ident(19)}
       |            ) VALUES (
       |                   ${valueColumnList.ident(19)}
       |            );'
       |        )
       |        TO OPERATOR
       |        (
       |            tpt_writer[1]
       |            ATTRIBUTES
       |            (
       |            UserName = '${dbConnection.username}',
       |            UserPassword = '${dbConnection.password}',
       |            TdpId = '${dbConnection.hostname}:${dbConnection.port}'
       |            )
       |        )
       |          SELECT
       |                  ${selectColumnList.ident(19)}
       |          FROM OPERATOR(
       |              tpt_reader[1]
       |          );
       |      );
       |   );
     """.stripMargin

}

object BaseTPTLoadScriptGen {

  def create(tptLoadConfig: TPTLoadConfig, loadSetting: TPTLoadSetting, connectionProfile: DBConnection) = {
    loadSetting.mode match {
      case "fastload" => new TPTFastLoadScrGen(tptLoadConfig, loadSetting, connectionProfile)
      case "default" => new TPTStreamScrGen(tptLoadConfig, loadSetting, connectionProfile)
      case _ => throw new IllegalArgumentException(s"mode ${loadSetting.mode} is not supported")
    }
  }

}
