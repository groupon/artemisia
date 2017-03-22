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

import com.typesafe.config.{Config, ConfigValueFactory}
import com.groupon.artemisia.task.ConfigurationNode
import com.groupon.artemisia.task.database.BasicLoadSetting
import com.groupon.artemisia.util.HoconConfigUtil.Handler
import com.groupon.artemisia.util.MemorySize

/**
  * Created by chlr on 6/30/16.
  */
case class TeraLoadSetting(override val skipRows: Int = 0,
                           override val delimiter: Char = ',',
                           override val quoting: Boolean = false,
                           override val quotechar: Char = '"',
                           override val escapechar: Char = '\\',
                           override val truncate: Boolean = false,
                           override val mode: String = "default",
                           override val batchSize: Int = 100,
                           override val errorTolerance: Option[Double] = None,
                           override val bulkLoadThreshold: Long = 104857600)
  extends BaseTeraLoadSetting(skipRows, delimiter, quoting, quotechar, escapechar, truncate, mode, batchSize, errorTolerance, bulkLoadThreshold) {

  override def setting: String = {
    BasicLoadSetting(skipRows, delimiter, quoting, quotechar, escapechar, truncate, mode, batchSize, errorTolerance).setting +
    s"bulk-threshold: $bulkLoadThreshold"
  }

  /**
    *
    * @param batchSize batch size
    * @param mode      load mode
    * @return
    */
  override def create(batchSize: Int, mode: String): BaseTeraLoadSetting = {
    copy(batchSize = batchSize, mode = mode)
  }

}

object TeraLoadSetting extends ConfigurationNode[TeraLoadSetting] {

  val structure = BasicLoadSetting.structure
            .withValue("bulk-threshold", ConfigValueFactory.fromAnyRef("100M @info()"))
            .withoutPath("batch-size")

  val fieldDescription = (BasicLoadSetting.fieldDescription - "batch-size") ++: Map(
    "mode" -> ("mode of loading the table. The allowed modes are" -> Seq("fastload", "small", "auto")),
    "bulk-threshold" -> "size of the source file(s) above which fastload mode will be selected if auto mode is enabled"
  )

  val defaultConfig = BasicLoadSetting.defaultConfig
            .withValue("bulk-threshold", ConfigValueFactory.fromAnyRef("100M"))


  def apply(config: Config): TeraLoadSetting = {
    val loadSetting = BasicLoadSetting(config)
    TeraLoadSetting(loadSetting.skipRows, loadSetting.delimiter, loadSetting.quoting
      ,loadSetting.quotechar, loadSetting.escapechar, loadSetting.truncate, loadSetting.mode
      ,if (loadSetting.mode == "fastload")  80000  else 1000
      ,loadSetting.errorTolerance,config.as[MemorySize]("bulk-threshold").toBytes
    )
  }


}
