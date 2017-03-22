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

import com.groupon.artemisia.task.settings.LoadSetting


/**
  *
  * @param skipRows no of rows to skip in load file
  * @param delimiter delimiter of the load file
  * @param quoting is file quoted
  * @param quotechar quote char used to enclose fields.
  * @param escapechar escape character to escape special symbols
  * @param truncate truncate target table
  * @param mode mode of operation.
  * @param batchSize size of batch insert
  * @param errorTolerance tolerance factor records rejection during load
  * @param bulkLoadThreshold threshold load size for fastload
  */
abstract class BaseTeraLoadSetting(override val skipRows: Int = 0,
                                   override val delimiter: Char = ',',
                                   override val quoting: Boolean = false,
                                   override val quotechar: Char = '"',
                                   override val escapechar: Char = '\\',
                                   override val truncate: Boolean = false,
                                   override val mode: String = "default",
                                   override val batchSize: Int = 100,
                                   override val errorTolerance: Option[Double] = None,
                                   val bulkLoadThreshold: Long) extends
  LoadSetting(skipRows, delimiter, quoting, quotechar, escapechar, truncate, mode, batchSize, errorTolerance)  {

  /**
    *
    * @param batchSize batch size
    * @param mode load mode
    * @return
    */
  def create(batchSize: Int, mode: String): BaseTeraLoadSetting

}

