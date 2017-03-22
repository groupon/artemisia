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

package com.groupon.artemisia.core

import java.io.{BufferedWriter, File, FileWriter}
import com.typesafe.config.{Config, ConfigFactory}
import com.groupon.artemisia.task.Component
import com.groupon.artemisia.util.HoconConfigUtil
import com.groupon.artemisia.util.HoconConfigUtil.Handler

/**
  * Created by chlr on 7/14/16.
  */
object ReferenceGenerator {

  def main(args: Array[String]): Unit = {
    val globalConfig = ConfigFactory parseFile new File(args(0))
    val components = globalConfig.asMap[String](s"${Keywords.Config.SETTINGS_SECTION}.components") map {
      case (name,component) => { Class.forName(component).getConstructor(classOf[String]).newInstance(name).asInstanceOf[Component] }
    }
   val result = components.foldLeft(ConfigFactory.empty()){ (carry: Config, y: Component) => y.consolidateDefaultConfig withFallback carry }
    val writer = new BufferedWriter(new FileWriter(args(0)))
    writer.write(HoconConfigUtil.render(globalConfig.withValue(Keywords.Config.DEFAULTS, result.root()).root()))
    writer.close()
  }

}
