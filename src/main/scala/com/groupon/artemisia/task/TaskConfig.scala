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

package com.groupon.artemisia.task

import com.typesafe.config._
import com.groupon.artemisia.core.Keywords.Task
import com.groupon.artemisia.core.{AppContext, BooleanEvaluator, Keywords}
import com.groupon.artemisia.util.HoconConfigUtil.Handler
import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._


/**
 * Created by chlr on 1/9/16.
 */


case class TaskConfig(retryLimit : Int = 1, cooldown: FiniteDuration =  1 seconds, conditions: Option[(Boolean, String)] = None,
                   ignoreFailure: Boolean = false, assertion: Option[(ConfigValue, String)] = None)

object TaskConfig {

  def apply(inputConfig: Config, appContext: AppContext): TaskConfig = {

    val default_config = ConfigFactory parseString {
      s"""
         |${Task.IGNORE_ERROR} = ${appContext.dagSetting.ignore_conditions}
         |${Keywords.Task.ATTEMPT} = ${appContext.dagSetting.attempts}
         |${Keywords.Task.COOLDOWN} = ${appContext.dagSetting.cooldown}
         |__context__ = {}
    """.stripMargin
    }

    val config = inputConfig withFallback default_config
      TaskConfig(config.as[Int](Keywords.Task.ATTEMPT),
      config.as[FiniteDuration](Keywords.Task.COOLDOWN),
      config.getAs[ConfigValue](Keywords.Task.CONDITION) map { parseConditionsNode } map { case (x,y) => BooleanEvaluator.evalBooleanExpr(x) -> y },
      config.as[Boolean](Keywords.Task.IGNORE_ERROR),
      config.getAs[ConfigValue](Keywords.Task.ASSERTION) map { parseConditionsNode }
      )
  }


  private[task] def parseConditionsNode(input: ConfigValue): (ConfigValue,String) = {
    input match {
      case x: ConfigObject => {
        x.keySet().asScala.toList.sorted match {
          case Seq(BooleanEvaluator.description, BooleanEvaluator.expression) =>
            x.toConfig.as[ConfigValue](BooleanEvaluator.expression) -> x.toConfig.as[String](BooleanEvaluator.description)
          case _ => x -> BooleanEvaluator.stringifyBoolExpr(x)
        }
      }
      case _ =>  input -> BooleanEvaluator.stringifyBoolExpr(input)
    }
  }
  
  
}
