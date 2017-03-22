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

import com.typesafe.config.ConfigFactory
import com.groupon.artemisia.TestSpec
import com.groupon.artemisia.core.BooleanEvaluator

/**
 * Created by chlr on 5/26/16.
 */
class TaskConfigSpec extends TestSpec {

  "TaskConfig" must "parse description and expression nodes properly" in {
    val config = ConfigFactory parseString
      s""" foo = {
        |  ${BooleanEvaluator.description} = "This is a description node"
        |  ${BooleanEvaluator.expression} = "1 == 1"
        |} """.stripMargin

    val (bool: Boolean, desc: String) = TaskConfig.parseConditionsNode(config.getConfig("foo").root()) match {
      case (x,y) => BooleanEvaluator.evalBooleanExpr(x) -> y
    }
    bool mustBe true
    desc mustBe "This is a description node"
  }

  it must "parse node with description and expression" in {

    val config = ConfigFactory parseString
      """
        | foo =  "100 == 1000"
      """.stripMargin
    val (bool: Boolean, desc: String) = TaskConfig.parseConditionsNode(config.getValue("foo")) match {
      case (x,y) => BooleanEvaluator.evalBooleanExpr(x) -> y
    }
    bool mustBe false
    desc mustBe "100 == 1000"
  }


}
