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

import com.typesafe.config.ConfigFactory
import com.groupon.artemisia.TestSpec

/**
  * Created by chlr on 5/26/16.
  */
class BooleanEvaluatorSpec extends TestSpec {

  val stringExpr = " 1 == 0 "
  val arrExpr1 = ConfigFactory parseString s"""foo = [ "1 == 1", "2 == 2", "3 == 2" ]"""
  val arrExpr2 = ConfigFactory parseString s"""foo = [ "1 == 1", "2 == 2", " 3 == 3" ]"""
  val objExpr1 = ConfigFactory parseString
    """
      | foo = {
      |  or = [ "1 == 0", " 1 == 1" ]
      |  and = [ "1 == 1", " 2 == 2 " ]
      |}
    """.stripMargin
  val objExpr2 = ConfigFactory parseString
    """
      | foo = {
      |  or = [ "1 == 0", " 1 == 1" ]
      |  and = [ "1 == 1", " 2 == 1" ]
      |}
    """.stripMargin


  "BooleanEvaluator" must "parse expression accurately" in {

    BooleanEvaluator evalBooleanExpr stringExpr mustBe false
    BooleanEvaluator evalBooleanExpr arrExpr1.getAnyRefList("foo") mustBe false
    BooleanEvaluator evalBooleanExpr arrExpr2.getAnyRefList("foo") mustBe true
    BooleanEvaluator evalBooleanExpr objExpr1.getConfig("foo").root().unwrapped() mustBe true
    BooleanEvaluator evalBooleanExpr objExpr2.getConfig("foo").root().unwrapped() mustBe false
    BooleanEvaluator evalBooleanExpr true mustBe true
    BooleanEvaluator evalBooleanExpr false mustBe false
    BooleanEvaluator evalBooleanExpr objExpr1.getValue("foo") mustBe true
    BooleanEvaluator evalBooleanExpr objExpr2.getValue("foo") mustBe false

  }


}
