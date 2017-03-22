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

package com.groupon.artemisia.util

import com.typesafe.config.ConfigFactory
import HoconConfigUtil.configToConfigEnhancer
import com.groupon.artemisia.TestSpec

/**
 * Created by chlr on 4/25/16.
 */
class HoconConfigEnhancerSpec extends TestSpec {

  "ConfigEnhancerSpec" must "resolve variable nested at arbitary depth" in {
    val testData = ConfigFactory parseString
      """
        |{
        |        "foo1": "bar1",
        |        "foo2": "bar2",
        |        "key1": "level1",
        |        "key2": [10, 20.9, {
        |                "key3": "level2",
        |                "key4": [{
        |                        "key5": "level3",
        |                        "key7": "${foo2}"
        |                }]
        |        }, "level1"],
        |        "key8": {
        |                "key9": {
        |                        "key10": "level3",
        |                        "key11": ["level4", ["level5", {
        |                                "key80": "${foo1}"
        |                        }]]
        |                }
        |        }
        |}
      """.stripMargin
    val resolvedConfig = testData.hardResolve()
    resolvedConfig.getAnyRefList("key2").get(2).asInstanceOf[java.util.Map[String, String]].get("key4")
      .asInstanceOf[java.util.List[AnyRef]].get(0).asInstanceOf[java.util.Map[String,String]]
      .get("key7") must be ("bar2")

    resolvedConfig.getConfig("key8").getConfig("key9").getAnyRefList("key11").get(1).asInstanceOf[java.util.List[AnyRef]]
      .get(1).asInstanceOf[java.util.Map[String,String]].get("key80") must be ("bar1")

  }


  it must "strip unnecessary leading space in quoted stings" in {
    val testData =
      """
        |  test1
        |    test2
      """.stripMargin
    val data = HoconConfigEnhancer.stripLeadingWhitespaces(testData)
    data.split("\n") map { _.length } must be (Seq(5,7))
  }

  it must "resolve variables which has dot character" in {
    val testData = ConfigFactory parseString
       """
        | {
        |   foo = {
        |      bar = baz
        |   }
        |   hello = ${foo.bar}
        | }
      """.stripMargin
    testData.hardResolve().getString("hello") must be ("baz")
  }

  it must "parse quoted strings properly" in {
    val testData = ConfigFactory parseString
      """
        | {
        |   foo.bar = baz
        |   hello = {
        |     world = [ { "foo.bar" = baz }, { "foo=bar"  = baz } ]
        |   }
        | }
      """.stripMargin
    val config = testData.hardResolve()
    config.getConfig("foo").getString("bar") must be ("baz")
    config.getConfig("hello").getConfigList("world").get(0).getString(""""foo.bar"""") must be ("baz")
    config.getConfig("hello").getConfigList("world").get(1).getString(""""foo=bar"""") must be ("baz")
  }

}
