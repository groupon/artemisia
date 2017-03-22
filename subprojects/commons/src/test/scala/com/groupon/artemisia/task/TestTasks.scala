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

import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import com.groupon.artemisia.util.HoconConfigUtil.Handler

/**
 * Created by chlr on 1/26/16.
 */
class TestAdderTask(name: String ,val num1: Int, val num2: Int, val result: String) extends Task(name) {

  override def setup(): Unit = {}
  override def work(): Config = {
    val config = ConfigFactory.empty.withValue(result, ConfigValueFactory.fromAnyRef(num1+num2))
    wrapAsStats(config) withFallback config
  }
  override def teardown(): Unit = {}

}

object TestAdderTask extends TaskLike {

  override def apply(name: String, param: Config) = {
    new TestAdderTask(name, param.as[Int]("num1"),param.as[Int]("num2"),param.as[String]("result_var"))
  }

  override val taskName: String = "TestAdderTask"
  override def doc(component: String): String = "TestAdderTask is a test addition task"
  override val info: String = "test add task"
  override val desc: String = ""
  override val paramConfigDoc = ConfigFactory.empty()
  override val fieldDefinition = Map[String, AnyRef]()
  override val defaultConfig: Config = ConfigFactory parseString
    s"""
       | {
       |   tkey1 = tval1
       | }
     """.stripMargin
  override val outputConfig: Option[Config] = None
  override val outputConfigDesc: String = ""
}


class TestFailTask(name: String) extends Task(name) {

  override def setup(): Unit = {}
  override def work(): Config = { throw new Exception("FailTask always fail") }
  override def teardown(): Unit = {}

}

object TestFailTask extends TaskLike {

  override def apply(name: String, param: Config) = {
    new TestFailTask(name)
  }
  override val taskName: String = "TestFailTask"
  override def doc(component: String): String = "This is a sample test task that always fail"
  override val info: String = "test fail task"
  override val desc: String = ""
  override val paramConfigDoc = ConfigFactory.empty()
  override val fieldDefinition = Map[String, AnyRef]()
  override val defaultConfig: Config = ConfigFactory parseString
    s"""
       | {
       |   tkey2 = tval2
       | }
     """.stripMargin
  override val outputConfig: Option[Config] = None
  override val outputConfigDesc: String = ""
}
