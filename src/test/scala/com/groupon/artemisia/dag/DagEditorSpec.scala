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

package com.groupon.artemisia.dag

import java.io.File

import com.groupon.artemisia.TestSpec
import com.groupon.artemisia.core.BasicCheckpointManager.CheckpointData
import com.groupon.artemisia.core.{Keywords, TestAppContext}
import com.groupon.artemisia.dag.Dag.Node
import com.groupon.artemisia.util.HoconConfigUtil.Handler
import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.JavaConverters._

/**
  * Created by chlr on 8/13/16.
  */
class DagEditorSpec extends TestSpec {


  "DagEditor" must "expand iterable nodes" in {
    val file = this.getClass.getResource("/code/iteration_test.conf").getFile
    val config = ConfigFactory.parseFile(new File(file))
//    val node4 = Node("step4", config.getConfig("step4"))
//    val node1 = Node("step1", config.getConfig("step1"))
//    val node2 = Node("step2", config.getConfig("step2"))
    val dag = Dag(new TestAppContext(config).init())
    val node3 = dag.graph.filter(_.name == "step3").head
    val (Seq(node3a, node3b, node3c),modifiedConfig) = new DagEditor.NodeIterationProcessor(node3, config).expand
    node3a.successParents must be (Seq())
    node3b.successParents must be (Seq())
    node3c.successParents must be (Seq(node3a, node3b))
    DagEditor.replaceNode(dag, node3, Seq(node3a, node3b, node3c), CheckpointData())
    modifiedConfig.getAs[Config]("step3") must be (None)
    modifiedConfig.getList(s""""step3$$1".${Keywords.Task.DEPENDENCY}.${Keywords.Task.SUCCESS_DEPENDENCY}""")
      .unwrapped.asScala must be (Seq("step1", "step2"))
    modifiedConfig.getList(s""""step3$$2".${Keywords.Task.DEPENDENCY}.${Keywords.Task.SUCCESS_DEPENDENCY}""")
      .unwrapped.asScala must be (Seq("step1", "step2"))
    modifiedConfig.getList(s""""step3$$3".${Keywords.Task.DEPENDENCY}.${Keywords.Task.SUCCESS_DEPENDENCY}""")
      .unwrapped.asScala must be (Seq("step3$1", "step3$2"))
  }

  it must "import worklets from file" in {
     val config = ConfigFactory
       .parseString(TestUtils.worklet_file_import(this.getClass.getResource("/code/multi_step_addition_job.conf").getFile))
     val appContext = new TestAppContext(config).init()
     val dag = Dag(appContext)
     val node2 = dag.graph.filter(_.name == "task2").head
     val (Seq(node2a, node2b), modifiedConfig) = new DagEditor.ModuleImporter(node2, config).importModule match {
       case (x: Seq[Node], y) => x.sortWith((node1, node2) => node1.name < node2.name) -> y
     }
     Seq(node2a.name, node2b.name) must contain only("task2$step1", "task2$step2")
     modifiedConfig.as[Int]("bravo") must be(100)
     node2b.successParents must contain only Node("task2$step1")
     node2a.payload.as[Config](s""""${node2a.name}"""").as[Int]("params.num1") must be(10)
    System.err.println("")
  }


  it must "import worklet from inline" in  {

    val config = ConfigFactory.parseFile(new File(this.getClass.getResource("/code/worklet_node_import.conf").getFile))
    val dag = Dag(new TestAppContext(config).init())
    val node2 = dag.graph.filter(_.name == "task2").head
    val (Seq(node2a, node2b), modifiedConfig) = new DagEditor.ModuleImporter(node2, config).importModule match {
      case (x: Seq[Node], y) => x.sortWith((node1, node2) => node1.name < node2.name) -> y
    }
    Seq(node2a.name, node2b.name) must contain only ("task2$step1","task2$step2")
    modifiedConfig.as[Int]("deadpool") must be (20)
    node2b.successParents must contain only Node("task2$step1")
  }

}
