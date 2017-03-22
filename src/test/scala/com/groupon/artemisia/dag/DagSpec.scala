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
import com.groupon.artemisia.core.Keywords
import com.groupon.artemisia.dag.Dag.Node
import com.groupon.artemisia.dag.Message.TaskStats
import com.groupon.artemisia.util.Util
import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Created by chlr on 1/3/16.
 */
class DagSpec extends TestSpec {


  "The Dag" must "handle sequentially dependent nodes" in {
    val node1 = NodeSpec.makeNode("node1")
    val node2 = NodeSpec.makeNode("node2", "[node1]")
    val node3 = NodeSpec.makeNode("node3","[node2]")
    val node4 = NodeSpec.makeNode("node4","[node3]")
    val node5 = NodeSpec.makeNode("node5","[node4]")
    val node6 = NodeSpec.makeNode("node6","[node5]")
    val node7 = NodeSpec.makeNode("node7","[node6]")
    val graph = node1 :: node2 :: node3 :: node4 :: node5 :: node6 :: node7 :: Nil
    val dag = Dag(graph)

    info("inspecting Dag size")
    dag.graph.size must be (7)
    info("Dag must be in same order as in source")
    for (i <- 1 to 7; currentNode: Node <- dag.getRunnableNodes) {
      currentNode must be (graph(i-1))
      dag.setNodeStatus(currentNode.name, Status.SUCCEEDED)
    }
  }


  it must "correctly identify if it can run" in {
    val dag = NodeSpec.complexDag
    val Seq(node1, node2, node3, node4, node5, node6, node7, node8, node9, node10) = dag.graph
    node1.isRunnable mustBe true
    node2.isRunnable mustBe true
    dag.setNodeStatus("node1", Status.SUCCEEDED)
    dag.setNodeStatus("node2", Status.SUCCEEDED)
    node3.isRunnable mustBe true
    dag.setNodeStatus("node3", Status.FAILED_RECOVERED)
    dag.getNodeStatus("node4") must be (Status.DEACTIVATE)
    dag.getNodeStatus("node7") must be (Status.DEACTIVATE)
    dag.getNodeStatus("node5") must be (Status.READY)
    dag.getNodeStatus("node6") must be (Status.READY)
    dag.setNodeStatus("node5", Status.SUCCEEDED)
    dag.getNodeStatus("node8") must be (Status.READY)
    dag.getNodeStatus("node9") must be (Status.DEACTIVATE)
    dag.getNodeStatus("node10") must be (Status.READY)
    dag.failedNodes must contain only node3
    dag.succeededNodes must contain only (node1, node2, node5)
    dag.completedNodes must contain only (node1, node2, node5, node3)
  }


  it must "identify leaf nodes" in {
    val dag = NodeSpec.complexDag
    val Seq(_, _, _, _, _, node6, node7, node8, node9, node10) = dag.graph
    dag.leafNodes must contain only (node6 ,node7, node8, node9, node10)
  }

  it must "identify cycles" in {

    val node1 = NodeSpec.makeNode("node1")
    val node2 = NodeSpec.makeNode("node2","[node1,node4]")
    val node3 = NodeSpec.makeNode("node3","[node2]")
    val node4 = NodeSpec.makeNode("node4", "[node3]")
    val graph = node1 :: node2 :: node3 :: node4 :: Nil
    val ex = intercept[DagException] {
      Dag(graph)
    }
    ex.getMessage must be ("Cycles Detected in Dag")
  }

  it must "report invalid dependency references" in {

    val node1 = NodeSpec.makeNode("node1")
    val node2 = NodeSpec.makeNode("node2","[node1, node5]")
    val node3 = NodeSpec.makeNode("node3", "[node2]")
    val node4 = NodeSpec.makeNode("node4", "[node3]")
    val graph = node1 :: node2 :: node3 :: node4 :: Nil
    val ex = intercept[DagException] {
      Dag(graph)
    }
    ex.getMessage must be (s"invalid dependency reference. node5 not found")

  }

  it must "produce right batches of steps to execute" in {

    val node1 = NodeSpec.makeNode("node1")
    val node2 = NodeSpec.makeNode("node2")
    val node3 = NodeSpec.makeNode("node3","[node2]")
    val node4 = NodeSpec.makeNode("node4","[node2]")
    val node5 = NodeSpec.makeNode("node5","[node4]")
    val node6 = NodeSpec.makeNode("node6","[node4]")
    val node7 = NodeSpec.makeNode("node7","[node5]")
    val graph = node1 :: node2 :: node3 :: node4 :: node5 :: node6 :: node7 :: Nil
    val dag = Dag(graph)

    dag.getRunnableNodes.toSet must be ((node1 :: node2 :: Nil).toSet)
    dag.setNodeStatus(node1.name, Status.SUCCEEDED)
    dag.getRunnableNodes must be ( node2 :: Nil)
    dag.setNodeStatus(node2.name, Status.SUCCEEDED)
    dag.getRunnableNodes must be ( node3 :: node4 :: Nil)
    dag.setNodeStatus(node4.name, Status.SUCCEEDED)
    dag.getRunnableNodes must be ( node3  :: node5 :: node6 :: Nil)

  }

  it must "resolve dependencies approriately" in  {

    val node1 = NodeSpec.makeNode("node1")
    val node2 = NodeSpec.makeNode("node2","[node1]")
    val node3 = NodeSpec.makeNode("node3","[node2]")
    val dag =  Dag(node1 :: node2 :: node3 :: Nil)
    dag.lookupNodeByName("node1").allParents must be (Nil)
    dag.lookupNodeByName("node2").allParents must be (node1 :: Nil)
    dag.lookupNodeByName("node3").allParents must be (node2 :: Nil)

  }

  it must "apply checkpoints correctly" in {
    val node1 = NodeSpec.makeNode("node1")
    val node2 = NodeSpec.makeNode("node2","[node1]")
    val node3 = NodeSpec.makeNode("node3","[node2]")
    val node4 = NodeSpec.makeNode("node4","[node3]")
    val checkpoints = Map(
                          "node1" -> DagSpec.taskStats("node1",Status.SUCCEEDED),
                          "node2" -> DagSpec.taskStats("node2",Status.SUCCEEDED)
                                  )
    val dag = Dag(node1 :: node2 :: node3 :: node4 :: Nil, CheckpointData(taskStatRepo = checkpoints))
    dag.getRunnableNodes must contain only node3
    dag.getNodeStatus("node1") must be (Status.SUCCEEDED)
    dag.getNodeStatus("node2") must be (Status.SUCCEEDED)
    dag.getNodeStatus("node3") must be (Status.READY)
    dag.getNodeStatus("node4") must be (Status.READY)
  }

  it must "parse Task Nodes correctly from the ConfigObject" in {

    val code = scala.io.Source.fromFile(this.getClass.getResource("/code/code_with_incorrect_blocks.conf").getFile).mkString
    val nodes = Dag.extractTaskNodes(ConfigFactory parseString code)
    val dummy_step1 = ConfigFactory parseString "{ Component: Dummy, Task: DummyTask ,params: { dummy_param1 = 11, dummy_param2 = no } }"
    nodes must be (mutable.Map("dummy_step1" -> dummy_step1))

  }

  it must "update each node with new payload" in {
    val dag = DagSpec.makeDagFromFile(this.getClass.getResource("/code/code_with_incorrect_blocks.conf").getFile)
    val content = ConfigFactory parseString
      """
        |dummy_step1 = { Component: Dummy, Task: DummyTask, params: {new_key = new_value } }
      """.stripMargin
    dag.updateNodePayloads(content)
    dag.lookupNodeByName("dummy_step1").payload.getString("params.new_key") must equal ("new_value")
  }

  it must "know if all nodes have completed execution and lookup nodes with given status" in {

    val node1 = NodeSpec.makeNode("node1")
    val node2 = NodeSpec.makeNode("node2","[node1]")
    val node3 = NodeSpec.makeNode("node3","[node2]")
    val dag =  Dag(node1 :: node2 :: node3 :: Nil)
    dag.setNodeStatus("node1", Status.SUCCEEDED)
    dag.hasCompleted must be (false)
    dag.setNodeStatus("node2", Status.SKIPPED)
    dag.hasCompleted must be (false)
    dag.setNodeStatus("node3", Status.SUCCEEDED)
    dag.hasCompleted must be (true)
    dag.nodesWithStatus(Status.SKIPPED) must be (node2 :: Nil)
    dag.nodesWithStatus(Status.SUCCEEDED) must be (node1 :: node3 :: Nil)

  }

  it must "generate context config for node" in {
    import Keywords.Config._
    val config = ConfigFactory parseString
      s"""
         |{
         |	"$DEFAULTS": {
         |		"TestComponent": {
         |			"TestAdderTask": {
         |				"result_var": "foo"
         |			}
         |		}
         |	},
         |	"$SETTINGS_SECTION": {
         |		"components": {
         |			"TestComponent": "com.groupon.artemisia.task.TestComponent"
         |		},
         |		"core": {
         |			"working_dir": "/var/tmp/ultron"
         |		},
         |	},
         |	$CONNECTION_SECTION = {
         |    connection1 = dummy_connection_config
         |  }
         |  $WORKLET = {
         |    my_worklet = dummy_worklet_config
         |  }
         |	"foo": "bar",
         |	"hello": "world",
         |  "baz": 10
         |}
       """.stripMargin
    val dag =  new Dag(Nil, CheckpointData()) {
      val contextConfig = referenceConfig(config)
      contextConfig.root.keySet().asScala must contain only ("foo", "hello", "baz")
      contextConfig.getString("foo") must be ("bar")
      contextConfig.getString("hello") must be ("world")
      contextConfig.getInt("baz") must be (10)
    }
  }

}

object DagSpec {

  def taskStats(name: String, status: Status.Value) = {
    TaskStats(startTime = Util.currentTime, endTime = Util.currentTime, status =  status, attempts = 1, taskOutput = ConfigFactory.empty())
  }

  def makeDagFromFile(file: String) = {
    val config = ConfigFactory.parseFile(new File(file))
    val node_list: Iterable[Node] = Dag.extractTaskNodes(config) map {
      case (name: String, body: Config) => Node(name,body)
    }
    Dag(node_list.toList)
  }

}
