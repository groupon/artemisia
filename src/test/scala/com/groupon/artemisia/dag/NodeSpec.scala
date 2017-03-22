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

import com.groupon.artemisia.TestSpec
import com.groupon.artemisia.core.Keywords
import com.groupon.artemisia.dag.Dag.Node
import com.typesafe.config.ConfigFactory

import scala.collection.JavaConverters._

/**
  * Created by chlr on 1/23/16.
  */
class NodeSpec extends TestSpec {


  it must "respect the laws of node equality" in {
    val node1 = NodeSpec.makeNode("testnode")
    val node2 = NodeSpec.makeNode("testnode")
    node1 must equal(node2)
  }

  it must "update payload when dependencies are changed" in {
    val node1 = NodeSpec.makeNode("node1")
    val node2 = NodeSpec.makeNode("node2")
    val node3 = NodeSpec.makeNode("node3")
    val node4 = NodeSpec.makeNode("node4")
    node1.successParents = Seq(node2)
    node1.failParents = Seq(node3)
    node1.completeParents = Seq(node4)
    node1.payload.getList(s"${Keywords.Task.DEPENDENCY}.${Keywords.Task.SUCCESS_DEPENDENCY}").unwrapped().asScala must be (Seq("node2"))
    node1.payload.getList(s"${Keywords.Task.DEPENDENCY}.${Keywords.Task.FAIL_DEPENDENCY}").unwrapped().asScala must be (Seq("node3"))
    node1.payload.getList(s"${Keywords.Task.DEPENDENCY}.${Keywords.Task.COMPLETE_DEPENDENCY}").unwrapped().asScala must be (Seq("node4"))
  }

  it must "node with terminal status cannot be updated with new status" in {
    val node1 = NodeSpec.makeNode("node1")
    val dag = Dag(node1 :: Nil)
    dag.setNodeStatus("node1", Status.SUCCEEDED)
    val ex = intercept[AssertionError](dag.setNodeStatus("node1", Status.FAILED))
    ex.getMessage must be ("assertion failed: node node1 with status SUCCEEDED cannot be set with new status FAILED")
  }


  it must "parse the dependency node in payload" in {
    new Node(
      "test_node",
      ConfigFactory parseString
        s"""
          | {
          |  ${Keywords.Task.DEPENDENCY} = {
          |    ${Keywords.Task.SUCCESS_DEPENDENCY} = [node1, node2]
          |    ${Keywords.Task.FAIL_DEPENDENCY} = [node3, node4]
          |    ${Keywords.Task.COMPLETE_DEPENDENCY} = [node5, node6]
          |  }
          | }
        """.stripMargin
    )  {
      val (success,fail,complete) = parseDependencyFromConfig
      success must contain only ("node1", "node2")
      fail must contain only ("node3", "node4")
      complete must contain only ("node5", "node6")
    }

    new Node(
      "test_node",
      ConfigFactory parseString
        s"""
           | {
           |  ${Keywords.Task.DEPENDENCY} = [node1, node2]
           | }
        """.stripMargin
    )  {
      val (success,Nil,Nil) = parseDependencyFromConfig
      success must contain only ("node1", "node2")
    }
  }

}

object NodeSpec {

  import Keywords.Task._

  def makeNode(name: String, dependencies: String = "[]", dependencyType: String = "success") =
    Node(name,
      ConfigFactory parseString {
        dependencyType match {
          case "success" => s"$DEPENDENCY.$SUCCESS_DEPENDENCY = $dependencies"
          case "fail" => s"$DEPENDENCY.$FAIL_DEPENDENCY = $dependencies"
          case "complete" => s"$DEPENDENCY.$COMPLETE_DEPENDENCY = $dependencies"
          case _ => throw new RuntimeException(s"unrecognized dependency type: $dependencyType")
        }
      })


  def complexDag = {
    val node1 = NodeSpec.makeNode("node1")
    val node2 = NodeSpec.makeNode("node2")
    val node3 = NodeSpec.makeNode("node3", "[node1, node2]")
    val node4 = NodeSpec.makeNode("node4", "[node3]")
    val node5 = NodeSpec.makeNode("node5", "[node3]", "fail")
    val node6 = NodeSpec.makeNode("node6", "[node3]", "complete")
    val node7 = NodeSpec.makeNode("node7", "[node4]", "fail")
    val node8 = NodeSpec.makeNode("node8", "[node5]")
    val node9 = NodeSpec.makeNode("node9", "[node5]", "fail")
    val node10 = NodeSpec.makeNode("node10", "[node5]", "complete")
    Dag(Seq(node1, node2, node3, node4, node5, node6, node7, node8, node9, node10))
  }
}