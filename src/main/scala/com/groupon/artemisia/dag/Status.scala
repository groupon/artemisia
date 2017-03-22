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

import com.groupon.artemisia.dag.Dag.Node

/**
  * Created by chlr on 1/7/16.
 */
object Status extends Enumeration {


  val READY, RUNNING, SUCCEEDED, FAILED, INIT_FAILED, FAILED_RECOVERED, UNKNOWN, SKIPPED, FAILURE_IGNORED, DEACTIVATE = Value

  /**
    * list of possible status for a successful node.
    */
  private val successfulComplete = SUCCEEDED :: SKIPPED :: FAILURE_IGNORED :: Nil


  /**
    * list of possible status for a failed node
    */
  private val failStatus = FAILED :: INIT_FAILED :: FAILED_RECOVERED :: Nil

  /**
    * list of possible status for a completed (succeeded or failed) node
    */
  private val completedStatus = successfulComplete ++ failStatus


  /**
    * list of terminal status.
    */
  private val terminalStatus = completedStatus :+ DEACTIVATE

  /**
    * check if status is one of the completed status
    *
    * @param status
    * @return
    */
  def isComplete(status: Status.Value) = {
    completedStatus contains status
  }

  /**
    * check if status is one of the successful execution status
    *
    * @param status
    * @return
    */
  def isSuccessful(status: Status.Value) = {
    successfulComplete contains status
  }


  /**
    * check if status is one of the failed execution status
    *
    * @param status
    * @return
    */
  def isFail(status: Status.Value) = {
    failStatus contains status
  }


  /**
    * check if status is terminal status
    *
    * @param status
    * @return
    */
  def isTerminal(status: Status.Value) = {
    terminalStatus contains status
  }

}

class DagEditException(val node: Node) extends Exception(s"failed to expand/edit node ${node.name}")

class DagException(message: String) extends Exception(message)

