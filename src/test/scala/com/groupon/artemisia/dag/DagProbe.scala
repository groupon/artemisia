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

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.TestProbe
import com.groupon.artemisia.dag.Message.{Messageable, TaskFailed, TaskSucceeded, TaskWrapper}
import scala.concurrent.duration._
import scala.concurrent.duration.{Duration, FiniteDuration}

/**
  * Created by chlr on 12/31/16.
  */

class DagProbe(system: ActorSystem) extends TestProbe(system) {

  def validateAndRelay(destination: ActorRef, duration: Duration = 5 seconds)
                      (validate: PartialFunction[Messageable, Unit]) = {
    this.expectMsgPF(duration) {
      case message: Messageable => {
        validate(message)
        destination.tell(message, this.ref)
      }
    }
  }


  def validateAndRelayMessages(msgCount: Int,
                               destination: ActorRef,
                               duration: FiniteDuration = 5 seconds)
                              (validate: PartialFunction[Seq[Messageable], Unit]) = {
    this.receiveN(msgCount, duration) match {
      case messages: Seq[Messageable]@unchecked =>
        validate {
          messages.map({
            case x@TaskSucceeded(name, _) => name -> x
            case x@TaskFailed(name, _, _) => name -> x
            case x@TaskWrapper(name, _) => name -> x
            case x => throw new RuntimeException(s"unexpected type ${x.getClass.getName}. unable to sort message")
          }).sortWith((x, y) => x._1 < y._1)
            .map({ case (name, message) => message })
            .toList
        }
        for (message <- messages) destination.tell(message, this.ref)
    }
  }
}
