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

import com.groupon.artemisia.inventory.exceptions.UnknownComponentException

/**
 * Created by chlr on 12/30/15.
 */
object Command {

  def run(cmd_line_params: AppSetting): Unit = {
    val appContext = new AppContext(cmd_line_params)
    appContext.init()
    Runner.run(appContext)
  }

  def doc(cmdLineParam: AppSetting): Unit = {
    val appContext = new AppContext(cmdLineParam)
    appContext.init()
    println(getDoc(appContext, cmdLineParam))
  }

  private[core] def getDoc(appContext: AppContext, cmdLineParam: AppSetting) = {
    cmdLineParam.component match {
      case Some(componentName) => {
        appContext.componentMapper.get(componentName) match {
          case Some(component) => cmdLineParam.task map { component.taskDoc } getOrElse component.doc
          case None => throw new UnknownComponentException(s"component ${cmdLineParam.component.get} doesn't exist")
        }
      }
      case None => appContext.componentMapper map { case(cName, cObj)  => s"$cName => ${cObj.info}" } mkString "\n"
    }
  }

}
