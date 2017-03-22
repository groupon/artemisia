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

/**
  * Created by chlr on 5/25/16.
  */

import java.util
import com.typesafe.config.{ConfigFactory, ConfigValue}
import com.groupon.artemisia.util.HoconConfigUtil.Handler
import java.util.{ArrayList => JavaArrayList}
import scala.collection.JavaConverters._
import scala.reflect.runtime.universe
import scala.tools.reflect.ToolBox
import scala.util.{Failure, Success, Try}


object BooleanEvaluator {

  val description = "description"
  val expression = "expression"

  def eval[A](expr: String): A = {
    val toolbox = universe.runtimeMirror(this.getClass.getClassLoader).mkToolBox()
    AppLogger debug s"executing expression $expr"
    toolbox.eval(toolbox.parse(expr)).asInstanceOf[A]
  }


  def evalBooleanExpr(exprTree: Any): Boolean = {

    def parser(tree: Any) = {
      tree match {
        case x: Boolean => x
        case x: String => Try((ConfigFactory parseString s" foo = $x ").as[Boolean]("foo")) match {
          case Success(bool) => bool
          case Failure(_) => eval[Boolean](x)
        }
        case x: util.List[String @unchecked] => {
          x.asScala map { eval[Boolean] } forall { in => in }
        }
        case x: util.Map[String, JavaArrayList[Any]] @unchecked => {
          x.asScala map {
            case ("or", value) => value.asScala map { evalBooleanExpr } exists { in => in }
            case ("and", value) => evalBooleanExpr(value)
          } forall { in => in }
        }
      }
    }

    exprTree match {
      case in: ConfigValue => parser(in.unwrapped())
      case in => parser(in)
    }

  }


  def stringifyBoolExpr(exprTree: Any): String = {

     def parser(tree: Any) = {
      tree match {
        case x: Boolean => x.toString
        case x: String => Try((ConfigFactory parseString s" foo = $x ").as[Boolean]("foo")) match {
          case Success(bool) => bool.toString
          case Failure(_) => x
        }
        case x: util.List[String @unchecked] => x.asScala map { stringifyBoolExpr } map { in => s"($in)" } mkString " and "
        case x: util.Map[String, JavaArrayList[Any]] @unchecked => {
          x.asScala map {
            case ("or", value) => value.asScala map { stringifyBoolExpr } map { in => s"($in)" } mkString " or "
            case ("and", value) => stringifyBoolExpr(value)
          } map (in => s"($in)") mkString " and "
        }
      }
    }

    exprTree match {
      case in: ConfigValue => parser(in.unwrapped())
      case in => parser(in)
    }
  }

}
