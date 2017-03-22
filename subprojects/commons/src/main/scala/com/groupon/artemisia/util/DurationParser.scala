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

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration

/**
  * Created by chlr on 7/30/16.
  */
class DurationParser(duration: String) {

  private val ns = "ns" :: "nano" :: "nanos" :: "nanosecond" :: "nanoseconds" :: Nil
  private val us = "us" :: "micro" :: "micros" :: "microsecond" :: "microseconds" :: Nil
  private val ms = "ms" :: "milli" :: "millis" :: "millisecond" :: "milliseconds" :: Nil
  private val se = "s" :: "second" :: "seconds" :: Nil
  private val mi = "m" :: "minute" :: "minutes" :: Nil
  private val hr = "h" :: "hour" :: "hours" :: Nil
  private val dy = "d" :: "day" :: "days" :: Nil


  def toFiniteDuration: FiniteDuration = {
    val patterns = ns ++ us ++ ms ++ se ++ mi ++ hr ++ dy
    patterns map { x => s"^([0-9]+)\\s*($x)$$".r } map {
      _.findFirstMatchIn(duration)
    } collect {
      case Some(x) => x.group(1) -> x.group(2)
    } match {
      case (value, unit) :: Nil if ns contains unit => FiniteDuration(value.toLong, TimeUnit.NANOSECONDS)
      case (value, unit) :: Nil if us contains unit => FiniteDuration(value.toLong, TimeUnit.MICROSECONDS)
      case (value, unit) :: Nil if ms contains unit => FiniteDuration(value.toLong, TimeUnit.MILLISECONDS)
      case (value, unit) :: Nil if se contains unit => FiniteDuration(value.toLong, TimeUnit.SECONDS)
      case (value, unit) :: Nil if mi contains unit => FiniteDuration(value.toLong, TimeUnit.MINUTES)
      case (value, unit) :: Nil if hr contains unit => FiniteDuration(value.toLong, TimeUnit.HOURS)
      case (value, unit) :: Nil if dy contains unit => FiniteDuration(value.toLong, TimeUnit.DAYS)
      case _ => throw new RuntimeException(s"$duration failed to be parsed as a duration")
    }
  }

}

object DurationParser {

  def apply(duration: String): DurationParser = new DurationParser(duration)

}


