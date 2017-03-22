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
import com.groupon.artemisia.TestSpec
import scala.concurrent.duration.FiniteDuration

/**
  * Created by chlr on 7/31/16.
  */
class ParserSpec extends TestSpec {

  "MemoryParser" must "parse memory string literal" in {

    MemorySize("10KiB").toBytes must be (10240L)
    MemorySize("10 kilobyte").toBytes must be (10240L)
    MemorySize("10kB").toBytes must be (10240L)
    MemorySize("10k").toBytes must be (10240L)


    MemorySize("10m").toBytes must be (10485760L)
    MemorySize("10M").toBytes must be (10485760L)
    MemorySize("10MB").toBytes must be (10485760L)
    MemorySize("10MiB").toBytes must be (10485760L)
    MemorySize("10 megabyte").toBytes must be (10485760L)

    MemorySize("10 byte").toBytes must be (10L)
    MemorySize("10B").toBytes must be (10L)
    MemorySize("10b").toBytes must be (10L)


    MemorySize("9G").toBytes must be (9663676416L)
    MemorySize("9g").toBytes must be (9663676416L)
    MemorySize("9GiB").toBytes must be (9663676416L)
    MemorySize("9 gigabyte").toBytes must be (9663676416L)

    MemorySize("9T").toBytes must be (9895604649984L)
    MemorySize("9Ti").toBytes must be (9895604649984L)
    MemorySize("9TiB").toBytes must be (9895604649984L)
    MemorySize("9 terabyte").toBytes must be (9895604649984L)

  }


  "DurationParser" must "parse duration literal" in {
    DurationParser("10ns").toFiniteDuration must be (FiniteDuration(10L, TimeUnit.NANOSECONDS))
    DurationParser("10 nanosecond").toFiniteDuration must be (FiniteDuration(10L, TimeUnit.NANOSECONDS))
    DurationParser("10 nanos").toFiniteDuration must be (FiniteDuration(10L, TimeUnit.NANOSECONDS))

    DurationParser("10us").toFiniteDuration must be (FiniteDuration(10L, TimeUnit.MICROSECONDS))
    DurationParser("10 micro").toFiniteDuration must be (FiniteDuration(10L, TimeUnit.MICROSECONDS))
    DurationParser("10 microsecond").toFiniteDuration must be (FiniteDuration(10L, TimeUnit.MICROSECONDS))

    DurationParser("10 milli").toFiniteDuration must be (FiniteDuration(10L, TimeUnit.MILLISECONDS))
    DurationParser("10ms").toFiniteDuration must be (FiniteDuration(10L, TimeUnit.MILLISECONDS))
    DurationParser("10 millisecond").toFiniteDuration must be (FiniteDuration(10L, TimeUnit.MILLISECONDS))

    DurationParser("9 second").toFiniteDuration must be (FiniteDuration(9L, TimeUnit.SECONDS))
    DurationParser("9s").toFiniteDuration must be (FiniteDuration(9L, TimeUnit.SECONDS))

    DurationParser("10m").toFiniteDuration must be (FiniteDuration(10L, TimeUnit.MINUTES))
    DurationParser("10 minute").toFiniteDuration must be (FiniteDuration(10L, TimeUnit.MINUTES))

    DurationParser("100h").toFiniteDuration must be (FiniteDuration(100L, TimeUnit.HOURS))
    DurationParser("100 hours").toFiniteDuration must be (FiniteDuration(100L, TimeUnit.HOURS))

    DurationParser("98d").toFiniteDuration must be (FiniteDuration(98L, TimeUnit.DAYS))
    DurationParser("98 days").toFiniteDuration must be (FiniteDuration(98L, TimeUnit.DAYS))


  }

}
