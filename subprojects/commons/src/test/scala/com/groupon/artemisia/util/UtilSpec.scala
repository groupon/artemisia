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

import java.io.{File, FileNotFoundException}
import com.groupon.artemisia.TestSpec


/**
 * Created by chlr on 1/1/16.
 */

class UtilSpec extends TestSpec {

  "The Util.readConfigFile" must "throw FileNotFoundException on non-existent file" in {
    intercept[FileNotFoundException] {
      Util.readConfigFile(new File("Some_Non_Existant_File.conf"))
    }
  }

  it must "give back global config file is explicitly set" in {
    FileSystemUtil.withTempFile(fileName = "global_file.txt") {
      file => {
        Util.getGlobalConfigFile(Some(file.toPath.toString)) must be (Some(file.toPath.toString))
      }
    }
  }


  it must "give back default config file when global is not set" in {
    FileSystemUtil.withTempFile(fileName = "global_file.txt") {
      file => {
        Util.getGlobalConfigFile(None, defaultConfig = file.toPath.toString) must be (Some(file.toPath.toString))
      }
    }
  }

  it must "convert character unicode code-point" in {
    Util.unicodeCode('a') must be ("\\u0061")
    Util.unicodeCode('\t') must be ("\\u0009")
  }


  it must "give back None when global config is not set and default doesn't exists" in {
     Util.getGlobalConfigFile(None, defaultConfig =  "a_dummy_non_existant_file") must be (None)
  }

  it must "pretty print an ascii table" in {
    val content = Array(Array("Col1", "Col2"), Array("r1c1", "r1c2"), Array("r2c1", "r2c2"))
    val result = Util.prettyPrintAsciiTable(content)
    result.head must be ("| Col1  | Col2  |")
    result(1) must be ("|-------|-------|")
    result(2) must be ("| r1c1  | r1c2  |")
  }


  it must "convert map to hocon config" in {
    val map = Map[String, Any](
      "harley" -> "quinn",
      "boy.friend" -> "joker",
      "age" -> 28
    )
    val config = Util.mapToConfig(map)
    config.getString("harley") must be ("quinn")
    config.getString(""""boy.friend"""") must be ("joker")
    config.getInt("age") must be (28)
  }

}
