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

import java.io.{BufferedWriter, File, FileWriter}
import java.nio.file.{Path, Paths}
import scala.collection.JavaConverters._
import org.apache.commons.io.FileUtils
import org.yaml.snakeyaml.Yaml
import com.groupon.artemisia.task.Component
import com.groupon.artemisia.util.FileSystemUtil

import scala.collection.mutable

/**
 * Created by chlr on 6/19/16.
 */
object DocGenerator {

   var baseDir: Path = _

  def main(args: Array[String]) = {
    baseDir = Paths.get(args(0))
    FileUtils.deleteDirectory(new File(FileSystemUtil.joinPath(baseDir.toString, "docs", "components")))
    getComponents foreach { case(a,b) => writeComponentDoc(a,b) }
    generateMkDocConfig(getComponents.toList map {_._1})
  }

  private def getComponents = {
    val appSetting = AppSetting(cmd = Some("doc"))
    new AppContext(appSetting).componentMapper
  }

  private def writeComponentDoc(componentName: String, component: Component) = {
    val fileName = s"${componentName.toLowerCase}.md"
    val filePath = FileSystemUtil.joinPath(baseDir.toString, "docs", "components", fileName)
    FileSystemUtil.writeFile(componentDoc(component), new File(filePath))
  }

  private def componentDoc(component: Component) = {

    s"""
       ! ${component.doc}
       !
       ! ${component.tasks map { _.doc(component.name) } mkString (System.lineSeparator * 4) }
       !
     """.stripMargin('!')

  }

  private def generateMkDocConfig(components: Seq[String]) = {
    val yaml = new Yaml()
    val config: mutable.Map[String, Object] = yaml.load(mkDocConfigFile).asInstanceOf[java.util.Map[String,Object]].asScala
    val componentConfig = components.map(x => Map(x -> s"components/${x.toLowerCase}.md").asJava ).asJava
    config("pages").asInstanceOf[java.util.List[Object]].get(2)
      .asInstanceOf[java.util.Map[String,Object]]
      .put("Components", componentConfig)
    val writer = new BufferedWriter(new FileWriter(new File(FileSystemUtil.joinPath(baseDir.toString, "mkdocs.yml"))))
    yaml.dump(config.asJava, writer)
  }

  private def mkDocConfigFile = {
   s"""
      |site_name: Artemisia
      |theme: readthedocs
      |pages:
      |   - Home: index.md
      |   - Getting Started:
      |      - Installation: started.md
      |      - Defining Jobs: concept.md
      |      - Worklets: worklet.md
      |   - Components: []
      |   - Examples:
      |      - Mysql: examples/mysql.md
      |      - Teradata: examples/teradata.md
    """.stripMargin
  }



}
