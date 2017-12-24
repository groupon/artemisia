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

package com.groupon.artemisia.task.localhost

import java.nio.file.{Path, Paths}
import com.typesafe.config.{Config, ConfigFactory, ConfigValue}
import com.groupon.artemisia.task.localhost.util.SFTPManager
import com.groupon.artemisia.task.{Task, TaskLike}
import com.groupon.artemisia.util.DocStringProcessor.StringUtil
import com.groupon.artemisia.util.HoconConfigUtil.Handler
import scala.collection.JavaConverters._

/**
  * Created by chlr on 6/22/16.
  */
class SFTPTask(name: String, val connection: SFTPConnection, val remoteToLocal: Seq[(Path, Option[Path])], val localToRemote: Seq[(Path, Option[Path])],
               val localWorkingDir: Option[String] = None, val remoteWorkingDir: Option[String] = None)
  extends Task(name) {

  val manager = new SFTPManager(connection)

  override def setup(): Unit = {}

  override def work(): Config = {

    localWorkingDir foreach { manager.setLCD }
    remoteWorkingDir foreach { manager.setRCD }
    remoteToLocal foreach {
      case (x,y) => manager.copyToLocal(x,y)
    }
    localToRemote foreach { case (x,y) => manager.copyFromLocal(x,y) }
    ConfigFactory.empty()
  }

  override def teardown(): Unit = {
    manager.terminate()
  }

}

object SFTPTask extends TaskLike {

  override val taskName: String = "SFTPTask"

  override val defaultConfig: Config = ConfigFactory.empty()
                .withValue("connection", SFTPConnection.defaultConfig.root())

  override def apply(name: String, config: Config): Task = {

    def parseFileMapping(mode: String) = {

       config.hasPath(mode) match {
          case true => config.as[List[AnyRef]](mode) map {
            case x: java.util.Map[String, String] @unchecked => x.asScala.toSeq.map(a => Paths.get(a._1) -> Some(Paths.get(a._2))).head
            case x: String =>  Paths.get(x) -> None
          }
          case false => Nil
       }
    }

    new SFTPTask(name,
        SFTPConnection.parseConnectionProfile(config.as[ConfigValue]("connection")),
        parseFileMapping("get"),
        parseFileMapping("put"),
        config.getAs[String]("local-dir"),
        config.getAs[String]("remote-dir")
    )

  }



  override val paramConfigDoc =  ConfigFactory parseString
    s"""| {
        |   params = {
        |      ${SFTPConnection.configStructure.ident(15)}
        |      get = "[{ '/root_sftp_dir/file1.txt' = '/var/tmp/file1.txt' },'/root_sftp_dir/file2.txt'] @type(array)"
        |      put = "[{ '/var/tmp/file1.txt' = '/sftp_root_dir/file1.txt' },'/var/tmp/file1.txt'] @type(array)"
        |      local-dir = "/var/tmp @default(your current working directory.) @info(current working directory)"
        |      remote-dir = "/root @info(remote working directory)"
        |   }
        | }
     """.stripMargin


  override val fieldDefinition = Map(
    "connection" -> SFTPConnection.fieldDefinition,
    "get" -> "array of object or strings providing source and target (optional if type is string) paths",
    "put" -> "array of object or strings providing source and target (optional if type is string) paths",
    "local-dir" -> "set local working directory. by default it will be your current working directory",
    "remote-dir" -> "set remote working directory"
  )


  override val info: String = s"$taskName supports copying files from remote sftp server to local filesystem and vice versa"

  override val desc: String =
    s"""
       |$taskName is used to perform `put` and `get` operations of SFTP.
       |
       |**PUT:**
       |
       | The PUT operation will move your file from local file-system to the SFTP server. The target path can be either the current
       | working directory of SFTP session (which can be set using the setting `remote-dir`) or a user provided path.
       | For example in the below setting we are uploading two local files to the SFTP server. The first file `/var/tmp/file1.txt`
       | is uploaded to the SFTP path `/sftp_root_dir/path/file1.txt`. The second local file `/var/tmp/file2.txt` is uploaded
       | to the path `/sftp_root_dir/dir1/files/file2.txt` because the current working directory of the SFTP session is
       | `/sftp_root_dir/dir1/files` as set by the `remote_dir` property.
       |
       |      put = [{ "/var/tmp/file1.txt" = /sftp_root_dir/path/file1.txt }, "/var/tmp/file2.txt" ]
       |      remote_dir = /sftp_root_dir/dir1/files
       |
       |**GET**:
       |
       | The GET operation will move files from your SFTP server to your local file-system. The target path can be either the current
       | local working directory (configurable via `local-dir` setting) or the path provided by the user. In the below
       | example we move two files `/root_sftp_dir/file1.txt` to local path `/var/tmp/file1.txt` and `/root_sftp_dir/file2.txt`
       | to `/var/tmp/file2.txt`
       |
       |      get = [{ '/root_sftp_dir/file1.txt' = '/var/tmp/file1.txt' },'/root_sftp_dir/file2.txt']
       |      local-dir = /var/tmp
       |
     """.stripMargin

  override val outputConfig: Option[Config] = None


  override val outputConfigDesc = "This task outputs a empty config object"

}

