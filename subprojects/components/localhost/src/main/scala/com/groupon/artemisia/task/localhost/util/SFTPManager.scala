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

package com.groupon.artemisia.task.localhost.util

import java.nio.file.Path
import com.jcraft.jsch.{ChannelSftp, JSch}
import com.groupon.artemisia.core.AppLogger
import com.groupon.artemisia.task.localhost.SFTPConnection
import com.groupon.artemisia.util.FileSystemUtil.joinPath

/**
  * Created by chlr on 6/22/16.
  */
class SFTPManager(connection: SFTPConnection) {

  val jsch = new JSch()
  connection.pkey foreach { x => jsch.addIdentity(x.getAbsolutePath) }

  private lazy val session = {
    val session = jsch.getSession(connection.username, connection.host, connection.port)
    val props = new java.util.Properties()
    props.put("StrictHostKeyChecking", "no")
    session.setConfig(props)
    session
  }

  private lazy val sftpChannel = {
    connection.password foreach { x => session.setPassword(x) }
    session.connect()
    val channel = session.openChannel("sftp").asInstanceOf[ChannelSftp]
    channel.connect()
    channel
  }

  def setLCD(path: String) = {
    sftpChannel.lcd(path)
  }

  def setRCD(path: String) = {
    sftpChannel.cd(path)
  }

  def copyToLocal(remote: Path, local: Option[Path] = None) = {

    AppLogger info s"copying remote file $remote to ${local.getOrElse(joinPath(sftpChannel.lpwd(), remote.getFileName.toString)).toString}"
    local match {
      case Some(x) => sftpChannel.get(remote.toString, x.toString)
      case None => sftpChannel.get(remote.toString, joinPath(sftpChannel.lpwd(), remote.getFileName.toString))
    }
  }

  def copyFromLocal(local: Path, remote: Option[Path] = None) = {
    AppLogger info s"copying local file $local to remote at ${remote.getOrElse(joinPath(sftpChannel.pwd(), local.toAbsolutePath.getFileName.toString))} "
    remote match {
      case Some(x) => sftpChannel.put(local.toString, x.toString)
      case None => sftpChannel.put(local.toString, joinPath(sftpChannel.pwd(), local.toAbsolutePath.getFileName.toString))
    }
  }

  def terminate() = {
    AppLogger debug "closing sftp channel"
    if (sftpChannel.isConnected) sftpChannel.exit()
    AppLogger debug "closing ssh channel"
    if (session.isConnected) session.disconnect()
  }

}
