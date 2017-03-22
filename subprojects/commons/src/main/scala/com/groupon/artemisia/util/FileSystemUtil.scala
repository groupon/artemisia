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

import java.io._
import java.net.URI
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes
import org.apache.commons.lang.SystemUtils
import com.groupon.artemisia.core.Keywords
import scala.collection.mutable
import scala.io.Source

/**
 * Created by chlr on 3/6/16.
 */


/**
 * An aggregation of Helper methods related to FileSystem based operations
 *
 */
object FileSystemUtil {

  /**
   * read a text-file using the classloader
   *
   * @param resource name of the resource to be read.
   * @return String content of the resource
   */
  def readResource(resource: String) = {
    val resource_stream = this.getClass.getResourceAsStream(resource)
    val buffered = new BufferedReader(new InputStreamReader(resource_stream))
    Stream.continually(buffered.readLine()).takeWhile(_ != null)
  }

  /**
   * get the basedir inside which the application can creates temp/checkpoint files required for execution
   * @return basedir for the application
   */
  def baseDir = {
    Paths.get(joinPath(System.getProperty("java.io.tmpdir"),Keywords.APP.toLowerCase))
  }

  /**
   * write the given content to text-file
   *
   * @param content content to be written
   * @param file path of the file
   * @param append set true to append content to the file.
   */
  def writeFile(content: String, file: File, append: Boolean = true) {
    file.getParentFile.mkdirs()
    val writer = new BufferedWriter(new FileWriter(file,append))
    writer.write(content)
    writer.close()
  }


  /**
   * returns a composed path joining input paths
   *
   * {{{
     *   joinPath("/home","/artemesia/","mydirectory")
   * }}}
   *
   * would return /home/artemesia/mydirectory
   *
   * @param path varags of paths to be joined
   * @return new path
   */
  def joinPath(path: String*) = {
    path.foldLeft(System.getProperty("file.separator"))((a: String, b: String) => new File(a, b).toString)
  }

  /**
   * build URI object from the given path. set file as the default scheme
   * @param path path to be converted
   * @return java.net.URI object
   */
  def makeURI(path: String) = {
    if (URI.create(path).getScheme == null)
       new File(path).toURI
    else
      new URI(path)
  }

  /**
   * an utility function that works similar to try with resources.
   * the resource here is the file and in the end the file is deleted
   * @param fileName name of the file to be created
   * @param body the code block that takes the newly created file as input
   * @return
   */
  def withTempFile(directory: String = null,fileName: String)(body: File => Unit): Unit = {
    val file = if (directory == null) File.createTempFile(fileName, null) else File.createTempFile(fileName, null, new File(directory))
    try
      body(file)
    finally
      file.delete()
  }


  /**
   * execute a block of code with a temp directory created and at the end of execution remove that directory
   *
   * @param directoryName name of the directory
   * @param body block of code to be executed
   */
  def withTempDirectory(directoryName: String)(body: File => Unit) = {
    val dir = Files.createTempDirectory(directoryName).toFile
    try
      body(dir)
    finally
      dir.delete()
  }


  /**
    * An utility to convert path with Java style globs to a list of file.
    * Java's glob patterns such as *, **, ? are supported.
    * @param path path to expand
    */
  def expandPathToFiles(path: Path, filesOnly: Boolean = true): Seq[File] = {

    def walkFileTree(baseDir: Path, glob: String, filesOnly: Boolean) = {
      val fileList = mutable.Buffer[File]()
      val fileSystem = FileSystems.getDefault
      val matcher = fileSystem.getPathMatcher("glob:" + joinPath(baseDir.toString, glob))
      val fileSystemVisitor = new SimpleFileVisitor[Path]() {
        override def visitFile(currentPath: Path, attribs: BasicFileAttributes) = {
          if (matcher.matches(currentPath) && (!filesOnly || currentPath.toFile.isFile)) {
            fileList += currentPath.toFile
            currentPath.toFile
          }
          FileVisitResult.CONTINUE
        }
      }
      Files.walkFileTree(baseDir, fileSystemVisitor)
      fileList
    }

    val globSymbols = "*" :: "?" :: "[" :: "{" :: Nil
    val inputPath = path.toAbsolutePath.toString.split(File.separator).toSeq
    val baseDir =  inputPath takeWhile {
      x => { ! globSymbols.exists(x.contains) }
    } mkString File.separator
   baseDir match {
      case x if x == path.toString => path.toFile :: Nil
      case _ => {
        val glob = inputPath drop baseDir.split(File.separator).length mkString File.separator
        walkFileTree(Paths.get(baseDir), glob, filesOnly)
      }
    }
  }

  /**
    * creates a single BufferedReader for multiple input files.
    * @param files
    * @return
    */
  def mergeFileStreams(files: Seq[File]) = {
    val identityStream: InputStream = new ByteArrayInputStream(Array[Byte]())
    files.foldLeft(identityStream) {
      (x, y) => new SequenceInputStream(x, new FileInputStream(y))
    }
  }

  /**
    * resolve a path to a sequence of files (multiple files if globs are used.)
    * gather info on the summation of size of the file.
    * @param path
    * @return
    */
  def getPathForLoad(path: Path) = {
    val files= expandPathToFiles(path)
    mergeFileStreams(files) -> files.map(_.length).sum
  }


  /**
    * create named pipe. This is method is supported only in OSX and Linux
    * @param file
    * @return
    */
  def createNamedPipe(file: String) = {
    assert(SystemUtils.IS_OS_MAC_OSX || SystemUtils.IS_OS_LINUX, "creating pipes is supported only in Mac OSX and Linux")
    CommandUtil.executeCmd(Seq("mkfifo", file))
  }


  /**
   * A pimp my library pattern for enhancing java.io.File object with BufferedWriter methods
   * @param file file object to be enhanced.
   */
  implicit class FileEnhancer(file: File) {

    private val writer = new BufferedWriter(new FileWriter(file))
    private val reader = new BufferedReader(new FileReader(file))

    /**
     * write provided content appended with a new line
     * @param content
     */
    def <<= (content: String) = {
      assert(file.isFile, "cannot write to a directory")
      writer.write(content+"\n")
      writer.flush()
    }

    /**
     *
     * @return content of the file
     */
    def content = {
      assert(file.isFile, "cannot write to a directory")
      Source.fromFile(file).getLines().mkString("\n")
    }

  }

}
