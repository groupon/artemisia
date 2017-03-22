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

import com.eclipsesource.json.Json
import com.groupon.artemisia.core.Keywords
import com.groupon.artemisia.task.localhost.util.RestEndPoint
import com.groupon.artemisia.task.{Task, TaskLike}
import com.groupon.artemisia.util.HoconConfigUtil.Handler
import com.typesafe.config._
import okhttp3._

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}


/**
  * Created by chlr on 12/4/16.
  */
class RestAPITask(override val taskName: String
                  ,val restEndPoint: RestEndPoint
                  ,val allowedStatusCodes: Seq[Int] = Seq(200)
                  ,val emitOutput: Boolean = false) extends Task(taskName) {


  val client = new OkHttpClient()

  override protected[task] def setup(): Unit = {
      require(RestEndPoint.allowedHttpMethods contains  restEndPoint.method,
      s"${restEndPoint.method} is not one of the allowed http methods. allowed http methods are ${RestEndPoint.allowedHttpMethods}")

      require(RestEndPoint.allowedPayloadTypes contains  restEndPoint.payloadType,
      s"${restEndPoint.payloadType} is not one of the allowed http methods. allowed payload types are ${RestEndPoint.allowedPayloadTypes}")
  }

  override protected[task] def work(): Config = {
    val response = client.newCall(request.build()).execute()
    assert(allowedStatusCodes contains response.code(), s"unexpected http status code. ${response.code()}. " +
      s"allowed status codes are ${allowedStatusCodes.mkString(",")}")
    parseResponse(response) -> emitOutput match {
      case (x, true) => x.as[Config]("body") withFallback ConfigFactory.empty.withValue(Keywords.TaskStats.STATS,x.root())
      case (x, false) => ConfigFactory.empty.withValue(Keywords.TaskStats.STATS,x.root())
    }
  }


  /**
    * prepare the request object
    *
    * @return
    */
  def request = {
    restEndPoint.header.foldLeft(new Request.Builder().url(restEndPoint.url)){
      (x,y) => {x.addHeader(y._1,y._2); x}
    } -> restEndPoint.method.toLowerCase match {
      case (x, "get") => x.get()
      case (x, "head") => x.head()
      case (x, "delete") => x.delete(requestBody(x))
      case (x, "post") => x.post(requestBody(x))
      case (x, "put") => x.put(requestBody(x))
      case (x, "patch") => x.patch(requestBody(x))
    }
  }


  /**
    * prepare the request body
    *
    * @param protoRequest
    * @return
    */
  def requestBody(protoRequest: Request.Builder) = {
    val emptyBody = RequestBody.create(null, Array[Byte]())
    def mediaType = {
      restEndPoint.payloadType match {
        case "json" =>
          MediaType.parse("application/json; charset=utf-8")
        case "xml" =>
          MediaType.parse("application/xml; charset=utf-8")
        case "text" =>
          MediaType.parse("plain/text; charset=utf-8")
      }
    }
    restEndPoint.body
      .map(body => RequestBody.create(mediaType, body.render(ConfigRenderOptions.concise())))
      .getOrElse(emptyBody)
  }


  protected def parseResponse(response: Response) = {
    {
      val respBody = {
        val x = response.body.string()
        response.body().close()
        x
      }

      Try(Json.parse(respBody)) match {
      case Success(x) => ConfigFactory parseString
        s"""
          | body = ${x.toString}
        """.stripMargin
      case Failure(_) => ConfigFactory.empty().withValue("body", ConfigValueFactory.fromAnyRef(respBody))
      }
    }
      .withValue("header",
       response.headers().names().asScala
      .foldLeft(ConfigFactory.empty()){
        (x,y) => x.withValue(y, ConfigValueFactory.fromIterable(response.headers(y)))
      }.root())
      .withValue("status", ConfigValueFactory.fromAnyRef(response.code()))
  }

  override protected[task] def teardown(): Unit = {}

}

object RestAPITask extends TaskLike {

  override val taskName: String = "RestAPITask"

  override val outputConfig: Option[Config] = Some {
    ConfigFactory parseString
      """
        | {
        |   header = {
        |     content-length = 1025
        |     connection = Keep-Alive
        |   }
        |   body = {
        |     foo = bar
        |     hello = world
        |   }
        |   status = 200
        | }
      """.stripMargin
  }
  override val info: String = "execute HTTP calls and handle results"

  override val desc: String =
    s"""
      | The Rest API task can be used to make HTTP calls. The following HTTP methods are supported
      | (${RestEndPoint.allowedHttpMethods.mkString(",")}).
      | The HTTP methods *get*, *head* cannot have request body while other methods can have request body. The payload can either
      | be of type *json*, *xml*, *text*. if the payload is of type *json* the body field type can be a Hocon type ie
      | Hocon Config Object type or Hocon Config Array since Hocon is a superset of Json. for eg the below setting is prefectly legal
      | and allowed. In the below case the *body* field type is a Hocon Config Object. It can also be of Hocon Config Array type.
      |
      |     request = {
      |       url = http://api.example.com
      |       body = {
      |         hello = world
      |         content = [foo, bar, baz]
      |       }
      |     }
      |
      | If your body is of type *xml* or *text* then the *body* field type has to be of type string. for example the below
      | example shows how to post a xml content.
      |
      |     request = {
      |       url = http://api.example.com/post
      |       method = post
      |       body = "<foo><bar>baz</bar></foo>"
      |     }
      |
      | *emit-output* field takes effect only if the HTTP response body is of type JSON. It parses the Json response body
      | and converts it to a Hocon config object and returns it as the output of the task. *emit-output* will also work
      | only when the JSON response is of type object and not an array. This is because the output of each task should be
      | of type ConfigObject and cannot be ConfigArray. for eg if the response of an API call was as shown below
      | and the *emit-output* field was set to true. Then the json response is parse and converted to Hocon config object
      | and it will be merged back to the job config. So the downstream tasks can refer to variable foo ($${foo}) and the value
      | will be resolved to "bar" successfully.
      |
      |       {
      |         "foo" : "bar"
      |       }
      |
      | This will not work if the response of the Rest API call was an json array like `["foo", "bar"]` due to above mentioned reasons.
      |
      | *allowed-status-codes* field takes a array of integer which represents the list of valid HTTP status code that an
      | HTTP call can have. For example if *allowed-status-codes* is set to `[200]` and if a Rest API call returns 404
      | then the API call is assumed to have failed and hence the task execution is failed.
      |
      |
    """.stripMargin

  override val outputConfigDesc: String =
    """
      |The **header**, **body**, **status** fields captures header, body and status of the HTTP call. if the returned
      |  response is of non json type, then the body is serialized as string value as shown below.
      |  ```
      |  body = "Hello world"
      |  ```
    """.stripMargin

  /**
    * Sequence of config keys and their associated values
    */
  override def paramConfigDoc: Config = ConfigFactory.empty()
    .withValue("request", RestEndPoint.structure.root)
    .withValue("emit-output", ConfigValueFactory.fromAnyRef("""yes"""))
    .withValue("allowed-status-codes", ConfigValueFactory.fromIterable(Seq(200, 100).asJava))

  /**
    *
    */
  override def defaultConfig: Config = ConfigFactory.empty()
    .withValue("allowed-status-codes", ConfigValueFactory.fromIterable(Seq(200).asJava))
    .withValue("request", RestEndPoint.defaultConfig.root)
    .withValue("emit-output", ConfigValueFactory.fromAnyRef(false))

  /**
    * definition of the fields in task param config
    */
  override def fieldDefinition: Map[String, AnyRef] = Map(
    "request" -> RestEndPoint.fieldDescription,
    "emit-output" -> ("if the api response body is a json object emit the result back to be merged with job config. " +
      "default value is false"),
    "allowed-status-codes" -> "list of allowed HTTP response status codes"
  )

  /**
    * config based constructor for task
    *
    * @param name   a name for the task
    * @param config param config node
    */
  override def apply(name: String, config: Config): Task = {
    new RestAPITask(name
      , RestEndPoint(config.as[Config]("request"))
      , config.as[List[Int]]("allowed-status-codes")
      , config.as[Boolean]("emit-output")
    )
  }

}
