package com.groupon.artemisia.task

/**
  * Created by chlr on 12/22/17.
  */

sealed trait APIType

object JavaAPIType extends APIType

object ScalaAPIType extends APIType
