package com.target_ready.data.pipeline.exceptions

class exceptions(massage: String, cause: Throwable) extends Exception(massage, cause) {
  def this(massage: String) = this(massage, None.orNull)
}

case class readFileException(message: String) extends exceptions(message)

case class writeFileException(message: String) extends exceptions(message)

case class sparkSessionException(message: String) extends exceptions(message)

case class dqNullCheckException(message: String) extends exceptions(message)

case class dqDupCheckException(message: String) extends exceptions(message)


