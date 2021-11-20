package es.us.idea.dmn4spark.dmn.exception

case class FailedToReadException(private val message: String = "",
                                 private val cause: Throwable = None.orNull)
  extends DMN4SparkException(message, cause)
