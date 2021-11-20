package es.us.idea.dmn4spark.dmn.exception

class DMN4SparkException(private val message: String = "",
                              private val cause: Throwable = None.orNull)
  extends Exception(message, cause)
