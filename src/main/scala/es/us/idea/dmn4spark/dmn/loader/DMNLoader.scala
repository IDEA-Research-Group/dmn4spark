package es.us.idea.dmn4spark.dmn.loader

import es.us.idea.dmn4spark.dmn.exception.{DMN4SparkException, FailedToCreateDMNExecutor, FailedToReadException}
import es.us.idea.dmn4spark.dmn.executor.DMNExecutor

import scala.util.Try

abstract class DMNLoader {

  def getByteArray(): Either[FailedToReadException, Array[Byte]]
  def load(): Either[DMN4SparkException, DMNExecutor] = getByteArray().map(arr => {
    val dmnExecutor = new DMNExecutor(arr)
    Try(dmnExecutor.dmnModelInstance).toEither match {
      case Right(value) => Right(dmnExecutor)
      case Left(exc) => Left(FailedToCreateDMNExecutor(s"Failed to create DMN executor: ${exc.getMessage}", exc))
    }
  }).joinRight

}
