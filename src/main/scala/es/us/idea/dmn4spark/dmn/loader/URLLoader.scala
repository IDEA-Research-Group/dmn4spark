package es.us.idea.dmn4spark.dmn.loader

import es.us.idea.dmn4spark.dmn.exception.FailedToReadException

import scala.io.Source

class URLLoader(url: String) extends DMNLoader {

  override def getByteArray(): Either[FailedToReadException, Array[Byte]] = {
    try {
      val content = Source.fromURL(url)
      Right(content.mkString.getBytes)
    } catch {
      case t: Throwable => Left(FailedToReadException(s"Filed to read from URL: ${t.getMessage}", t))
    }
  }

}
