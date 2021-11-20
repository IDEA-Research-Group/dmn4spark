package es.us.idea.dmn4spark.dmn.loader

import es.us.idea.dmn4spark.dmn.exception.FailedToReadException
import org.apache.commons.io.IOUtils

import java.io.InputStream
import scala.util.Try

class InputStreamLoader(is: InputStream) extends DMNLoader {

  override def getByteArray(): Either[FailedToReadException, Array[Byte]] =
    Try(IOUtils.toByteArray(is))
      .toEither
      .left
      .map(t => FailedToReadException(s"Failed to generate Array of bytes from input stream: ${t.getMessage}" , t))
}
