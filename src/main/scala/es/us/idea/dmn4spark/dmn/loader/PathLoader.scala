package es.us.idea.dmn4spark.dmn.loader

import es.us.idea.dmn4spark.dmn.exception.FailedToReadException
import org.apache.commons.io.IOUtils

import java.io.FileInputStream
import scala.util.Try

class PathLoader(path: String) extends DMNLoader {

  override def getByteArray(): Either[FailedToReadException, Array[Byte]] =
    Try(IOUtils.toByteArray(new FileInputStream(path)))
      .toEither.left.map(t => FailedToReadException(s"Failed to read from the specified path: ${t.getMessage}" , t))

}
