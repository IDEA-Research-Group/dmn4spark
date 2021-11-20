package es.us.idea.dmn4spark.dmn.loader

import es.us.idea.dmn4spark.dmn.exception.FailedToReadException
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import java.net.{MalformedURLException, URI}

class HDFSLoader(uri: String, configuration: Configuration = new Configuration()) extends DMNLoader {

  override def getByteArray(): Either[FailedToReadException, Array[Byte]] = {
    val hdfsUrlPattern = "((hdfs?)(:\\/\\/)(.*?)(^:[0-9]*$)?\\/)".r

    try {
      val firstPart = hdfsUrlPattern.findFirstIn(uri) match {
        case Some(s) => s
        case _ => throw new MalformedURLException(s"The provided HDFS URI is not valid: $uri")
      }

      val uriParts = uri.split(firstPart)
      if(uriParts.length != 2) throw new MalformedURLException(s"The provided HDFS URI is not valid. Path not found: $uri")

      val path = uriParts.lastOption match {
        case Some(s) => s
        case _ => throw new MalformedURLException(s"The provided HDFS URI is not valid. Path not found: $uri")
      }

      val fs = FileSystem.get(new URI(firstPart), configuration)
      val filePath = if(!new Path(path).isAbsolute) new Path(s"/$path") else new Path(path)

      val fsDataInputStream = fs.open(filePath);

      Right(IOUtils.toByteArray(fsDataInputStream.getWrappedStream))
    } catch {
      case t: Throwable => Left(FailedToReadException(s"Failed to read from HDFS: ${t.getMessage}", t))
    }
  }

}
