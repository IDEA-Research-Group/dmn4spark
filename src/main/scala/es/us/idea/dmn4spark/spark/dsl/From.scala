package es.us.idea.dmn4spark.spark.dsl

import es.us.idea.dmn4spark.dmn.loader.{DMNLoader, HDFSLoader, InputStreamLoader, PathLoader, URLLoader}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.DataFrame

import java.io.InputStream
import scala.language.implicitConversions

class From(df: DataFrame) {

  implicit def dmnLoaderToLoad(dmnLoader: DMNLoader): Load = {
    dmnLoader.load() match {
      case Right(dmnExecutor) => new Load(df, dmnExecutor)
      case Left(exception) => throw exception
    }
  }

  def localFile(path: String): Load = new PathLoader(path)
  def hdfs(uri: String, configuration: Configuration = new Configuration()): Load =
    new HDFSLoader(uri, configuration)
  def url(url: String): Load = new URLLoader(url)
  def inputStream(is: InputStream): Load = new InputStreamLoader(is)
}