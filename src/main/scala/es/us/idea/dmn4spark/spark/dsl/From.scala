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

  def fromLocalFile(path: String): Load = new PathLoader(path)
  def fromHDFS(uri: String, configuration: Configuration = new Configuration()): Load =
    new HDFSLoader(uri, configuration)
  def fromURL(url: String): Load = new URLLoader(url)
  def fromInputStream(is: InputStream): Load = new InputStreamLoader(is)
}