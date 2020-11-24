package es.us.idea.dmn4spark.dmn

import java.io.{File, FileInputStream, IOException, InputStream}
import java.net.{MalformedURLException, URI}

import es.us.idea.dmn4spark.spark.{SparkDataConversor, Utils}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.commons.io.{FileUtils, IOUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.spark.sql.api.java.{UDF0, UDF1}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

class DMNSparkEngine(df: DataFrame, selectedDecisions: Seq[String] = Seq())  {

  def setDecisions(decisions: String*): DMNSparkEngine = new DMNSparkEngine(df, decisions)

  def loadFromLocalPath(path:String) = execute(IOUtils.toByteArray(new FileInputStream(path)))

  def loadFromHDFS(uri: String, configuration: Configuration = new Configuration()) = {

    val hdfsUrlPattern = "((hdfs?)(:\\/\\/)(.*?)(^:[0-9]*$)?\\/)".r

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

    execute(IOUtils.toByteArray(fsDataInputStream.getWrappedStream))
  }

  def loadFromURL(url: String) = {
    val content = Source.fromURL(url)
    val bytes = content.mkString.getBytes
    execute(bytes)
  }

  def loadFromInputStream(is: InputStream) = execute(IOUtils.toByteArray(is))

  private def execute(input: Array[Byte]) = {
    val tempColumn = s"__${System.currentTimeMillis().toHexString}"

    val dmnExecutor = new DMNExecutor(input, selectedDecisions)

    val decisionColumns = dmnExecutor.decisionKeys
    val selectedColumns = if(selectedDecisions.isEmpty) decisionColumns else selectedDecisions

    val originalColumns = df.columns.map(col).toSeq

    val func = new UDF1[Row, Row] {
      override def call(t1: Row): Row = {
        val map = SparkDataConversor.spark2javamap(t1)
        val result = dmnExecutor.getDecisionsResults(map)
        Row.apply(result: _*)
      }
    }
    val dmnUdf = udf(func, Utils.createStructType(decisionColumns))

    val names = selectedColumns.map(d => (s"$tempColumn.$d", d))

    val transformedDF = df.withColumn(tempColumn, explode(array(dmnUdf(struct(originalColumns: _*)))))

    names.foldLeft(transformedDF)((acc, n) => acc.withColumn(n._2, col(n._1))).drop(col(tempColumn))
  }


}
