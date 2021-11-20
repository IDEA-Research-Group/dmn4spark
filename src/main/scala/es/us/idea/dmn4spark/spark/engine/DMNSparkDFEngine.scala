package es.us.idea.dmn4spark.spark.engine

import es.us.idea.dmn4spark.dmn.executor.DMNExecutor
import es.us.idea.dmn4spark.spark.{SparkRowToScalaTypesConversor, Utils}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, Row}

import java.io.{FileInputStream, InputStream}
import java.net.{MalformedURLException, URI}
import scala.io.Source

// TODO: Include a parameter to allow hiding logs
/***
 *
 * @param df the DataFrame to be processed
 * @param selectedDecisions the name of the DMN tables to include in the resulting DataFrame
 */
class DMNSparkDFEngine(
                        df: DataFrame,
                        dmnExecutor: DMNExecutor,
                        selectedDecisions: Seq[String] = Seq(),
                        dmnOutputColumnName: Option[String] = None,
                        evaluationRuntime: Boolean = true) extends Serializable {

  def evaluateDecisions(decisions: String*): DMNSparkDFEngine = new DMNSparkDFEngine(df, dmnExecutor, decisions, dmnOutputColumnName, evaluationRuntime)
  def withOutputColumn(name: String): DMNSparkDFEngine = new DMNSparkDFEngine(df, dmnExecutor, selectedDecisions, Some(name), evaluationRuntime)
  def appendOutput(): DMNSparkDFEngine = new DMNSparkDFEngine(df, dmnExecutor, selectedDecisions, None, evaluationRuntime)
  def activateEvaluationRuntime(): DMNSparkDFEngine = new DMNSparkDFEngine(df, dmnExecutor, selectedDecisions, dmnOutputColumnName, true)
  def deactivateEvaluationRuntime(): DMNSparkDFEngine = new DMNSparkDFEngine(df, dmnExecutor, selectedDecisions, dmnOutputColumnName, false)

  def execute(/*input: Array[Byte], selectedDmnDecisions: Seq[String] = Seq()*/) = {
    val tempColumn = s"__${System.currentTimeMillis().toHexString}"

    //val dmnExecutor = new DMNExecutor(input)

    val allDecisionColumns = dmnExecutor.decisionKeys
    // TODO: runtime should be selected through parameter
    val selectedColumns = (if(selectedDecisions.isEmpty) allDecisionColumns else selectedDecisions) :+ "runTime"

    val originalColumns = df.columns.map(col).toSeq

    val func = new UDF1[Row, Row] {
      override def call(t1: Row): Row = {
        val map = SparkRowToScalaTypesConversor.spark2map(t1)
        val start = System.currentTimeMillis()
        val result = dmnExecutor.getDecisionsResults(map, selectedDecisions).map(_.get.result) // TODO this optional should be safely accessed!!
        val time = System.currentTimeMillis() - start
        Row.apply((result :+ time.toString) : _*)
      }
    }
    val dmnUdf = udf(func, Utils.createStructType(selectedColumns))

    val names = selectedColumns.map(d => (s"$tempColumn.$d", d))
    //println("****************************************")
    //println(names)
    // TODO runtime optional
    // TODO support DMN datatype conversion
    val transformedDF = df.withColumn(tempColumn, explode(array(dmnUdf(struct(originalColumns: _*)))))
      .withColumn(tempColumn,
        struct(
          (selectedColumns.filter(_ != "runTime").map(c => col(s"$tempColumn.$c")) :+
            col(s"$tempColumn.runTime").cast(LongType).alias("runTime")) : _*
        )) // drop not working in nested structures in Spark >= 3
    if(dmnOutputColumnName.isDefined)
      transformedDF.withColumnRenamed(tempColumn, dmnOutputColumnName.get)
    else names.foldLeft(transformedDF)((acc, n) => acc.withColumn(n._2, col(n._1))).drop(col(tempColumn))
  }
}
