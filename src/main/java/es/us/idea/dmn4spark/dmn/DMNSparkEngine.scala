package es.us.idea.dmn4spark.dmn

import es.us.idea.dmn4spark.spark.SparkDataConversor
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row}

class DMNSparkEngine(df: DataFrame)  {

  def loadFromLocalPath(path:String) = {

    val dmnExecutor = new DMNExecutor(path)

    val originalColumns = df.columns.map(col).toSeq

    val dmnUdf = udf((row: Row) => {
      val map = SparkDataConversor.spark2javamap(row)
      val result = dmnExecutor.getDecision(map) // Here is where Drools is invoked
      result
    })

    df.withColumn("DQ Assessment", explode(array(dmnUdf(struct(originalColumns: _*)))))

  }

}
