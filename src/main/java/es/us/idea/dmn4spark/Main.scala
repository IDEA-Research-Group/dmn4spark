package es.us.idea.dmn4spark

import es.us.idea.spark4drools
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("Sample").getOrCreate()

    import spark4drools.implicits._
    import implicits._
    import spark.implicits._

    val df = spark.read.option("header", "true").csv("input/movies_dataset_enriched.csv")
      .drools.fromLocalPath("input/moviesDatasetBR.drl")//.filter($"BR3" === false)
      .dmn.loadFromLocalPath("models/simulation(7).dmn")

    df.printSchema
    df.show()
  }
}
