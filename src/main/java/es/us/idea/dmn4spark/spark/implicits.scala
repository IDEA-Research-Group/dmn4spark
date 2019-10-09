package es.us.idea.dmn4spark.spark

import es.us.idea.dmn4spark.dmn.DMNSparkEngine
import org.apache.spark.sql.DataFrame

object implicits {

  implicit class Droos4Spark(df: DataFrame) {
    def dmn: DMNSparkEngine = new DMNSparkEngine(df)
  }

}
