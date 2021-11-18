package es.us.idea.dmn4spark.spark.dsl

import es.us.idea.dmn4spark.spark.engine.DMNSparkEngine
import org.apache.spark.sql.DataFrame

object implicits {

  implicit class DMN4Spark(df: DataFrame) {
    def dmn: DMNSparkEngine = new DMNSparkEngine(df)
  }

}
