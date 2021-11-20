package es.us.idea.dmn4spark.spark.dsl

import es.us.idea.dmn4spark.spark.engine.DMNSparkDFEngine
import org.apache.spark.sql.DataFrame

object implicits {

  implicit class DMN4Spark(df: DataFrame) {
    def dmn: DMNSparkDFEngine = new DMNSparkDFEngine(df)
  }

}
