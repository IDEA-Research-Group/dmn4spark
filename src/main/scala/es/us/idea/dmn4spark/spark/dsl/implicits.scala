package es.us.idea.dmn4spark.spark.dsl

import org.apache.spark.sql.DataFrame

object implicits {

  implicit class DMN4Spark(df: DataFrame) {
    def dmn: From = new From(df)
  }

}
