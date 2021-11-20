package es.us.idea.dmn4spark.spark.dsl

import es.us.idea.dmn4spark.dmn.executor.DMNExecutor
import es.us.idea.dmn4spark.spark.engine.DMNSparkDFEngine
import org.apache.spark.sql.DataFrame

class Load(df: DataFrame, dmnExecutor: DMNExecutor) {
  def load() = new DMNSparkDFEngine(df, dmnExecutor)
}
