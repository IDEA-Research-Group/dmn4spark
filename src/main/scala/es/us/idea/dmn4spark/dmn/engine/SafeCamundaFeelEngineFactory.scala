package es.us.idea.dmn4spark.dmn.engine

import org.camunda.bpm.dmn.feel.impl.FeelEngine
import org.camunda.feel.integration.CamundaFeelEngineFactory

class SafeCamundaFeelEngineFactory extends CamundaFeelEngineFactory{
  override def createInstance(): FeelEngine = new SafeCamundaFeelEngine
}
