package es.us.idea.dmn4spark.dmn

import java.io.File

import org.camunda.bpm.dmn.engine.impl.DefaultDmnEngineConfiguration
import org.camunda.bpm.dmn.engine.{DmnDecision, DmnDecisionTableResult, DmnEngine, DmnEngineConfiguration}
import org.camunda.bpm.model.dmn.Dmn
import org.camunda.feel.integration.CamundaFeelEngineFactory

class DMNExecutor(dmnPath: String) extends Serializable{


  lazy val dmnEngine: DmnEngine = {
    val dmnEngineConfig: DefaultDmnEngineConfiguration = DmnEngineConfiguration.createDefaultDmnEngineConfiguration.asInstanceOf[DefaultDmnEngineConfiguration]
    dmnEngineConfig.setFeelEngineFactory(new CamundaFeelEngineFactory)
    dmnEngineConfig.setDefaultOutputEntryExpressionLanguage("feel")
    dmnEngineConfig.buildEngine
  }

  //lazy val is: InputStream = new FileInputStream(dmnPath)
  lazy val dmnModelInstance = Dmn.readModelFromFile(new File(dmnPath));

  def getDecision(map: java.util.HashMap[String, AnyRef]):String = {
    val decision: DmnDecision = dmnEngine.parseDecision("dq", dmnModelInstance) //parseDecision("accuracy", )
    val result: DmnDecisionTableResult = dmnEngine.evaluateDecisionTable(decision, map)
    val str: String = result.getFirstResult.getEntry("dq")
    str
  }

}
