package es.us.idea.dmn4spark.dmn

import java.io.{FileInputStream, InputStream}

import org.camunda.bpm.dmn.engine.impl.DefaultDmnEngineConfiguration
import org.camunda.bpm.dmn.engine.{DmnDecision, DmnDecisionTableResult, DmnEngine, DmnEngineConfiguration}
import org.camunda.feel.integration.CamundaFeelEngineFactory

object DMNTest {
  def main(args: Array[String]): Unit = {

    // configure and build the DMN engine
    //DmnEngine dmnEngine = DmnEngineConfiguration.createDefaultDmnEngineConfiguration().buildEngine();
    val dmnEngineConfig: DefaultDmnEngineConfiguration = DmnEngineConfiguration.createDefaultDmnEngineConfiguration.asInstanceOf[DefaultDmnEngineConfiguration]
    dmnEngineConfig.setFeelEngineFactory(new CamundaFeelEngineFactory)
    dmnEngineConfig.setDefaultOutputEntryExpressionLanguage("feel")
    val dmnEngine: DmnEngine = dmnEngineConfig.buildEngine


    val is: InputStream = new FileInputStream("models/simulation(7).dmn")

    // parse a decision
    val decision: DmnDecision = dmnEngine.parseDecision("dq", is)
    //List<DmnDecision> decisions = dmnEngine.parseDecisions(is);


    val data = new java.util.HashMap[String, Any]()
    data.put("$BR3", true)
    data.put("$BR4", true)
    data.put("$BR5", true)

    data.put("$BR9", true)
    data.put("$BR10", true)
    data.put("$BR11", true)

    // evaluate a decision
    val result: DmnDecisionTableResult = dmnEngine.evaluateDecisionTable(decision, data.asInstanceOf[java.util.HashMap[String, AnyRef]])

    val r: String = result.getFirstResult.getEntry("dq")

    System.out.println(result)
    System.out.println(r)

    println("Hello")
  }
}
