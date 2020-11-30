package es.us.idea.dmn4spark

import java.io.FileInputStream

import es.us.idea.dmn4spark.dmn.DMNExecutor
import org.apache.commons.io.IOUtils
import org.camunda.bpm.dmn.engine.impl.DmnDecisionTableImpl

import scala.collection.JavaConverters.collectionAsScalaIterableConverter

object MainDMNAnalysis {

  def main(args: Array[String]): Unit = {
    val path = "models/business-rules-2.dmn"



    val dmnExecutor = new DMNExecutor(IOUtils.toByteArray(new FileInputStream(path)))

    val modelInstasnce = dmnExecutor.dmnModelInstance
    val dmnEngine = dmnExecutor.dmnEngine

    val graph = dmnEngine.parseDecisionRequirementsGraph(modelInstasnce)

    val decisions = graph.getDecisions.asScala

    for(decision<-decisions) {
      val requiredDecisions = decision.getRequiredDecisions.asScala

      val decisionLogic = decision.getDecisionLogic
      val decisionLogicImpl = decisionLogic.asInstanceOf[DmnDecisionTableImpl]
      val rules = decisionLogicImpl.getRules.asScala
      //for(rule<-rules){
      //  rule.
      //}

      println(decisionLogicImpl)
    }

    /*
    val definitions = modelInstasnce.getDefinitions

    val artifacts = definitions.getArtifacts // EMPTY
    val businessContextElements = definitions.getBusinessContextElements // EMPTY

    val dgrElements = definitions.getDrgElements.asScala

    for(element <- dgrElements) {
      element.
      println(element)
    }
*/
    println("finished")


  }

}
