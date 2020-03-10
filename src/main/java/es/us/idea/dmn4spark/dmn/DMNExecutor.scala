package es.us.idea.dmn4spark.dmn

import java.io.{ByteArrayInputStream, File, InputStream}

import org.apache.xerces.dom.DeferredDocumentImpl
import org.camunda.bpm.dmn.engine.impl.DefaultDmnEngineConfiguration
import org.camunda.bpm.dmn.engine.{DmnDecision, DmnDecisionTableResult, DmnEngine, DmnEngineConfiguration}
import org.camunda.bpm.model.dmn.{Dmn, DmnModelInstance}
import org.camunda.bpm.model.dmn.impl.instance.DecisionImpl
import org.camunda.bpm.model.dmn.instance.DecisionTable
import org.camunda.bpm.model.xml.impl.instance.DomElementImpl
import org.camunda.feel.integration.CamundaFeelEngineFactory

import scala.collection.JavaConverters

class DMNExecutor(input: Array[Byte], selectedDecisions: Seq[String]) extends Serializable{

  /*
   * @transient: since some of these objects are not serializable, by making them transient it forces them not to be
   * serialized. Instead, they will be recalculated.
   * lazy val: these fields will be calculated only the first time they are accessed instead of being calculated at the
   * object instantiation.
   */

  /**
   * Instantiate and configure the Camunda DMN Engine.
   * */
  @transient lazy val dmnEngine: DmnEngine = {
    val dmnEngineConfig: DefaultDmnEngineConfiguration = DmnEngineConfiguration.createDefaultDmnEngineConfiguration.asInstanceOf[DefaultDmnEngineConfiguration]
    dmnEngineConfig.setFeelEngineFactory(new CamundaFeelEngineFactory())
    dmnEngineConfig.setDefaultOutputEntryExpressionLanguage("feel-scala")
    dmnEngineConfig.buildEngine
  }


  // TODO read from hdfs??
  /**
   * Instantiate and configure the Camunda DMN Engine.
   * */
  //@transient lazy val dmnModelInstance: DmnModelInstance = Dmn.readModelFromFile(new File(dmnPath));
  @transient lazy val dmnModelInstance: DmnModelInstance = Dmn.readModelFromStream(new ByteArrayInputStream(input))

  @transient lazy val decisions: Seq[DmnDecision] = JavaConverters.collectionAsScalaIterableConverter(dmnEngine.parseDecisions(dmnModelInstance)).asScala.toSeq
  @transient lazy val decisionKeys: Seq[String] = decisions.map(_.getKey)

  def getDecisionsResults(map: java.util.HashMap[String, AnyRef]): Seq[String] = {
    decisions.map(d => dmnEngine.evaluateDecisionTable(d, map).getFirstResult.getEntry(d.getKey).toString)
  }

}
