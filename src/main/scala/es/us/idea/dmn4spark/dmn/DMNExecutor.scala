package es.us.idea.dmn4spark.dmn

import java.io.ByteArrayInputStream

import es.us.idea.dmn4spark.dmn.engine.SafeCamundaFeelEngineFactory
import org.camunda.bpm.dmn.engine.impl.DefaultDmnEngineConfiguration
import org.camunda.bpm.dmn.engine.{DmnDecision, DmnEngine, DmnEngineConfiguration}
import org.camunda.bpm.model.dmn.{Dmn, DmnModelInstance}

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
    dmnEngineConfig.setFeelEngineFactory(new SafeCamundaFeelEngineFactory)
    dmnEngineConfig.setDefaultOutputEntryExpressionLanguage("feel")
    dmnEngineConfig.buildEngine
  }


  /**
   * Instantiate and configure the Camunda DMN Engine.
   * */
  @transient lazy val dmnModelInstance: DmnModelInstance = Dmn.readModelFromStream(new ByteArrayInputStream(input))

  /**
   * Contains information on the DMN tables.
   * */
  @transient lazy val decisions: Seq[DmnDecision] = JavaConverters.collectionAsScalaIterableConverter(dmnEngine.parseDecisions(dmnModelInstance)).asScala.toSeq

  /**
   * Decision keys are employed to include them in the resulting column names.
   * */
  @transient lazy val decisionKeys: Seq[String] = decisions.map(_.getName)

  // TODO: catch and handle NullPointerExceptions
  def getDecisionsResults(map: java.util.HashMap[String, AnyRef]): Seq[String] = {
    decisions.map(d => dmnEngine.evaluateDecisionTable(d, map).getFirstResult.getEntry(d.getKey).toString)
  }

}
