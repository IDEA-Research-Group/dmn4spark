package es.us.idea.dmn4spark.dmn

import java.io.ByteArrayInputStream
import es.us.idea.dmn4spark.dmn.engine.SafeCamundaFeelEngineFactory
import org.camunda.bpm.dmn.engine.impl.{DefaultDmnEngineConfiguration, DmnDecisionTableImpl}
import org.camunda.bpm.dmn.engine.{DmnDecision, DmnEngine, DmnEngineConfiguration}
import org.camunda.bpm.model.dmn.{Dmn, DmnModelInstance}

import collection.JavaConverters._
import scala.collection.{JavaConverters, mutable}

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

  /**
   * Name of the input attributes to the DMN
   */
  @transient lazy val modelInputs: Seq[String] =
    decisions.flatMap(d => dmnEngine.parseDecision(d.getKey, dmnModelInstance)
      .getDecisionLogic.asInstanceOf[DmnDecisionTableImpl].getInputs.asScala.map(_.getName))
      .filterNot(d => decisions.map(_.getName).contains(d))

  /***
   * TODO: Receive a scala map instead of java map
   * @param map input tuple from the DataFrame
   * @return a sequence indicating the decision made for each DMN table in the DMN diagram.
   *         If the DMN table has more than one output, it will print the results as a JSON object.
   *         If the DMN table returns more than one output, it will print the results as a JSON array.
   */
  def getDecisionsResults(map: java.util.HashMap[String, AnyRef]): Seq[Option[String]] = {
    // TODO: check mdoelinputs, and instantiate java map with the values of data map. If input not present, set it as null

    decisions.map(d => {

      val evaluation = dmnEngine.evaluateDecisionTable(d, map)

      val resultList = evaluation.getResultList

      val processMap = (x: mutable.Map[String, AnyRef]) =>
        x.keySet.size match {
          case 1 => Some(x.head._2.toString)
          case i: Int if i>1 => Some("{" + x.map(t => s""""${t._1}": ${t._2.toString}""").mkString(", ") + "}")
          case _ => System.err.println(s"[WARNING] DMNExecutor: The evaluation of the DMN Table with name ${d.getName} yielded no " +
            s"results.\n Please, revise your DMN Diagram:\n" +
            s"  1) Does the following input match any rule? ${map.asScala.toString}\n" +
            s"  2) Have you correctly specified the dependencies? (${d.getName} requires the following tables: ${d.getRequiredDecisions.asScala.map(_.getName).mkString(", ")})\n" +
            s"  3) Have you correctly specified table keys?\n"); None
        }

      resultList.size() match {
        case 1 => processMap(resultList.get(0).asScala)
        case i: Int if i>1 => Some(s"[${resultList.asScala.map(m => processMap(m.asScala).orNull).mkString(", ")}]")
        case _ => System.err.println(s"[WARNING] DMNExecutor: The evaluation of the DMN Table with name ${d.getName} yielded no " +
          s"results.\n Please, revise your DMN Diagram:\n" +
          s"  1) Does the following input match any rule? ${map.asScala.toString}\n" +
          s"  2) Have you correctly specified the dependencies? (${d.getName} requires the following tables: ${d.getRequiredDecisions.asScala.map(_.getName).mkString(", ")})\n" +
          s"  3) Have you correctly specified table keys?\n"); None
      }
    })
  }

}
