package es.us.idea.dmn4spark.dmn.executor

import es.us.idea.dmn4spark.dmn.engine.SafeCamundaFeelEngineFactory
import org.camunda.bpm.dmn.engine.impl.{DefaultDmnEngineConfiguration, DmnDecisionTableImpl}
import org.camunda.bpm.dmn.engine.{DmnDecision, DmnEngine, DmnEngineConfiguration}
import org.camunda.bpm.model.dmn.{Dmn, DmnModelInstance}

import java.io.ByteArrayInputStream
import scala.collection.JavaConverters._
import scala.collection.{JavaConverters, mutable}
import org.apache.log4j.Logger

class DMNExecutor(input: Array[Byte]) extends Serializable{

  @transient lazy val logger: Logger = Logger.getLogger("DMNExecutor")

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
  def getDecisionsResults(inputData: Map[String, AnyRef], decisionsToEvaluate: Seq[String] = Seq()): Seq[Option[DecisionResult]] = {
    // TODO: check mdoelinputs, and instantiate java map with the values of data map. If input not present, set it as null

    val javaMap = new java.util.HashMap[String, AnyRef]()
    val dmnDecisions = if(decisionsToEvaluate.isEmpty) decisions else decisionsToEvaluate.map(str => decisions.find(_.getName == str).get)

    modelInputs.foreach(d => if(inputData.keySet.contains(d)) javaMap.put(d, inputData(d)) else javaMap.put(d, null) )

    dmnDecisions
      .map(d => {

        val evaluation = dmnEngine.evaluateDecisionTable(d, javaMap)

        val resultList = evaluation.getResultList

        val processMap = (x: mutable.Map[String, AnyRef]) =>
          x.keySet.size match {
            case 1 => Some(DecisionResult(x.head._1, x.head._2.toString))
            //case i: Int if i>1 => Some("{" + x.map(t => s""""${t._1}": ${t._2.toString}""").mkString(", ") + "}")
            case i: Int if i>1 => Some(DecisionResult(decisionName = "["+x.map(t => s""""${t._1}""" ).mkString(", ")+"]",
              result = "["+x.map(t => s""""${t._2.toString}""" ).mkString(", ")+"]"))
            case _ => logger.warn(s"[WARNING] DMNExecutor: The evaluation of the DMN Table with name ${d.getName} yielded no " +
              s"results.\n Please, revise your DMN Diagram:\n" +
              s"  1) Does the following input match any rule? ${javaMap.asScala.toString}\n" +
              s"  2) Have you correctly specified the dependencies? (${d.getName} requires the following tables: ${d.getRequiredDecisions.asScala.map(_.getName).mkString(", ")})\n" +
              s"  3) Have you correctly specified table keys?\n"); None
          }

        // Check how many results have been returned by this DMN table.
        // If 1: returns a single DecisionResult
        // If >1: returns a single DecisionResult, but decision=
        resultList.size() match {
          case i: Int => processMap(resultList.get(0).asScala)
          case _ => logger.warn(s"[WARNING] DMNExecutor: The evaluation of the DMN Table with name ${d.getName} yielded no " +
            s"results.\n Please, revise your DMN Diagram:\n" +
            s"  1) Does the following input match any rule? ${javaMap.asScala.toString}\n" +
            s"  2) Have you correctly specified the dependencies? (${d.getName} requires the following tables: ${d.getRequiredDecisions.asScala.map(_.getName).mkString(", ")})\n" +
            s"  3) Have you correctly specified table keys?\n"); None
        }


        //DecisionResult(d.getName, dmnEngine.evaluateDecisionTable(d, javaMap).getFirstResult.getEntry(d.getKey).toString)
      })

  }

}
