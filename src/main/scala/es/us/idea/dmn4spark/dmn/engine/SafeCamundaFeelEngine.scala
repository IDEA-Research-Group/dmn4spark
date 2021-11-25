package es.us.idea.dmn4spark.dmn.engine

import org.apache.log4j.Logger
import org.camunda.bpm.dmn.feel.impl.FeelException
import org.camunda.bpm.engine.variable.context.VariableContext
import org.camunda.feel.integration.CamundaValueMapper
import org.camunda.feel.interpreter.{RootContext, ValueMapper, VariableProvider}
import org.camunda.feel.spi.SpiServiceLoader

/**
 * This engine has the same behaviour as CamundaFeelEngine but it modifies the behaviour of evaluateSimpleUnaryTests
 * so that it does not throws any exception when evaluating the expressions.
 * */
class SafeCamundaFeelEngine extends org.camunda.bpm.dmn.feel.impl.FeelEngine {

  @transient lazy val logger: Logger = Logger.getLogger("SafeCamundaFeelEngine")

  private lazy val engine =
    new org.camunda.feel.FeelEngine(
      valueMapper = new CamundaValueMapper,
      functionProvider = SpiServiceLoader.loadFunctionProvider
    )

  private def asVariableProvider(ctx: VariableContext,
                                 valueMapper: ValueMapper): VariableProvider = (name: String) => {
    if (ctx.containsVariable(name)) {
      Some(valueMapper.toVal(ctx.resolve(name).getValue))
    } else {
      None
    }
  }

  override def evaluateSimpleExpression[T](expression: String,
                                           ctx: VariableContext): T = {
    val context = new RootContext(
      variableProvider = asVariableProvider(ctx, engine.valueMapper))
    engine.evalExpression(expression, context) match {
      case Right(value) => value.asInstanceOf[T]
      case Left(failure) => throw new FeelException(failure.message)
    }
  }

  override def evaluateSimpleUnaryTests(expression: String,
                                        inputVariable: String,
                                        ctx: VariableContext): Boolean = {
    val context = new RootContext(
      Map(RootContext.inputVariableKey -> inputVariable),
      variableProvider = asVariableProvider(ctx, engine.valueMapper))

    try {
      engine.evalUnaryTests(expression, context) match {
        case Right(value) => value
        case Left(failure) =>  logger.warn(s"SAFE FeelEngine: Failure in evalUnaryTests: ${failure.message}." +
          s"Expression: $expression, " + s"inputVariable: $inputVariable, value: ${context.variable(inputVariable)}");
          false //throw new FeelException(failure.message)
      }
    } catch {
      case e: Exception =>
        logger.warn(s"SAFE FeelEngine: Exception caught in evalUnaryTests: ${e}. Expression: $expression, " +
          s"inputVariable: $inputVariable, value: ${context.variable(inputVariable)}"); false
    }
  }

}
