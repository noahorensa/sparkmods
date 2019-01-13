import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeReference, BinaryExpression, EqualTo, Expression, IsNotNull, Literal, Or, PredicateHelper, UnaryExpression}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{LogicalRDD, SparkPlan}

case class SmartJoin(joinId: String, left: LogicalPlan, right: LogicalPlan) extends BinaryNode {

  def getCachedJoin: Seq[SparkPlan] = CachedJoin.get(joinId, left.output, right.output) :: Nil

  override def output: Seq[Attribute] = CachedJoin.get(joinId, left.output, right.output).output
}

object SmartJoinStrategy extends Strategy with Serializable {

  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case s: SmartJoin => s.getCachedJoin
    case _ => Nil // return an empty list if we don't know how to handle this plan.
  }
}

object SmartJoinOptimization extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case j @ Join(left: Filter, right: Filter, Inner, Some(EqualTo(a: AttributeReference, b: AttributeReference))) =>
      val lTable = left.child match {
        case relation: LogicalRDD => relation.rdd.name
        case _ => ""
      }
      val rTable = right.child match {
        case relation: LogicalRDD => relation.rdd.name
        case _ => ""
      }
      val joinId = lTable + "_" + rTable + "_on_" + a.name + "_eq_" + b.name

      if (CachedJoin.has(joinId))
        Filter(And(left.condition, right.condition), SmartJoin(joinId, left.child, right.child))
      else j

    case j @ Join(left: Filter, right: LogicalRDD, Inner, Some(EqualTo(a: AttributeReference, b: AttributeReference))) =>
      val lTable = left.child match {
        case relation: LogicalRDD => relation.rdd.name
        case _ => ""
      }

      val joinId = lTable + "_" + right.rdd.name + "_on_" + a.name + "_eq_" + b.name

      if (CachedJoin.has(joinId))
        Filter(left.condition, SmartJoin(joinId, left.child, right))
      else j

    case j @ Join(left: LogicalRDD, right: Filter, Inner, Some(EqualTo(a: AttributeReference, b: AttributeReference))) =>
      val rTable = right.child match {
        case relation: LogicalRDD => relation.rdd.name
        case _ => ""
      }
      val joinId = left.rdd.name + "_" + rTable + "_on_" + a.name + "_eq_" + b.name

      if (CachedJoin.has(joinId))
        Filter(right.condition, SmartJoin(joinId, left, right.child))
      else j

    case j @ Join(left: LogicalRDD, right: LogicalRDD, Inner, Some(EqualTo(a: AttributeReference, b: AttributeReference))) =>
      val joinId = left.rdd.name + "_" + right.rdd.name + "_on_" + a.name + "_eq_" + b.name

      if (CachedJoin.has(joinId))
        SmartJoin(joinId, left, right)
      else j
  }
}
