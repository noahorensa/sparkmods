import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.{LogicalRDD, SparkPlan}

/** Find cases of inner joins on equal conditions */
object SmartJoin extends Strategy with Serializable {

  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case Join(l:Filter, r:Filter, Inner, Some(EqualTo(a: AttributeReference, b: AttributeReference))) =>
      val lTable = l.child match {
        case relation: LogicalRDD => relation.rdd.name
        case _ => ""
      }
      val rTable = r.child match {
        case relation: LogicalRDD => relation.rdd.name
        case _ => ""
      }
      val joinId = lTable + "_" + rTable + "_on_" + a.name + "_eq_" + b.name

      println("je suis")

      if (CachedJoin.has(joinId)) CachedJoin.get(joinId, l.output, r.output) :: Nil
      else Nil      // join is not cached, let Spark calculate it

    case _ => Nil // Return an empty list if we don't know how to handle this plan.
  }
}
