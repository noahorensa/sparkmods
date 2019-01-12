import java.sql.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, GenericInternalRow}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

/** A join that returns the internal rdd of a cached SmartRelation */
case class CachedJoin(join: SmartRelation, leftOutput: Seq[Attribute], rightOutput: Seq[Attribute]) extends SparkPlan {

  override protected def doExecute(): RDD[InternalRow] =
    join.rdd.map(row => new RowAsInternalRow(row.toSeq.toArray).asInstanceOf[InternalRow])

  override def output: Seq[Attribute] = leftOutput ++ rightOutput

  override def children: Seq[SparkPlan] = Nil
}

class RowAsInternalRow(val v: Array[Any]) extends GenericInternalRow(v) {

  override def getUTF8String(ordinal: Int): UTF8String =
    UTF8String.fromString(genericGet(ordinal).asInstanceOf[String])

  override def getInt(ordinal: Int): Int = {
    val value = genericGet(ordinal)
    value match {
      case d: Date => d.getDate
      case i:Int => i
    }
  }
}

object CachedJoin {

  private var registeredJoins: Map[String, SmartRelation] = Map()

  def registerJoin(joinData: List[Row],
                   leftTable: SmartRelation, leftCol: String,
                   rightTable: SmartRelation, rightCol: String): Unit = {
    val joinName = leftTable.name + "_" + rightTable.name + "_on_" + leftCol + "_eq_" + rightCol
    val join = new SmartRelation(joinData, StructType(leftTable.schema.union(rightTable.schema)), joinName)
    registeredJoins += (joinName -> join)
  }

  def has(joinName:String): Boolean = registeredJoins contains joinName

  def get(joinName:String, leftOutput: Seq[Attribute], rightOutput: Seq[Attribute]): CachedJoin =
    CachedJoin(registeredJoins(joinName), leftOutput, rightOutput)
}
