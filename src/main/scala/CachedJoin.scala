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
    join.buildScan().map(row => new RowAsInternalRow(row.toSeq.toArray).asInstanceOf[InternalRow])

  override def output: Seq[Attribute] = leftOutput ++ rightOutput

  override def children: Seq[SparkPlan] = Nil
}

class RowAsInternalRow(val v: Array[Any]) extends GenericInternalRow(v) {

  v.transform {
    case s: String => UTF8String.fromString(s)
    case d: Date => (d.getTime / 1000l / 60l / 60l / 24l).toInt
    case e: Any => e
  }

//  override def getUTF8String(ordinal: Int): UTF8String =
//    UTF8String.fromString(genericGet(ordinal).asInstanceOf[String])
//
//  override def getInt(ordinal: Int): Int = {
//    val value = genericGet(ordinal)
//    value match {
//      case d: Date => d.getDate
//      case i:Int => i
//    }
//  }
}

class JoinIdentity(val left: String, val right: String) {

  def this(leftTable: String, leftCol: String, rightTable: String, rightCol: String) =
    this(leftTable + "_" + leftCol, rightTable + "_" + rightCol)

  override def equals(obj: Any): Boolean = obj match {
    case that: JoinIdentity =>
      this.left == that.left && this.right == that.right

    case _ => false
  }

  override def hashCode(): Int = (left + right).hashCode

  def tableName: String = left + "_smartjoin_" + right
}

object JoinIdentity {
  def apply(left: String, right: String): JoinIdentity =
    new JoinIdentity(left, right)

  def apply(leftTable: SmartRelation, leftCol: String, rightTable: SmartRelation, rightCol: String): JoinIdentity =
    new JoinIdentity(leftTable.name, leftCol, rightTable.name, rightCol)

  def apply(leftTable: String, leftCol: String, rightTable: String, rightCol: String): JoinIdentity =
    new JoinIdentity(leftTable, leftCol, rightTable, rightCol)
}

object CachedJoin {

  private var registeredJoins: Map[JoinIdentity, SmartRelation] = Map()

  def registerJoin(joinData: List[Row],
                   leftTable: SmartRelation, leftCol: String,
                   rightTable: SmartRelation, rightCol: String): Unit = {
    val id = JoinIdentity(leftTable, leftCol, rightTable, rightCol)
    val join = SmartRelation(joinData, StructType(leftTable.schema.union(rightTable.schema)), id.tableName)
    registeredJoins += (id -> join)
  }

  def has(joinIdentity: JoinIdentity): Boolean =
    registeredJoins.contains(joinIdentity) || registeredJoins.contains(JoinIdentity(joinIdentity.right, joinIdentity.left))

  def get(joinIdentity: JoinIdentity, leftOutput: Seq[Attribute], rightOutput: Seq[Attribute]): CachedJoin =
    if (registeredJoins.contains(joinIdentity))
      CachedJoin(registeredJoins(joinIdentity), leftOutput, rightOutput)
    else
      CachedJoin(registeredJoins(JoinIdentity(joinIdentity.right, joinIdentity.left)), rightOutput, leftOutput)
}
