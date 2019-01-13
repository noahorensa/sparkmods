import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.StructType

class SmartRelation(data: List[Row], s:StructType, tableName: String) extends BaseRelation with TableScan {
  val rdd: RDD[Row] = Spark.sc.parallelize(data)
  rdd.setName(name)
  val df: DataFrame = Spark.spark.createDataFrame(rdd, s)
  df.createOrReplaceTempView(name)

  def name: String = tableName

  def buildScan(): RDD[Row] = df.rdd

  override def sqlContext: SQLContext = Spark.sqlContext

  override def schema: StructType = df.schema
}
