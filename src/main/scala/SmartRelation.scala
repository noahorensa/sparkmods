import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.StructType

class SmartRelation(tableName: String) extends BaseRelation with TableScan with Serializable {

  def name: String = tableName

  def buildScan(): RDD[Row] = SmartRelation.getRDD(tableName)

  override def sqlContext: SQLContext = Spark.sqlContext

  override def schema: StructType = SmartRelation.getDF(tableName).schema
}

object SmartRelation {

  var relation: Map[String, (RDD[Row], DataFrame)] = Map()

  def registerRelation(name: String, rdd: RDD[Row], df: DataFrame): Unit = {
    relation += (name -> (rdd, df))
  }

  def getRDD(name: String): RDD[Row] = relation(name)._1

  def getDF(name: String): DataFrame = relation(name)._2

  def apply(data: List[Row], schema: StructType, tableName: String): SmartRelation = {
    val rdd = Spark.sc.parallelize(data)
    rdd.setName(tableName)
    val df: DataFrame = Spark.spark.createDataFrame(rdd, schema)
    df.createOrReplaceTempView(tableName)
    registerRelation(tableName, rdd, df)
    new SmartRelation(tableName)
  }
}
