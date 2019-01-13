import java.{lang, util}

import org.apache.spark.annotation.{DeveloperApi, Experimental, InterfaceStability}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.StructType
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

import scala.reflect.runtime.universe

object Spark {
  private var sparkContext: SparkContext = _
  private var sparkSession: SparkSession = _

  private def createSparkContext = {
    val conf = new SparkConf()
    conf.setMaster("local[4]")
    conf.setAppName("custom-joins")
    sparkContext = new SparkContext(conf)
    sparkContext
  }

  def setSparkContext(sparkContext: SparkContext): Unit = this.sparkContext = sparkContext

  def sc: SparkContext = if (sparkContext != null) sparkContext else createSparkContext

  private def createSparkSession = {
    sc
    sparkSession = SparkSession.builder().getOrCreate()
    sparkSession
  }

  def setSparkSession(sparkSession: SparkSession): Unit = this.sparkSession = sparkSession

  def spark: SparkSession = if (sparkSession != null) sparkSession else createSparkSession

  def sqlContext: SQLContext = spark.sqlContext

  def addStrategy(strategy: Strategy): Unit =
    sqlContext.experimental.extraStrategies = sqlContext.experimental.extraStrategies :+ strategy

  def addOptimization(optimization: Rule[LogicalPlan]): Unit =
    sqlContext.experimental.extraOptimizations = sqlContext.experimental.extraOptimizations :+ optimization

  // delegate methods to sparkSession
  @InterfaceStability.Evolving
  @Experimental
  def emptyDataset[T](implicit evidence$1: Encoder[T]): Dataset[T] = spark.emptyDataset(evidence$1)

  @InterfaceStability.Evolving
  @Experimental
  def createDataFrame[A <: Product](rdd: RDD[A])(implicit evidence$2: universe.TypeTag[A]): DataFrame = spark.createDataFrame(rdd)(evidence$2)

  @InterfaceStability.Evolving
  @Experimental
  def createDataFrame[A <: Product](data: Seq[A])(implicit evidence$3: universe.TypeTag[A]): DataFrame = spark.createDataFrame(data)(evidence$3)

  @InterfaceStability.Evolving
  @DeveloperApi
  def createDataFrame(rowRDD: RDD[Row], schema: StructType): DataFrame = spark.createDataFrame(rowRDD, schema)

  @InterfaceStability.Evolving
  @DeveloperApi
  def createDataFrame(rowRDD: JavaRDD[Row], schema: StructType): DataFrame = spark.createDataFrame(rowRDD, schema)

  @InterfaceStability.Evolving
  @DeveloperApi
  def createDataFrame(rows: util.List[Row], schema: StructType): DataFrame = spark.createDataFrame(rows, schema)

  def createDataFrame(rdd: RDD[_], beanClass: Class[_]): DataFrame = spark.createDataFrame(rdd, beanClass)

  def createDataFrame(rdd: JavaRDD[_], beanClass: Class[_]): DataFrame = spark.createDataFrame(rdd, beanClass)

  def createDataFrame(data: util.List[_], beanClass: Class[_]): DataFrame = spark.createDataFrame(data, beanClass)

  def baseRelationToDataFrame(baseRelation: BaseRelation): DataFrame = spark.baseRelationToDataFrame(baseRelation)

  @InterfaceStability.Evolving
  @Experimental
  def createDataset[T](data: Seq[T])(implicit evidence$4: Encoder[T]): Dataset[T] = spark.createDataset(data)(evidence$4)

  @InterfaceStability.Evolving
  @Experimental
  def createDataset[T](data: RDD[T])(implicit evidence$5: Encoder[T]): Dataset[T] = spark.createDataset(data)(evidence$5)

  @InterfaceStability.Evolving
  @Experimental
  def createDataset[T](data: util.List[T])(implicit evidence$6: Encoder[T]): Dataset[T] = spark.createDataset(data)(evidence$6)

  @InterfaceStability.Evolving
  @Experimental
  def range(end: Long): Dataset[lang.Long] = spark.range(end)

  @InterfaceStability.Evolving
  @Experimental
  def range(start: Long, end: Long): Dataset[lang.Long] = spark.range(start, end)

  @InterfaceStability.Evolving
  @Experimental
  def range(start: Long, end: Long, step: Long): Dataset[lang.Long] = spark.range(start, end, step)

  @InterfaceStability.Evolving
  @Experimental
  def range(start: Long, end: Long, step: Long, numPartitions: Int): Dataset[lang.Long] = spark.range(start, end, step, numPartitions)

  def table(tableName: String): DataFrame = spark.table(tableName)

  def sql(sqlText: String): DataFrame = spark.sql(sqlText)

  def time[T](f: => T): T = spark.time(f)

  def stop(): Unit = spark.stop()

  def close(): Unit = spark.close()
}
