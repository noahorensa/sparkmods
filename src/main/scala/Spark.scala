import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

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
}
