import java.sql.Date

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

object SmartJoinTest {
  def main(args: Array[String]): Unit = {

    Spark addStrategy SmartJoinStrategy
    Spark addOptimization SmartJoinOptimization

    CompanySchema.init()

    import Spark._

    val query1 = spark.sql("SELECT * " +
      "FROM employee JOIN department ON employee.department_id = department.id " +
      "WHERE employee.department_id = 1")
    query1.explain(true)
    query1.show()

    val query2 = spark.sql("SELECT * " +
      "FROM (employee JOIN department ON employee.department_id = department.id) JOIN project ON employee.department_id = project.department_id")
    query2.explain(true)
    query2.show()
  }
}
