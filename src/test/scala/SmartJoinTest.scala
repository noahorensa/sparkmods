object SmartJoinTest {
  def main(args: Array[String]): Unit = {
    CompanySchema.init()

    import Spark._

    val queries = Array(
      "SELECT * " +
      "FROM employee JOIN department ON employee.department_id = department.id " +
      "WHERE employee.department_id = 1",

      "SELECT * " +
        "FROM (employee JOIN department ON employee.department_id = department.id) JOIN project ON employee.department_id = project.department_id",

      "SELECT * " +
        "FROM (employee JOIN department ON employee.department_id = department.id) JOIN project ON employee.department_id = project.department_id " +
        "WHERE employee.department_id = 1"
    )

    val df1 = queries.map(spark.sql(_))
    val data1 = df1.map(_.collect())

    Spark addStrategy SmartJoinStrategy
    Spark addOptimization SmartJoinOptimization

    val df2 = queries.map(spark.sql(_))
    val data2 = df2.map(_.collect())
  }
}
