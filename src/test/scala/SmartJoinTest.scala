import org.apache.spark.sql.Row

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
        "WHERE employee.department_id = 1",

      "SELECT * " +
        "FROM employee JOIN project ON employee.department_id = project.department_id",

      "SELECT * " +
        "FROM project JOIN department ON project.department_id = department.id",

      "SELECT * " +
        "FROM department JOIN project ON department.id = project.department_id"
    )

    val df1 = queries.map(spark.sql(_))
    val data1 = df1.map(_.collect())

    df1(5).explain(true)

    Spark addStrategy SmartJoinStrategy
    Spark addOptimization SmartJoinOptimization

    val df2 = queries.map(spark.sql(_))
    val data2 = df2.map(_.collect())

    df2(5).explain(true)

    def checkSame(data1: Any, data2: Any): Unit = {
      (data1, data2) match {
        case (d1: Array[Row], d2: Array[Row]) =>
          assert(d1.length == d2.length)
          for (i <- d1.indices)
            checkSame(d1(i).toSeq.toArray, d2(i).toSeq.toArray)

        case (d1: Array[Any], d2: Array[Any]) =>
          assert(d1.length == d2.length)
          for (i <- d1.indices)
            checkSame(d1(i), d2(i))

        case (d1: Any, d2: Any) =>
          print(d1)
          print(", ")
          println(d2)
          assert(d1 equals d2)
      }
    }

    checkSame(data1, data2)
  }
}
