import java.sql.Date

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

object CustomJoin {
  def main(args: Array[String]): Unit = {

    Spark addStrategy SmartJoin

    val empData = List(
      Row(101, "John",  "Smith",                new Date(65, 1, 5),   "CEO",                  100000.5,      1),
      Row(102, "John",  "Not Smith",            new Date(64, 11, 5),  "Manager of Stuff",     100.0,         1),
      Row(103, "John",  "Definitely Not Smith", new Date(30, 2, 9),   "Not CEO",              1.11,          1),
      Row(104, "Mann",  "The Man",              new Date(66, 9, 21),  "Some Guy",             999.5,         1),
      Row(105, "Janet", "Smith",                new Date(64, 10, 20), "CTO",                  999999.9,      1),
      Row(106, "Kirk",  "Taylor",               new Date(55, 11, 6),  "Chief Marketing",      10000.5,       2),
      Row(107, "Lana",  "Lola",                 new Date(42, 9, 9),   "COO",                  89999.0,       1),
      Row(108, "Sara",  "Look",                 new Date(59, 8, 7),   "Marketing Specialist", 1000.5,        2),
      Row(109, "Stan",  "The Man",              new Date(22, 11, 28), "Awesome man",          999999999.999, 3),
      Row(110, "Bob",   "Blart",                new Date(65, 7, 13),  "HR Manager",           10.5,          4)
    )
    val empSchema = StructType(Array[StructField](
      StructField("empid", DataTypes.IntegerType),
      StructField("fname", DataTypes.StringType),
      StructField("lname", DataTypes.StringType),
      StructField("birthday", DataTypes.DateType),
      StructField("position", DataTypes.StringType),
      StructField("salary", DataTypes.DoubleType),
      StructField("depid", DataTypes.IntegerType)
    ))
    val emp = new SmartRelation(empData, empSchema, "emp")

    val depData = List(
      Row(1, "Top Management"),
      Row(2, "Marketing"),
      Row(3, "Awesomeness"),
      Row(4, "HR")
    )
    val depSchema = StructType(Array[StructField](
      StructField("depid", DataTypes.IntegerType),
      StructField("name", DataTypes.StringType)
    ))
    val dep = new SmartRelation(depData, depSchema, "dep")

    val emp_depData = List(
      Row(101, "John",  "Smith",                new Date(65, 1, 5),   "CEO",                  100000.5,      1, 1, "Top Managgment"),
      Row(102, "John",  "Not Smith",            new Date(64, 11, 5),  "Manager of Stuff",     100.0,         1, 1, "Top Management"),
      Row(103, "John",  "Definitely Not Smith", new Date(30, 2, 9),   "Not CEO",              1.11,          1, 1, "Top Management"),
      Row(104, "Mann",  "The Man",              new Date(66, 9, 21),  "Some Guy",             999.5,         1, 1, "Top Management"),
      Row(105, "Janet", "Smith",                new Date(64, 10, 20), "CTO",                  999999.9,      1, 1, "Top Management"),
      Row(106, "Kirk",  "Taylor",               new Date(55, 11, 6),  "Chief Marketing",      10000.5,       2, 2, "Marketing"),
      Row(107, "Lana",  "Lola",                 new Date(42, 9, 9),   "COO",                  89999.0,       1, 1, "Top Management"),
      Row(108, "Sara",  "Look",                 new Date(59, 8, 7),   "Marketing Specialist", 1000.5,        2, 2, "Marketing"),
      Row(109, "Stan",  "The Man",              new Date(22, 12, 28), "Awesome man",          999999999.999, 3, 3, "Awesomeness"),
      Row(110, "Bob",   "Blart",                new Date(65, 7, 13),  "HR Manager",           10.5,          4, 4, "HR")
    )
    CachedJoin.registerJoin(emp_depData, emp, "depid", dep, "depid")

    Spark.sql("SELECT * FROM emp JOIN dep ON emp.depid = dep.depid").show()
  }
}
