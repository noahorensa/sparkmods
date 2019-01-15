import java.sql.Date

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

object CompanySchema {

  def init(): Unit = {
    val employee = SmartRelation(List(
      Row(101, "John",  "Smith",                new Date(65, 1, 5),   "CEO",                  100000.5,      1),
      Row(102, "John",  "Not Smith",            new Date(64, 11, 5),  "Manager of Stuff",     100.0,         1),
      Row(103, "John",  "Definitely Not Smith", new Date(30, 2, 9),   "Not CEO",              1.11,          1),
      Row(104, "Mann",  "The Man",              new Date(66, 9, 21),  "Some Guy",             999.5,         1),
      Row(105, "Janet", "Smith",                new Date(64, 10, 20), "CTO",                  999999.9,      1),
      Row(106, "Kirk",  "Taylor",               new Date(55, 11, 6),  "Chief Marketing",      10000.5,       2),
      Row(107, "Lana",  "Lola",                 new Date(42, 9, 9),   "COO",                  89999.0,       1),
      Row(108, "Sara",  "Look",                 new Date(59, 8, 7),   "Marketing Specialist", 1000.5,        2),
      Row(109, "Stan",  "The Man",              new Date(22, 11, 28), "Awesome man",          999999999.999, 3),
      Row(110, "Bob",   "Blart",                new Date(65, 7, 13),  "HR Manager",           10.5,          4),
      Row(111, "Dan",   "Mann",                 new Date(65, 8, 13),  "Something Engineer",   9.22,          5),
      Row(112, "Sam",   "Summers",              new Date(52, 2, 12),  "Something Engineer",   8.45,          5),
      Row(113, "Noah",  "Taylor",               new Date(52, 7, 17),  "Electrical Engineer",  8.24,          5),
      Row(114, "Paul",  "Smartpants",           new Date(45, 1, 6),   "Head of Operations",   10.5,          6),
      Row(115, "Ahmed", "Ops",                  new Date(66, 9, 18),  "Dev Ops",              10.5,          6),
      Row(116, "John",  "Ops",                  new Date(66, 5, 14),  "Dev Ops",              10.5,          6)
    ),
      StructType(Array(
        StructField("id", DataTypes.IntegerType),
        StructField("fname", DataTypes.StringType),
        StructField("lname", DataTypes.StringType),
        StructField("birthday", DataTypes.DateType),
        StructField("position", DataTypes.StringType),
        StructField("salary", DataTypes.DoubleType),
        StructField("department_id", DataTypes.IntegerType)
      )), "employee")

    val department = SmartRelation(List(
      Row(1, "Top Management"),
      Row(2, "Marketing"),
      Row(3, "Awesomeness"),
      Row(4, "HR"),
      Row(5, "Engineering"),
      Row(6, "Operations")
    ),
      StructType(Array(
        StructField("id", DataTypes.IntegerType),
        StructField("name", DataTypes.StringType)
      )), "department")

    val project = SmartRelation(List(
      Row(1, "Project X", 5),
      Row(2, "Project Y", 5),
      Row(3, "Project Z", 6),
      Row(4, "Project A", 2),
      Row(5, "Management Stuff", 1)
    ),
      StructType(Array(
        StructField("id", DataTypes.IntegerType),
        StructField("name", DataTypes.StringType),
        StructField("department_id", DataTypes.IntegerType)
      )), "project")

    CachedJoin.registerJoin(List(
      Row(101, "John",  "Smith",                new Date(65, 1, 5),   "CEO",                  100000.5,      1, 1, "Top Management"),
      Row(102, "John",  "Not Smith",            new Date(64, 11, 5),  "Manager of Stuff",     100.0,         1, 1, "Top Management"),
      Row(103, "John",  "Definitely Not Smith", new Date(30, 2, 9),   "Not CEO",              1.11,          1, 1, "Top Management"),
      Row(104, "Mann",  "The Man",              new Date(66, 9, 21),  "Some Guy",             999.5,         1, 1, "Top Management"),
      Row(105, "Janet", "Smith",                new Date(64, 10, 20), "CTO",                  999999.9,      1, 1, "Top Management"),
      Row(106, "Kirk",  "Taylor",               new Date(55, 11, 6),  "Chief Marketing",      10000.5,       2, 2, "Marketing"),
      Row(107, "Lana",  "Lola",                 new Date(42, 9, 9),   "COO",                  89999.0,       1, 1, "Top Management"),
      Row(108, "Sara",  "Look",                 new Date(59, 8, 7),   "Marketing Specialist", 1000.5,        2, 2, "Marketing"),
      Row(109, "Stan",  "The Man",              new Date(22, 12, 28), "Awesome man",          999999999.999, 3, 3, "Awesomeness"),
      Row(110, "Bob",   "Blart",                new Date(65, 7, 13),  "HR Manager",           10.5,          4, 4, "HR"),
      Row(111, "Dan",   "Mann",                 new Date(65, 8, 13),  "Something Engineer",   9.22,          5, 5, "Engineering"),
      Row(112, "Sam",   "Summers",              new Date(52, 2, 12),  "Something Engineer",   8.45,          5, 5, "Engineering"),
      Row(113, "Noah",  "Taylor",               new Date(52, 7, 17),  "Electrical Engineer",  8.24,          5, 5, "Engineering"),
      Row(114, "Paul",  "Smartpants",           new Date(45, 1, 6),   "Head of Operations",   10.5,          6, 6, "Operations"),
      Row(115, "Ahmed", "Ops",                  new Date(66, 9, 18),  "Dev Ops",              10.5,          6, 6, "Operations"),
      Row(116, "John",  "Ops",                  new Date(66, 5, 14),  "Dev Ops",              10.5,          6, 6, "Operations")
    ), employee, "department_id", department, "id")

    CachedJoin.registerJoin(List(
      Row(1, "Project X", 5, 5, "Engineering"),
      Row(2, "Project Y", 5, 5, "Engineering"),
      Row(3, "Project Z", 6, 6, "Operations")
    ), project, "department_id", department, "id")
  }
}
