package part2DataFrames

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * @author sahilgogna on 2020-03-17
  */
object Joins extends App {

  val spark = SparkSession.builder()
    .config("spark.master","local")
    .appName("join")
    .getOrCreate()

  val bandsDf = spark.read
    .option("inferSchema","true")
    .json("src/main/resources/data/bands.json")

  val guitarPlayersDF = spark.read
    .option("inferSchema","true")
    .json("src/main/resources/data/guitarPlayers.json")

  val guitarsDF = spark.read
    .option("inferSchema","true")
    .json("src/main/resources/data/guitars.json")

  // inner joins
  val joinCondition = guitarPlayersDF.col("band") === bandsDf.col("id")
  val guitaristsBandsDF = guitarPlayersDF.join(bandsDf, joinCondition, "inner")

  // outer joins
  // left outer = everything in the inner join + all the rows in the LEFT table, with nulls in where the data is missing
  guitarPlayersDF.join(bandsDf, joinCondition, "left_outer")

  // right outer = everything in the inner join + all the rows in the RIGHT table, with nulls in where the data is missing
  guitarPlayersDF.join(bandsDf, joinCondition, "right_outer")

  // outer join = everything in the inner join + all the rows in BOTH tables, with nulls in where the data is missing
  guitarPlayersDF.join(bandsDf, joinCondition, "outer")

  // semi-joins = everything in the left DF for which there is a row in the right DF satisfying the condition
  guitarPlayersDF.join(bandsDf, joinCondition, "left_semi")

  // anti-joins = everything in the left DF for which there is NO row in the right DF satisfying the condition
  guitarPlayersDF.join(bandsDf, joinCondition, "left_anti")


  // things to bear in mind
  // guitaristsBandsDF.select("id", "band").show // this crashes

  // option 1 - rename the column on which we are joining
  guitarPlayersDF.join(bandsDf.withColumnRenamed("id", "band"), "band")

  // option 2 - drop the dupe column
  guitaristsBandsDF.drop(bandsDf.col("id"))

  // option 3 - rename the offending column and keep the data
  val bandsModDF = bandsDf.withColumnRenamed("id", "bandId")
  guitarPlayersDF.join(bandsModDF, guitarPlayersDF.col("band") === bandsModDF.col("bandId"))

  // using complex types
  guitarPlayersDF.join(guitarsDF.withColumnRenamed("id", "guitarId"), expr("array_contains(guitars, guitarId)"))

  /**
    * Exercises
    *
    * 1. show all employees and their max salary
    * 2. show all employees who were never managers
    * 3. find the job titles of the best paid 10 employees in the company
    */

  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  def readTable(tableName: String) = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", s"public.$tableName")
    .load()

  val employeesDF = readTable("employees")
  val salariesDF = readTable("salaries")
  val deptManagersDF = readTable("dept_manager")
  val titlesDF = readTable("titles")

  //1
  val salariesGBEmpDf = salariesDF.groupBy(col("emp_no")).max("salary")
  val empMaxSalDf = employeesDF.join(salariesGBEmpDf, "emp_no" )

  //2
  val noManagerDf = employeesDF.join(deptManagersDF, employeesDF.col("emp_no") === deptManagersDF.col("emp_no"), "left_anti")
  noManagerDf.show()

  //3


}
