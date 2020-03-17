package part2DataFrames

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{StructField, _}

/**
  * @author sahilgogna on 2020-02-01
  * Reading and writing the data frames
  */
object DataSources extends App {
  // creating a SparkSession
  val spark = SparkSession.builder()
    .appName("DataSources and formats")
    .config("spark.master", "local")
    .getOrCreate()

  // schema
  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", StringType),
    StructField("Origin", StringType)
  ))

  /**
    Reading DF
    - format
    - schema or infer schema = true
    - zero or more options
    */
  val carsDF = spark.read
    .format("json")
    .schema(carsSchema) // enforcing a schema
    .option("mode","failFast") // dropMalFormed, permissive(default)
    .load("src/main/resources/data/cars.json")

  /**
    Writing DF
     - format
     - saveMode = overwrite, append, ignore, errorIfExists
     - path
     - zero or more options
    */

  carsDF.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars_dup.json")

  // json flags
  spark.read
    .schema(carsSchema)
    .option("dateFormat","YYYY-MM-DD") // couple with schema, if spark fails parsing , it will put null
    .option("allowSingleQuotes","true")
    .option("compression","uncompressed")
    .json("src/main/resources/data/cars.json")

  //CSV Flags
  val stockSchema = StructType(Array(
    StructField("symbol", StringType),
      StructField("date", DateType),
      StructField("price", DoubleType)
  ))

  spark.read
    .schema(stockSchema)
    .option("dateFormat","MMM dd YYYY")
    .option("header","true")
    .option("sep",",")
    .option("nullValue","")
    .csv("src/main/resources/data/stocks.csv")

  // Parquet - open source compressed binary data storage format optimised for fast  reading of columns
  carsDF.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/cars.parquet")

  // text files
  // reading data from remote database
  val employeesDF = spark.read
    .format("jdbc")
    .option("driver","org.postgresql.Driver")
    .option("url","jdbc:postgresql://localhost:5432/rtjvm")
    .option("user","docker")
    .option("password","docker")
    .option("dbtable", "public.employees")
    .load()

  employeesDF.show()

}
