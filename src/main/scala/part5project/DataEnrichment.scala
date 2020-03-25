package part5project

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
  * @author sahilgogna on 2020-03-24
  */
object DataEnrichment extends App {

  // creating a SparkSession
  val spark = SparkSession.builder()
    .appName("DataFrames Basics")
    .config("spark.master", "local")
    .getOrCreate()

  // schemas
  val calenderSchema = StructType(Array(
    StructField("serviceId", StringType),
    StructField("monday", IntegerType),
    StructField("tuesday", IntegerType),
    StructField("wednesday", IntegerType),
    StructField("thursday", IntegerType),
    StructField("friday", IntegerType),
    StructField("saturday", IntegerType),
    StructField("sunday", IntegerType),
    StructField("startDate", StringType),
    StructField("endDate", StringType)
  ))

  val tripSchema = StructType(Array(
    StructField("routeId", IntegerType),
    StructField("serviceId", StringType),
    StructField("tripId", StringType),
    StructField("tripHeadSign", StringType),
    StructField("directionId", IntegerType),
    StructField("shapeId", IntegerType),
    StructField("wheelchairAccessible", IntegerType),
    StructField("noteFr", StringType),
    StructField("noteEn", StringType)
  ))

  val routeSchema = StructType(Array(
    StructField("routeId", IntegerType),
    StructField("agencyId", StringType),
    StructField("routeShortName", IntegerType),
    StructField("routeLongName", StringType),
    StructField("routeType", IntegerType),
    StructField("routeUrl", StringType),
    StructField("routeColor", StringType),
    StructField("routeTextColor", StringType)
  ))

  // reading a Calender DF
  val calenderDF = spark.read
      .option("header","true")
      .schema(calenderSchema)
      .csv("/Users/sahilgogna/Documents/Big Data College/Course 2/Assignments/Scala Project/calendar.txt")

  val tripsDf = spark.read
    .option("header","true")
    .schema(tripSchema)
    .csv("/Users/sahilgogna/Documents/Big Data College/Course 2/Assignments/Scala Project/trips.txt")

  val routesDf = spark.read
    .option("header","true")
    .schema(routeSchema)
    .csv("/Users/sahilgogna/Documents/Big Data College/Course 2/Assignments/Scala Project/routes.txt")

  routesDf.show()
  println(routesDf.count())

}