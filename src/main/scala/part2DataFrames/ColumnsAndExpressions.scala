package part2DataFrames

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}

/**
  * @author sahilgogna on 2020-03-15
  */
object ColumnsAndExpressions extends App{

  val spark = SparkSession.builder()
    .appName("DF Columns and Expressions")
    .config("spark.master","local")
    .getOrCreate()

  var carsDF = spark.read
    .option("inferSchema","true")
    .json("src/main/resources/data/cars.json")

  // defining a column
  var firstColumn = carsDF.col("Name")

  // select - (projection)
  var carNames = carsDF.select(firstColumn)

  carNames.show()

  // various select methods
  import spark.implicits._
  carsDF.select(
    carsDF.col("Name"),
    col("Acceleration"),
    column("Weight_in_lbs"),
    'Year, // Scala Symbol, auto-converted to column
    $"Horsepower", // fancier interpolated string, returns a Column object
    expr("Origin") // EXPRESSION
  )

  // select with plain column names
  carsDF.select("Name", "Year")

  /**
    * Exercises
    *
    * 1. Read the movies DF and select 2 columns of your choice
    * 2. Create another column summing up the total profit of the movies = US_Gross + Worldwide_Gross + DVD sales
    * 3. Select all COMEDY movies with IMDB rating above 6
    *
    * Use as many versions as possible
    */

  val moviesDF = spark.read.option("inferSchema","true").json("src/main/resources/data/movies.json")
  val moviesDF1 = moviesDF.select("Title","Release_Date")

  val newMoviesDF = moviesDF.select(
    col("Title"),
    expr("US_Gross + Worldwide_Gross").as("Total profit")
  )
  newMoviesDF.show()

  val comedyMovieDF = moviesDF.select("Title","IMDB_Rating").filter( col("Major_Genre") === "Comedy" and col("IMDB_Rating") > 8.5)
  comedyMovieDF.show()

}
