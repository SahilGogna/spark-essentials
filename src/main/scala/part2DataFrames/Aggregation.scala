package part2DataFrames

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * @author sahilgogna on 2020-03-17
  */
object Aggregation extends App{
   val spark = SparkSession.builder()
     .appName("Aggregation and Grouping")
     .config("spark.master", "local")
     .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema","true")
    .json("src/main/resources/data/movies.json")

  // counting
  val genresCountDF = moviesDF.select(count(col("Major_Genre"))).show()

  // counting all
  moviesDF.select(count("*")) // count all the rows, and will INCLUDE nulls

  // counting distinct
  moviesDF.select(countDistinct(col("Major_Genre"))).show()

  // approximate count
  moviesDF.select(approx_count_distinct(col("Major_Genre")))

  // min and max
  val minRatingDF = moviesDF.select(min(col("IMDB_Rating")))
  moviesDF.selectExpr("min(IMDB_Rating)")

  // sum
  moviesDF.select(sum(col("US_Gross")))
  moviesDF.selectExpr("sum(US_Gross)")

  // avg
  moviesDF.select(avg(col("Rotten_Tomatoes_Rating")))
  moviesDF.selectExpr("avg(Rotten_Tomatoes_Rating)")

  // data science
  moviesDF.select(
    mean(col("Rotten_Tomatoes_Rating")),
    stddev(col("Rotten_Tomatoes_Rating"))
  )

  // Grouping

  val countByGenreDF = moviesDF
    .groupBy(col("Major_Genre")) // includes null
    .count()  // select count(*) from moviesDF group by Major_Genre

  val avgRatingByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .avg("IMDB_Rating")

  val aggregationsByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .agg(
      count("*").as("N_Movies"),
      avg("IMDB_Rating").as("Avg_Rating")
    )
    .orderBy(col("Avg_Rating"))

  /**
    * Exercises
    *
    * 1. Sum up ALL the profits of ALL the movies in the DF
    * 2. Count how many distinct directors we have
    * 3. Show the mean and standard deviation of US gross revenue for the movies
    * 4. Compute the average IMDB rating and the average US gross revenue PER DIRECTOR
    */

  moviesDF.select(expr("US_Gross + Worldwide_Gross").as("Total profit")).select(sum("Total profit")).show()
  moviesDF.select(countDistinct(col("Director"))).show()
  moviesDF.select(mean("US_Gross"),stddev("US_Gross")).show()
  moviesDF.groupBy("Director").agg(avg("IMDB_Rating"),avg("US_Gross")).show()

}
