package sparkcore


import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object SparkCoreRdd extends App {

  val sparkConf = new SparkConf()
    .setAppName("SparkCoreRdd")
    .setMaster("local[*]")

  // 1. SPARK CONTEXT
  val sc : SparkContext = new SparkContext(sparkConf)

  //2. Business Logic
  // create 100 random numbers

  val randomNumbers = sc.parallelize((1 to 1000).map(_ => Random.nextInt))

  println(s"Number of partitions are ${randomNumbers.getNumPartitions}")
  randomNumbers.foreachPartition( x => println("===>" + x.toList.size))

//  val rr2 = randomNumbers.repartition(2)
  val rr2 = randomNumbers.coalesce(2)

  println(s"Number of partitions are ${rr2.getNumPartitions}")
  rr2.foreachPartition( x => println("===>" + x.toList.size))

  // apply transformation + action
  val x1 = randomNumbers.map(_ * 2).take(10)

  val x2 = randomNumbers.filter( _ < 100).take(10)

  x1 foreach println

  x2 foreach println

  while(true){
    // do nothing, just to keep and check spark UI
  }

  // 3. stop the context
  sc.stop()

}
