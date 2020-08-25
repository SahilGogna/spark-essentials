package mapfunctiondemo

object Demo2 extends App {

  /**
    * Using anonymous function
    *
    * we want to get cubes of all the numbers in the list
    */

  val collection = List(1, 2, 3, 4, 5, 6, 7)

  // multiply each number by 2
  // way number 2
  collection.map(a => a * 2)

  collection.map(_ * 2)

  val updatedCollection = collection.map(x => x * x * x)

  val updatedCollection2 = collection.map(Math.pow(_, 3))

  val updatedCollection3 = collection.map(_ * 2)

  println(updatedCollection)
  println(updatedCollection2)
  println(updatedCollection3)

}
