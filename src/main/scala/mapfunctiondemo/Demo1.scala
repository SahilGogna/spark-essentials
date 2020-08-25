package mapfunctiondemo

object Demo1 extends App{
  /**
    collection = (e1, e2, e3, ...)
    collection.map(func)
    returns collection(func(e1), func(e2), func(e3), ...)
    */

  //map
  //  map() takes some function as a parameter.
  //  map() applies the function to every element of the source collection.
  //  map() returns a new collection of the same type as the source collection.

  // name of a function
  // return type Unit, String, Int , ....
  // parameters -> the input that we take in the function

  def multiply(one: Int, two: Int):Int = {
     one * two
  }

//  println(multiply(2,3))

  def square(a:Int):Int = a * a

  val collection = List(1,2,3,4,5,6,7)
  println(collection)

  // write a function that multiply a number by 2

  def multiplyByTwo(number: Int) : Int = number * 2

  collection.map(multiplyByTwo)

  /**
    collection -> 1,2,3,4,5,6,7

    collection.map(multiplyByTwo)

    1
    multiplyByTwo(1) -> 2      [2,]

    2
    multiplyByTwo(2) -> 4      [2,4]
    .
    .
    .
    [2,4,6,8,10,12,14] -> print on the screen
    print([2,4,6,8,10,12,14])

    [2,4,6,8,10,12,14] -> store it in a variable
    val answer = [2,4,6,8,10,12,14]
    */

    println(collection.map(multiplyByTwo))
  val answer = collection.map(multiplyByTwo)

  val updatedCollection = collection.map(square)

//  println(updatedCollection)

}
