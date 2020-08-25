package mapfunctiondemo

object Demo4 extends App {

//  val words: List[String] = List("Apple", "Mango", "Potato", "Onion")
//
//  val flattenedWords: List[Char] = words.flatMap(_.toLowerCase)
//
//  println(flattenedWords)

  val fruits: List[String] = List("Apple", "Banana")
  val vegetables: List[String] = List("Onion", "Garlic")
  val dairyProducts: List[String] = List("Milk", "Cheese")

  val food: List[List[String]] = List(fruits, vegetables, dairyProducts)

  println(food)

//  List(Apple, Banana, Onion, Garlic, Milk, Cheese)

  val foodList1 = food.flatten
  val foodList2 = food.flatten.map(_.toUpperCase())
  val foodList3 = food.flatten.map(_.toUpperCase()).sorted // function chaining

  println(foodList1)
  println(foodList2)
  println(foodList3)

}
