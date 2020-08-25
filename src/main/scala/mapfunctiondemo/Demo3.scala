package mapfunctiondemo

object Demo3 extends App {

  case class Student(id: Int, name: String, city: String)

  val rawData: List[String] = List("1,Name1,Toronto",
    "2,Name2,Toronto",
    "3,Name3,Montreal",
    "4,Name4,Halifax",
    "5,Name5,Montreal")

  val studentList = rawData.map(_.split(","))
    .map(student => Student(student(0).toInt, student(1), student(2)))

  studentList.foreach(println)

  println(studentList)

}
