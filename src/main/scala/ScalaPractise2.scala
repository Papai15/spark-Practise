object ScalaPractise2 extends App {

  def getPrime(n: Int) = {
    if (n < 4) n
    else {
      val c = (2 until n).count(n%_==0)
      if (c == 0) n else 0
    }
  }
  val numList = List(2,5,6,7,11,16,23,45,51)
  println(numList.map(getPrime).filter(_!=0))

  val patterns = List("apples",2,"grapes","mangoes",7,"pineapples",4,9)

  patterns.foreach {
    case _: String => println("String of a fruit")
    case n: Int => println(s"total $n no. of the said fruit")
  }
  val line = "Hello, Uday! nice meeting you Uday, you have a nice day!"
  val wordCount = line.split("\\W+").toList.groupBy(identity).mapValues(_.size).toSeq.sortWith(_._2 > _._2)
  wordCount.take(3).foreach(println)


  val nums = List(1,2,4,6,7,8,11,12)
  var boolList = List.newBuilder[Any]
  for (i <- nums.indices) {
    if (i == 0) boolList += null
    else if (nums(i) == nums(i-1) + 1) boolList += true
    else boolList += false
  }
  println(boolList.result)
  println(nums.take(5) ++ nums.drop(6))

  val aList = List(1,2,3,4,5)
  val ListPatterns = aList match {
   // case 1 :: _ => "the list starts with 1"
    case List(_,2,n,_*) => s"the 3rd element is $n"
  }
  println(ListPatterns)

}
