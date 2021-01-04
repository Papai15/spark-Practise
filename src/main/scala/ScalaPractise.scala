import scala.annotation.tailrec

object ScalaPractise extends App {

  @tailrec
  def nTimesSub(f: Int => Int, n: Int, x: Int): Int = {
    if(n < 1) x
    else nTimesSub(f, n-1, f(x))
  }

  def add = (x: Int) => 1 + x
  println(nTimesSub(add,10,1))

  def getHighest(n: Int) = {
      val digits = n.toString.map(_.asDigit)
      digits.sortWith(_>_).map(_.toString).mkString("").toInt
  }
  println(getHighest(453681))

  val l = List(1,2,3,41,3,4,7,8,10,10,4,1)
  val c = l.groupBy(identity).mapValues(_.size)

  println(c)
  println(c.maxBy(_._2))
  println(l.filter(_!= l.max).max)

  case class person(name: String, age: Int, salary: Int)

  val employees = List(person("Uday",28,50000),
                       person("Modi",67,100000),
                       person("Rahul",48,20000),
                       person("Mamata",55,30000))

  val mostInfluentials = employees.filter(_.age > 40).sortWith(_.salary > _.salary).map(_.name).take(2)
  mostInfluentials.foreach(println)

  val strings = List("a","b","c","d")
  val numbers = List(1,2,3)
  
  val zipped = strings.flatMap(x => numbers.map(y => x + y.toString))
  println(zipped)

  def factorial(n: Int) = {
    if (n <= 1) 1
    else (1 to n).product
  }

  println(factorial(5))
}
