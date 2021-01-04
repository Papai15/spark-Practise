import org.apache.spark.sql._
import org.scalatest.{BeforeAndAfterEach, FunSuite}

import scala.collection.mutable

  class WordCountUnitTest extends FunSuite with BeforeAndAfterEach {

    var spark : SparkSession = _

    override def beforeEach(): Unit = {
       spark = SparkSession.builder().master("local[2]").appName("WCTest").getOrCreate()
    }

    test("testing the wordcount program") {
      val myrdd = spark.sparkContext.parallelize(List("This cat is hat","that mat is not cat","this hat"))
      val actualRDD = WordCount.wordCountFunc(myrdd) collect
      val myMap = new mutable.HashMap[String,Int]()

      actualRDD.foreach(x=>myMap.put(x._1,x._2))
      assert(myMap.get("cat")==Option(2),"it should be 2")
    }

    override def afterEach(): Unit = {
      spark.stop()
    }

  }

