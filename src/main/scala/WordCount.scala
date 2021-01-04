import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object WordCount {

  def wordCountFunc(value: RDD[String]): RDD[(String, Int)] = {
      val WCRDD = value.flatMap(_.split("\\W+")).map(x => (x.toLowerCase,1)).reduceByKey(_+_)
         WCRDD
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("WordCount").master("local[2]").getOrCreate()

    val myRDD = spark.sparkContext.textFile("B:/Big data/spark-essentials-master/README.md")

    wordCountFunc(myRDD)
  }
}
