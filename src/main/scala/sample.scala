import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.log4j._

object sample {

  Logger.getLogger("org").setLevel(Level.OFF)

  case class data(col1: String, col2: String, col3: String)

  def main(args: Array[String]): Unit = {
      val spark = SparkSession.builder().appName("sample").master("local[2]").getOrCreate()

      import spark.implicits._
      val myRDD = spark.sparkContext.textFile("C:/Users/uday/OneDrive/Desktop/data.txt")
      val myDF = myRDD.map{ x =>
            val e = x.split(" ")
            data(e(0),e(1),e(2))
      }.toDF

      val DF1 = myDF.groupBy("col1").count.alias("col_count").withColumn("col_Type",lit("random"))
      val DF2 = myDF.groupBy("col2").count.alias("col_count").withColumn("col_Type",lit("type"))
      val DF3 = myDF.groupBy("col3").count.alias("col_count").withColumn("col_Type",lit("status"))

      val mergedDF = DF1.union(DF2).union(DF3)
      mergedDF.show

  }

}
