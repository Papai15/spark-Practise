import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object certification {

  Logger.getLogger("org").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("databricks_certification").master("local[2]").getOrCreate()
    import spark.implicits._

    val peopleRDD = spark.sparkContext.parallelize(Seq(
      ("Ali", 0, Seq(100)),
      ("Barbara", 1, Seq(300, 250, 100)),
      ("Cesar", 1, Seq(350, 100)),
      ("Dongmei", 1, Seq(400, 100)),
      ("Eli", 2, Seq(250)),
      ("Florita", 2, Seq(500, 300, 100)),
      ("Gatimu", 3, Seq(300, 100))
    ))

    val peopleDF = spark.createDataFrame(peopleRDD).toDF("name", "department", "score")

    val winFunc = Window.partitionBy("department").orderBy($"scores".desc)

    peopleDF.select($"department",$"name",explode($"score").alias("scores"))
      .withColumn("score_rank",dense_rank().over(winFunc)).filter($"score_rank"===1)
      .drop("score_rank").orderBy("department").show

  }
}
