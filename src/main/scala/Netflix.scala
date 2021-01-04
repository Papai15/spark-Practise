
import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Netflix {

  Logger.getLogger("org").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("Netflix").master("local[2]").getOrCreate()

    import spark.implicits._

    val netflixDF = spark.read.option("header","true").option("inferSchema","true")
      .csv("B:\\Big data\\Tableau\\netflix_titles.csv").drop("description")

    val cleanDF = netflixDF.withColumn("Actors",explode(split($"cast",",")))
      .withColumn("made_by",explode(split($"director",",")))
      .withColumn("where",explode(split($"country",",")))
      .withColumn("genre",explode(split($"listed_in",",")))
      .withColumn("month",split($"date_added"," ").getItem(0))
      .drop("cast","director","country","listed_in","date_added")

    val updated = cleanDF.withColumn("check_year",$"release_year".cast("Int").isNotNull)
      .filter($"check_year"==="true").drop("check_year")

    val winFunc = Window.partitionBy("show_id","genre").orderBy("genre")

    updated.filter($"where"==="India" && $"type"==="Movie" && $"release_year" > 2016)
      .withColumn("row_num",row_number.over(winFunc)).filter($"row_num"===1)
      .select($"release_year",trim($"genre").alias("genre"))
      .groupBy("release_year","genre").agg(count("genre").alias("genre_count"))
      .orderBy($"genre_count".desc).drop("row_num").show(false)

  }

}
