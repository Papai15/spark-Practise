import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object IPLAnalysis {

  Logger.getLogger("org").setLevel(Level.OFF)

  def BestSlogger(DF: DataFrame, year: Int): DataFrame = {

    val SloggerBatsmen = DF.filter(col("season") === year && col("over") > 14)
      .groupBy("batsman").agg(sum("batsman_runs").alias("runs"))
      .orderBy(col("runs").desc)

     SloggerBatsmen
  }

  def MostEconomicalBowlers(df: DataFrame, season: Int): DataFrame = {
      val result = df.filter(col("season") === season).groupBy("city","bowler","over")
        .agg(sum("total_runs").alias("runs_conceded"))

      val returning = result.withColumn("overs_bowled",count("over")
          .over(Window.partitionBy("city","bowler")))
        .withColumn("totalRunsConceded",sum("runs_conceded")
          .over(Window.partitionBy("city","bowler")))
        .withColumn("EconomyRate",bround(col("totalRunsConceded").cast("float")/col("overs_bowled"),2))

       returning.drop("runs_conceded","over")
  }

  def main(args: Array[String]): Unit = {

      val spark = SparkSession.builder().appName("IPLAnalysis").master("local[2]").getOrCreate()
      import spark.implicits._

      val everyDelivery = spark.read.option("header","true").option("inferSchema","true")
        .csv("B:/iplData/deliveries.csv")

      val matches = spark.read.option("header","true").option("inferSchema","true")
        .csv("B:/iplData/matches.csv")

      val joinedDF = matches.join(everyDelivery,$"id" === $"match_id").cache()
      //joinedDF.show(false)

      val winFunc = Window.partitionBy("match_id").orderBy("match_id")
      joinedDF.filter("season > 2014 and toss_decision = 'field' and toss_winner = winner")
        .withColumn("rowNum",row_number.over(winFunc))
        .filter($"rowNum" === 1)

      BestSlogger(joinedDF,2016)


      val winFunc2 = Window.partitionBy("match_id").orderBy("city")
      joinedDF.filter("""
        | (toss_decision = 'field' and toss_winner = winner) or (toss_decision = 'bat' and toss_winner != winner)
        |""".stripMargin)
        .withColumn("RowNum",row_number().over(winFunc2))
        .filter($"RowNum" === 1).drop("RowNum").groupBy("city")
        .agg(count("match_id").alias("winCount"))
        .orderBy($"winCount".desc)


      val winFunc3 = Window.partitionBy("city","bowler").orderBy("EconomyRate")
      MostEconomicalBowlers(joinedDF,2015).withColumn("overs_rn",row_number().over(winFunc3))
        .filter("overs_rn = 1 and overs_bowled > 3").drop("overs_rn")
        .orderBy("EconomyRate").show(false)

  }

}
