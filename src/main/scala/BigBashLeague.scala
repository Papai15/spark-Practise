package playground

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


object BigBashLeague {
  Logger.getLogger("org").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("BigBashLeague").getOrCreate()

    val ballByballDF = spark.read.option("inferschema","true").option("header","true")
        .csv("B:/Big data/SparkScala/SparkScala/ballByball.csv")

    val cleanedBallByballDF = ballByballDF.drop("extras_byes","extras_legbyes","extras_noballs","extras_wides","wicket_fielders","wicket_kind")
      .drop("MatchID","value replacements_match","value runs_non_boundary","replacements_role", "L9 replacements_role","value replacements_role")
    // cleanedBallByballDF.show

    val matchDF = spark.read.option("inferschema","true").option("header","true")
        .csv("B:/Big data/SparkScala/SparkScala/Match.csv")

    // matchDF.show

    val matchBallDF = cleanedBallByballDF.join(broadcast(matchDF),cleanedBallByballDF("Match_ID") === matchDF("MatchID"))
        .drop("Match_date","Match_ID","MatchDateSK","Innings","InningsDesc","Over","Winning_Team","non_striker","runs_batsman","runs_extras")

    import spark.implicits._

    val strikersOfTheMatch = matchBallDF.filter($"Team 1"===$"Winner")
      .groupBy("Season_Year","Team 1","Striker")
      .agg(sum("runs_total").as("score")).orderBy(desc("score"))
   // strikersOfTheMatch.show

    val updatedMatchBallDF = matchBallDF.withColumn("Loser",when($"Team 1".notEqual($"Winner"),$"Team 1")
      .otherwise($"Team 2"))
      .drop("Team 1","Team 2").cache()

   // updatedMatchBallDF.coalesce(1).write.option("compression","snappy").orc("B:/ORCOutput")


    val bestPlayerOfTheSeason = updatedMatchBallDF.groupBy("Season_Year", "MOM").count()

    val windowFunc = Window.partitionBy("Season_Year").orderBy(desc("count"))

    val MostMOM = bestPlayerOfTheSeason.withColumn("Rank",dense_rank.over(windowFunc))
    MostMOM.filter($"Rank" === 1).show

    val finalist = updatedMatchBallDF.filter($"IsFinal" === 1)
      .select("Winner","Season_Year","IsFinal")
      .withColumn("RowNumber",row_number.over(Window.partitionBy("Season_Year").orderBy("IsFinal")))
      .filter($"RowNumber" === 1).drop("IsFinal","RowNumber")
   // finalist.show

    val bowlersTurn = updatedMatchBallDF.filter($"wicket_player_out" =!= "NA")
      .groupBy("Season_Year","Bowler").count.withColumnRenamed("count","Wickets")

    val bestBowlers = bowlersTurn.withColumn("BowlingRank",dense_rank.over(Window.partitionBy("Season_Year")
    .orderBy(desc("Wickets")))).filter($"BowlingRank" === 1).drop("BowlingRank")
    bestBowlers.show
  }

}
