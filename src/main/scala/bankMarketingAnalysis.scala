import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object bankMarketingAnalysis {

  Logger.getLogger("org").setLevel(Level.OFF)

  def doPrecise(value: Double): Double = {
    BigDecimal(value).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
  }

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder
      .appName("bankMarketingAnalysis")
      .master("local[2]")
      .getOrCreate

    val bankDF: DataFrame = spark.read.option("inferSchema", "true")
      .option("delimiter", "|")
      .option("header", "true")
      .csv("B:\\Big data\\Projects\\BDHS_Projects\\Project for submission\\Project 1\\Bank.csv")

    import spark.implicits._

    //check the marketing success rate
    val subscribed = bankDF.filter($"default"==="yes" || $"housing"==="yes" || $"loan"==="yes").count
    val marketingSuccessRate = subscribed.toFloat*100/bankDF.count

    println(doPrecise(marketingSuccessRate).toString+"%")

    //percentage of each job type taken housing loan

    val totalCountDF = bankDF.groupBy("job").count
      .withColumnRenamed("count","totalCount")
    val withHLDF = bankDF.filter($"housing"==="yes" ).groupBy("job").count
      .withColumnRenamed("count","withHousingLoan")

    val havingHousingLoan = col("withHousingLoan").cast("Float")*100/col("totalCount")

    val housingLoanPerJob = totalCountDF.join(withHLDF,"job")
      .withColumn("havingHousingLoan%",bround(havingHousingLoan,2))
      .drop("totalCount","withHousingLoan")
      .orderBy($"havingHousingLoan%".desc)
     housingLoanPerJob show

    //check balance per education for single guys
    val balanceWin = Window.partitionBy("education").orderBy($"balance".desc)

    val maxBalancePerQualification = bankDF.filter("marital = 'single' and age < 40")
      .withColumn("balanceRank",dense_rank.over(balanceWin))
      .filter($"balanceRank" < 6 && $"education".notEqual("unknown"))
      .drop("balanceRank")
    maxBalancePerQualification.select("age","education","balance") show

  }
}
