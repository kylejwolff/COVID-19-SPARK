package queries

import tools._
import clean._

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.log4j.Logger
import org.apache.log4j.Level


object TransmissionRates {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("Queries")
      .config("spark.master", "local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    transCountrySeason(spark)
    spark.stop()
  }

  def transCountrySeason(spark: SparkSession): Unit = {
    val df1 = cleanLocationNames.begin(spark)
    val df2 = LastUpdateCleaner.cleanDF(spark,df1)
    df2.createOrReplaceTempView("global_confirmed")
    // val df3 = spark.sql("SELECT g.country country, SUM(g.confirmed) confirmed " +
    //   "FROM (SELECT country, state, MAX(confirmed) confirmed FROM global_confirmed WHERE confirmed is not null GROUP BY state, country) g " +
    //   "GROUP BY g.country " +
    //   "ORDER BY g.country")
    spark.sql("SELECT country, state, MAX(confirmed) confirmed FROM global_confirmed WHERE confirmed is not null GROUP BY state, country").show()
    //Writer.writeCSV(df3, "out/trans_by_country_by_season.csv", true, true)
  }
}
