package queries

import org.apache.spark.sql
import org.apache.spark.sql.{SparkSession, DataFrame}
import tools.{Cleaner, Loader}

object queryConfirmedCases {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Queries")
      .config("spark.master", "local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    val global_confirmed_path = "raw_data/time_series_covid_19_confirmed.csv"
    val uid_lookup_path = "raw_data/uid_lookup_table.csv"
    val uid_lookup = Cleaner.cleanUIDLookup(spark, Loader.loadCSV(spark, uid_lookup_path))
    val global_confirmed = Cleaner.cleanGlobalTimeSeries(spark, Loader.loadCSV(spark, global_confirmed_path))
    //queryPerCapita(spark, global_confirmed, uid_lookup)
    queryTropical(spark, global_confirmed, uid_lookup)
  }

  def queryPerCapita(spark: SparkSession, global_confirmed: DataFrame, uid_lookup:DataFrame) =  {
    global_confirmed.createOrReplaceTempView("global_confirmed")
    uid_lookup.createOrReplaceTempView("uid_lookup")
    global_confirmed.show()

    spark.sql("SELECT g.region, g.Country, (element_at(g.counts,(466))/u.Population) transmitionRate FROM global_confirmed g join uid_lookup u " +
      "on g.region = u.region and g.Country = u.Country order by transmitionRate desc").show()

    spark.sql("SELECT g.Country, (element_at(g.counts,466)/u.Population) transmitionRate FROM global_confirmed g join uid_lookup u " +
      "on g.Country = u.Country where u.region is null order by transmitionRate desc").show()

  }

  def queryTropical(spark: SparkSession, global_confirmed: DataFrame, uid_lookup:DataFrame) = {
    global_confirmed.createOrReplaceTempView("global_confirmed")
    uid_lookup.createOrReplaceTempView("uid_lookup")

    spark.sql("SELECT (sum(element_at(g.counts,466))), (sum(u.Population)),(sum(element_at(g.counts,466))/sum(u.Population)) transmitionRate " +
      "FROM global_confirmed g join uid_lookup u on g.latitude = u.latitude where u.latitude<23.5 and u.latitude>-23.5 order by transmitionRate desc").show()
    spark.sql("SELECT (sum(element_at(g.counts,466))), (sum(u.Population)),(sum(element_at(g.counts,466))/sum(u.Population)) transmitionRate " +
      "FROM global_confirmed g join uid_lookup u on g.latitude = u.latitude where u.latitude>23.5 or u.latitude<-23.5 order by transmitionRate desc").show()
  }

}
