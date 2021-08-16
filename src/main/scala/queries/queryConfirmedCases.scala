package queries

import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, SparkSession}
import tools._

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
    //val uid_lookup = Cleaner.cleanUIDLookup(spark, Loader.loadCSV(spark, uid_lookup_path))
    //val global_confirmed = Cleaner.cleanGlobalTimeSeries(spark, Loader.loadCSV(spark, global_confirmed_path))
    //queryPerCapita(spark, global_confirmed, uid_lookup)
    //queryTropical(spark, global_confirmed, uid_lookup)
    val global_deaths_path = "raw_data/time_series_covid_19_deaths.csv"
    val global_recovered_path = "raw_data/time_series_covid_19_recovered.csv"
    val global_deaths = Cleaner.cleanGlobalTimeSeries(spark, Loader.loadCSV(spark, global_deaths_path))
    val global_recovered = Cleaner.cleanGlobalTimeSeries(spark, Loader.loadCSV(spark, global_recovered_path))
    queryDeathRates(spark, global_deaths, global_recovered)

  }

  def queryPerCapita(spark: SparkSession, global_confirmed: DataFrame, uid_lookup:DataFrame) =  {
    global_confirmed.createOrReplaceTempView("global_confirmed")
    uid_lookup.createOrReplaceTempView("uid_lookup")
    //global_confirmed.show()

    val regionRates = spark.sql("SELECT g.region, g.Country, (element_at(g.counts,(466))/u.Population) transmitionRate FROM global_confirmed g join uid_lookup u " +
      "on g.region = u.region and g.Country = u.Country order by transmitionRate desc")

    val countryRates = spark.sql("SELECT g.Country, (element_at(g.counts,466)/u.Population) transmitionRate FROM global_confirmed g join uid_lookup u " +
      "on g.Country = u.Country where u.region is null order by transmitionRate desc")
    //regionRates.show()
    Writer.writeCSV(regionRates, "out/trans_by_region.csv", true, true)
    Writer.writeCSV(countryRates, "out/trans_by_country.csv", true, true)

  }

  def queryTropical(spark: SparkSession, global_confirmed: DataFrame, uid_lookup:DataFrame) = {
    global_confirmed.createOrReplaceTempView("global_confirmed")
    uid_lookup.createOrReplaceTempView("uid_lookup")

    val tropical = spark.sql("SELECT (sum(element_at(g.counts,466))), (sum(u.Population)),(sum(element_at(g.counts,466))/sum(u.Population)) transmitionRate " +
      "FROM global_confirmed g join uid_lookup u on g.latitude = u.latitude where u.latitude<23.5 and u.latitude>-23.5 order by transmitionRate desc")
    val nonTropical = spark.sql("SELECT (sum(element_at(g.counts,466))), (sum(u.Population)),(sum(element_at(g.counts,466))/sum(u.Population)) transmitionRate " +
      "FROM global_confirmed g join uid_lookup u on g.latitude = u.latitude where u.latitude>23.5 or u.latitude<-23.5 order by transmitionRate desc")
    Writer.writeCSV(tropical, "out/trans_tropical.csv", true, true)
    Writer.writeCSV(nonTropical, "out/trans_nonTropical.csv", true, true)

  }

  def queryDeathRates(spark: SparkSession, global_deaths: DataFrame, global_recovered: DataFrame): Unit ={
    global_deaths.createOrReplaceTempView("global_deaths")
    global_recovered.createOrReplaceTempView("global_recovered")
    val deathRates = spark.sql("SELECT d.country, (d.counts) as Deaths, r.counts as Recoveries from global_deaths d " +
    "join global_recovered r on d.country = r.country where d.country in ('US','India', 'Germany', 'Japan', 'Brazil', 'Nigeria')").show()
  }

}
