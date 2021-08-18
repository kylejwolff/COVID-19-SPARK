package queries

import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql.functions.{arrays_zip, col, explode}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{lag, udf}
import org.apache.spark.sql.expressions.Window
import java.time.LocalDate
import java.time.format.DateTimeFormatter

object Queries {

  private def formatDate(start_date: LocalDate, plus_day: Int, format: String="MM/dd/yyyy"): String = {
    return start_date.plusDays(plus_day).format(DateTimeFormatter.ofPattern(format))
  }

  def mergeGlobal(confirmed_df: DataFrame, deaths_df: DataFrame, recovered_df: DataFrame): DataFrame = {
      val confirmed_deaths_df = confirmed_df.
          join(deaths_df, (confirmed_df("region") <=> deaths_df("region")) && (confirmed_df("country") <=> deaths_df("country")), "outer").
          select(confirmed_df("*"), deaths_df("counts").as("deaths")).
          withColumnRenamed("counts", "confirmed")

      val merged_global_df = confirmed_deaths_df.
          join(recovered_df, (confirmed_deaths_df("region") <=> recovered_df("region")) && (confirmed_deaths_df("country") <=> recovered_df("country")), "outer").
          select(confirmed_deaths_df("*"), recovered_df("counts").as("recovered"))

      return merged_global_df
  }

  def mergeUS(confirmed_df: DataFrame, deaths_df: DataFrame): DataFrame = {
      val merged_us_df = confirmed_df.
          join(deaths_df, confirmed_df("uid") <=> deaths_df("uid"), "outer").
          select(confirmed_df("*"), deaths_df("counts").as("deaths"), deaths_df("population")).
          withColumnRenamed("counts", "confirmed")

      return merged_us_df
  }

  def getGlobalCountUnpacked(global_merged_df: DataFrame, which: String, start_date: LocalDate, format: String="MMM dd ''yy"): DataFrame = {

      val num_days = global_merged_df.first.getAs[scala.collection.mutable.WrappedArray[String]](which).length

      return global_merged_df.
          select((Seq(global_merged_df("country"), global_merged_df("latitude"), global_merged_df("longitude")) ++ Range(0, num_days).map(i => global_merged_df(which)(i).as(formatDate(start_date, i, format)))):_*).
          where(global_merged_df("region").isNull)
  }

  def mergeGlobalVertical(confirmed_vertical_df: DataFrame, deaths_vertical_df: DataFrame, recovered_vertical_df: DataFrame): DataFrame = {
      val confirmed_deaths_vertical_df = confirmed_vertical_df.
          join(deaths_vertical_df, (confirmed_vertical_df("id") <=> deaths_vertical_df("id")) && (confirmed_vertical_df("date") <=> deaths_vertical_df("date")), "outer").
          select(confirmed_vertical_df("*"), deaths_vertical_df("counts").as("deaths")).
          withColumnRenamed("counts", "confirmed")

      val merged_global_vertical_df = confirmed_deaths_vertical_df.
          join(recovered_vertical_df, (confirmed_deaths_vertical_df("id") <=> recovered_vertical_df("id")) && (confirmed_deaths_vertical_df("date") <=> recovered_vertical_df("date")), "outer").
          select(confirmed_deaths_vertical_df("*"), recovered_vertical_df("counts").as("recovered"))

      return merged_global_vertical_df
  }

  def mergeUSVertical(confirmed_vertical_df: DataFrame, deaths_vertical_df: DataFrame): DataFrame = {
      val merged_us_vertical_df = confirmed_vertical_df.
          join(deaths_vertical_df, (confirmed_vertical_df("uid") <=> deaths_vertical_df("uid") && confirmed_vertical_df("date") <=> deaths_vertical_df("date")), "outer").
          select(confirmed_vertical_df("*"), deaths_vertical_df("counts").as("deaths"), deaths_vertical_df("population")).
          withColumnRenamed("counts", "confirmed")

      return merged_us_vertical_df
  }

  def getGlobalCountVertical(global_vertical_df: DataFrame): DataFrame = {
      return global_vertical_df.
          select("*").
          where(global_vertical_df("region").isNull).
          drop("region")
  }

  def getUSCountVertical(us_vertical_df: DataFrame): DataFrame = {
      return us_vertical_df.
          select("*").
          drop("country")
  }

  def getGrowth(df: DataFrame, which: String, partition_col: String=null, vertical: Boolean=false): DataFrame = {
      if (vertical) {
          val df_with_lagged = df.
              withColumn(s"${which}_lagged", lag(which, 1, null.asInstanceOf[Integer]).over(Window.partitionBy(partition_col).orderBy("date")))
              
          return df_with_lagged.
              withColumn(s"${which}_growth", df(which).minus(df_with_lagged(s"${which}_lagged"))).
              drop(s"${which}_lagged")
      }

      def calculateGrowth(counts: Array[Integer]): Array[Integer] = {
          return Array[Integer](null.asInstanceOf[Integer]) ++ counts.slice(1, counts.length).
              zip(counts.slice(0, counts.length - 1)).
              map{case (a, b) => (a - b).asInstanceOf[Integer]}
      }

      val calculateGrowthUDF = udf(calculateGrowth _)

      return df.
          withColumn(s"${which}_growth", calculateGrowthUDF(df(which)))
  }

  def getIncidence(spark: SparkSession, global_confirmed: DataFrame, uid_lookup:DataFrame): DataFrame =  {

    global_confirmed.createOrReplaceTempView("global_confirmed")
    uid_lookup.createOrReplaceTempView("uid_lookup")

    val countryRates = spark.sql("SELECT g.Country, (element_at(g.counts,466)/u.Population) incidence FROM global_confirmed g join uid_lookup u " +
      "on g.Country = u.Country where u.region is null order by incidence desc")

    return countryRates

  }

  def getTropicalVsNonTropical(spark: SparkSession, global_confirmed: DataFrame, uid_lookup: DataFrame): DataFrame = {

    global_confirmed.createOrReplaceTempView("global_confirmed")
    uid_lookup.createOrReplaceTempView("uid_lookup")

    val tropical = spark.sql("SELECT 'tropical' climate, (sum(element_at(g.counts,466))) sum_confirmed, (sum(u.Population)) sum_population,(sum(element_at(g.counts,466))/sum(u.Population)) incidence " +
      "FROM global_confirmed g join uid_lookup u on g.latitude = u.latitude where u.latitude<23.5 and u.latitude>-23.5 order by incidence desc")

    val nonTropical = spark.sql("SELECT 'non-tropical' climate, (sum(element_at(g.counts,466))) sum_confirmed, (sum(u.Population)) sum_population,(sum(element_at(g.counts,466))/sum(u.Population)) incidence " +
      "FROM global_confirmed g join uid_lookup u on g.latitude = u.latitude where u.latitude>23.5 or u.latitude<-23.5 order by incidence desc")

      return tropical.union(nonTropical)
  }

  def getDeathRates(spark: SparkSession, global_deaths: DataFrame, global_confirmed: DataFrame): DataFrame = {

    global_deaths.createOrReplaceTempView("global_deaths")
    global_confirmed.createOrReplaceTempView("global_confirmed")

    val deathRates = spark.sql("SELECT d.country, (d.counts) as Deaths, r.counts as Cases from global_deaths d " +
    "join global_confirmed r on d.country = r.country where d.country in ('US','India', 'Germany', 'Japan', 'Brazil', 'Nigeria') order by d.country asc")

    val recoveredData = deathRates.withColumn("vars", explode(arrays_zip(col("Deaths"),col("Cases")))).select(
      "Country", "vars.Deaths", "vars.Cases")

    val withRate = recoveredData.withColumn("Death Rate", col("Deaths")/col("Cases"))

    return withRate
  }

}
