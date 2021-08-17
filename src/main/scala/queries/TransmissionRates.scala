package queries

import tools._
import clean._

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.row_number
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
    val df = usPercentByQuarter(spark)
    df.show(250)
    spark.stop()
  }

  def usPercentByQuarter(spark: SparkSession): DataFrame = {
    val df1 = cleanLocationNames.begin(spark)
    val df2 = LastUpdateCleaner.cleanDF(spark,df1)
    df2.createOrReplaceTempView("global_confirmed")
    val uid_rdd = spark.sparkContext.textFile("raw_data/uid_lookup_table.csv")
    val uid = Cleaner.cleanUIDLookup(spark,Loader.loadCSV(spark, "raw_data/uid_lookup_table.csv"))
    val us_pop = uid.select(uid("country"), uid("region"), uid("population")).where(uid("region").isNull)

    //US Q1
    val usq1 = spark.sql("SELECT * " +
      "FROM (SELECT g.country country, date, SUM(g.confirmed) confirmed FROM (SELECT country, state, date, MAX(confirmed) confirmed FROM global_confirmed GROUP BY state, country, date) AS g GROUP BY g.country, g.date) " +
      "WHERE confirmed is not null AND country = \"US\" AND (date LIKE '%2020') AND (date BETWEEN '01/01/2020' AND '03/31/2020')" +
      "ORDER BY date")
    val usq1min = usq1.select(usq1("country"), usq1("date"), usq1("confirmed"))
      .groupBy("country").min("confirmed")
    val usq1max = usq1.select(usq1("country"), usq1("date"), usq1("confirmed"))
      .groupBy("country").max("confirmed")
    val usq1con = usq1min.join(usq1max, (usq1min("country") <=> usq1max("country")), "inner")
      .select(usq1min("country"), (usq1max("max(confirmed)") - usq1min("min(confirmed)")))
      .withColumnRenamed("(max(confirmed) - min(confirmed))", "q1_confirmed")
    val usq1percent = usq1con.join(us_pop, (us_pop("country") <=> usq1con("country")), "inner")
      .select(usq1con("country"), (usq1con("q1_confirmed") / us_pop("population")))
      .withColumnRenamed("(q1_confirmed / population)", "% of new cases per capita")

    //US Q2
    val usq2 = spark.sql("SELECT * " +
      "FROM (SELECT g.country country, date, SUM(g.confirmed) confirmed FROM (SELECT country, state, date, MAX(confirmed) confirmed FROM global_confirmed GROUP BY state, country, date) AS g GROUP BY g.country, g.date) " +
      "WHERE confirmed is not null AND country = \"US\" AND (date LIKE '%2020') AND (date BETWEEN '03/31/2020' AND '06/30/2020')" +
      "ORDER BY date")
    val usq2min = usq2.select(usq2("country"), usq2("date"), usq2("confirmed"))
      .groupBy("country").min("confirmed")
    val usq2max = usq2.select(usq2("country"), usq2("date"), usq2("confirmed"))
      .groupBy("country").max("confirmed")
    val usq2con = usq2min.join(usq2max, (usq2min("country") <=> usq2max("country")), "inner")
      .select(usq2min("country"), (usq2max("max(confirmed)") - usq2min("min(confirmed)")))
      .withColumnRenamed("(max(confirmed) - min(confirmed))", "q2_confirmed")
    val usq2percent = usq2con.join(us_pop, (us_pop("country") <=> usq2con("country")), "inner")
      .select(usq2con("country"), (usq2con("q2_confirmed") / us_pop("population")))
      .withColumnRenamed("(q2_confirmed / population)", "% of new cases per capita")

    //US Q3
    val usq3 = spark.sql("SELECT * " +
      "FROM (SELECT g.country country, date, SUM(g.confirmed) confirmed FROM (SELECT country, state, date, MAX(confirmed) confirmed FROM global_confirmed GROUP BY state, country, date) AS g GROUP BY g.country, g.date) " +
      "WHERE confirmed is not null AND country = \"US\" AND (date LIKE '%2020') AND (date BETWEEN '06/30/2020' AND '08/30/2020')" +
      "ORDER BY date")
    val usq3min = usq3.select(usq3("country"), usq3("date"), usq3("confirmed"))
      .groupBy("country").min("confirmed")
    val usq3max = usq3.select(usq3("country"), usq3("date"), usq3("confirmed"))
      .groupBy("country").max("confirmed")
    val usq3con = usq3min.join(usq3max, (usq3min("country") <=> usq3max("country")), "inner")
      .select(usq3min("country"), (usq3max("max(confirmed)") - usq3min("min(confirmed)")))
      .withColumnRenamed("(max(confirmed) - min(confirmed))", "q3_confirmed")
    val usq3percent = usq3con.join(us_pop, (us_pop("country") <=> usq3con("country")), "inner")
      .select(usq3con("country"), (usq3con("q3_confirmed") / us_pop("population")))
      .withColumnRenamed("(q3_confirmed / population)", "% of new cases per capita")

    //US Q4
    val usq4 = spark.sql("SELECT * " +
      "FROM (SELECT g.country country, date, SUM(g.confirmed) confirmed FROM (SELECT country, state, date, MAX(confirmed) confirmed FROM global_confirmed GROUP BY state, country, date) AS g GROUP BY g.country, g.date) " +
      "WHERE confirmed is not null AND country = \"US\" AND (date LIKE '%2020') AND (date BETWEEN '08/30/2020' AND '12/31/2020')" +
      "ORDER BY date")
    val usq4min = usq4.select(usq4("country"), usq4("date"), usq4("confirmed"))
      .groupBy("country").min("confirmed")
    val usq4max = usq4.select(usq4("country"), usq4("date"), usq4("confirmed"))
      .groupBy("country").max("confirmed")
    val usq4con = usq4min.join(usq4max, (usq4min("country") <=> usq4max("country")), "inner")
      .select(usq4min("country"), (usq4max("max(confirmed)") - usq4min("min(confirmed)")))
      .withColumnRenamed("(max(confirmed) - min(confirmed))", "q4_confirmed")
    val usq4percent = usq4con.join(us_pop, (us_pop("country") <=> usq4con("country")), "inner")
      .select(usq4con("country"), (usq4con("q4_confirmed") / us_pop("population")))
      .withColumnRenamed("(q4_confirmed / population)", "% of new cases per capita")

    val uspercent = usq1percent.union(usq2percent).union(usq3percent).union(usq4percent)
    val windowSpec = Window.partitionBy("country").orderBy("country")
    val uspercentbyquarter = uspercent.withColumn("quarter",row_number.over(windowSpec))
    Writer.writeCSV(uspercentbyquarter, "out/us_percent_by_quarter", true, true)
    return uspercentbyquarter.withColumn("quarter",row_number.over(windowSpec))
    // val trans_country_pop = df3.join(uid_pop, (uid_pop("country") <=> df3("country")), "inner")
    //   .select(df3("country"), df3("last_update"), (df3("confirmed") / uid_pop("sum(population)").as("percent")))
    //   .where((uid_pop("sum(population)") =!= 0) && (uid_pop("country") === "US") && (df3("last_update").contains("Apr"))).show(250)
  }
}
