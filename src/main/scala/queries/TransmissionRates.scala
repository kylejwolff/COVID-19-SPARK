package queries

import tools._
import clean._

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{row_number, col}
import org.apache.spark.sql.types._
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
    val df = globalPercentByQuarter(spark)
    df.show()
    spark.stop()
  }

  def usPercentByQuarter(spark: SparkSession): DataFrame = {
    val df1 = cleanLocationNames.begin(spark)
    val df2 = LastUpdateCleaner.cleanDF(spark,df1).cache()
    df2.createOrReplaceTempView("global_confirmed")
    val uid_rdd = spark.sparkContext.textFile("raw_data/uid_lookup_table.csv")
    val uid = Cleaner.cleanUIDLookup(spark,Loader.loadCSV(spark, "raw_data/uid_lookup_table.csv"))
    val us_pop = uid.select(uid("country"), uid("region"), uid("population")).where(uid("region").isNull)

    val usq = df2.select(df2("country"), df2("state"), df2("date"), df2("confirmed"))
      .withColumn("confirmed", col("confirmed").cast("int"))
      .groupBy("state", "country", "date").max("confirmed")
      .select("country", "date", "max(confirmed)")
      .groupBy("country", "date").sum("max(confirmed)")
      .withColumnRenamed("sum(max(confirmed))", "confirmed").cache()

    //US Q1
    val usq1 = usq.select(usq("country"), usq("date"), usq("confirmed"))
      .where(usq("country") === "US" && usq("date").like("%2020") && usq("date").between("01/01/2020","03/31/2020"))
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
    val usq2 = usq.select(usq("country"), usq("date"), usq("confirmed"))
      .where(usq("country") === "US" && usq("date").like("%2020") && usq("date").between("03/31/2020","06/30/2020"))
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
    val usq3 = usq.select(usq("country"), usq("date"), usq("confirmed"))
      .where(usq("country") === "US" && usq("date").like("%2020") && usq("date").between("06/30/2020","08/30/2020"))
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
    val usq4 = usq.select(usq("country"), usq("date"), usq("confirmed"))
      .where(usq("country") === "US" && usq("date").like("%2020") && usq("date").between("08/30/2020","12/31/2020"))
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

    //US Q5
    val usq5 = usq.select(usq("country"), usq("date"), usq("confirmed"))
      .where(usq("country") === "US" && usq("date").like("%2021") && usq("date").between("01/01/2021","03/31/2021"))
    val usq5min = usq5.select(usq5("country"), usq5("date"), usq5("confirmed"))
      .groupBy("country").min("confirmed")
    val usq5max = usq5.select(usq5("country"), usq5("date"), usq5("confirmed"))
      .groupBy("country").max("confirmed")
    val usq5con = usq5min.join(usq5max, (usq5min("country") <=> usq5max("country")), "inner")
      .select(usq5min("country"), (usq5max("max(confirmed)") - usq5min("min(confirmed)")))
      .withColumnRenamed("(max(confirmed) - min(confirmed))", "q5_confirmed")
    val usq5percent = usq5con.join(us_pop, (us_pop("country") <=> usq5con("country")), "inner")
      .select(usq5con("country"), (usq5con("q5_confirmed") / us_pop("population")))
      .withColumnRenamed("(q5_confirmed / population)", "% of new cases per capita")

    //US Q6
    val usq6 = usq.select(usq("country"), usq("date"), usq("confirmed"))
      .where(usq("country") === "US" && usq("date").like("%2021") && usq("date").between("03/31/2020","06/30/2020"))
    val usq6min = usq6.select(usq6("country"), usq6("date"), usq6("confirmed"))
      .groupBy("country").min("confirmed")
    val usq6max = usq6.select(usq6("country"), usq6("date"), usq6("confirmed"))
      .groupBy("country").max("confirmed")
    val usq6con = usq6min.join(usq6max, (usq6min("country") <=> usq6max("country")), "inner")
      .select(usq6min("country"), (usq6max("max(confirmed)") - usq6min("min(confirmed)")))
      .withColumnRenamed("(max(confirmed) - min(confirmed))", "q6_confirmed")
    val usq6percent = usq6con.join(us_pop, (us_pop("country") <=> usq6con("country")), "inner")
      .select(usq6con("country"), (usq6con("q6_confirmed") / us_pop("population")))
      .withColumnRenamed("(q6_confirmed / population)", "% of new cases per capita")

    val uspercent = usq1percent.union(usq2percent).union(usq3percent).union(usq4percent).union(usq5percent).union(usq6percent)
    val windowSpec = Window.partitionBy("country").orderBy("country")
    val uspercentbyquarter = uspercent.withColumn("quarter",row_number.over(windowSpec))
    //Writer.writeCSV(uspercentbyquarter, "out/us_percent_by_quarter", true, true)
    return uspercentbyquarter.withColumn("quarter",row_number.over(windowSpec))
  }

  def globalPercentByQuarter(spark: SparkSession): DataFrame = {
    val df1 = cleanLocationNames.begin(spark)
    val df2 = LastUpdateCleaner.cleanDF(spark,df1).cache()
    df2.createOrReplaceTempView("global_confirmed")
    val uid_rdd = spark.sparkContext.textFile("raw_data/uid_lookup_table.csv")
    val uid = Cleaner.cleanUIDLookup(spark,Loader.loadCSV(spark, "raw_data/uid_lookup_table.csv"))
    val us_pop = uid.select(uid("country"), uid("region"), uid("population")).where(uid("region").isNull)

    val gq = df2.select(df2("country"), df2("state"), df2("date"), df2("confirmed"))
      .withColumn("confirmed", col("confirmed").cast("int"))
      .groupBy("state", "country", "date").max("confirmed")
      .select("country", "date", "max(confirmed)")
      .groupBy("country", "date").sum("max(confirmed)")
      .withColumnRenamed("sum(max(confirmed))", "confirmed").cache()

    //Global Q1
    val gq1 = gq.select(gq("country"), gq("date"), gq("confirmed"))
      .where(gq("date").like("%2020") && gq("date").between("01/01/2020","03/31/2020"))
    val gq1min = gq1.select(gq1("country"), gq1("date"), gq1("confirmed"))
      .groupBy("country").min("confirmed")
    val gq1max = gq1.select(gq1("country"), gq1("date"), gq1("confirmed"))
      .groupBy("country").max("confirmed")
    val gq1con = gq1min.join(gq1max, (gq1min("country") <=> gq1max("country")), "inner")
      .select(gq1min("country"), (gq1max("max(confirmed)") - gq1min("min(confirmed)")))
      .withColumnRenamed("(max(confirmed) - min(confirmed))", "q6_confirmed")
    val gq1percent = gq1con.join(us_pop, (us_pop("country") <=> gq1con("country")), "inner")
      .select(gq1con("country"), (gq1con("q6_confirmed") / us_pop("population")))
      .withColumnRenamed("(q6_confirmed / population)", "% of new cases per capita")

    //Global Q2
    val gq2 = gq.select(gq("country"), gq("date"), gq("confirmed"))
      .where(gq("date").like("%2020") && gq("date").between("03/31/2020","06/30/2020"))
    val gq2min = gq2.select(gq2("country"), gq2("date"), gq2("confirmed"))
      .groupBy("country").min("confirmed")
    val gq2max = gq2.select(gq2("country"), gq2("date"), gq2("confirmed"))
      .groupBy("country").max("confirmed")
    val gq2con = gq2min.join(gq2max, (gq2min("country") <=> gq2max("country")), "inner")
      .select(gq2min("country"), (gq2max("max(confirmed)") - gq2min("min(confirmed)")))
      .withColumnRenamed("(max(confirmed) - min(confirmed))", "q6_confirmed")
    val gq2percent = gq2con.join(us_pop, (us_pop("country") <=> gq2con("country")), "inner")
      .select(gq2con("country"), (gq2con("q6_confirmed") / us_pop("population")))
      .withColumnRenamed("(q6_confirmed / population)", "% of new cases per capita")

    //Global Q3
    val gq3 = gq.select(gq("country"), gq("date"), gq("confirmed"))
      .where(gq("date").like("%2020") && gq("date").between("06/30/2020","08/30/2020"))
    val gq3min = gq3.select(gq3("country"), gq3("date"), gq3("confirmed"))
      .groupBy("country").min("confirmed")
    val gq3max = gq3.select(gq3("country"), gq3("date"), gq3("confirmed"))
      .groupBy("country").max("confirmed")
    val gq3con = gq3min.join(gq3max, (gq3min("country") <=> gq3max("country")), "inner")
      .select(gq3min("country"), (gq3max("max(confirmed)") - gq3min("min(confirmed)")))
      .withColumnRenamed("(max(confirmed) - min(confirmed))", "q6_confirmed")
    val gq3percent = gq3con.join(us_pop, (us_pop("country") <=> gq3con("country")), "inner")
      .select(gq3con("country"), (gq3con("q6_confirmed") / us_pop("population")))
      .withColumnRenamed("(q6_confirmed / population)", "% of new cases per capita")

    //Global Q4
    val gq4 = gq.select(gq("country"), gq("date"), gq("confirmed"))
      .where(gq("date").like("%2020") && gq("date").between("08/30/2020","12/31/2020"))
    val gq4min = gq4.select(gq4("country"), gq4("date"), gq4("confirmed"))
      .groupBy("country").min("confirmed")
    val gq4max = gq4.select(gq4("country"), gq4("date"), gq4("confirmed"))
      .groupBy("country").max("confirmed")
    val gq4con = gq4min.join(gq4max, (gq4min("country") <=> gq4max("country")), "inner")
      .select(gq4min("country"), (gq4max("max(confirmed)") - gq4min("min(confirmed)")))
      .withColumnRenamed("(max(confirmed) - min(confirmed))", "q6_confirmed")
    val gq4percent = gq4con.join(us_pop, (us_pop("country") <=> gq4con("country")), "inner")
      .select(gq4con("country"), (gq4con("q6_confirmed") / us_pop("population")))
      .withColumnRenamed("(q6_confirmed / population)", "% of new cases per capita")

    //Global Q5
    val gq5 = gq.select(gq("country"), gq("date"), gq("confirmed"))
      .where(gq("date").like("%2021") && gq("date").between("01/01/2021","03/31/2021"))
    val gq5min = gq5.select(gq5("country"), gq5("date"), gq5("confirmed"))
      .groupBy("country").min("confirmed")
    val gq5max = gq5.select(gq5("country"), gq5("date"), gq5("confirmed"))
      .groupBy("country").max("confirmed")
    val gq5con = gq5min.join(gq5max, (gq5min("country") <=> gq5max("country")), "inner")
      .select(gq5min("country"), (gq5max("max(confirmed)") - gq5min("min(confirmed)")))
      .withColumnRenamed("(max(confirmed) - min(confirmed))", "q6_confirmed")
    val gq5percent = gq5con.join(us_pop, (us_pop("country") <=> gq5con("country")), "inner")
      .select(gq5con("country"), (gq5con("q6_confirmed") / us_pop("population")))
      .withColumnRenamed("(q6_confirmed / population)", "% of new cases per capita")

    //Global Q6
    val gq6 = gq.select(gq("country"), gq("date"), gq("confirmed"))
      .where(gq("date").like("%2021") && gq("date").between("03/31/2021","06/30/2021"))
    val gq6min = gq6.select(gq6("country"), gq6("date"), gq6("confirmed"))
      .groupBy("country").min("confirmed")
    val gq6max = gq6.select(gq6("country"), gq6("date"), gq6("confirmed"))
      .groupBy("country").max("confirmed")
    val gq6con = gq6min.join(gq6max, (gq6min("country") <=> gq6max("country")), "inner")
      .select(gq6min("country"), (gq6max("max(confirmed)") - gq6min("min(confirmed)")))
      .withColumnRenamed("(max(confirmed) - min(confirmed))", "q6_confirmed")
    val gq6percent = gq6con.join(us_pop, (us_pop("country") <=> gq6con("country")), "inner")
      .select(gq6con("country"), (gq6con("q6_confirmed") / us_pop("population")))
      .withColumnRenamed("(q6_confirmed / population)", "% of new cases per capita")

    val gqpercent = gq1percent.union(gq2percent).union(gq3percent).union(gq4percent).union(gq5percent).union(gq6percent)
    val windowSpec = Window.partitionBy("country").orderBy("country")
    val global_percent_by_quarter = gqpercent.withColumn("quarter",row_number.over(windowSpec))
    return global_percent_by_quarter
  }
}
