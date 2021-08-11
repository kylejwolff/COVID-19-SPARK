package clean

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, trim, when}

object cleanLocationNames {

  def main(args: Array[String]): Unit = {
    begin()
  }
  def begin()={
    System.setProperty("hadoop.home.dir", "C:\\winutils")

    val spark = SparkSession
      .builder
      .appName("Cleaning Province and Country Data")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")


    val csvfile = "raw_data/covid_19_data.csv"
    val df = spark.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("raw_data/covid_19_data.csv")
    println("Covid_19_data.csv read")
    val dfP = df.withColumn("Province/State", when(col("Province/State").rlike("Diamond Princess"),
      "Diamond Princess").when(col("Province/State").rlike("Grand Princess"),
      "Grand Princess").when(col("Province/State")===col("Country/Region")||col("Province/State")==="None"||col("Province/State").rlike("Unknown"), null)
      .otherwise(trim(col("Province/State"))))



    val dfC = dfP.withColumn(
      "Country/Region",
      when(
        col("Country/Region") === "('St. Martin',)",
        "St. Martin").when(col("Country/Region").rlike("Bahamas"),
        "Bahamas").when(col("Country/Region").rlike("Gambia"),
        "Gambia").otherwise(trim(col("Country/Region")))
    )


    dfC.createOrReplaceTempView("covid_19_data")
    spark.sql("SELECT * FROM covid_19_data").show()

    /*
        val conf = spark.read.format("csv")
          .option("inferSchema", "true")
          .option("header", "true")
          .load("data/time_series_covid_19_confirmed.csv")
        conf.createOrReplaceTempView("confirmed")
        spark.sql("SELECT * FROM confirmed WHERE `Country/Region` LIKE '%South%'").show()
    */
    spark.sql("SELECT * FROM covid_19_data WHERE `Country/Region` = 'Gambia'").show()
    //spark.sql("SELECT `Province`, count(`Province`), max(Confirmed) FROM covid_19_data group by `Province` order by count(`Province`) desc").show(250)
    //spark.sql("SELECT `Country`, count(`Country`), max(Confirmed) FROM covid_19_data group by `Country` order by count(`Country`) asc").show(250)
    dfC.write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).save("clean_data/covid_19_data.csv")
  }

}
