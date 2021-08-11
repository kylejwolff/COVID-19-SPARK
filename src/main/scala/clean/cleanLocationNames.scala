package clean

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, when}

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
    val dfP = df.withColumn("Province", when(col("Province/State").rlike("Diamond Princess"),
      "Diamond Princess").when(col("Province/State")===col("Country/Region"), null).otherwise(col("Province/State")))

    val dfProvince = dfP.drop(col("Province/State"))

    val dfC = dfProvince.withColumn(
      "Country",
      when(
        col("Country/Region") === "('St. Martin',)",
        "St. Martin"
      ).otherwise(col("Country/Region"))
    )
    val dfCountry = dfC.drop(col("Country/Region"))

    dfCountry.createOrReplaceTempView("covid_19_data")
    spark.sql("SELECT * FROM covid_19_data LIMIT 5").show()

    /*
        val conf = spark.read.format("csv")
          .option("inferSchema", "true")
          .option("header", "true")
          .load("raw_data/time_series_covid_19_confirmed.csv")
        conf.createOrReplaceTempView("confirmed")
        spark.sql("SELECT * FROM confirmed WHERE `Country/Region` LIKE '%South%'").show()
    */
    //spark.sql("SELECT `Province`, count(`Province`), max(Confirmed) FROM covid_19_data group by `Province` order by count(`Province`) desc").show(250)
    //spark.sql("SELECT `Country`, count(`Country`), max(Confirmed) FROM covid_19_data group by `Country` order by count(`Country`) desc").show(250)
    dfCountry.write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).save("clean_data/covid_19_data.csv")
  }

}
