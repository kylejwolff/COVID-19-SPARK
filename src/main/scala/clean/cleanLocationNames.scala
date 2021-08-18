package clean

import org.apache.spark.sql
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, trim, when}

object cleanLocationNames {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Clean Location Names")
      .config("spark.master", "local")
      .getOrCreate()
    val df = spark.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("raw_data/covid_19_data.csv")
    begin(spark,df)
  }
  def begin(spark: SparkSession, df: sql.DataFrame): sql.DataFrame={
    spark.sparkContext.setLogLevel("WARN")

    //Fix issues in province/state column
    val dfP = df.withColumn("Province/State", when(col("Province/State").rlike("Diamond Princess"),
      "Diamond Princess").when(col("Province/State").rlike("Grand Princess"),
      "Grand Princess").when(col("Province/State")===col("Country/Region")||col("Province/State")==="None"||col("Province/State").rlike("Unknown"), null)
      .otherwise(trim(col("Province/State"))))
    //Fix issues in Country/Region column
    val dfC = dfP.withColumn(
      "Country/Region",
      when(
        col("Country/Region") === "('St. Martin',)",
        "St. Martin").when(col("Country/Region").rlike("Bahamas"),
        "Bahamas").when(col("Country/Region").rlike("Gambia"),
        "Gambia").otherwise(trim(col("Country/Region")))
    )

    //Move data into spark.sql view
    //dfC.createOrReplaceTempView("covid_19_data")
    //spark.sql("SELECT * FROM covid_19_data").show()

    return dfC
  }

}
