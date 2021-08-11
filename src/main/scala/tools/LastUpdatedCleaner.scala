package tools

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{when, col, regexp_replace}
import org.apache.spark.sql.Column

import java.text.SimpleDateFormat
import java.util.Date


object LastUpdatedCleaner {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Last Updated Cleaner")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    println("created spark session")
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    val covidRDD = spark.sparkContext.textFile("raw_data/covid_19_data.csv")
      .mapPartitionsWithIndex((index, line) => if(index == 0) line.drop(1) else line)
      .map(line => line.split(","))
      .map(x => x.map(y => y.replaceAll("^\\d/\\d{2}/\\d{4} \\d{2}:\\d{2}$", formatDate(y))))
      .map(x => (x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7)))
    val covidDF = spark.createDataFrame(covidRDD)
      .toDF("SNo","ObservationDate","Province/State","Country/Region","Last Update",
        "Confirmed","Deaths","Recovered").show()


  }

  def formatDate(line: String): String = {
    if(line.matches("^\\d/\\d{2}/\\d{4} \\d{2}:\\d{2}$")){
      val date_format = new SimpleDateFormat("M/dd/yyyy hh:mm")
      val new_date = date_format.parse(line)
      val date_text = new_date.toString
      return date_text
    }
    else return line

    //return "is this thing on?"
  }
}
