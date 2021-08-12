package tools

import org.apache.spark.sql.SparkSession

import java.text.SimpleDateFormat


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

    val covidRDD = spark.sparkContext.textFile("raw_data/covid_19_data.csv")
      .mapPartitionsWithIndex((index, line) => if(index == 0) line.drop(1) else line)
      .map(line => line.split(","))
      .map(x => x.map(y => y.replaceAll("\\d{1,2}/\\d{1,2}/\\d{2,4} \\d{1,2}:\\d{2}", formatDate(y))))
      .map(x => x.map(y => y.replaceAll("\\d{2,4}-\\d{1,2}-\\d{1,2}T? ?\\d{1,2}:\\d{1,2}:\\d{1,2}", formatDate(y))))
      .map(x => (x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7)))
    val covidDF = spark.createDataFrame(covidRDD)
      .toDF("SNo","ObservationDate","Province/State","Country/Region","Last Update",
        "Confirmed","Deaths","Recovered")
    covidDF.write
      .option("header",true)
      .csv("clean_data/covid_19_data.csv")


  }

  def formatDate(line: String): String = {
    if(line.matches("^\\d{1,2}/\\d{1,2}/\\d{2,4} \\d{1,2}:\\d{2}$")){
            val date_format = new SimpleDateFormat("M/dd/yy hh:mm")
            val new_date = date_format.parse(line)
            val date_text = new_date.toString
            return date_text
    }
    else if(line.matches("^\\d{2,4}-\\d{1,2}-\\d{1,2}T? ?\\d{1,2}:\\d{1,2}:\\d{1,2}$")){
      val temp = line.replace("T", " ")
      val date_format = new SimpleDateFormat("yy-MM-dd hh:mm:ss")
      val new_date = date_format.parse(temp)
      val date_text = new_date.toString
      return date_text
    }
    else{
      return "UNKNOWN DATE FORMAT ERROR"
    }
  }
}
