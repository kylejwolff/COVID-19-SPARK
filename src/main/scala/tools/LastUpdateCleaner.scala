package tools

import org.apache.spark.sql.{AnalysisException, SparkSession, DataFrame, Row}
import org.apache.spark.rdd.RDD

import java.text.SimpleDateFormat
import java.time.LocalDate


object LastUpdateCleaner {
  case class covid19data (sno:String, observationDate:String, State:String, country:String, last_update:String, confirmed:Float, deaths:Float, recovered:Float)

  // def main(args: Array[String]): Unit = {
  //   val spark = SparkSession
  //     .builder()
  //     .appName("Last Updated tools.Cleaner")
  //     .config("spark.master", "local")
  //     //.enableHiveSupport()
  //     .getOrCreate()
  //   println("created spark session")
  //   spark.sparkContext.setLogLevel("ERROR")
  //   cleanCSV(spark)
  //   spark.stop()

  // }
  def cleanCSV(spark: SparkSession): DataFrame = {
    print("Cleaning \"Last Update\" from covid_19_data.csv...")
    val covidRDD = spark.sparkContext.textFile("raw_data/covid_19_data.csv")
      .mapPartitionsWithIndex((index, line) => if(index == 0) line.drop(1) else line)
      .map(line => line.split(","))
      .map(x => x.map(y => y.replaceAll("\\d{1,2}/\\d{1,2}/\\d{2,4} \\d{1,2}:\\d{2}", formatDate(y))))
      .map(x => x.map(y => y.replaceAll("\\d{2,4}-\\d{1,2}-\\d{1,2}T? ?\\d{1,2}:\\d{1,2}:\\d{1,2}", formatDate(y))))
      .map(x => (x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7)))
    val covidDF = spark.createDataFrame(covidRDD)
      .toDF("SNo","ObservationDate","Province/State","Country/Region","Last_Update",
        "Confirmed","Deaths","Recovered")
      // val covidDF = spark.createDataFrame(covidRDD
      //   .map(x => covid19data(x(0), x(1), x(2), x(3), x(4), x(5)toFloat, x(6).toFloat, x(7)toFloat)))
    
      return covidDF
  }

  def cleanDF(spark: SparkSession, covid_19_dataframe: DataFrame): DataFrame = {
    print("Cleaning \"Last Update\" from covid_19_dataframe...")
    val covidRDD = covid_19_dataframe.rdd
      .map(line => line.toString)
      .mapPartitionsWithIndex((index, line) => if(index == 0) line.drop(1) else line)
      .map(line => line.split(","))
      .map(x => x.map(y => y.replaceAll("\\d{1,2}/\\d{1,2}/\\d{2,4} \\d{1,2}:\\d{2}", formatDate(y))))
      .map(x => x.map(y => y.replaceAll("\\d{2,4}-\\d{1,2}-\\d{1,2}T? ?\\d{1,2}:\\d{1,2}:\\d{1,2}", formatDate(y))))
      .map(x => (x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7)))
    val covidDF = spark.createDataFrame(covidRDD)
      .toDF("SNo","Date","State","Country","Last_Update",
        "Confirmed","Deaths","Recovered")
    // val covidDF = spark.createDataFrame(covidRDD
    //     .map(x => covid19data(x(0), x(1), x(2), x(3), x(4), x(5)toFloat, x(6).toFloat, x(7)toFloat)))
    return covidDF
  }
  def formatDate(line: String): String = {
    if(line.matches("^\\d{1,2}/\\d{1,2}/\\d{2,4} \\d{1,2}:\\d{2}$")){
      val date_format = new SimpleDateFormat("M/dd/yyyy hh:mm")
      val new_date = date_format.parse(line)
      val format_date = new SimpleDateFormat("MM-dd-yyyy").format(new_date)
      val date_text = format_date.toString
      return date_text
    }
    else if(line.matches("^\\d{2,4}-\\d{1,2}-\\d{1,2}T? ?\\d{1,2}:\\d{1,2}:\\d{1,2}$")){
      val temp = line.replace("T", " ")
      val date_format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
      val new_date = date_format.parse(temp)
      val format_date = new SimpleDateFormat("MM-dd-yyyy").format(new_date)
      val date_text = format_date.toString
      return date_text
    }
    else{
      return "UNKNOWN DATE FORMAT ERROR"
    }
  }
}
