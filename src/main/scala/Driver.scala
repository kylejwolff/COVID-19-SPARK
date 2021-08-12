import tools._
import scala.io.StdIn.readLine

import clean._


import org.apache.spark.sql.SparkSession

object Driver {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Last Updated tools.Cleaner")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    println("created spark session")
    spark.sparkContext.setLogLevel("ERROR")
    var run = true
    while(run){
      println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
      println("+ Main menu                                               +")
      println("+ 1 - Clean \"Last Update\" in covid_19_data.csv          +")
      println("+ 2 - Clean Location Names                                +")
      println("+ 3 - Clean Time Series                                   +")
      println("+ x - exit the program                                    +")
      println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
      println("Enter a menu option from the list:")
      val userEntry = readLine()
      userEntry match {
        case "1" => LastUpdateCleaner.clean(spark)
        case "2" => cleanLocationNames.begin(spark)
        case "3" => {
          //Dataset CSV paths
          val uid_lookup_path = "raw_data/uid_lookup_table.csv"
          val global_confirmed_path = "raw_data/time_series_covid_19_confirmed.csv"
          val global_deaths_path = "raw_data/time_series_covid_19_deaths.csv"
          val global_recovered_path = "raw_data/time_series_covid_19_recovered.csv"
          val us_confirmed_path = "raw_data/time_series_covid_19_confirmed_US.csv"
          val us_deaths_path = "raw_data/time_series_covid_19_deaths_US.csv"

//          val spark = SparkSession
//            .builder()
//            .config("spark.master", "local[*]")
//            .appName("Spark-COVID")
//            .getOrCreate()

          // Test Case 1: Load, clean, display and count rows in UID Lookup Table
          val uid_lookup = Cleaner.cleanUIDLookup(spark, Loader.loadCSV(spark, uid_lookup_path))
          uid_lookup.show()
          println(uid_lookup.count())

          // Test Case 2: Load, clean, display and count rows in Global Confirmed Timeseries
          val global_confirmed = Cleaner.cleanGlobalTimeSeries(spark, Loader.loadCSV(spark, global_confirmed_path))
          global_confirmed.show()
          println(global_confirmed.count())

          // Test Case 3: Load, clean, display and count rows in Global Deaths Timeseries
          val global_deaths = Cleaner.cleanGlobalTimeSeries(spark, Loader.loadCSV(spark, global_deaths_path))
          global_deaths.show()
          println(global_deaths.count())

          // Test Case 4: Load, clean, display and count rows in Global Recovered Timeseries
          val global_recovered = Cleaner.cleanGlobalTimeSeries(spark, Loader.loadCSV(spark, global_recovered_path))
          global_recovered.show()
          println(global_recovered.count())

          // Test Case 5: Load, clean, display and count rows in US Confirmed Timeseries
          val us_confirmed = Cleaner.cleanUSTimeSeries(spark, Loader.loadCSV(spark, us_confirmed_path))
          us_confirmed.show()
          println(us_confirmed.count())

          // Test Case 6: Load, clean, display and count rows in US Deaths Timeseries
          val us_deaths = Cleaner.cleanUSTimeSeries(spark, Loader.loadCSV(spark, us_deaths_path))
          us_deaths.show()
          println(us_deaths.count())
        }
        case "x" => run = false
        case _ =>
      }
    }
    spark.stop()
  }
}

