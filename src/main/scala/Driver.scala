import tools._
import scala.io.StdIn.readLine

import clean._
import queries._

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.log4j.Logger
import org.apache.log4j.Level

object Driver {

  def clear(): Unit = {
    print("\u001b[2J")
  }

  def query_1(global_merged_df: DataFrame, start_date: LocalDate): Unit = {
    /*
      Query 1: Global Confirmed Cases (Unpacked)
      
      Description: Selects only the confirmed cases from the merged global data and 
        unpack the arrays in the "confirmed" column into a single column for every date-value.
    */

    Query.getGlobalCountUnpacked(global_merged_df, "confirmed", start_date).show()
  }

  def query_2(global_merged_vertical_df: DataFrame): Unit = {
    /*
      Query 2: Global Confirmed Cases (Vertical)
      
      Description: Using the row-based timeseries of the merged global data, 
        selects only the confirmed cases of country-level entries from the merged global data.
        Region-level entries are omitted to prevent duplicates.
    */

    Query.getGlobalCountVertical(global_merged_vertical_df).drop("deaths", "recovered").show()
  }

  def query_3(global_merged_df: DataFrame, start_date: LocalDate): Unit = {
    /*
      Query 3: Global Confirmed Cases Daily Growth (Unpacked)
      
      Description: Using the merged global data, creates an additional column "confirmed_growth" for daily growths of "confirmed".
        Selects only the "confirmed_growth" from the resulting query and unpack into a single column for every date-value.
        The "confirmed_growth" column indicates the daily difference in confirmed cases.
    */

    Query.getGlobalCountUnpacked(Query.getGrowth(global_merged_df, "confirmed"), "confirmed_growth", start_date).show()
  }

  def query_4(spark: SparkSession, global_confirmed: DataFrame, uid_lookup: DataFrame): Unit ={
    /*
      Query 4: Global Confirmed Cases Each Country

      Description: Using the global confirmed cases data, gets the incidence rate of covid in each country by dividing the
      final number of confirmed cases by that countries population. Also return the number of confirmed cases and population.
    */
    queries.queryConfirmedCases.queryPerCapita(spark, global_confirmed, uid_lookup).show()
  }

  def query_5(spark: SparkSession, global_confirmed: DataFrame, uid_lookup: DataFrame): Unit ={
    /*
      Query 5: Global Cases Tropical and Non-tropical Countries

      Description: Using the global confirmed cases data, for countries with latitudes that make them tropical sums the
      confirmed cases and population to get a total incidence rate. Then does the same for country outside the tropical range.
    */
    queries.queryConfirmedCases.queryTropical(spark, global_confirmed, uid_lookup)
  }

  def query_6(spark: SparkSession, global_deaths: DataFrame, global_confirmed: DataFrame): Unit ={
    /*
      Query 6: Global Death Rates

      Description: Using the global deaths data, for the countries of Brazil, Germany, India, Japan, Nigeria, and US gets the
      confirmed cases, deaths, and death rate at each date in the dataset.
    */
    queries.queryConfirmedCases.queryDeathRates(spark, global_deaths, global_confirmed).show()
  }

  def query_7(spark: SparkSession, cleanedNames: DataFrame, uid_lookup: DataFrame): Unit ={
    /*
      Query 7: US Confirmed Cases Per Capita by quarter

      Description: Using covid_19_data.csv, returns the confirmed cases per capita in the United States by quarter.
    */
    queries.TransmissionRates.usPercentByQuarter(spark, cleanedNames, uid_lookup).show()
  }

  def query_8(spark: SparkSession, cleanedNames: DataFrame, uid_lookup: DataFrame): Unit ={
    /*
      Query 8: US Confirmed Cases, Deaths, and Recovered Per Capita by quarter

      Description: Using covid_19_data.csv, returns the confirmed cases, deaths,
      and recovered cases per capita in the United States by quarter.
    */
    queries.TransmissionRates.usAllPercentByQuarter(spark, cleanedNames, uid_lookup).show()
  }

  def query_9(spark: SparkSession, cleanedNames: DataFrame, uid_lookup: DataFrame): Unit ={
    /*
      Query 9: Global Confirmed Cases Per Capita by quarter

      Description: Using covid_19_data.csv, returns the confirmed cases per capita
      for each country by quarter.
    */
    queries.TransmissionRates.globalPercentByQuarter(spark, cleanedNames, uid_lookup).show()
  }

  def main(args: Array[String]): Unit = {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("COVID-19-SPARK")
      .config("spark.master", "local")
      .getOrCreate()

    println("Created SparkSession")

    // Constants
    val start_date = LocalDate.parse("01-22-2020", DateTimeFormatter.ofPattern("MM-dd-yyyy"))

    val uid_lookup_path = "raw_data/uid_lookup_table.csv"
    val global_confirmed_path = "raw_data/time_series_covid_19_confirmed.csv"
    val global_deaths_path = "raw_data/time_series_covid_19_deaths.csv"
    val global_recovered_path = "raw_data/time_series_covid_19_recovered.csv"
    val us_confirmed_path = "raw_data/time_series_covid_19_confirmed_US.csv"
    val us_deaths_path = "raw_data/time_series_covid_19_deaths_US.csv"
    val covid_19_data_path = "raw_data/covid_19_data.csv"

    println("Preparing DataFrames...")

    // Clean-up
    val uid_lookup = Cleaner.cleanUIDLookup(spark, Loader.loadCSV(spark, uid_lookup_path))

    val global_confirmed_timeseries = Cleaner.cleanGlobalTimeSeries(spark, Loader.loadCSV(spark, global_confirmed_path))
    val global_deaths_timeseries = Cleaner.cleanGlobalTimeSeries(spark, Loader.loadCSV(spark, global_deaths_path))
    val global_recovered_timeseries = Cleaner.cleanGlobalTimeSeries(spark, Loader.loadCSV(spark, global_recovered_path))
    val us_confirmed_timeseries = Cleaner.cleanUSTimeSeries(spark, Loader.loadCSV(spark, us_confirmed_path))
    val us_deaths_timeseries = Cleaner.cleanUSTimeSeries(spark, Loader.loadCSV(spark, us_deaths_path), with_population=true)

    val global_confirmed_timeseries_vertical = Cleaner.cleanGlobalTimeSeries_Vertical(spark, Loader.loadCSV(spark, global_confirmed_path), start_date)
    val global_deaths_timeseries_vertical = Cleaner.cleanGlobalTimeSeries_Vertical(spark, Loader.loadCSV(spark, global_deaths_path), start_date)
    val global_recovered_timeseries_vertical = Cleaner.cleanGlobalTimeSeries_Vertical(spark, Loader.loadCSV(spark, global_recovered_path), start_date)
    val us_confirmed_timeseries_vertical = Cleaner.cleanUSTimeSeries_Vertical(spark, Loader.loadCSV(spark, us_confirmed_path), start_date)
    val us_deaths_timeseries_vertical = Cleaner.cleanUSTimeSeries_Vertical(spark, Loader.loadCSV(spark, us_deaths_path), start_date, with_population=true)

    // Merge Timeseries
    val global_merged_timeseries = Query.mergeGlobal(global_confirmed_timeseries, global_deaths_timeseries, global_recovered_timeseries)
    val us_merged_timeseries = Query.mergeUS(us_confirmed_timeseries, us_deaths_timeseries)

    val global_merged_timeseries_vertical = Query.mergeGlobalVertical(global_confirmed_timeseries_vertical, global_deaths_timeseries_vertical, global_recovered_timeseries_vertical)
    val us_merged_timeseries_vertical = Query.mergeUSVertical(us_confirmed_timeseries_vertical, us_deaths_timeseries_vertical)

    // Clean-up covid_19_data.csv
    val cleanedDates = LastUpdateCleaner.cleanCSV(spark)
    val cleanedNames = cleanLocationNames.begin(spark, cleanedDates)

    println("All DataFrames ready.\n")

    var run = true

    while(run) {
      println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
      println("+ Main Menu                                                      +")
      println("+ 1 - Query 1: Global Confirmed Cases (Unpacked)                 +")
      println("+ 2 - Query 2: Global Confirmed Cases (Vertical)                 +")
      println("+ 3 - Query 3: Global Confirmed Cases Daily Growth (Unpacked)    +")
      println("+ 4 - Query 4: Global Confirmed Cases Each Country               +")
      println("+ 5 - Query 5: Global Cases Tropical and Non-tropical Countries  +")
      println("+ 6 - Query 6: Global Death Rates                                +")
      println("+ x - Exit the program                                           +")
      println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
      println("Enter a menu option from the list:")

      val userEntry = readLine()

      clear()

      println("Running query...")
      
      userEntry match {
        case "1" => query_1(global_merged_timeseries, start_date)
        case "2" => query_2(global_merged_timeseries_vertical)
        case "3" => query_3(global_merged_timeseries, start_date)
        case "4" => query_4(spark, global_confirmed_timeseries, uid_lookup)
        case "5" => query_5(spark, global_confirmed_timeseries, uid_lookup)
        case "6" => query_6(spark, global_deaths_timeseries, global_confirmed_timeseries)
        case "7" => query_7(spark, cleanedNames, uid_lookup)
        case "8" => query_8(spark, cleanedNames, uid_lookup)
        case "9" => query_9(spark, cleanedNames, uid_lookup)
        case "x" => run = false
        case _ =>
      }
    }
    spark.stop()
  }
}

object QueryTester {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    // Constant
    val start_date = LocalDate.parse("01-22-2020", DateTimeFormatter.ofPattern("MM-dd-yyyy"))

    // Dataset CSV paths
    val uid_lookup_path = "raw_data/uid_lookup_table.csv"
    val global_confirmed_path = "raw_data/time_series_covid_19_confirmed.csv"
    val global_deaths_path = "raw_data/time_series_covid_19_deaths.csv"
    val global_recovered_path = "raw_data/time_series_covid_19_recovered.csv"
    val us_confirmed_path = "raw_data/time_series_covid_19_confirmed_US.csv"
    val us_deaths_path = "raw_data/time_series_covid_19_deaths_US.csv"

    val spark = SparkSession
      .builder()
      .config("spark.master", "local[*]")
      .appName("Spark-COVID")
      .getOrCreate()

    // Test Case 1: Load, clean, display Global Confirmed Timeseries Vertical growths
    val global_vertical_confirmed_growth = Query.getGrowth(Cleaner.cleanGlobalTimeSeries_Vertical(spark, Loader.loadCSV(spark, global_confirmed_path), start_date), "counts", "id", vertical=true)
    global_vertical_confirmed_growth.where(global_vertical_confirmed_growth("country") === "Indonesia").orderBy("date").show(500)

    // Test Case 2: Load, clean, display Global Confirmed Timeseries growths
    val global_confirmed_growth = Query.getGrowth(Cleaner.cleanGlobalTimeSeries(spark, Loader.loadCSV(spark, global_confirmed_path)), "counts")
    println(global_confirmed_growth.first.getAs[scala.collection.mutable.WrappedArray[String]]("counts").mkString(","))
    println(global_confirmed_growth.first.getAs[scala.collection.mutable.WrappedArray[String]]("growth").mkString(","))

    println()

    // Test Case 3: Load, clean, display US Deaths Timeseries Vertical growths
    val us_vertical_deaths = Query.getGrowth(Cleaner.cleanUSTimeSeries_Vertical(spark, Loader.loadCSV(spark, us_deaths_path), start_date, with_population=true), "counts", "uid", vertical=true)
    us_vertical_deaths.where(us_vertical_deaths("uid") === 84001001).orderBy("date").show(500)

    // Test Case 4: Load, clean, display US Deaths Timeseries growths
    val us_deaths_growth = Query.getGrowth(Cleaner.cleanUSTimeSeries(spark, Loader.loadCSV(spark, us_deaths_path), with_population=true), "counts")
    println(us_deaths_growth.first.getAs[scala.collection.mutable.WrappedArray[String]]("counts").mkString(","))
    println(us_deaths_growth.first.getAs[scala.collection.mutable.WrappedArray[String]]("growth").mkString(","))

    // Test Case 5: Load, clean US Confirmed Timeseries growths & unpack
    val global_confirmed = Cleaner.cleanGlobalTimeSeries(spark, Loader.loadCSV(spark, global_confirmed_path))
    val global_confirmed_growth2 = Query.getGrowth(global_confirmed.withColumnRenamed("counts", "confirmed"), "confirmed")
    val global_confirmed_growth2_unpacked = Query.getGlobalCountUnpacked(global_confirmed_growth2, "confirmed_growth", start_date)

    global_confirmed_growth2_unpacked.show()

    spark.stop()
  }
}