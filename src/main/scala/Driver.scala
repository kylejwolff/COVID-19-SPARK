import tools._
import queries._

import scala.io.StdIn.readLine

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

    Queries.getGlobalCountUnpacked(global_merged_df, "confirmed", start_date).show()
  }

  def query_2(global_merged_vertical_df: DataFrame): Unit = {
    /*
      Query 2: Global Confirmed Cases (Vertical)
      
      Description: Using the row-based timeseries of the merged global data, 
        selects only the confirmed cases of country-level entries from the merged global data.
        Region-level entries are omitted to prevent duplicates.
    */

    Queries.getGlobalCountVertical(global_merged_vertical_df).drop("deaths", "recovered").show()
  }

  def query_3(global_merged_df: DataFrame, start_date: LocalDate): Unit = {
    /*
      Query 3: Global Confirmed Cases Daily Growth (Unpacked)
      
      Description: Using the merged global data, creates an additional column "confirmed_growth" for daily growths of "confirmed".
        Selects only the "confirmed_growth" from the resulting query and unpack into a single column for every date-value.
        The "confirmed_growth" column indicates the daily difference in confirmed cases.
    */

    Queries.getGlobalCountUnpacked(Queries.getGrowth(global_merged_df, "confirmed"), "confirmed_growth", start_date).show()
  }

  def query_4(spark: SparkSession, global_confirmed: DataFrame, uid_lookup: DataFrame): Unit ={
    /*
      Query 4: Global Confirmed Cases Each Country

      Description: Using the global confirmed cases data, gets the incidence rate of covid in each country by dividing the
      final number of confirmed cases by that countries population. Also return the number of confirmed cases and population.
    */
    Queries.getIncidence(spark, global_confirmed, uid_lookup).show()
  }

  def query_5(spark: SparkSession, global_confirmed: DataFrame, uid_lookup: DataFrame): Unit ={
    /*
      Query 5: Global Cases Tropical and Non-tropical Countries

      Description: Using the global confirmed cases data, for countries with latitudes that make them tropical sums the
      confirmed cases and population to get a total incidence rate. Then does the same for country outside the tropical range.
    */
    Queries.getTropicalVsNonTropical(spark, global_confirmed, uid_lookup).show()
  }

  def query_6(spark: SparkSession, global_deaths: DataFrame, global_confirmed: DataFrame): Unit ={
    /*
      Query 6: Global Death Rates

      Description: Using the global deaths data, for the countries of Brazil, Germany, India, Japan, Nigeria, and US gets the
      confirmed cases, deaths, and death rate at each date in the dataset.
    */
    Queries.getDeathRates(spark, global_deaths, global_confirmed).show()
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
    val global_merged_timeseries = Queries.mergeGlobal(global_confirmed_timeseries, global_deaths_timeseries, global_recovered_timeseries)
    val us_merged_timeseries = Queries.mergeUS(us_confirmed_timeseries, us_deaths_timeseries)

    val global_merged_timeseries_vertical = Queries.mergeGlobalVertical(global_confirmed_timeseries_vertical, global_deaths_timeseries_vertical, global_recovered_timeseries_vertical)
    val us_merged_timeseries_vertical = Queries.mergeUSVertical(us_confirmed_timeseries_vertical, us_deaths_timeseries_vertical)

    // Clean-up covid_19_data.csv
    val covid_19_data = Cleaner.cleanCovid19Data(spark, Loader.loadCSV(spark, covid_19_data_path))

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
        case "x" => run = false
        case _ =>
      }
    }
    spark.stop()
  }
}