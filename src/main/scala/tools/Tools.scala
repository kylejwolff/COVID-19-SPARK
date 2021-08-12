package tools

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object ToolsTester {
    def main(args: Array[String]): Unit = {

        Logger.getLogger("org").setLevel(Level.ERROR)
        Logger.getLogger("akka").setLevel(Level.ERROR)

        //Dataset CSV paths
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

        spark.stop()
    }
}


object Loader {
    def loadCSV(session: SparkSession, path: String, drop_header: Boolean=true): RDD[String] = {
        if (drop_header) {
            return session.sparkContext.textFile(path).mapPartitionsWithIndex {
                        (idx, iter) => if (idx == 0) iter.drop(1) else iter 
                    }
        }
        else {
            return session.sparkContext.textFile(path)
        }
    }
}

object Writer {
    def writeCSV(data_df: DataFrame, path: String, include_header: Boolean=false): Unit =  {
        if (include_header) {
            data_df.
            write.
            option("header", true).
            csv(path)
        }
        else {
            data_df.
            write.
            csv(path)
        }
    }
}

object Cleaner {

    case class UIDLookupRow(uid:Int, iso2:String, iso3:String, code3:Int, FIPS:Int, Admin2:String, region:String, country:String, latitude:Float, longitude:Float, combined_key:String, population:Int)
    case class GlobalTimeSeriesRow(region:String, country:String, latitude:Float, longitude:Float, counts:Array[Int])
    case class USTimeSeriesRow(uid:Int, iso2:String, iso3:String, code3:Int, FIPS:Int, Admin2:String, region:String, country:String, latitude:Float, longitude:Float, combined_key:String, counts:Array[Int])

    def cleanGlobalTimeSeries(session: SparkSession, data_rdd: RDD[String]): DataFrame = {
        return session.createDataFrame(data_rdd.
            map(x => x.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")). // str.split(pattern, -1) is to treat lines terminating with , as the final column having empty string
            map(x => x.map(y => y.replaceAll("\"", "")).map(y => if (y.length == 0) null else y)).
            map(x => GlobalTimeSeriesRow(x(0), x(1), if (x(2) == null) 0F else x(2).toFloat, if (x(3) == null) 0F else x(3).toFloat, x.slice(4, x.length - 1).map(y => if (y == null) 0 else y.toInt))))
    }

    def cleanUSTimeSeries(session: SparkSession, data_rdd: RDD[String]): DataFrame = {
        return session.createDataFrame(data_rdd.
            map(x => x.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")). // str.split(pattern, -1) is to treat lines terminating with , as the final column having empty string
            map(x => x.map(y => y.replaceAll("\"", "")).map(y => if (y.length == 0) null else y)).
            map(x => USTimeSeriesRow(x(0).toInt, x(1), x(2), if (x(3) == null) 0 else x(3).toInt, if (x(4) == null) 0 else x(4).toFloat.toInt, x(5), x(6), x(7), if (x(8) == null) 0F else x(8).toFloat, if (x(9) == null) 0F else x(9).toFloat, x(10), x.slice(11, x.length - 1).map(y => if (y == null) 0 else y.toInt))))
    }

    def cleanUIDLookup(session: SparkSession, data_rdd: RDD[String]): DataFrame = {
        return session.createDataFrame(data_rdd.
            map(x => x.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)). // str.split(pattern, -1) is to treat lines terminating with , as the final column having empty string
            map(x => x.map(y => y.replaceAll("\"", "")).map(y => if (y.length == 0) null else y)).
            map(x => UIDLookupRow(x(0).toInt, x(1), x(2), if (x(3) == null) 0 else x(3).toInt, if (x(4) == null) 0 else x(4).toInt, x(5), x(6), x(7), if (x(8) == null) 0F else x(8).toFloat, if (x(9) == null) 0F else x(9).toFloat, x(10), if (x(11) == null) 0 else x(11).toInt)))
    }
}