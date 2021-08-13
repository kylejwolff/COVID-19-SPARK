package tools

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import java.time.LocalDate
import java.time.format.DateTimeFormatter

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
    def writeCSV(data_df: DataFrame, path: String, include_header: Boolean=false, single_file: Boolean=true): Unit =  {
        var new_df: DataFrame = null

        if (single_file) {
            new_df = data_df.coalesce(1)
        }
        else {
            new_df = data_df
        }

        if (include_header) {
            new_df.
            write.
            option("header", true).
            csv(path)
        }
        else {
            new_df.
            write.
            csv(path)
        }
    }
}

object Cleaner {

    case class UIDLookupRow(uid:Integer, iso2:String, iso3:String, code3:Integer, FIPS:Integer, Admin2:String, region:String, country:String, latitude:Float, longitude:Float, combined_key:String, population:Integer)
    case class GlobalTimeSeriesRow(region:String, country:String, latitude:Float, longitude:Float, counts:Array[Integer])
    case class USTimeSeriesRow(uid:Integer, iso2:String, iso3:String, code3:Integer, FIPS:Integer, Admin2:String, region:String, country:String, latitude:Float, longitude:Float, combined_key:String, counts:Array[Integer], population: Integer=null.asInstanceOf[Integer])

    private def castInteger(value: String): Integer = {
        return if (value == null) null.asInstanceOf[Integer] else Integer.parseInt(value)
    }

    def cleanGlobalTimeSeries(session: SparkSession, data_rdd: RDD[String]): DataFrame = {
        return session.createDataFrame(data_rdd.
            map(x => x.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)). // str.split(pattern, -1) is to treat lines terminating with "," as the final column having empty string
            map(x => x.map(y => y.replaceAll("\"", "")).map(y => if (y.length == 0) null else y)).
            map(x => GlobalTimeSeriesRow(x(0), x(1), if (x(2) == null) 0F else x(2).toFloat, if (x(3) == null) 0F else x(3).toFloat, x.slice(4, x.length).map(y => castInteger(y)))))
    }

    def cleanUSTimeSeries(session: SparkSession, data_rdd: RDD[String], with_population: Boolean=false): DataFrame = {
        val us_rdd = data_rdd.
            map(x => x.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)). // str.split(pattern, -1) is to treat lines terminating with "," as the final column having empty string
            map(x => x.map(y => y.replaceAll("\"", "")).map(y => if (y.length == 0) null else y))

        if (with_population) {
            return session.createDataFrame(us_rdd.
                map(x => USTimeSeriesRow(x(0).toInt, x(1), x(2), if (x(3) == null) 0 else x(3).toInt, if (x(4) == null) 0 else x(4).toFloat.toInt, x(5), x(6), x(7), if (x(8) == null) 0F else x(8).toFloat, if (x(9) == null) 0F else x(9).toFloat, x(10), x.slice(12, x.length).map(y => castInteger(y)), population=castInteger(x(11)))))
        }
        else {
            return session.createDataFrame(us_rdd.
                map(x => USTimeSeriesRow(x(0).toInt, x(1), x(2), if (x(3) == null) 0 else x(3).toInt, if (x(4) == null) 0 else x(4).toFloat.toInt, x(5), x(6), x(7), if (x(8) == null) 0F else x(8).toFloat, if (x(9) == null) 0F else x(9).toFloat, x(10), x.slice(11, x.length).map(y => castInteger(y)))))
        }
    }

    def cleanUIDLookup(session: SparkSession, data_rdd: RDD[String]): DataFrame = {
        return session.createDataFrame(data_rdd.
            map(x => x.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)). // str.split(pattern, -1) is to treat lines terminating with "," as the final column having empty string
            map(x => x.map(y => y.replaceAll("\"", "")).map(y => if (y.length == 0) null else y)).
            map(x => UIDLookupRow(x(0).toInt, x(1), x(2), castInteger(x(3)), castInteger(x(4)), x(5), x(6), x(7), if (x(8) == null) 0F else x(8).toFloat, if (x(9) == null) 0F else x(9).toFloat, x(10), if (x(11) == null) 0 else x(11).toInt)))
    }
}

object Query {
    def mergeGlobal(confirmed_df: DataFrame, deaths_df: DataFrame, recovered_df: DataFrame): DataFrame = {
        val confirmed_deaths_df = confirmed_df.
            join(deaths_df, (confirmed_df("region") <=> deaths_df("region")) && (confirmed_df("country") <=> deaths_df("country")), "outer").
            select(confirmed_df("*"), deaths_df("counts").as("deaths")).
            withColumnRenamed("counts", "confirmed")

        val merged_global_df = confirmed_deaths_df.
            join(recovered_df, (confirmed_deaths_df("region") <=> recovered_df("region")) && (confirmed_deaths_df("country") <=> recovered_df("country")), "outer").
            select(confirmed_deaths_df("*"), recovered_df("counts").as("recovered"))

        return merged_global_df
    }

    def mergeUS(confirmed_df: DataFrame, deaths_df: DataFrame): DataFrame = {
        val merged_us_df = confirmed_df.
            join(deaths_df, confirmed_df("uid") <=> deaths_df("uid"), "outer").
            select(confirmed_df("*"), deaths_df("counts").as("deaths"), deaths_df("population")).
            withColumnRenamed("counts", "confirmed")

        return merged_us_df
    }

    def getGlobalCountUnpacked(global_merged_df: DataFrame, which: String, start_date: LocalDate, format: String="MMM dd ''yy"): DataFrame = {

        def formatDateCol(start_date: LocalDate, plus_day: Int): String = {
            return start_date.plusDays(plus_day).format(DateTimeFormatter.ofPattern("MMM dd ''yy"))
        }

        val num_days = global_merged_df.first.getAs[scala.collection.mutable.WrappedArray[String]](which).length

        return global_merged_df.
            select((Seq(global_merged_df("country")) ++ Range(0, num_days).map(i => global_merged_df(which)(i).as(formatDateCol(start_date, i)))):_*).
            where(global_merged_df("region").isNull)
    }
}