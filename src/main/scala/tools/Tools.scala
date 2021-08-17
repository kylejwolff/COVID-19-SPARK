package tools

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{lag, udf}
import org.apache.spark.sql.expressions.Window
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
    case class GlobalTimeSeriesRow_Vertical(id: Integer, region:String, country:String, latitude:Float, longitude:Float, date:LocalDate, counts: Integer)
    case class USTimeSeriesRow(uid:Integer, iso2:String, iso3:String, code3:Integer, FIPS:Integer, Admin2:String, region:String, country:String, latitude:Float, longitude:Float, combined_key:String, counts:Array[Integer], population: Integer=null.asInstanceOf[Integer])
    case class USTimeSeriesRow_Vertical(uid:Integer, iso2:String, iso3:String, code3:Integer, FIPS:Integer, Admin2:String, region:String, country:String, latitude:Float, longitude:Float, combined_key:String, date:LocalDate, counts:Integer, population: Integer=null.asInstanceOf[Integer])


    private def castInteger(value: String): Integer = {
        return if (value == null) null.asInstanceOf[Integer] else Integer.parseInt(value)
    }

    private def cleanTimeSeries(data_rdd: RDD[String]): RDD[Array[String]] = {
        return data_rdd.
            map(x => x.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)). // str.split(pattern, -1) is to treat lines terminating with "," as the final column having empty string
            map(x => x.map(y => y.replaceAll("\"", "")).map(y => if (y.length == 0) null else y))
    }

    def cleanGlobalTimeSeries(session: SparkSession, data_rdd: RDD[String]): DataFrame = {
        val timeseries_rdd = cleanTimeSeries(data_rdd)

        return session.createDataFrame(timeseries_rdd.
            map(x => GlobalTimeSeriesRow(x(0), x(1), if (x(2) == null) 0F else x(2).toFloat, if (x(3) == null) 0F else x(3).toFloat, x.slice(4, x.length).map(y => castInteger(y)))))
    }

    def cleanGlobalTimeSeries_Vertical(session: SparkSession, data_rdd: RDD[String], start_date: LocalDate, format: String="MM/dd/yyyy"): DataFrame = {
        val timeseries_rdd = cleanTimeSeries(data_rdd)

        return session.createDataFrame(timeseries_rdd.
            zipWithIndex().
            flatMap{case (value, key) => value.slice(4, value.length).zipWithIndex.map{case (x, i) => Seq(s"${key}", value(0), value(1), value(2), value(3), s"${i}", x)}}.
            map(x => GlobalTimeSeriesRow_Vertical(x(0).toInt, x(1), x(2), if (x(3) == null) 0F else x(3).toFloat, if (x(4) == null) 0F else x(4).toFloat, start_date.plusDays(x(5).toInt), castInteger(x(6)))))
    }

    def cleanUSTimeSeries(session: SparkSession, data_rdd: RDD[String], with_population: Boolean=false): DataFrame = {
        val timeseries_rdd = cleanTimeSeries(data_rdd)

        if (with_population) {
            return session.createDataFrame(timeseries_rdd.
                map(x => USTimeSeriesRow(x(0).toInt, x(1), x(2), if (x(3) == null) 0 else x(3).toInt, if (x(4) == null) 0 else x(4).toFloat.toInt, x(5), x(6), x(7), if (x(8) == null) 0F else x(8).toFloat, if (x(9) == null) 0F else x(9).toFloat, x(10), x.slice(12, x.length).map(y => castInteger(y)), population=castInteger(x(11)))))
        }
        else {
            return session.createDataFrame(timeseries_rdd.
                map(x => USTimeSeriesRow(x(0).toInt, x(1), x(2), if (x(3) == null) 0 else x(3).toInt, if (x(4) == null) 0 else x(4).toFloat.toInt, x(5), x(6), x(7), if (x(8) == null) 0F else x(8).toFloat, if (x(9) == null) 0F else x(9).toFloat, x(10), x.slice(11, x.length).map(y => castInteger(y)))))
        }
    }

    def cleanUSTimeSeries_Vertical(session: SparkSession, data_rdd: RDD[String], start_date: LocalDate, format: String="MM/dd/yyyy", with_population: Boolean=false): DataFrame = {
        val timeseries_rdd = cleanTimeSeries(data_rdd)

        if (with_population) {
            return session.createDataFrame(timeseries_rdd.
                flatMap(row => row.slice(12, row.length).zipWithIndex.map{case (x, i) => row.slice(0, 11) ++ Seq(s"${i}", x, row(12))}).
                map(x => USTimeSeriesRow_Vertical(x(0).toInt, x(1), x(2), if (x(3) == null) 0 else x(3).toInt, if (x(4) == null) 0 else x(4).toFloat.toInt, x(5), x(6), x(7), if (x(8) == null) 0F else x(8).toFloat, if (x(9) == null) 0F else x(9).toFloat, x(10), start_date.plusDays(x(11).toInt), x(12).toInt, population=castInteger(x(13)))))
        }
        else {
            return session.createDataFrame(timeseries_rdd.
                flatMap(row => row.slice(12, row.length).zipWithIndex.map{case (x, i) => row.slice(0, 11) ++ Seq(s"${i}", x)}).
                map(x => USTimeSeriesRow_Vertical(x(0).toInt, x(1), x(2), if (x(3) == null) 0 else x(3).toInt, if (x(4) == null) 0 else x(4).toFloat.toInt, x(5), x(6), x(7), if (x(8) == null) 0F else x(8).toFloat, if (x(9) == null) 0F else x(9).toFloat, x(10), start_date.plusDays(x(11).toInt), x(12).toInt)))
        }
    }

    def cleanUIDLookup(session: SparkSession, data_rdd: RDD[String]): DataFrame = {
        val timeseries_rdd = cleanTimeSeries(data_rdd)

        return session.createDataFrame(timeseries_rdd.
            map(x => UIDLookupRow(x(0).toInt, x(1), x(2), castInteger(x(3)), castInteger(x(4)), x(5), x(6), x(7), if (x(8) == null) 0F else x(8).toFloat, if (x(9) == null) 0F else x(9).toFloat, x(10), if (x(11) == null) 0 else x(11).toInt)))
    }
}

object Query {

    private def formatDate(start_date: LocalDate, plus_day: Int, format: String="MM/dd/yyyy"): String = {
        return start_date.plusDays(plus_day).format(DateTimeFormatter.ofPattern(format))
    }

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

        val num_days = global_merged_df.first.getAs[scala.collection.mutable.WrappedArray[String]](which).length

        return global_merged_df.
            select((Seq(global_merged_df("country"), global_merged_df("latitude"), global_merged_df("longitude")) ++ Range(0, num_days).map(i => global_merged_df(which)(i).as(formatDate(start_date, i, format)))):_*).
            where(global_merged_df("region").isNull)
    }

    def mergeGlobalVertical(confirmed_vertical_df: DataFrame, deaths_vertical_df: DataFrame, recovered_vertical_df: DataFrame): DataFrame = {
        val confirmed_deaths_vertical_df = confirmed_vertical_df.
            join(deaths_vertical_df, (confirmed_vertical_df("id") <=> deaths_vertical_df("id")) && (confirmed_vertical_df("date") <=> deaths_vertical_df("date")), "outer").
            select(confirmed_vertical_df("*"), deaths_vertical_df("counts").as("deaths")).
            withColumnRenamed("counts", "confirmed")

        val merged_global_vertical_df = confirmed_deaths_vertical_df.
            join(recovered_vertical_df, (confirmed_deaths_vertical_df("id") <=> recovered_vertical_df("id")) && (confirmed_deaths_vertical_df("date") <=> recovered_vertical_df("date")), "outer").
            select(confirmed_deaths_vertical_df("*"), recovered_vertical_df("counts").as("recovered"))

        return merged_global_vertical_df
    }

    def mergeUSVertical(confirmed_vertical_df: DataFrame, deaths_vertical_df: DataFrame): DataFrame = {
        val merged_us_vertical_df = confirmed_vertical_df.
            join(deaths_vertical_df, (confirmed_vertical_df("uid") <=> deaths_vertical_df("uid") && confirmed_vertical_df("date") <=> deaths_vertical_df("date")), "outer").
            select(confirmed_vertical_df("*"), deaths_vertical_df("counts").as("deaths"), deaths_vertical_df("population")).
            withColumnRenamed("counts", "confirmed")

        return merged_us_vertical_df
    }

    def getGlobalCountVertical(global_vertical_df: DataFrame): DataFrame = {
        return global_vertical_df.
            select("*").
            where(global_vertical_df("region").isNull).
            drop("region")
    }

    def getUSCountVertical(us_vertical_df: DataFrame): DataFrame = {
        return us_vertical_df.
            select("*").
            drop("country")
    }

    def getGrowth(df: DataFrame, which: String, partition_col: String=null, vertical: Boolean=false): DataFrame = {
        if (vertical) {
            val df_with_lagged = df.
                withColumn(s"${which}_lagged", lag(which, 1, null.asInstanceOf[Integer]).over(Window.partitionBy(partition_col).orderBy("date")))
                
            return df_with_lagged.
                withColumn(s"${which}_growth", df(which).minus(df_with_lagged(s"${which}_lagged"))).
                drop(s"${which}_lagged")
        }

        def calculateGrowth(counts: Array[Integer]): Array[Integer] = {
            return Array[Integer](null.asInstanceOf[Integer]) ++ counts.slice(1, counts.length).
                zip(counts.slice(0, counts.length - 1)).
                map{case (a, b) => (a - b).asInstanceOf[Integer]}
        }

        val calculateGrowthUDF = udf(calculateGrowth _)

        return df.
            withColumn(s"${which}_growth", calculateGrowthUDF(df(which)))
    }
}