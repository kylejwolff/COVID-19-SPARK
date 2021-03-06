package tools

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{lag, udf, col, trim, when}
import org.apache.spark.sql.expressions.Window
import java.sql.Date
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.util.matching.Regex

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
    case class Covid19DataRow(id:Integer, observed_date:LocalDate, region:String, country:String, last_updated:LocalDate, confirmed:Integer, deaths:Integer, recovered:Integer)

    private val date_pattern_formatters = List[(String, DateTimeFormatter)](
        ("^(\\d{1,2})/(\\d{1,2})/(\\d{4}) (\\d{1,2}):(\\d{2})$", DateTimeFormatter.ofPattern("M/d/yyyy H:mm")),
        ("^(\\d{1,2})/(\\d{1,2})/(\\d{2}) (\\d{2}):(\\d{2})$", DateTimeFormatter.ofPattern("M/d/yy HH:mm")),
        ("^(\\d{1,2})/(\\d{1,2})/(\\d{2}) (\\d{1,2}):(\\d{2})$", DateTimeFormatter.ofPattern("M/d/yy h:mm")),
        ("^(\\d{2,4})-(\\d{1,2})-(\\d{1,2})T(\\d{1,2}):(\\d{1,2}):(\\d{1,2})$", DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")),
        ("^(\\d{2,4})-(\\d{1,2})-(\\d{1,2}) (\\d{1,2}):(\\d{1,2}):(\\d{1,2})$", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    )

    private val observed_date_formatter = DateTimeFormatter.ofPattern("MM/dd/yyyy")

    private def castInteger(value: String): Integer = {
        return if (value == null) null.asInstanceOf[Integer] else Integer.parseInt(value)
    }

    private def formatDate(line: String): LocalDate = {
        val formatter = date_pattern_formatters.find{case (pattern, formatter) => line.matches(pattern)}.getOrElse(null)

        if (formatter == null) null.asInstanceOf[LocalDate]

        else LocalDate.parse(line, formatter._2)
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

    def cleanCovid19Data(session: SparkSession, data_rdd: RDD[String]): DataFrame = {
        val covid_rdd = cleanTimeSeries(data_rdd)

        val covid_df = session.createDataFrame(covid_rdd.
            map(x => Covid19DataRow(castInteger(x(0)), LocalDate.parse(x(1), observed_date_formatter), x(2), x(3), formatDate(x(4)), castInteger(x(5).slice(0, x(5).length - 2)), castInteger(x(6).slice(0, x(6).length - 2)), castInteger(x(7).slice(0, x(7).length - 2)))))

        //Fix issues in province/state column
        val dfP = covid_df.withColumn("region", when(col("region").rlike("Diamond Princess"),
            "Diamond Princess").when(col("region").rlike("Grand Princess"),
            "Grand Princess").when(col("region")===col("region")||col("region")==="None"||col("region").rlike("Unknown"), null)
            .otherwise(trim(col("region"))))

        //Fix issues in Country/Region column
        val dfC = dfP.withColumn(
        "country",
        when(
            col("country") === "('St. Martin',)",
            "St. Martin").when(col("country").rlike("Bahamas"),
            "Bahamas").when(col("country").rlike("Gambia"),
            "Gambia").otherwise(trim(col("country")))
        )

        return dfC
    }
}