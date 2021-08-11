package tools

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

object Loader {
    def loadCSV(session: SparkSession, hdfs_path: String): RDD[String] = {
        return session.sparkContext.textFile(hdfs_path)
    }
}

object Writer {
    def writeCSV(data_df: DataFrame, hdfs_path: String, include_header: Boolean=false): Unit =  {
        if (include_header) {
            data_df.
            write.
            option("header", true).
            csv(hdfs_path)
        }
        else {
            data_df.
            write.
            csv(hdfs_path)
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