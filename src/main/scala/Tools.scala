package tools

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

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

object Query {
    def mergeGlobal(confirmed_df: DataFrame, deaths_df: DataFrame, recovered_df: DataFrame): DataFrame = {
        return confirmed_df.
            join(deaths_df, ($"confirmed_df.region" <=> $"deaths_df.region") && ($"confirmed_df.country" <=> $"deaths_df.country"), "outer").
            alias("confirmed_deaths_df").
            join(recovered_df, ($"confirmed_deaths_df.region" <=> $"recovered_df.region") && ($"confirmed_deaths_df.country" <=> $"recovered_df.country"), "outer").
            alias("merged_global_df")
            //select($"region", $"country", confirmed_df("longitude"), confirmed_df("counts").as("confirmed"), deaths_df("counts").as("deaths"), recovered_df("counts").as("recovered"))
    }

    def mergeUS(confirmed_df: DataFrame, deaths_df: DataFrame): DataFrame = {
        return confirmed_df.
            join(deaths_df, (confirmed_df("region") === deaths_df("region")) && (confirmed_df("country") === deaths_df("country")), "inner")
    }
}