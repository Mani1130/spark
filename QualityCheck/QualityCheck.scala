package QualityCheck

import org.apache.spark.sql.{DataFrame, SparkSession,SQLContext}
import org.apache.log4j._
import org.apache.spark.sql.types.{StringType, StructType, TimestampType}
import org.apache.spark.sql.functions._
import com.crealytics.spark.excel._

object QualityCheck {

  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Spark session config
    val spark = SparkSession.builder().appName("Data Quality check").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")


    // val DataSchema = new StructType()
    //   .add("UserName", StringType)
    //  .add("Date", StringType)

    val data = spark.read
      .format("com.crealytics.spark.excel")

      .option("addColorColumns","false")
      .option("useHeader", "false")
      .option("treatEmptyValuesAsNulls", "false")
      .option("inferSchema", "false")
      .load("F:\\hcpcs2020_anweb\\HCPC2020_ANWEB_w_disclaimer.xls")


    data.createOrReplaceTempView("whole_data")

    val resultsDF = spark.sql("select * from whole_data where size(split(_c0,' ')) >=1 and size(split(_c0,' ')) <=5")
    //"where size(split(_c0,' ')) = 1")

    print("count",resultsDF.count())
    resultsDF.show(10)
  }

}
