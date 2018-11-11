package demo

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object hello {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val conf = new SparkConf().setMaster("local[2]").setAppName("Spark on NYC TLC")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder
      .config(conf)
      .getOrCreate()

//    var access = "***"
//    var secret = "***"
//    var bucket = "nyc-tlc"
//    sc.hadoopConfiguration.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
//    sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", access)
//    sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", secret)
//
//    var df = sc.textFile("s3n://nyc-tlc/yellow.csv")

    val df = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/main/resources/yellow.csv")

    println("Number of rows: " + df.count())

    val df_limited = df.limit(5)
    df_limited.show()
    println("Number of rows limited to: " + df_limited.count())
  }
}
