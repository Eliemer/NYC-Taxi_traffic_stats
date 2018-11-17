package demo

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.SaveMode
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

    // AWS s3n bucket access code
    val access = "***" // AWS Access Key ID
    val secret = "***"        // AWS secret key
    val bucket = "nyc-tlc"    // AWS s3n Bucket name
    sc.hadoopConfiguration.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", access)
    sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", secret)


    // Access and save data set from AWS bucket to val df
    val df = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/main/resources/big_yellow.csv")

    println("Number of rows: " + df.count())

    df.limit(100)
      .repartition(1)
      .write.format("com.databricks.spark.csv")
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .save("src/main/resources/small_yellow_cabs.csv")



    // Statistics to be examined:

    //groupByHour(df = df)




  }

  def groupByHour(df: DataFrame) = {
    //df.col()
  }


  def printResults(dataSet: Any) = {

  }

}
