package ceng790.hw1

import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.ByteType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.LongType
import java.net.URLDecoder
import org.apache.spark.sql.Row
import org.apache.spark.sql.Encoders
import org.apache.log4j.{Level, Logger}



object Part1 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    var spark: SparkSession = null
    try {
      
      spark = SparkSession.builder().appName("Flickr using dataframes").config("spark.master", "local[*]").getOrCreate()

      //   * Photo/video identifier
      //   * User NSID
      //   * User nickname
      //   * Date taken
      //   * Date uploaded
      //   * Capture device
      //   * Title
      //   * Description
      //   * User tags (comma-separated)
      //   * Machine tags (comma-separated)
      //   * Longitude
      //   * Latitude
      //   * Accuracy
      //   * Photo/video page URL
      //   * Photo/video download URL
      //   * License name
      //   * License URL
      //   * Photo/video server identifier
      //   * Photo/video farm identifier
      //   * Photo/video secret
      //   * Photo/video secret original
      //   * Photo/video extension original
      //   * Photos/video marker (0 = photo, 1 = video)

      val customSchemaFlickrMeta = StructType(Array(
        StructField("photo_id", LongType, true),
        StructField("user_id", StringType, true),
        StructField("user_nickname", StringType, true),
        StructField("date_taken", StringType, true),
        StructField("date_uploaded", StringType, true),
        StructField("device", StringType, true),
        StructField("title", StringType, true),
        StructField("description", StringType, true),
        StructField("user_tags", StringType, true),
        StructField("machine_tags", StringType, true),
        StructField("longitude", FloatType, false),
        StructField("latitude", FloatType, false),
        StructField("accuracy", StringType, true),
        StructField("url", StringType, true),
        StructField("download_url", StringType, true),
        StructField("license", StringType, true),
        StructField("license_url", StringType, true),
        StructField("server_id", StringType, true),
        StructField("farm_id", StringType, true),
        StructField("secret", StringType, true),
        StructField("secret_original", StringType, true),
        StructField("extension_original", StringType, true),
        StructField("marker", ByteType, true)))

      val originalFlickrMeta = spark.sqlContext.read
        .format("csv")
        .option("delimiter", "\t")
        .option("header", "false")
        .schema(customSchemaFlickrMeta)
        .load("flickrSample.txt")
        
        // YOUR CODE HERE
      originalFlickrMeta.show()
        
      // q1.
      originalFlickrMeta.createOrReplaceTempView("myDF")
      val desiredDF = spark.sql("SELECT photo_id, longitude, latitude, license FROM myDF")
      desiredDF.show()

      // q2.
      desiredDF.createOrReplaceTempView("desiredDF")
      val desiredDF2 = spark.sql("SELECT * FROM desiredDF WHERE license IS NOT NULL AND latitude != -1 AND longitude != -1")
      desiredDF2.show()
      //desiredDF2.select("license").show()
      
      // q3.
      desiredDF2.explain()
      
      // q4.
      desiredDF2.show()
      
      // q5.
      
      // reading the license property file. 
      val licenceProperties = spark.sqlContext.read
        .format("csv")
        .option("delimiter", "\t")
        .option("header", "true")
        .load("flickrLicense.txt")
      
      licenceProperties.createOrReplaceTempView("licenceProperties")
      desiredDF2.createOrReplaceTempView("desiredDF2") 
      // creating tables so that we can join them.
      val left = spark.sql("SELECT * FROM desiredDF2")
      val right = spark.sql("SELECT * FROM licenceProperties")
      // JOIN operation of left and right tables.
      val desiredDF3 = left.join(right, "license")
      desiredDF3.show()
      
      // alternative JOIN
      //val desiredDF3 = spark.sql("SELECT * FROM desiredDF2 LEFT OUTER JOIN licenceProperties ON Name=licence")

      desiredDF3.createOrReplaceTempView("desiredDF3")
      // selecting interesting and NonDerivative Licensed images.
      val desiredDF4 = spark.sql("SELECT * FROM desiredDF3 WHERE NonDerivative=1 ")
      desiredDF4.show()
      desiredDF4.explain()
      
      // q6.
      
      left.cache()
      right.cache()
      licenceProperties.createOrReplaceTempView("licenceProperties")     
      val desiredDF5 = left.join(right, "license")
      //val desiredDF3 = left.join(right, left.col("license") === right.col("Name")) 
      //val desiredDF3 = spark.sql("SELECT * FROM desiredDF2 LEFT OUTER JOIN licenceProperties ON Name=licence")
      desiredDF5.explain()
      
      

      // q7.
      desiredDF4.write.format("csv").save("/Users/emre/workspace/ceng790.hw1/part1Result.csv")

      

    } catch {
      case e: Exception => throw e
    } finally {
      spark.stop()
    }
    println("done")
  }
}