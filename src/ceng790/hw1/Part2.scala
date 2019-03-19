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
import org.apache.spark.rdd.RDD
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext




object Part2 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    var spark: SparkSession = null
    try {
      spark = SparkSession.builder().appName("Flickr using dataframes").config("spark.master", "local[*]").getOrCreate()
      val originalFlickrMeta: RDD[String] = spark.sparkContext.textFile("flickrSampleManipulated.txt")

      // question 1:

      println("first five elements of the RDD:")
      //originalFlickrMeta.take(5).foreach(f => println(f))
      println("\n\nthe number of elements in this RDD is  "+originalFlickrMeta.count()+"\n\n\n")


      // question 2:
      // created an array so that I can directly feed data to the Picture objects.
      val rddAsArray = originalFlickrMeta.map(x => x.split("\t"))

      // creation of RDD of Picture objects from rddAsArray.
      val pictureRDD: RDD[Picture] = rddAsArray.map(x => new Picture(x))

      // filtering data to achieve interesting pictures with non-null usertags and country attributes.
      val interestingPictureRDD: RDD[Picture] = pictureRDD.filter(f => f.c != null && f.userTags != "null" && f.userTags.size > 0  && f.lat != -1 && f.lon != -1 )
      interestingPictureRDD.take(5).foreach(f => println(f))

      
      println("")
      
      
      // question 3:
      
      // grouping interestingPictureRDD by their country attribute.
      val groupedByCountryRDD = interestingPictureRDD.groupBy(_.c)
      groupedByCountryRDD.foreach(println)
      
      println("\ntype is equal to : val groupedByCountryRDD: RDD[(Country, Iterable[Picture])]\n")

      
      // question 4:
      
       // transforming RDD[(Country, Iterable[Picture])] to RDD[(Country, List[List[String]])]
       val com_ = groupedByCountryRDD.map(x => (x._1, x._2.toList.map(f => f.userTags.toList)))

       // transforming RDD[(Country, List[List[String]])] to RDD[(Country, List[String])].
       val com_2 = com_.map(f =>(f._1, f._2.flatMap(f => f)))
       // this is the concatenated version.
       com_2.foreach(println)
       println("\nflag\n")
       
       
       // question 5:
       
       // to have each tag with its frequency in the form of RDD[(Country, Map[String, Int])].
       
       val freqAddedVersion = com_2.map(f =>(f._1, (f._2.groupBy(identity).mapValues(_.size))))
       freqAddedVersion.foreach(println)


    } catch {
      case e: Exception => throw e
    } finally {
      spark.stop()
    }
    println("done")
  }
}