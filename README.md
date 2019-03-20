# flickr-metadata-processing
A simple Apache Spark application to process metadata of images taken from Flickr.  
<br>
CENG790 Big Data Analytics Course Assignment #1.  
Assignment details can be found [here](./documentation/Assignment1.pdf).  
<br>
The assignment consists of 2 parts:
- Processing data using Spark DataFrame API
- Processing data using RDDs

<br> 
Notice that, this is kind of a Spark tutorial on Scala and includes introductory data processing applications.


## Part 1: Processing data using the DataFrame API

In this part, Flickr metadata is processed through Spark DataFrames. Dataframe is a column-based data structure with a schema. They are quite popular as they support SQL queries.  
The main purpose in this part to result with images which include valid GPS information (not null) and have NonDerivative license type.  
Source code of this part can be accessed from [here](./src/ceng790/hw1/Part1.scala)

## Part 2: Processing Data using RDDs (Resilient Distributed Datasets):

RDD is a logical reference to a dataset partitioned across many different machines. Data is immutable stored in RDD. For more information on RDDs, check out [the paper](https://www2.eecs.berkeley.edu/Pubs/TechRpts/2011/EECS-2011-82.pdf).  
In this part, metadata of images is represented within a RDD of objects. For each image, an object of Picture[(see the Picture.scala)](./src/ceng790/hw1/Picture.scala) is created by using metadata and all Picture objects are kept by using RDD.  

Then, all images are grouped with repect to the location information(in which country the picture is taken). And finally, user tags of images and frequency of these tags in Flickr are kept for each country.  
Source code of this part can be accessed from [here](./src/ceng790/hw1/Part2.scala)


A more comprehensive technical report can be accessed from [here](./documentation/Assignment1.pdf).
