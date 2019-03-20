# flickr-metadata-processing
A simple Apache Spark application to process metadata of images taken from Flickr.  
Assignment of CENG790 Big Data Analytics Course.

The assignment consists of 2 parts:
- Processing data using Spark DataFrame API
- Processing data using RDDs

- Notice that, this is kind of a Spark tutorial on Scala and includes introductory data processing applications.


## Part 1: Processing data using the DataFrame API

In this part, Flickr metadata is processed through Spark DataFrames. Dataframe is a column-based data structure with a schema. They are quite popular as they support SQL queries.  
The main purpose in this part to result with images which include valid GPS information (not null) and have NonDerivative license type.


## Part 2: Processing Data using RDDs (Resilient Distributed Datasets):

RDD is a logical reference to a dataset partitioned across many different machines. Data is immutable stored in RDD. For more information on RDDs, check out [the paper](https://www2.eecs.berkeley.edu/Pubs/TechRpts/2011/EECS-2011-82.pdf).
