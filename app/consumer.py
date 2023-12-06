####################
# modules and libs #
####################
import findspark
findspark.init()
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql import functions as F
SparkSession.builder.config(conf=SparkConf())


######################
# init spark session #
######################



spark = SparkSession.builder \
    .appName("movies-ratings-app") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4") \
    .getOrCreate()


