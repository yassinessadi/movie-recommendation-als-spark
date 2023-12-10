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
    .appName("train-model") \
    .getOrCreate()


movie_ratings = spark.read.json('../data/movies.json')
movie_ratings.show(10)