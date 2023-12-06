####################
# modules and libs #
####################

import findspark
findspark.init()
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType,DoubleType,LongType
SparkSession.builder.config(conf=SparkConf())



######################
# init spark session #
######################



spark = SparkSession.builder \
    .appName("movies-ratings-app") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4") \
    .getOrCreate()


###############################################
# Read a stream from a Kafka topic with Spark #
###############################################

df = spark.readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "ratingmoviestopic") \
  .option("startingOffsets", "earliest") \
  .load()

value_df = df.selectExpr("CAST(value AS STRING)")

###################################################
# give the column names and type using StructType #
###################################################


schema = StructType([
    StructField("age", IntegerType(), True),
    StructField("function", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("genre", ArrayType(StringType()), True),
    StructField("movieId", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("number", StringType(), True),
    StructField("rating", DoubleType(), True),
    StructField("release_date", StringType(), True),
    StructField("timestamp", LongType(), True),
    StructField("url", StringType(), True),
    StructField("userId", IntegerType(), True)
])


###################################################
# give the column names and type using StructType #
###################################################
selected_df = value_df.withColumn("values", F.from_json(value_df["value"], schema)).selectExpr("values")



###########################################################
# Ensure that the process runs and outputs on the console #
###########################################################

query = selected_df.writeStream.outputMode("append").format("console").start()
query.awaitTermination()