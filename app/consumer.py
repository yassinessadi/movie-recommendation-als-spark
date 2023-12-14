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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType,DoubleType,LongType
from elasticsearch import Elasticsearch



######################
# init spark session #
######################
spark = SparkSession.builder \
    .appName("movies-ratings-app") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4,"
            "org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0") \
    .getOrCreate()


###############################################
# Read a stream from a Kafka topic with Spark #
###############################################
df = spark.readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "moviesrecommendationallinone") \
  .option("startingOffsets", "earliest") \
  .load()
value_df = df.selectExpr("CAST(value AS STRING)")



#####################
# schema definition #
#####################
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


############################################
# JSON column transformation and selection #
############################################
selected_movies_df = value_df.withColumn("values", F.from_json(value_df["value"], schema)).selectExpr("values")


#**************************#
#   Transformations movie  #
#**************************#
# selected_movies_df = selected_movies_df.where("values.movieId != null")

movie_df = selected_movies_df.select(
    F.col("values.movieId").alias("movieId"),
    F.col("values.name").alias("title"),
    F.col("values.genre").alias("genre"),
    F.col("values.release_date").alias("release_date"),
    F.col("values.url").alias("url"),
)
movie_df = movie_df.where("release_date != ''")
movie_df = movie_df.where("url != ''")


#**************************#
#   Transformations user   #
#**************************#
user_df = selected_movies_df.select(
    F.col("values.age").alias("age"),
    F.col("values.function").alias("function"),
    F.col("values.gender").alias("gender"),
    F.col("values.userId").alias("userId"),
    F.col("values.number").alias("zipCode")
)



#**************************#
# Transformations ratings  #
#**************************#
rating_df = selected_movies_df.select(
    F.col("values.rating").alias("rating"),
    F.from_unixtime(F.col("values.timestamp") / 1000, "yyyy-MM-dd HH:mm:ss").alias("rating_date"),
    F.col("values.movieId").alias("movieId"),
    F.col("values.userId").alias("userId"),
    F.concat_ws("",
            F.col("values.movieId"),
            F.col("values.userId")).alias("id"),
)


#*****************************#
# Transformations all in One  #
#*****************************#
allInOne_df = selected_movies_df.select(
    F.col("values.rating").alias("rating"),
    F.from_unixtime(F.col("values.timestamp") / 1000, "yyyy-MM-dd HH:mm:ss").alias("rating_date"),
    F.col("values.movieId").alias("movieId"),
    F.col("values.userId").alias("userId"),
    F.col("values.age").alias("age"),
    F.col("values.function").alias("function"),
    F.col("values.gender").alias("gender"),
    F.col("values.number").alias("zipCode"),
    F.col("values.name").alias("title"),
    F.col("values.genre").alias("genre"),
    F.col("values.release_date").alias("release_date"),
    F.col("values.url").alias("url"),
)

##################################
# Define mapping for movie index #
##################################
movie_mapping = {
    "mappings": {
        "properties": {
            "title": {"type": "text"},
            "genre": {"type": "keyword"},
            "release_date": {"type": "date", "format": "dd-MMM-yyyy"},
            "url": {"type": "text"},
            "movieId": {"type": "integer"},
        }
    }
}

##################################
# Define mapping for rating index #
##################################
rating_mapping = {
    "mappings": {
        "properties": {
            "rating": {"type": "float"},
            "rating_date": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"},
            "movieId": {"type": "integer"},
            "userId": {"type": "integer"},
            "id": {"type": "integer"},
        }
    }
}

#################################
# Define mapping for user index #
#################################
user_mapping = {
    "mappings": {
        "properties": {
            "age": {"type": "integer"},
            "function": {"type": "text"},
            "gender": {"type": "text"},
            "userId": {"type": "integer"},
            "zipCode": {"type": "text"},
        }
    }
}


#######################################
# Define mapping for all index in one #
#######################################

allMovie_mapping = {
    "mappings": {
        "properties": {
            "userId": {"type": "integer"},
            "title": {"type": "text"},
            "genre": {"type": "keyword"},
            "release_date": {"type": "date", "format": "dd-MMM-yyyy"},
            "url": {"type": "text"},
            "movieId": {"type": "integer"},
            "age": {"type": "integer"},
            "function": {"type": "text"},
            "gender": {"type": "text"},
            "zipCode": {"type": "text"},
            "rating": {"type": "float"},
            "rating_date": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"},
        }
    }
}


#+---+--------+------+------+-------------------+------++-------------------+------+#
#                 connect to elasticsearch and insert into the target index         #
#+---+--------+------+------+-------------------+------++-------------------+------+#
def insert_data(index_name, df, checkpointlocation,_id):
    """
    `index_name` : Elastic search index \n
    `df` : Dataframe that you want to insert into elastic search \n
    `checkpointlocation` : To truncate the logical plan of this DataFrame \n
    `_id` : specefiy the documment id in elasticsearch
    """
    query = df.writeStream \
        .format("org.elasticsearch.spark.sql") \
        .outputMode("append") \
        .option("es.resource", f"{index_name}") \
        .option("es.nodes", "localhost") \
        .option("es.port", "9200") \
        .option("es.mapping.id", _id) \
        .option("es.nodes.wan.only", "false") \
        .option("checkpointLocation", checkpointlocation) \
        .option("es.write.operation", "index") \
        .start()
    return query


#+---+--------+------+------+-------------------+------++-------------------+------+------+#
#                 connect to elasticsearch and insert into the target all in one index     #
#+---+--------+------+------+-------------------+------++-------------------+------+------+#
def insert_data_in_single_index(index_name, df, checkpointlocation):
    """
    `index_name` : Elastic search index \n
    `df` : Dataframe that you want to insert into elastic search \n
    `checkpointlocation` : To truncate the logical plan of this DataFrame \n
    """
    query = df.writeStream \
        .format("org.elasticsearch.spark.sql") \
        .outputMode("append") \
        .option("es.resource", f"{index_name}") \
        .option("es.nodes", "localhost") \
        .option("es.port", "9200") \
        .option("es.nodes.wan.only", "false") \
        .option("checkpointLocation", checkpointlocation) \
        .option("es.write.operation", "index") \
        .start()
    return query


#+---+--------+------+------+-------------------+------+#
#                 insert into movie index               #
#+---+--------+------+------+-------------------+------+#
es = Elasticsearch([{'host': 'localhost', 'port':9200, 'scheme':'http'}])
es.options(ignore_status=400).indices.create(index="movies_moviesindex",mappings=movie_mapping)
query = insert_data("movies_moviesindex", movie_df, "./checkpointLocation/movies/","movieId")


#+---+--------+------+------+-------------------+------+#
#                insert into ratings index              #
#+---+--------+------+------+-------------------+------+#
es.options(ignore_status=400).indices.create(index="movies_ratingsindex",mappings=rating_mapping)
query =  insert_data("movies_ratingsindex", rating_df, "./checkpointLocation/ratings/","id")


#+---+--------+------+------+-------------------+------+#
#                insert into users index                #
#+---+--------+------+------+-------------------+------+#
es.options(ignore_status=400).indices.create(index="movies_usersindex",mappings=user_mapping)
query = insert_data("movies_usersindex",user_df,"./checkpointLocation/users/","userId")

#+---+--------+------+------+-------------------+------+#
#                insert into all in single index        #
#+---+--------+------+------+-------------------+------+#

es.options(ignore_status=400).indices.create(index="movies_singleindex",mappings=allMovie_mapping)
query = insert_data_in_single_index("movies_singleindex", allInOne_df, "./checkpointLocation/all_MoviesineOne/")


###########################################################
# Ensure that the process runs and outputs on the console #
###########################################################
query = movie_df.writeStream.outputMode("append").format("console").start()
query = user_df.writeStream.outputMode("append").format("console").start()
query = rating_df.writeStream.outputMode("append").format("console").start()
query = rating_df.writeStream.outputMode("append").format("console").start()
query.awaitTermination()