import findspark
findspark.init()

from flask import Flask,request,render_template,jsonify
from elasticsearch import Elasticsearch as es
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALSModel 
from math import ceil
import pandas as pd


app = Flask(__name__)

#-----+---------+------+----------#
# spark init and load model here  #  
#-----+---------+------+----------#

spark = SparkSession.builder \
        .appName("recommend-movies") \
        .master("local") \
        .getOrCreate()

# # # Load your ALS model
model_path = "../model/als-model"  # Path to your ALS model
als_model = ALSModel.load(model_path)

# ------------+------------ #
# connect to elasticsearch  #
# ------------+------------ #

client = es("http://localhost:9200")  # Elasticsearch endpoint



def fetch_data(index,keyword , value):
    """
    #### Retrieve the data from Elasticsearch by providing the parameters below :

     `index` : name of the index in the elasticsearch \n
     `keyword` : The term you want to retrieve the data based on \n
     `value` : The value based on which you want to retrieve the data \n
    """
    response = client.search(index=index, body={'query': {'term': {keyword: value}}})
    hits = response["hits"]["hits"]
    return [hit["_source"] for hit in hits] if hits else []



# ------------+------------ #
# integration of als model  #
# ------------+------------ #
def getUsers(movieId,limit):
    """
    #### get users from the dataset and return a dataframe contains userId (ids)
     `movieId` : the movieId for specific movie selected by or filterd by users \n
     `limit` : The number of users who rated that movie. 
    """
    response = fetch_data("movies_singleindex","movieId",movieId)
    df = pd.DataFrame(response)
    df = df['userId'].tolist()
    df = spark.createDataFrame(df)
    return df.select("userId").distinct().limit(limit)




# get movie
@app.route("/movie/",methods=["GET"])
def getMovie():
    title = str(request.args.get("title"))
    hits = fetch_data("movies_moviesindex","title",title)
    if len(hits) == 0:
        return jsonify("Movie Not Found")
    movieId = hits[0]['movieId']
    users = getUsers(movieId,3)
    userSubsetRecs = als_model.recommendForUserSubset(users, 2)
    return jsonify(userSubsetRecs.collect())




# ------------+------------ ------------+------------ #
#      number of the movies in elasticsearch          #
# ------------+------------ ------------+------------ #
def movies_count():
    query = {
        "query": {
             "match_all": {}
        }
    }
    result = client.search(index='movies_moviesindex', body=query)
    total_movies = result['hits']['total']['value']
    return total_movies



def fetch_all_movies(index , page, size):

    """
    #### Retrieve the data from Elasticsearch by providing the parameters below :

     `index` : name of the index in the elasticsearch \n
     `page` : the page number, where each number corresponds to a size number number of page by default is 1. \n
     `size` : the number of movies you want to return on one page by default 10 movies for each page.
    """

    # ---------------+------------- #
    # query with simple pagination  #
    # ---------------+------------- #
    es_query = {
        "query": {
            "match_all": {}
        },
        "from": (page - 1) * size,
        "size": size
    }
    response = client.search(index=index, body=es_query)
    hits = response["hits"]["hits"]

    return [hit["_source"] for hit in hits] if hits else []


@app.route('/recommend', methods=['GET'])
def recommend():
    _id = int(request.args.get('userId', 1))
    # Fetch user data from Elasticsearch
    user_data = fetch_data('movies_usersindex',"userId",_id)

    if not user_data:
        return jsonify({"error": "User not found"}), 404

    return jsonify(user_data)

# ------------+--------------- #
# show all movies in home page #
# ------------+--------------- #
@app.route('/',methods=["GET"])
def index():
    #--------+-------#
    # params +-------#
    #--------+-------#
    page = int(request.args.get("page",1))
    size = int(request.args.get("size",30)) 

    #--------+---------------+---------------+---------------+-----------------#
    # Retrieve movies based on the parameters provided by users in the request #
    #--------+---------------+---------------+---------------+-----------------#
    movies = fetch_all_movies('movies_moviesindex',page=page,size=size)
    #--------+-------#
    # number of page #
    #--------+-------#
    total_movies = movies_count()
    total_pages = ceil(total_movies / size)

    # return jsonify(movies)
    return render_template("index.html" , movies=movies, page=page, size=size,total_pages=total_pages)