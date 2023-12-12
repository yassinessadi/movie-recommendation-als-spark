import findspark
findspark.init()

from flask import Flask,request,render_template,jsonify
from elasticsearch import Elasticsearch as es
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALSModel 


app = Flask(__name__)

SparkSession.builder \
        .appName("recommend-movies") \
        .master("local") \
        .getOrCreate()

# Load your ALS model
model_path = "../model/als-model"  # Path to your ALS model
als_model = ALSModel.load(model_path)

# ------------+------------ #
# connect to elasticsearch  #
# ------------+------------ #
client = es(
    "http://localhost:9200",  # Elasticsearch endpoint
    )

def fetch_data_from_elasticsearch(index, query):
    result = es.search(index=index, body=query)
    return result


@app.route('/recommend', methods=['GET'])
def recommend():
    # data = request.get_json()
    # user_id = str(request.args.get('user_id'))

    # Fetch user data from Elasticsearch
    # user_data = fetch_data_from_elasticsearch('movies_usersindex', {'query': {'match': {'user_id': user_id}}})

    # Use the ALS model to generate recommendations based on user data
    # recommendations = als_model.transform(user_data)

    # Process and return recommendations
    return jsonify("")




























# def get_all_movies():
#     query = {
#         "query": {
#              "match_all": {}
#         }
#     }
#     result = client.search(index='movies_usersindex', body=query)
#     total_movies = result['hits']
#     return total_movies

# @app.route("/movie/" , methods=["GET"])
# def getAll():
#     title = request.args.get("title",None)
#     responce = get_all_movies()
#     return jsonify(responce)