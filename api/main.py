from flask import Flask,request,jsonify
import json
from flask_caching import Cache
import pandas as pd



app = Flask(__name__)

cache = Cache(app, config={'CACHE_TYPE': 'simple'})

df = pd.read_json(path_or_buf='../data/movies.json', orient='records')

@cache.cached(timeout=60)
@app.route('/api/movie', methods=['GET'])
def index():
    shape = df.shape[0]
    _id = int(request.args.get('id', 1))

    print(shape)

    if shape <= _id:
        return jsonify("user not found")

    df_row = df.iloc[_id]
    response = df_row.to_json()
    return json.loads(response)



if __name__ == '__main__':
    app.run(debug=True)



#---------------------#
# old code vs caching #
#---------------------# 

# get user by userId
# @app.route('/api/movie', methods=['GET'])
# def index():
#     df = pd.read_json(path_or_buf='../data/movies.json',orient='records')
#     shape = df.shape[0]
#     _id = int(request.args.get('id', 1))
#     print(shape)
#     if shape <= _id:
#         return jsonify("user not found")
#     df = df.iloc[_id]
#     responce = df.to_json()
#     return json.loads(responce)