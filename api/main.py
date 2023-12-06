from flask import Flask,request,jsonify
import json
import pandas as pd



app = Flask(__name__)


# get user by userId

@app.route('/api/movie', methods=['GET'])

def index():
    df = pd.read_json(path_or_buf='../data/movies.json',orient='records')
    shape = df.shape[0]
    _ID = int(request.args.get('ID', 1))
    print(shape)
    if shape <= _ID:
        return jsonify("user not found")
    df = df.iloc[_ID]

    responce = df.to_json()

    return json.loads(responce)





# # get movie by movieId

# @app.route('/api/movie/<int:movieId>',methods=['GET'])
# def get_movie(movieId):
#     df = pd.read_json(path_or_buf='../data/movies.json',orient='records')
#     df = df[df['movieId'] == movieId]
#     responce = df.to_json(orient='records')

#     return json.loads(responce)

# # get ratings 

# @app.route('/api/ratings',methods=['GET'])
# def get_all():
#     df = pd.read_json(path_or_buf='../data/ratings.json')

#     #Query to get the specefic movie & user
#     userId = int(request.args.get('userId', 1))
#     # movieId = int(request.args.get('movieId', 1))

#     # get the user rated the movie
#     # df = df[(df['userId'] == userId)&(df['movieId'] == movieId)]
#     df = df[df['userId'] == userId]
#     # df = df[df['movieId'] == movieId]
    
#     # return the responce
#     responce = df.to_json(orient='records')
#     return json.loads(responce)

if __name__ == '__main__':
    app.run(debug=True)