from flask import Flask,request,jsonify
import json
import pandas as pd



app = Flask(__name__)


# get user by userId

@app.route('/api/movie', methods=['GET'])

def index():
    df = pd.read_json(path_or_buf='../data/movies.json',orient='records')
    shape = df.shape[0]
    _id = int(request.args.get('id', 1))
    print(shape)
    if shape <= _id:
        return jsonify("user not found")
    df = df.iloc[_id]

    responce = df.to_json()

    return json.loads(responce)


if __name__ == '__main__':
    app.run(debug=True)