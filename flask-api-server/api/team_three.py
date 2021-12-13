from flask import Blueprint
from flask import jsonify
from flask import make_response
from flask import request

import json

team_three = Blueprint('team_three', __name__)



@team_three.route("/")
def index():
    return "This is Api Service for Team Three"

@team_three.route("/kobis")
def kobis():
    from modules import Kobis_movie_ticketing
    
    result = Kobis_movie_ticketing.Kobis_movie_ticketing("")
    if result:
        return "success"
    else:
        return "fail"

@team_three.route("/genre")
def genrerecommend():
    from flask import current_app
    from modules import genrerun
    import pickle
    x = int(request.args.get('movie_id',0))

    # if g.similarity_genre is None:
    #     with open('modules/similarity_genre.pickle', "rb") as f:
    #         similarity_genre = pickle.load(f)
    #         g.similarity_genre = similarity_genre

    with open('modules/movie_data.pickle', "rb") as f:
        data = pickle.load(f)
        data = data.reset_index(drop=True)

    similarity_genre = current_app.config["similarity_genre"]

    target_movie_index = data[data['movie_id'] == x].index.values
    recommended_movies = genrerun.recommend_movies(target_movie_index, similarity_genre, data, 30)
    return jsonify(recommended_movies)

@team_three.route("/demo-one")
def demo_one() :
    return {
        "name": "John Doe",
        "email": "johndoe@example.com",
        "phone": "010-9438-4907",
        "birth": "1990-07-23"
    }

@team_three.route("/demo-two")
def demo_two() :
    return jsonify([{
        "name": "John Doe",
        "email": "johndoe@example.com",
        "phone": "010-9438-4907",
        "birth": "1990-07-23"
    }, {
        "name": "홍길동",
        "email": "hkd@example.com",
        "phone": "010-8687-2399",
        "birth": "1990-04-21"
    }])

@team_three.route("/demo-three")
def demo_three() :
    response = make_response(json.dumps([{
                "name": "John Doe",
                "email": "johndoe@example.com",
                "phone": "010-9438-4907",
                "birth": "1990-07-23"
            }, {
                "name": "홍길동",
                "email": "hkd@example.com",
                "phone": "010-8687-2399",
                "birth": "1990-04-21"
            }], ensure_ascii=False, indent=4))
    response.content_type = "application/json;charset=utf-8"

    return response