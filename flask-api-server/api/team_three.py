from flask import Blueprint
from flask import jsonify
from flask import make_response
from flask import request
import os
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
    from flask import g
    from flask import make_response
    from modules import genrerun
    import pickle
    import json
    x = int(request.args.get('movie_id',0))

    similarity_genre2 = current_app.config["similarity_genre2"]
    data = current_app.config['data']

    target_movie_index = data[data['movie_id'] == x].index.values
    recommended_movies = genrerun.recommend_movies(target_movie_index, similarity_genre2, data, 30)
    return recommended_movies

@team_three.route("/recommend")
def userrecommend():
    from modules import surprise_recommender as sr
    import pickle
    x = request.args.get('id',0)

    if not os.path.exists("algo.pickle"):
        sr.surprise_recommender()
    with open('algo.pickle', "rb") as f:
        algo = pickle.load(f)
    
    sr.movies()    
    with open('movies.pickle', "rb") as f:
        movies = pickle.load(f)
    
    sr.ratings()
    with open('ratings.pickle', "rb") as f:
        ratings = pickle.load(f)

    if not os.path.exists("unseen_movies.pickle"):
        unseen_list = sr.get_unseen_surprise(ratings, movies, x)
    with open('unseen_movies.pickle', "rb") as f:
        unseen_list= pickle.load(f)

    top_movies_preds = sr.recomm_movie_by_surprise(algo, x, unseen_list,movies, top_n=10)

    return top_movies_preds

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