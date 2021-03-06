import os
from flask import Flask, g
from api.team_one import team_one
from api.team_two import team_two
from api.team_three import team_three

app = Flask(__name__)
app.register_blueprint(team_one, url_prefix='/team-one')
app.register_blueprint(team_two, url_prefix='/team-two')
app.register_blueprint(team_three, url_prefix='/team-three')

with app.app_context():
    import pickle
    from modules import genrerun
    if not os.path.exists("movie_data.pickle"):
        genrerun.set_movie_data()

    with open('movie_data.pickle', "rb") as f:
        data = pickle.load(f)
        data = data.reset_index(drop=True)
        app.config['data'] = data

    if not os.path.exists("similarity_genre2.pickle"):
        genrerun.train_recommender_system(data)

    with open('similarity_genre2.pickle', "rb") as f:
        similarity_genre2 = pickle.load(f)
        app.config["similarity_genre2"] = similarity_genre2
        
   
        
@app.route("/")
def index():
    return "This is API Service App"

# if __name__ == "__main__":
    # app.run(host="0.0.0.0", port=5003)