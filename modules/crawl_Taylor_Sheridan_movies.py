

############################################ Taylor_Sheridan 감독 영화 ###################################################
def Taylor_Sheridan_movies(output_path):
    import requests
    import json
    import pandas as pd
    import numpy as np
    import pymysql
    import pymysql
    import os

    Taylor_Sheridan_url="https://api.themoviedb.org/3/discover/movie?api_key=36927ad1d2817ff0fa31947e47d186fd&with_crew=1215399"
    response = requests.get(Taylor_Sheridan_url)
    json_object = response.content
    movie_data = json.loads(json_object)
    movie_data

    Taylor_Sheridan_movies=pd.DataFrame(movie_data['results'])
    Taylor_Sheridan_movies.sort_values(by="vote_average", ascending=False)

if __name__ == "__main__":
    # Taylor_Sheridan_movies("../data-files/Taylor_Sheridan_movies.csv")
    pass
