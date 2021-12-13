
############################################ Tom Holland 출연영화 ###################################################
def Tom_Holland_movies(output_path):
    import requests
    import json
    import pandas as pd
    import numpy as np
    import pymysql
    import pymysql
    import os

    Tom_Holland_url="https://api.themoviedb.org/3/discover/movie?api_key=36927ad1d2817ff0fa31947e47d186fd&with_cast=1136406"
    response = requests.get(Tom_Holland_url)
    json_object = response.content
    movie_data = json.loads(json_object)
    movie_data

    Tom_Holland_movies=pd.DataFrame(movie_data['results'])
    Tom_Holland_movies.sort_values(by="vote_average", ascending=False)

if __name__ == "__main__":
    # Tom_Holland_movie("../data-files/Tom_Holland_movie.csv")
    pass

