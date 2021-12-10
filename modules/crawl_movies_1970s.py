

############################################1970년대 영화 ###################################################

def crawl_movies_1970s(output_path):
    import requests
    import json
    import pandas as pd
    import numpy as np
    import pymysql
    import os

    movies_1970s = []
    for i in range(1,1000):
        url = 'https://api.themoviedb.org/3/discover/movie?api_key=36927ad1d2817ff0fa31947e47d186fd&primary_release_date.gte=1970-01-01&primary_release_date.lte=1979-12-31&sort_by=popularity.desc&include_video=false&page={0}'.format(i)
        resp = requests.get(url)
        movies_1970s.append(json.loads(resp.content))

        if i == 10:
            break

    movies = []
    for row in movies_1970s:
        for result in row['results']:
            movies.append(result)
    
    movies_1970s=pd.DataFrame(movies)

    # na가 제거된 1970년대 영화리스트를 csv파일로 저장
    movies_1970s.to_csv("movies_1970s.csv", mode='w')
    print("Crawling success")

if __name__ == "__main__":
    crawl_movies_1970s("../data-files/movies_1970s.csv")
    pass

