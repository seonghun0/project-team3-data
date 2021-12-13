
############################################ popular movies ###################################################
def crawl_popular_movies(output_path):
    import requests
    import json
    import pandas as pd
    import numpy as np
    import pymysql
    import os

    popular = []
    for i in range(1,1000):
        url = 'https://api.themoviedb.org/3/movie/popular?api_key={1}&language=ko-KR&page={0}'.format(i,'36927ad1d2817ff0fa31947e47d186fd')
        resp = requests.get(url)
        popular.append(json.loads(resp.content))

        if i == 10:
            break

    popular_movies = []
    for row in popular:
        for result in row ['results']:
            popular_movies.append(result)

    popular_movies=pd.DataFrame(popular_movies)
    popular_movies=popular_movies.drop(['adult', 'backdrop_path', 'video','genre_ids'], axis=1)
    popular_movies=popular_movies.rename(columns={'id':'movie_id', 'original_title':'subtitle','poster_path':'posterpath'})

    popular_movies_ids=popular_movies[["movie_id"]]
    
    print("Crawling success")

    conn = pymysql.connect(host="localhost",
                       database="finalteam3",
                       user="kdigital",
                       password="mysql",
                       charset="utf8")
    cursor = conn.cursor()

    # cursor.execute('DELETE FROM popular_movies')
    error_list=[]
    for movie_id in popular_movies_ids.values:
        # print(type(movie_id), movie_id)  # movie_id type이 nd.arrary임.
        try:
            sql= "INSERT INTO popular_movies VALUES(%s)"
            cursor.execute(sql, movie_id[0]) 
        except:
            error_list.append(movie_id)
        
    conn.commit()
    cursor.close()
    conn.close()

    print("popular_movie insertion success")

    ## movie에 없는 영화 삽입 code는 duplication error 발생.

if __name__ == "__main__":
    crawl_popular_movies("../data-files/popular_movies.csv")
    pass

