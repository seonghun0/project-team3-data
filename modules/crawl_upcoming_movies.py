
############################################ upcoming movies ###################################################
def crawl_upcoming_movies(output_path):
    import json
    import pandas as pd
    import requests
    import pymysql
    import numpy as np

    upcoming = []
    for i in range(1,1000):
        url = 'https://api.themoviedb.org/3/movie/upcoming?api_key={1}&language=ko-KR&primary_release_date.gte=2022-01-01&page={0}'.format(i,'36927ad1d2817ff0fa31947e47d186fd')
        resp = requests.get(url)
        upcoming.append(json.loads(resp.content))

        if i == 10:
            break

    upcoming_movies = []
    for row in upcoming:
        for result in row ['results']:
            upcoming_movies.append(result)

    upcoming_movies=pd.DataFrame(upcoming_movies)
    # upcoming_movies.to_csv("../data-files/upcoming_movies.csv", header=True, index=True)
    upcoming_movies=upcoming_movies.drop(['adult', 'backdrop_path', 'video','genre_ids'], axis=1)
    upcoming_movies=upcoming_movies.rename(columns={'id':'movie_id', 'original_title':'subtitle','poster_path':'posterpath'})
    upcoming_movies_ids=upcoming_movies[["movie_id"]]
    print("Crawling success")


    conn = pymysql.connect(host="localhost",
                        database="finalteam3",
                        user="kdigital",
                        password="mysql",
                        charset="utf8")
    cursor = conn.cursor()

    # cursor.execute('DELETE FROM now_playing_movies')
    error_list=[]
    for movie_id in upcoming_movies_ids.values:
        try:
        # print(type(movie_id), movie_id)  # movie_id type이 nd.arrary임. 
            sql= "INSERT INTO upcoming_movies VALUES(%s)"
            cursor.execute(sql, movie_id[0]) 
        except:
            error_list.append(movie_id)
        
    conn.commit()
    cursor.close()
    conn.close()
    print("DB insertion success")

if __name__ == "__main__":
    crawl_upcoming_movies("../data-files/upcoming_movies.csv")
    pass
