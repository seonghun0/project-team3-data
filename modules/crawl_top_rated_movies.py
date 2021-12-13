
############################################ top_rated movies ###################################################
def crawl_top_rated_movies(output_path):
    import json
    import pandas as pd
    import requests
    import pymysql
    import numpy as np

    top_rated = []
    for i in range(1,1000):
        url = 'https://api.themoviedb.org/3/movie/top_rated?api_key={1}&language=ko-KR&page={0}'.format(i,'36927ad1d2817ff0fa31947e47d186fd')
        resp = requests.get(url)
        top_rated.append(json.loads(resp.content))

        if i == 10:
            break

    top_rated_movies = []
    for row in top_rated:
        for result in row ['results']:
            top_rated_movies.append(result)
    
    top_rated_movies=pd.DataFrame(top_rated_movies)
    top_rated_movies=top_rated_movies.drop(['adult', 'backdrop_path', 'video', 'genre_ids'], axis=1)
    top_rated_movies=top_rated_movies.rename(columns={'id':'movie_id', 'original_title':'subtitle','poster_path':'posterpath'})
    top_rated_movies_ids=top_rated_movies[["movie_id"]]

    print("Crawling success")

    #DB에 저장

    conn = pymysql.connect(host="localhost",
                        database="finalteam3",
                        user="kdigital",
                        password="mysql",
                        charset="utf8")
    cursor = conn.cursor()

    # cursor.execute('DELETE FROM top_rated_movies')
    error_list=[]
    for movie_id in top_rated_movies_ids.values:
        # print(type(movie_id), movie_id)  # movie_id type이 nd.arrary임.
        try:
            sql= "INSERT INTO top_rated_movies VALUES(%s)"
            cursor.execute(sql, movie_id[0]) 
        except:
            error_list.append(movie_id)
        
    conn.commit()
    cursor.close()
    conn.close()
    print("DB insertion success")
    
if __name__ == "__main__":
    crawl_top_rated_movies("../data-files/top_rated_movies.csv")
    pass

