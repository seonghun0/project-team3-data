

############################################현재 상영 중인 영화정보 ###################################################
def crawl_now_playing_movies(output_path):
    import json
    import pandas as pd
    import requests
    import pymysql

    # 현재 상영 중인 영화 200편 크롤링하기 

    now_playing = []
    for i in range(1,1000):
        url = 'https://api.themoviedb.org/3/movie/now_playing?api_key={1}&language=ko-KR&page={0}'.format(i,'5ecfbcb3280f5e9f7898c3ca00acca81')
        resp = requests.get(url)
        now_playing.append(json.loads(resp.content))

        if i == 10:       
            break

    now_playing_movies = []
    for row in now_playing:
        for result in row ['results']:
            now_playing_movies.append(result)
    
    now_playing_movies=pd.DataFrame(now_playing_movies)

    now_playing_movies=now_playing_movies.drop(['adult', 'backdrop_path', 'video','genre_ids'], axis=1)
    
    # 칼럼이름을 DB 칼럼이름과 동일하게 변경
    now_playing_movies=now_playing_movies.rename(columns={'id':'movie_id', 'original_title':'subtitle','poster_path':'posterpath'})

    now_playing_movies_ids=now_playing_movies[["movie_id"]]

    print("Crawling success")
    #DB에 저장
    conn = pymysql.connect(host="localhost",
                        database="finalteam3",
                        user="kdigital",
                        password="mysql",
                        charset="utf8")
    cursor = conn.cursor()

    error_list = []

    cursor.execute('DELETE FROM crew')
    cursor.execute('DELETE FROM actor')
    cursor.execute('DELETE FROM now_playing_movies')
    for movie_id in now_playing_movies_ids.values:
        try:
            # print(type(movie_id), movie_id)  # movie_id type이 nd.arrary임. 삽입시 일차원으로 변경필요.
            sql= "INSERT INTO now_playing_movies VALUES(%s)"
            cursor.execute(sql, movie_id[0])
        except:
            error_list.append(movie_id)
        
    conn.commit()
    cursor.close()
    conn.close()

    #없는 영화 업데이트

    movies = []
    for i in error_list:
            for e in i:
                    url="https://api.themoviedb.org/3/movie/{0}?api_key={1}&language=ko-KR".format(e,'36927ad1d2817ff0fa31947e47d186fd')
                    response = requests.get(url)
                    movies.append(json.loads(response.content))
    movies2 = pd.DataFrame(movies)

    # 3. DB에 저장 
    import numpy as np
    # 1. connect
    conn = pymysql.connect(host="127.0.0.1", port=3306, db='finalteam3', user="kdigital", password="mysql", charset="utf8")

    # 2. get command object
    cursor = conn.cursor()

    # cursor.execute("delete from movie") #기존 데이터를 지울 때 주석을 풀어주세요 

    error_list = []   
    movies2['overview'] = movies2['overview'].apply(lambda ov : ov if str(ov) != 'nan' else '')
    movies2['release_date'] = movies2['release_date'].apply(lambda ov : ov if str(ov) != 'nan' else '')
    movies2['poster_path'] = movies2['poster_path'].apply(lambda ov : ov if str(ov) != 'nan' else '')
    print(movies2.values.shape)

    # 3. execute command
    
    insert_sql = """insert into movie (movie_id, title, subtitle, original_language, overview, popularity, release_date, vote_average, vote_count, posterpath) 
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    for movie in movies2.values:
        cursor.execute(insert_sql, list(movie[list(np.array([9, 24, 12, 11, 13, 14, 18, 26, 27, 15]) - 3)]))
  
    conn.commit() # confirm previous execution

    # 4. close resource
    cursor.close()
    conn.close()
    print("DB insertion success")

if __name__ == "__main__":
    crawl_now_playing_movies("../data-files/now_playing_movies.csv")
    pass

