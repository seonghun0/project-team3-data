############################################현재 상영 중인 영화 cast 정보 ###################################################
def crawl_cast_for_now_playing_movies(output_path):
    import requests
    import json
    import pandas as pd
    import numpy as np
    import pymysql

    now_playing_movies=pd.read_csv("../data-files/now_playing_movies.csv", index_col=0)

    now_playing_movies_id_list = now_playing_movies[["id"]]

    # 현재 상영 중인 영화 id를 하나씩 넣어 cast 정보 crawling해오기 

    now_playing_movies_id_list = now_playing_movies[["id"]]

    all_cast_list = []
    error_list = []
    for idx,  movie_id in enumerate(now_playing_movies_id_list["id"].values):  # movie_id 목록을 기준으로 인덱스와 movie_id를 반환
        try :
            cast_url="https://api.themoviedb.org/3/movie/{0}?api_key=36927ad1d2817ff0fa31947e47d186fd&append_to_response=credits"
            response = requests.get(cast_url.format(movie_id))
            json_object = response.content
            movie_data = json.loads(json_object)
            cast_list = movie_data["credits"]["cast"]
            for cast in cast_list:                                   
                cast["movie_id"] = movie_id
                all_cast_list.append(cast)

        except:
            error_list.append(movie_id)

    #if idx == 100:
        #break

    # cast 정보 확인 및 필요없는 컬럼 제거

    casts=pd.DataFrame(all_cast_list)
    casts=casts.drop(['order'],axis=1)


    #casts.to_csv(output_path, header=True, index=True)
    print("Crawling success")

    # cast정보 DB(mysql)에 저장 

    conn = pymysql.connect(host="localhost",
                       database="finalteam3",
                       user="kdigital",
                       password="mysql",
                       charset="utf8")

    cursor = conn.cursor()
    # cursor.execute('DELETE FROM actor')

    error_list=[]
    for a in casts.values:
    
        sql= """INSERT INTO actor (adult, gender, id, known_for_department, name, original_name, popularity, profile_path, cast_id, role, credit_id, movie_id) 
                VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""" 
        cursor.execute(sql, list(a))



    conn.commit() # 이전에 실행된 SQL 결과를 확정
    cursor.close()
    conn.close()
    print("DB insertion success")



if __name__ == "__main__":
    crawl_cast_for_now_playing_movies("../data-files/cast-test.csv")
    pass


