

############################################현재 상영 중인 영화 crew 정보 ###################################################
def crawl_crew_for_now_playing_movies(output_path):
    import requests
    import json
    import pandas as pd
    import numpy as np
    import pymysql

    now_playing_movies=pd.read_csv("../data-files/now_playing_movies.csv", index_col=0)

    now_playing_movies_id_list = now_playing_movies[["id"]]

    # 현재상영 중인 영화id를 하나씩 넣어 crew정보 크롤링하기 

    all_crew_list = []
    error_list = []
    for idx,  movie_id in enumerate(now_playing_movies_id_list["id"].values):
        try :
            crew_url="https://api.themoviedb.org/3/movie/{0}?api_key=36927ad1d2817ff0fa31947e47d186fd&append_to_response=credits"
            response = requests.get(crew_url.format(movie_id))
            json_object = response.content
            movie_data = json.loads(json_object)
            
            # crew_list += movie_data["credit"]['crew']
            crew_list = movie_data["credits"]["crew"]
            for crew in crew_list:
                crew["movie_id"] = movie_id
                all_crew_list.append(crew)
        except:
            error_list.append(movie_id)

        # crew 정보 확인 
    crews=pd.DataFrame(all_crew_list)

    crews.to_csv(output_path, header=True, index=True)
    print("crawl success")

    conn = pymysql.connect(host="localhost",
                       database="finalteam3",
                       user="kdigital",
                       password="mysql",
                       charset="utf8")

    cursor = conn.cursor()
    cursor.execute('DELETE FROM crew')
    #error_list=[]
    for a in crews.values:
        sql= """INSERT INTO crew 
        (adult, gender, id, known_for_department, name, original_name, popularity, profile_path, credit_id, department, job, movie_id) 
                VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        cursor.execute(sql, list(a))
        #except:
            ##error_list.append(a)


    conn.commit() # 이전에 실행된 SQL 결과를 확정
    cursor.close()
    conn.close()
    print("DB insertion success")

if __name__ == "__main__":
    crawl_crew_for_now_playing_movies("../data-files/crew-test.csv")
    pass


