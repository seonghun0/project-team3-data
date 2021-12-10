############################################3. 영화장르 정보###################################################
def crawl_movie_genre_table(output_path):
    import requests
    import json
    import pandas as pd
    import numpy as np
    import pymysql
    import os

    # 1. 영화 id 정보추출을 위해 영화정보 파일 가져오기 
    tmdb_movie_list = pd.read_csv('../data-files/total_tmdbmovielist.csv',index_col=0,lineterminator='\n')

    # 2. 영화테이블에서 장르id만 추출하여 genre_ids칼럼을 만들기  

    # str자료형을 list로 변경하는 함수
    error_list_x = []
    def genres_str_to_list(genres_str):
        import json
        # print(genres_str)
        genre_id_list = []
        try :
            genres_list = json.loads(genres_str.replace("\'", "\""))
            for genre in genres_list:
                genre_id_list.append(genre['id'])

        except:
            error_list_x.append(genres_str)
        
        return genre_id_list

    # 장르 id를 추출하여 genre_ids칼럼 추가
    tmdb_movie_list['genre_ids'] = tmdb_movie_list["genres"].map(genres_str_to_list)

    # genre_ids가 없는 행 제거 

    print( type(tmdb_movie_list['genre_ids'][0]))
    tmdb_movie_list['genre_ids'].map(lambda x : True if len(x) > 0 else False)
    tmdb_movie_list_1=tmdb_movie_list[tmdb_movie_list['genre_ids'].map(lambda x : True if len(x) > 0 else False)]

    movie_genre_ids_df=tmdb_movie_list_1[['id','genre_ids']]

    #3. 영화_장르 연결테이블 2)movie_id, genre_id삽입

    conn = pymysql.connect(host="localhost",
                        database="finalteam3",
                        user="kdigital",
                        password="mysql",
                        charset="utf8")
    cursor = conn.cursor()
    cursor.execute('DELETE FROM movie_genre')

    error_list2 = []
    for movie_genre_ids in movie_genre_ids_df.values:
        #print(movie_genre_ids[1])
        #break
        current_genre_id = None
        try:
            movie_id = movie_genre_ids[0]
            for genre_id in movie_genre_ids[1]:
                current_genre_id = genre_id
                sql= "INSERT INTO movie_genre VALUES (%s, %s)"
                cursor.execute(sql, (movie_id, genre_id))
                # print((movie_id, genre_id))
        except:
            error_list2.append((movie_id, current_genre_id))
    conn.commit()
    cursor.close()
    print('close')
    conn.close()

if __name__ == "__main__":
    crawl_movie_genre_table("../data-files/movie_genre.csv")
    pass
