

############################################장르 정보 ###################################################
def crawl_genre(output_path):
    import requests
    import json
    import pandas as pd
    import numpy as np
    import pymysql

    # 1. 영화장르 정보 crawling 후 csv파일로 저장 

    genre_url="https://api.themoviedb.org/3/genre/movie/list?api_key=36927ad1d2817ff0fa31947e47d186fd&language=ko-KR"
    response = requests.get(genre_url)
    json_object = response.content
    genre_data = json.loads(json_object)
    genre_data
    genre_data_df = pd.DataFrame(genre_data['genres'])

    # genre_data_df.to_csv("genre.csv")
    print("Crawling success")

    # 2. mysql genre table에 영화 장르정보 삽입

    conn = pymysql.connect(host="localhost",
                        database="finalteam3",
                        user="kdigital",
                        password="mysql",
                        charset="utf8")
    cursor = conn.cursor()

    # cursor.execute('DELETE FROM genre')
    for id, name in genre_data_df.values:
        # print("{0}/{1}".format(id, name))
        sql= "INSERT INTO genre VALUES(%s, %s)"
        cursor.execute(sql, (id, name))

    conn.commit()
    cursor.close()
    conn.close()
    print("DB insertion success")

if __name__ == "__main__":
    crawl_genre("../data-files/genre.csv")
    pass

