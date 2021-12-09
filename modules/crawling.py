
############################################영화 정보 ###################################################
def crawl_movies(output_path):
    import requests
    import json
    import pandas as pd
    import numpy as np
    import pymysql
    import os

    # 1. 영화정보 크롤링 후 csv파일로 저장(10000개씩)

    # 1000개씩 영화정보 크롤링해오기
    def crawling(idx):    
        import json
        import pandas as pd
        import requests

        start = idx
        end = idx + 9999
        movies = []
        for i in range(start, end):
            url="https://api.themoviedb.org/3/movie/{0}?api_key={1}&language=ko-KR".format(i,'api-key')
            response = requests.get(url)
            movies.append(json.loads(response.content))
            

        movies
        tmdbmovielist = pd.DataFrame(movies)
        tmdbmovielist.to_csv("../data-files/tmdbmovielist({0}-{1}).csv".format(start, end), header=True, index=True)

    # 2. 크롤링해서 data-files에 저장한 csv자료 불러오기 

    files = os.listdir('../data-files/')
    files

    # 3. DB에 저장 

    import numpy as np

    # 1. connect
    conn = pymysql.connect(host="127.0.0.1", port=3306, db='finalteam3', user="kdigital", password="mysql", charset="utf8")

    # 2. get command object
    cursor = conn.cursor()

    #cursor.execute("delete from movie") #기존 데이터를 지울 때 주석을 풀어주세요 

    error_list = []
    movie3 = []
    for file in files:
        # if file.find('tmdbmovielist') == 0 and file.find('100000-149999') == -1 and file.find('20000-99999') == -1:
        if file.find('tmdbmovielist') == 0:    #파일명에 'tmdbmovielist'가 포함된 파일 찾기
            print(file)
            try:
                try :
                    movies = pd.read_csv('../data-files/{0}'.format(file), encoding="utf-8")
                except:
                    movies = pd.read_csv('../data-files/{0}'.format(file), encoding="utf-8", lineterminator='\n')

                movies.drop(movies.columns[0], axis=1, inplace=True) # "Unnamed: 0" 컬럼 제거

                movies2 = movies[movies['status_message'].isna()] # "The resource you requested could not be found" 인 행은 제거 

                movies2 = movies2.drop(["success","status_code","status_message"], axis=1) # "success","status_code","status_message" 컬럼제거 
                
                movies2['overview'] = movies2['overview'].apply(lambda ov : ov if str(ov) != 'nan' else '')  # csv파일로 읽어올 때 nan값이 인식되지 않아 빈칸으로 변경
                movies2['release_date'] = movies2['release_date'].apply(lambda ov : ov if str(ov) != 'nan' else '')
                movies2['poster_path'] = movies2['poster_path'].apply(lambda ov : ov if str(ov) != 'nan' else '')
                print(movies2.values.shape)
                
                movie3.append(list(movies2.values))   # DB에 저장할 자료 movie3에 넣기
                
                # 3. execute command
                sql = """insert into movie (movie_id, title, subtitle, original_language, overview, popularity, release_date, vote_average, vote_count, posterpath) 
                        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                for movie in movies2.values:
                    cursor.execute(sql, list(movie[list(np.array([9, 24, 12, 11, 13, 14, 18, 26, 27, 15]) - 3)]))
                    
                    

            except:
                error_list.append(movie[list(np.array([9, 24, 12, 11, 13, 14, 18, 26, 27, 15]) - 3)])

            # break

    conn.commit() # confirm previous execution
    # 4. close resource
    cursor.close()
    conn.close()

    if __name__ == "__main__":
    # crawl_movies("../data-files/total_movielist.csv")
        pass

############################################영화장르 정보 ###################################################
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

    genre_data_df.to_csv("genre.csv")

    # 2. mysql genre table에 영화 장르정보 삽입

    conn = pymysql.connect(host="localhost",
                        database="finalteam3",
                        user="kdigital",
                        password="mysql",
                        charset="utf8")
    cursor = conn.cursor()

    cursor.execute('DELETE FROM genre')
    for id, name in genre_data_df.values:
        # print("{0}/{1}".format(id, name))
        sql= "INSERT INTO genre VALUES(%s, %s)"
        cursor.execute(sql, (id, name))

    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    # crawl_genre("../data-files/genre.csv")
    pass
############################################영화장르 정보###################################################
def movie_genre_table(output_path):
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
    # cmovie_genre_table("../data-files/movie_genre.csv")
    pass

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
    sql = """insert into movie (movie_id, title, subtitle, original_language, overview, popularity, release_date, vote_average, vote_count, posterpath) 
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    for movie in movies2.values:
        cursor.execute(sql, list(movie[list(np.array([9, 24, 12, 11, 13, 14, 18, 26, 27, 15]) - 3)]))

    conn.commit() # confirm previous execution
    # 4. close resource
    cursor.close()
    conn.close()


if __name__ == "__main__":
    # crawl_now_playing_movies("../data-files/now_playing_movies.csv")
    pass

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

if __name__ == "__main__":
    # crawl_crew_for_now_playing_movies("../data-files/crew-test.csv")
    pass



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


    casts.to_csv(output_path, header=True, index=True)

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



if __name__ == "__main__":
    # crawl_cast_for_now_playing_movies("../data-files/crew-test.csv")
    pass


############################################ Kobis 영화예매율 ###################################################
def Kobis_movie_ticketing(output_path):
    import requests # 웹 요청 도구
    from bs4 import BeautifulSoup # html에서 데이터 읽는 도구
    from bs4.element import NavigableString, Tag, Comment, ProcessingInstruction
    from selenium import webdriver
    import pandas as pd
    import pymysql
    import os
    import numpy as np

    browser_proxy = webdriver.Chrome("/Users/attitude/workspace/tools/chromedriver")
    browser_proxy.get("https://www.kobis.or.kr/kobis/business/stat/boxs/findRealTicketList.do")
    soup = BeautifulSoup(browser_proxy.page_source)
    table = soup.select_one('.tbl_comm.th_sort')

    th_list = table.select('thead tr th')
    columns = []
    for th in th_list:
        for c in th.children:
            # print(c)
            if type(c) == NavigableString and len(c.strip()) > 0:
                columns.append(c.strip())

    tr_list = table.select('tbody tr')
    values_list = []
    for tr in tr_list:
        values = []
        for td in tr.select('td'):
            values.append(td.text.strip().replace("%", '').replace(",", ""))
        values_list.append(values)

    data = pd.DataFrame(values_list, columns=columns)
    data.groupby(by=["순위"])["순위"].count()

    # csv파일 DB(mysql)에 저장  
    # 1. connect
    conn = pymysql.connect(host="127.0.0.1", port=3306, db='finalteam3', user="kdigital", password="mysql", charset="utf8")

    # 2. get command object
    cursor = conn.cursor()
    #cursor.execute("delete from movie") #기존 데이터를 지울 때 주석을 풀어주세요 

    # 3. execute command
    sql = """insert into movie_ticketing (ranked, title, release_date, reserve_rate, reserve_revenue, accumulated_revenue, ticketing_view, accumulated_view) 
            values (%s, %s, %s, %s, %s, %s, %s, %s)"""
    for movie in data.values:
        cursor.execute(sql, list(movie))
                            
    # break

    conn.commit() # confirm previous executio

    # 4. close resource
    cursor.close()
    conn.close()

    # browser_proxy.close() #주석 풀어서 사용하세요) 가장 마지막 크롬 하나만 종료
    # browser_proxy.quit() #주석 풀어서 사용하세요) 크롬s 모두 종료

if __name__ == "__main__":
    # Kobis_movie_ticketing("../data-files/kobis.csv")
    pass


############################################1970년대 영화 ###################################################

def movies_1970s(output_path):
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

        if i == 3:
            break

    movies = []
    for row in movies_1970s:
        for result in row['results']:
            movies.append(result)
    
    movies_1970s=pd.DataFrame(movies)

    # na가 제거된 1970년대 영화리스트를 csv파일로 저장
    movies_1970s.to_csv("movies_1970s.csv", mode='w')

if __name__ == "__main__":
    # movies_1970s("../data-files/movies_1970s.csv")
    pass

############################################ popular movies ###################################################
def popular_movies(output_path):
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

    #없는 영화 업데이트

    popular_movies = []
    for i in error_list:
            for e in i:
                    url="https://api.themoviedb.org/3/movie/{0}?api_key={1}&language=ko-KR".format(e,'36927ad1d2817ff0fa31947e47d186fd')
                    response = requests.get(url)
                    popular_movies.append(json.loads(response.content))
    
    movies2 = pd.DataFrame(popular_movies)

    # DB에 저장 
    import numpy as np
    # 1. connect
    conn = pymysql.connect(host="127.0.0.1", port=3306, db='finalteam3', user="kdigital", password="mysql", charset="utf8")

    # 2. get command object
    cursor = conn.cursor()

    # cursor.execute("delete from movie") #기존 데이터를 지울 때 주석을 풀어주세요 

    error_list = []   ##결과 18개
                
    # movies2['original_language'] = movies2['original_language'].apply(lambda ol: ol if ol else '')
    movies2['overview'] = movies2['overview'].apply(lambda ov : ov if str(ov) != 'nan' else '')
    movies2['release_date'] = movies2['release_date'].apply(lambda ov : ov if str(ov) != 'nan' else '')
    movies2['poster_path'] = movies2['poster_path'].apply(lambda ov : ov if str(ov) != 'nan' else '')
    print(movies2.values.shape)
    ##결과: 630186 entries  ???????????????

    # 3. execute command
    sql = """insert into movie (movie_id, title, subtitle, original_language, overview, popularity, release_date, vote_average, vote_count, posterpath) 
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    for movie in movies2.values:
        cursor.execute(sql, list(movie[list(np.array([9, 24, 12, 11, 13, 14, 18, 26, 27, 15]) - 3)]))

    # movie3.append(list(movies2.values)) ##결과: 467108 entries    ????????????????????
                    

    conn.commit() # confirm previous execution
    # 4. close resource
    cursor.close()
    conn.close()

if __name__ == "__main__":
    # popular_movies("../data-files/popular_movies.csv")
    pass

############################################ top_rated movies ###################################################
def top_rated_movies(output_path):
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

    #없는 영화 업데이트

    movies = []
    for i in error_list:
            for e in i:
                    url="https://api.themoviedb.org/3/movie/{0}?api_key={1}&language=ko-KR".format(e,'36927ad1d2817ff0fa31947e47d186fd')
                    response = requests.get(url)
                    movies.append(json.loads(response.content))
    movies2 = pd.DataFrame(movies)

    # DB에 저장 
    conn = pymysql.connect(host="127.0.0.1", port=3306, db='finalteam3', user="kdigital", password="mysql", charset="utf8")
    cursor = conn.cursor()
    # cursor.execute("delete from movie") #기존 데이터를 지울 때 주석을 풀어주세요 

    error_list = []  
                
    movies2['overview'] = movies2['overview'].apply(lambda ov : ov if str(ov) != 'nan' else '')
    movies2['release_date'] = movies2['release_date'].apply(lambda ov : ov if str(ov) != 'nan' else '')
    movies2['poster_path'] = movies2['poster_path'].apply(lambda ov : ov if str(ov) != 'nan' else '')
    print(movies2.values.shape)


    # 3. execute command
    sql = """insert into movie (movie_id, title, subtitle, original_language, overview, popularity, release_date, vote_average, vote_count, posterpath) 
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    for movie in movies2.values:
        cursor.execute(sql, list(movie[list(np.array([9, 24, 12, 11, 13, 14, 18, 26, 27, 15]) - 3)]))

    conn.commit() # confirm previous execution
    # 4. close resource
    cursor.close()
    conn.close()

if __name__ == "__main__":
    # top_rated_movies("../data-files/top_rated_movies.csv")
    pass

############################################ upcoming movies ###################################################
def upcoming_movies(output_path):
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

if __name__ == "__main__":
    # upcoming_movies("../data-files/upcoming_movies.csv")
    pass

############################################ 영화 비디오 ###################################################

def crawl_video(output_path):
    import requests
    import json
    import pandas as pd
    import numpy as np
    import pymysql
    import os

    def video_crawling(start):
        import pandas as pd
    
        # 영화id를 사용하기 위해 클롤링한 영화 정보를 csv파일로 저장해둔 파일 읽어오기 
        movielist = pd.read_csv("../data-files/total_tmdbmovielist.csv", index_col=0)   
        print(movielist)
        
        # 영화정보 중 영화id만 가져오기
        movie_ids = []           
        for i in movielist['id'].values:
            movie_ids.append(i)
        print(movie_ids)
        
        # 영화id를 하나씩 넣으면서 영화비디오 정보 크롤링  
        videos=[]
        error_list =[]
        end = start + 49999
        for movie_id in movie_ids[start : end+1]:
            import json
            import pandas as pd
            import requests
            
            movies=[]
            video_url = "https://api.themoviedb.org/3/movie/{0}/videos?api_key={1}&language=ko-KR".format(movie_id,'5ecfbcb3280f5e9f7898c3ca00acca81')
            response = requests.get(video_url)
            movies = json.loads(response.content)
            try:
                if (movies['results'] == []):
                    pass
                else:
                    a = pd.DataFrame(movies['results'])
                    a['movie_id'] = movies['id']
                    for b in a.values:
                        videos.append(b)
                video_list = pd.DataFrame(videos)
                video_lists=video_list.to_csv("../data-files/video_list({0}-{1}).csv".format(start, end), header=True, index=True)   #영화 비디오 리스트 csv파일로 저장 
            except:
                error_list.append(movies)
    
    video_crawling(1)

    # 2. 크롤링해서 data-files에 저장한 비디오 csv자료 불러오기 

    a = []
    files = os.listdir('../data-files/')
    for file in files:
        if file.find('video_list') == 0:
            # print(file)
            videos = pd.read_csv('../data-files/{0}'.format(file), encoding="utf-8",index_col=0)
            video2 = videos.drop(videos.columns[[0,1,5,7,8,9]],axis=1)
            # print(video2.shape)
            if len(a) == 0 :
                a = video2
            else:
                a.append(video2)

    b = pd.DataFrame(a)

    files = os.listdir('../data-files/')

    error_list = []
    video3 = []
    for file in files:
        if file.find('video_list') == 0:
            print(file)
            videos = pd.read_csv('../data-files/{0}'.format(file),index_col=0, encoding='utf-8')
            video2 = videos.drop(videos.columns[[0,1,5,7,8,9]],axis=1)
            print(video2.values.shape)

            for video in video2.values:
                video3.append(video)
    videos3 = pd.DataFrame(video3)

    # DB저장 
    conn = pymysql.connect(host="127.0.0.1", port=3306, db='finalteam3', user="kdigital", password="mysql", charset="utf8mb4")
    cursor = conn.cursor()

    cursor.execute("delete from video") #기존 데이터를 지울 때 주석을 풀어주세요 
    error_list = []
    sql = """insert into video ( name, videokey, site, videotype, movie_id) 
                        values (%s, %s, %s, %s, %s)"""
    for video in videos3.values:
        try:
            cursor.execute(sql, list(video))
        except:
            error_list.append(video)

    conn.commit() # confirm previous execution
    # 4. close resource
    cursor.close()
    conn.close()


if __name__ == "__main__":
    # crawl_video("../data-files/videos.csv")
    pass

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

############################################ Taylor_Sheridan 감독 영화 ###################################################
def Taylor_Sheridan_movies(output_path):
    import requests
    import json
    import pandas as pd
    import numpy as np
    import pymysql
    import pymysql
    import os

    Taylor_Sheridan_url="https://api.themoviedb.org/3/discover/movie?api_key=36927ad1d2817ff0fa31947e47d186fd&with_crew=1215399"
    response = requests.get(Taylor_Sheridan_url)
    json_object = response.content
    movie_data = json.loads(json_object)
    movie_data

    Taylor_Sheridan_movies=pd.DataFrame(movie_data['results'])
    Taylor_Sheridan_movies.sort_values(by="vote_average", ascending=False)

if __name__ == "__main__":
    # Taylor_Sheridan_movies("../data-files/Taylor_Sheridan_movies.csv")
    pass

########################################### Lana_Wachowski 집필 영화 ###################################################
def Lana_Wachowski(output_path):
    import requests
    import json
    import pandas as pd
    import numpy as np
    import pymysql
    import pymysql
    import os

    Lana_Wachowski_url="https://api.themoviedb.org/3/discover/movie?api_key=36927ad1d2817ff0fa31947e47d186fd&with_crew=9340"
    response = requests.get(Lana_Wachowski_url)
    json_object = response.content
    movie_data = json.loads(json_object)
    movie_data

    Lana_Wachowski_movies=pd.DataFrame(movie_data['results'])
    Lana_Wachowski_movies.sort_values(by="vote_average", ascending=False)

if __name__ == "__main__":
    # Lana_Wachowski_movies("../data-files/Lana_Wachowski_movies.csv")
    pass