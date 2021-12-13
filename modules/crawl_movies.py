
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
        print("Crawling success")

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
    print("DB insertion success")

    if __name__ == "__main__":
        crawl_movies("../data-files/total_movielist.csv")
        pass

