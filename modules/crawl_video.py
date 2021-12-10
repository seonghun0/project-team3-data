
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
        # print(movielist)
        
        # 영화정보 중 영화id만 가져오기
        movie_ids = []           
        for i in movielist['id'].values:
            movie_ids.append(i)
        # print(movie_ids)
        
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
    print("Crawling success")

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
            # print(file)
            videos = pd.read_csv('../data-files/{0}'.format(file),index_col=0, encoding='utf-8')
            video2 = videos.drop(videos.columns[[0,1,5,7,8,9]],axis=1)
            # print(video2.values.shape)

            for video in video2.values:
                video3.append(video)
    videos3 = pd.DataFrame(video3)

    # DB저장 
    conn = pymysql.connect(host="127.0.0.1", port=3306, db='finalteam3', user="kdigital", password="mysql", charset="utf8mb4")
    cursor = conn.cursor()

    # cursor.execute("delete from video") #기존 데이터를 지울 때 주석을 풀어주세요 
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
    print("DB insertion success")


if __name__ == "__main__":
    crawl_video("../data-files/videos.csv")
    pass
