def load_movie_list():
    import pandas as pd
    movie_list = pd.read_csv("../data-files/movie_list_result.csv", encoding='utf-8')
    return movie_list

def load_movie_detail():
    import pandas as pd
    movie_detail = pd.read_csv('../data-files/movie_info_result.csv', encoding='utf-8')
    return movie_detail

def load_processing_naver_score():
    import pandas as pd
    df = pd.read_csv("../data-files/naverScore.csv", encoding='utf-8')
    processed_df = df
    processed_df['naverScore'] = (round(df['naverScore']/2, 1))
    scores = processed_df.values.tolist()
    return scores

def load_processing_shortContent():
    import pandas as pd
    df = pd.read_csv("../data-files/naverStory.csv", on_bad_lines='skip', encoding='utf-8')
    df = df.drop(['Unnamed: 0'], axis=1)
    df = df.where((pd.notnull(df)), "내용없음")
    stories = df.values.tolist()
    return stories

def processing_movie_data(movie_list):
    import pandas as pd
    df = movie_list.where((pd.notnull(movie_list)), None)
    df['movieCd'] = df['movieCd'].astype('int').astype('str')
    df['prdtYear'] = df['prdtYear'].astype('int').astype('str')
    df['openDt'] = df['openDt'].astype('int').astype('str')
    movie_data = df.values.tolist()
    return movie_data

def processing_actor(movie_detail):
    import pandas as pd
    test = movie_detail
    test['actors_ko'] = test['actors_ko'].str.strip("[]")
    test['actors_ko2'] = test['actors_ko'].map(lambda x : x.split(","))
    itsKo = []
    for idx, item in enumerate(test['actors_ko2']):
        wow = []
        test['actors_ko2'][idx] = wow
        for it in item:
            it = it.strip(" ").strip("'")
            wow.append(it)
            itsKo.append(it)
    aKo = itsKo
    processed_aKo = set(aKo)
    return processed_aKo

def processing_director(movie_detail):
    import pandas as pd
    test = movie_detail
    test['directors_ko'] = test['directors_ko'].str.strip("[]")
    test['directors_ko2'] = test['directors_ko'].map(lambda x : x.split(","))
    itsKo = []
    for idx, item in enumerate(test['directors_ko2']):
        wow = []
        test['directors_ko2'][idx] = wow
        for it in item:
            it = it.strip(" ").strip("'")
            wow.append(it)
            itsKo.append(it)
    dKo = itsKo
    processed_dKo = set(dKo)
    return processed_dKo

def processing_genre(movie_list):
    import pandas as pd
    df = movie_list.where((pd.notnull(movie_list)), None)
    df['movieCd'] = df['movieCd'].astype('int').astype('str')
    df['prdtYear'] = df['prdtYear'].astype('int').astype('str')
    df['openDt'] = df['openDt'].astype('int').astype('str')
    genres = df['repGenreNm']
    processed_genres = set(genres)
    return processed_genres

def processing_grade(movie_detail):
    import pandas as pd
    df = movie_detail
    df['watchGradeNm2'] = df['watchGradeNm'].str.strip("[]")
    df['watchGradeNm2'] = df['watchGradeNm2'].map(lambda x : x.split(","))
    itsGr = []
    for idx, item in enumerate(df['watchGradeNm2']):
        wow = []
        df['watchGradeNm2'][idx] = wow
        for it in item:
            it = it.strip(" ").strip("'")
            wow.append(it)
            itsGr.append(it)
    grade = itsGr
    processed_gr = set(grade)
    processed_gr.remove('watchGradeNm')
    return processed_gr

def processing_nation(movie_list):
    import pandas as pd
    df = movie_list.where((pd.notnull(movie_list)), '-')
    df['movieCd'] = df['movieCd'].astype('int').astype('str')
    df['prdtYear'] = df['prdtYear'].astype('int').astype('str')
    df['openDt'] = df['openDt'].astype('int').astype('str')
    nation = df['repNationNm']
    processed_nation = set(nation)
    return processed_nation

def processing_showTm(movie_detail):
    import pandas as pd
    processed_showTm = []
    df = movie_detail
    df = df.fillna(0)
    df = df.drop(df.loc[df['showTm'] == 'showTm'].index)
    df['showTm'] = df['showTm'].astype('float').astype('int64')
    processed_df = df[['movieCd', 'showTm']]
    processed_df = processed_df.values.tolist()
    processed_showTm.append(processed_df)
    return processed_showTm

def processing_actor_match(movie_detail, all_actors):
    import pandas as pd
    test = movie_detail
    test['actors_ko'] = test['actors_ko'].str.strip("[]")
    test['actors_ko2'] = test['actors_ko'].map(lambda x : x.split(","))
    itsaKo = []
    for idx, item in enumerate(test['actors_ko2']):
        wow = []
        test['actors_ko2'][idx] = wow
        for it in item:
            it = it.strip(" ").strip("'")
            wow.append(it)
            itsaKo.append(it)
    repo = [] # 각 행의 영화배우를 저장할 임시 저장소
    actor_matching = [] # 임시저장소에 있는 정보를 movieCd와 배우명을 매칭시켜 넣는 저장소
    movie_order = 0
    for movie in test['movieCd']:
        if movie == test.iloc[movie_order]['movieCd']:
            for actor in test.iloc[movie_order]['actors_ko2']:
                repo = [movie, actor]
                actor_matching.append(repo)
            movie_order += 1
        else:
            continue
    aDf = pd.DataFrame(actor_matching, columns=['movieCd', 'peopleNm'])
    actor_list = list(all_actors[0])
    actor_df = pd.DataFrame(actor_list, columns=['actorCd', 'peopleNm'])
    result_actor = pd.merge(aDf, actor_df, on='peopleNm')
    result_actor2 = result_actor.drop(result_actor.loc[result_actor['movieCd'] == 'movieCd'].index)
    result_actor2.drop_duplicates(keep = 'first', inplace = True)
    aMatch_result_list = result_actor2.values.tolist()
    return aMatch_result_list

def processing_director_match(movie_detail, all_directors):
    import pandas as pd
    test = movie_detail
    test['directors_ko'] = test['directors_ko'].str.strip("[]")
    test['directors_ko2'] = test['directors_ko'].map(lambda x : x.split(","))
    itsKo = []
    for idx, item in enumerate(test['directors_ko2']):
        wow = []
        test['directors_ko2'][idx] = wow
        for it in item:
            it = it.strip(" ").strip("'")
            wow.append(it)
            itsKo.append(it)
    repo = []
    director_matching = []
    movie_order = 0
    for movie in test['movieCd']:
        if movie == test.iloc[movie_order]['movieCd']:
            for director in test.iloc[movie_order]['directors_ko2']:
                repo = [movie, director]
                director_matching.append(repo)
            movie_order += 1
        else:
            continue
    df = pd.DataFrame(director_matching, columns=['movieCd', 'peopleNm'])
    director_list = list(all_directors[0])
    director_df = pd.DataFrame(director_list, columns=['directorCd', 'peopleNm'])
    result = pd.merge(df, director_df, on='peopleNm')
    result = result.drop(result.loc[result['movieCd'] == 'movieCd'].index)
    result.drop_duplicates(keep = 'first', inplace = True)
    dMatch_result_list = result.values.tolist()
    return dMatch_result_list

def processing_genre_match(movie_list, all_genres):
    import pandas as pd
    movie_list_result = movie_list.where((pd.notnull(movie_list)), None)
    movie_list_result['movieCd'] = movie_list_result['movieCd'].astype('int').astype('str')
    movie_list_result['prdtYear'] = movie_list_result['prdtYear'].astype('int').astype('str')
    movie_list_result['openDt'] = movie_list_result['openDt'].astype('int').astype('str')
    test = movie_list_result
    test["genreAlt2"] = test["genreAlt"].map(lambda x : x.split(","))
    repo = []
    genre_matching = []
    movie_order = 0
    for movie in test['movieCd']:
        if movie == test.iloc[movie_order]['movieCd']:
            for genre in test.iloc[movie_order]['genreAlt2']:
                repo = [movie, genre]
                genre_matching.append(repo)
                continue
            movie_order += 1
        else:
            continue
    df = pd.DataFrame(genre_matching, columns=['movieCd', 'genreNm'])
    genre_list = list(all_genres[0])
    genre_df = pd.DataFrame(genre_list, columns=['genreCd', 'genreNm'])
    result = pd.merge(df, genre_df, on='genreNm')
    grMatch_result_list = result.values.tolist()
    return grMatch_result_list

def processing_grade_match(movie_detail, all_grades):
    import pandas as pd
    df = movie_detail
    df['watchGradeNm2'] = df['watchGradeNm'].str.strip("[]")
    df['watchGradeNm2'] = df['watchGradeNm2'].map(lambda x : x.split(","))
    itsGr = []
    for idx, item in enumerate(df['watchGradeNm2']):
        wow = []
        df['watchGradeNm2'][idx] = wow
        for it in item:
            it = it.strip(" ").strip("'")
            wow.append(it)
            itsGr.append(it)
    repo = []
    grade_matching = []
    movie_order = 0
    for movie in df['movieCd']:
        if movie == df.iloc[movie_order]['movieCd']:
            for grade in df.iloc[movie_order]['watchGradeNm2']:
                repo = [movie, grade]
                grade_matching.append(repo)
            movie_order += 1
        else:
            continue
    df2 = pd.DataFrame(grade_matching, columns=['movieCd', 'watchGradeNm'])
    grade_list = list(all_grades[0])
    grade_df = pd.DataFrame(grade_list, columns=['watchGradeCd', 'watchGradeNm'])
    result = pd.merge(df2, grade_df, on='watchGradeNm')
    result.drop_duplicates(keep = 'first', inplace = True)
    gdMatch_result_list = result.values.tolist()
    return gdMatch_result_list

def processing_nation_match(movie_list, all_nation):
    import pandas as pd
    movie_list_result = movie_list.where((pd.notnull(movie_list)), None)
    movie_list_result['movieCd'] = movie_list_result['movieCd'].astype('int').astype('str')
    movie_list_result['prdtYear'] = movie_list_result['prdtYear'].astype('int').astype('str')
    movie_list_result['openDt'] = movie_list_result['openDt'].astype('int').astype('str')
    test = movie_list_result
    test["nationAlt2"] = test["nationAlt"].map(lambda x : x.split(","))
    repo = []
    nation_matching = []
    movie_order = 0
    for movie in test['movieCd']:
        if movie == test.iloc[movie_order]['movieCd']:
            for nation in test.iloc[movie_order]['nationAlt2']:
                repo = [movie, nation]
                nation_matching.append(repo)
                continue
            movie_order += 1
        else:
            continue
    df = pd.DataFrame(nation_matching, columns=['movieCd', 'nationNm'])
    nation_list = list(all_nation[0])
    nation_df = pd.DataFrame(nation_list, columns=['nationCd', 'nationNm'])
    result = pd.merge(df, nation_df, on='nationNm')
    nMatch_result_list = result.values.tolist()
    return nMatch_result_list





def access_database():
    import pymysql
    conn = pymysql.connect(host="localhost",
                       database="firstkino",
                       user="kdigital",
                       password="mysql",
                       charset="utf8")

    return conn

def input_movie_data(movie_data):
    import pymysql
    conn = access_database()
    with conn.cursor() as cursor:
        for post in movie_data:
            cursor.execute("INSERT INTO movie (movieCd, movieNm, movieNmEn, prdtYear, openDt) values (%s, %s, %s, %s, %s)", (post[0], post[2], post[3], post[4], post[5]))
        conn.commit()
    conn.close()

def input_actor_data(processed_aKo):
    import pymysql
    conn = access_database()
    with conn.cursor() as cursor:
        for post in processed_aKo:
            cursor.execute("INSERT INTO Actor (peopleNm) values (%s)", (post))
        conn.commit()
    conn.close()

def input_director_data(processed_dKo):
    import pymysql
    conn = access_database()
    with conn.cursor() as cursor:
        for post in processed_dKo:
            cursor.execute("INSERT INTO Director (peopleNm) values (%s)", (post))
        conn.commit()
    conn.close()

def input_genre_data(processed_genres):
    import pymysql
    conn = access_database()
    with conn.cursor() as cursor:
        for post in processed_genres:
            cursor.execute("INSERT INTO Genre (genreNm) values (%s)", (post))
        conn.commit()
    conn.close()

def input_grade_data(processed_gr):
    import pymysql
    conn = access_database()
    with conn.cursor() as cursor:
        for post in processed_gr:
            cursor.execute("INSERT INTO WatchGrade (watchGradeNm) values (%s)", (post))
        conn.commit()
    conn.close()


def input_nation_data(processed_nation):
    import pymysql
    conn = access_database()
    with conn.cursor() as cursor:
        for post in processed_nation:
            cursor.execute("INSERT INTO Nation (nationNm) values (%s)", (post))
        conn.commit()
    conn.close()

def input_showTm(processed_showTm):
    import pymysql
    conn = access_database()
    not_found_list = []
    with conn.cursor() as cursor:
        for idx, post in enumerate(processed_showTm[0]): # enumerate() 함수는 순서형 자료형의 인덱스와 값을 전달하는 기능하는 함수
            cursor.execute("SELECT showTm FROM movie WHERE movieCd=%s", (post[0]))
            selected_showtime = cursor.fetchall() # 위 코드를 실행하면 SELECT문을 실행하여 가져온 데이터가 selected_scores에 저장
            if len(selected_showtime) == 0:
                not_found_list.append(post)
            else:
                showtime =  selected_showtime[0][0] if selected_showtime[0][0] != None else post[1]
                cursor.execute("UPDATE movie set showTm = %s WHERE movieCd = %s", (showtime, post[0]))
    conn.commit()
    conn.close()

def input_actor_match(aMatch_result_list):
    import pymysql
    conn = access_database()
    with conn.cursor() as cursor:
        for post in aMatch_result_list:
            cursor.execute("INSERT INTO MovieActor (movieCd, actorCd) values (%s, %s)", (post[0], post[2]))
        conn.commit()
    conn.close()

def input_director_match(dMatch_result_list):
    import pymysql
    conn = access_database()
    with conn.cursor() as cursor:
        for post in dMatch_result_list:
            cursor.execute("INSERT INTO MovieDirector (movieCd, directorCd) values (%s, %s)", (post[0], post[2]))
        conn.commit()
    conn.close()

def input_genre_match(grMatch_result_list):
    import pymysql
    conn = access_database()
    with conn.cursor() as cursor:
        for post in grMatch_result_list:
            cursor.execute("INSERT INTO MovieGenre (movieCd, genreCd) values (%s, %s)", (post[0], post[2]))
        conn.commit()
    conn.close()

def input_grade_match(gdMatch_result_list):
    import pymysql
    conn = access_database()
    not_found_list = []
    with conn.cursor() as cursor:
        for idx, post in enumerate(gdMatch_result_list): # enumerate() 함수는 순서형 자료형의 인덱스와 값을 전달하는 기능하는 함수
            cursor.execute("SELECT watchGradeCd FROM movie WHERE movieCd=%s", (post[0]))
            selected_grade = cursor.fetchall() # 위 코드를 실행하면 SELECT문을 실행하여 가져온 데이터가 selected_scores에 저장
            if len(selected_grade) == 0:
                not_found_list.append(post)
            else:
                watchGrade =  selected_grade[0][0] if selected_grade[0][0] != None else post[2]
                cursor.execute("UPDATE movie set watchGradeCd = %s WHERE movieCd = %s", (watchGrade, post[0]))
    conn.commit()
    conn.close()

def input_nation_match(nMatch_result_list):
    import pymysql
    conn = access_database()
    with conn.cursor() as cursor:
        for post in nMatch_result_list:
            cursor.execute("INSERT INTO MovieNation (movieCd, nationCd) values (%s, %s)", (post[0], post[2]))
        conn.commit()
    conn.close()

def input_naver_score(scores):
    import pymysql
    conn = access_database()
    not_found_list = []
    with conn.cursor() as cursor:
        for idx, post in enumerate(scores): # enumerate() 함수는 순서형 자료형의 인덱스와 값을 전달하는 기능하는 함수
            cursor.execute("SELECT naverScore FROM movie WHERE movieNm=%s", (post[0]))
            selected_scores = cursor.fetchall() # 위 코드를 실행하면 SELECT문을 실행하여 가져온 데이터가 selected_scores에 저장
            if len(selected_scores) == 0:   # naverScore에는 존재하지만 movie_list에는 없는 영화를 저장
                not_found_list.append(post)
            else:                           # 영화가 movie_list에 존재하면 naverScore를 넣는다.
                naverScore =  selected_scores[0][0] if selected_scores[0][0] != None else post[1]
                cursor.execute("UPDATE movie set naverScore = %s WHERE movieNm = %s", (naverScore, post[0]))
    conn.commit()
    conn.close()

def input_shortContent(stories):
    import pymysql
    conn = access_database()
    not_found_list = []
    with conn.cursor() as cursor:
        for idx, post in enumerate(stories): # enumerate() 함수는 순서형 자료형의 인덱스와 값을 전달하는 기능하는 함수
            cursor.execute("SELECT shortContent FROM movie WHERE movieNm=%s", (post[0]))
            selected_stories = cursor.fetchall() # 위 코드를 실행하면 SELECT문을 실행하여 가져온 데이터가 selected_scores에 저장
            if len(selected_stories) == 0:
                not_found_list.append(post)
            else:
                naverStory =  selected_stories[0][0] if selected_stories[0][0] != None else post[1]
                cursor.execute("UPDATE movie set shortContent = %s WHERE movieNm = %s", (naverStory, post[0]))
    conn.commit()
    conn.close()




def load_actor_from_database():
    import pymysql
    conn = access_database()
    all_actors = []
    with conn.cursor() as cursor:
        cursor.execute("SELECT actorCd, peopleNm FROM Actor")
        actors = cursor.fetchall()
        all_actors.append(actors)
    conn.commit()
    conn.close()
    return all_actors

def load_director_from_database():
    import pymysql
    conn = access_database()
    all_directors = []
    with conn.cursor() as cursor:
        cursor.execute("SELECT directorCd, peopleNm FROM Director")
        directors = cursor.fetchall()
        all_directors.append(directors)
    conn.commit()
    conn.close()
    return all_directors

def load_genre_from_database():
    import pymysql
    conn = access_database()
    all_genres = []
    with conn.cursor() as cursor:
        cursor.execute("SELECT * FROM Genre")
        genres = cursor.fetchall()
        all_genres.append(genres)
    conn.commit()
    conn.close()
    return all_genres

def load_grade_from_database():
    import pymysql
    conn = access_database()
    all_grades = []
    with conn.cursor() as cursor:
        cursor.execute("SELECT watchGradeCd, watchGradeNm FROM WatchGrade")
        grades = cursor.fetchall()
        all_grades.append(grades)
    conn.commit()
    conn.close()
    return all_grades

def load_nation_from_database():
    import pymysql
    
    conn = access_database()

    all_nation = []

    with conn.cursor() as cursor:
        cursor.execute("SELECT * FROM Nation")
        nations = cursor.fetchall()
        all_nation.append(nations)
    conn.commit()
    conn.close()
    return all_nation

if __name__ == "__main__":
    # 1. movie_list_result.csv, movie_info_result.csv 파일을 읽어들이는 작업
    movie_list = load_movie_list()
    movie_detail = load_movie_detail()
    print("Process 1 Completed")

    # 2. DB에 데이터를 넣기 전 전처리하는 작업
    movie_data = processing_movie_data(movie_list)
    processed_aKo = processing_actor(movie_detail)
    processed_dKo = processing_director(movie_detail)
    processed_genres = processing_genre(movie_list)
    processed_gr = processing_grade(movie_detail)
    processed_nation = processing_nation(movie_list)
    processed_showTm = processing_showTm(movie_detail)
    scores = load_processing_naver_score()
    stories = load_processing_shortContent()
    print("Process 2 Completed")

    input_movie_data(movie_data)
    input_actor_data(processed_aKo)
    input_director_data(processed_dKo)
    input_genre_data(processed_genres)
    input_grade_data(processed_gr)
    input_nation_data(processed_nation)
    input_showTm(processed_showTm)
    input_naver_score(scores)
    input_shortContent(stories)
    print("Process 3 Completed")

    all_actors = load_actor_from_database()
    all_directors = load_director_from_database()
    all_genres = load_genre_from_database()
    all_grades = load_grade_from_database()
    all_nation = load_nation_from_database()
    print("Process 4 Completed")

    aMatch_result_list = processing_actor_match(movie_detail, all_actors)
    dMatch_result_list = processing_director_match(movie_detail, all_directors)
    grMatch_result_list = processing_genre_match(movie_list, all_genres)
    gdMatch_result_list = processing_grade_match(movie_detail, all_grades)
    nMatch_result_list = processing_nation_match(movie_list, all_nation)
    print("Process 5 Completed")
    
    input_actor_match(aMatch_result_list)
    input_director_match(dMatch_result_list)
    input_genre_match(grMatch_result_list)
    input_grade_match(gdMatch_result_list)
    input_nation_match(nMatch_result_list)
    print("Data Input Process Completed")
