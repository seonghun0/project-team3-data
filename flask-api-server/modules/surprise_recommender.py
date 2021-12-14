def surprise_recommender():
    from surprise import SVD,Dataset

    # 내장 데이터인 무비렌즈 데이터 로드하고 학습/테스트 데이터로 분리
    data = Dataset.load_builtin('ml-100k')
    train = data.build_full_trainset()

    algo = SVD(n_factors=200, n_epochs=20, lr_all=0.06, reg_all=0.01,random_state=42)
    algo.fit(train)

    with open('algo.pickle','wb') as f:
        pickle.dump(algo, f)

def movies():
    import pymysql
    import pandas as pd
    import pickle
    # 1. connect
    conn = pymysql.connect(host="3.38.186.130", port=3306, db="finalteam3", user="kdigital3", password="mysql", charset="utf8")

    # 2. get command object
    cursor = conn.cursor()
    # movie데이터 가져오기

    # 3. execute command
    sql = """select title, movie_id, vote_average
            from movie"""
    cursor.execute(sql)

    rows = cursor.fetchall()
    movies = pd.DataFrame(rows, columns=['title','movie_id','vote_average'])

    # 4. close resource
    cursor.close()
    conn.close()

    with open('movies.pickle','wb') as f:
        pickle.dump(movies, f)

def ratings():
    import pymysql
    import pandas as pd
    import pickle
    # ratings 가져오기 
    # 1. connect
    conn = pymysql.connect(host="3.38.186.130", port=3306, db="finalteam3", user="kdigital3", password="mysql", charset="utf8")

    # 2. get command object
    cursor = conn.cursor()

    # 3. execute command
    sql = """select member_id, movie_id, rating
            from rating"""
    cursor.execute(sql)

    rows = cursor.fetchall()
    ratings = pd.DataFrame(rows, columns=['member_id','movie_id','rating'])

    # 4. close resource
    cursor.close()
    conn.close()

    def scale(x):
        r = x['rating']
        return (r / 2)

    ratings['rating'] = ratings.apply(scale, axis=1)

    with open('ratings.pickle','wb') as f:
        pickle.dump(ratings, f)

def get_unseen_surprise(ratings, movies, member_id):
    import pickle
    # 특정 유저가 본 movie id들을 리스트로 할당
    seen_movies = ratings[ratings['member_id']==member_id]['movie_id'].tolist()
    # 모든 영화들의 movie id들 리스트로 할당
    total_movies = movies['movie_id'].tolist()
    
    # 모든 영화들의 movie id들 중 특정 유저가 본 movie id를 제외한 나머지 추출
    unseen_movies = [movie for movie in total_movies if movie not in seen_movies]
    
    with open('unseen_movies.pickle', 'wb') as f:
        pickle.dump(unseen_movies, f)
    return unseen_movies
     
def recomm_movie_by_surprise(algo, userId, unseen_movies, movies, top_n=10):
    import pandas as pd
    # 알고리즘 객체의 predict()를 이용해 특정 userId의 평점이 없는 영화들에 대해 평점 예측
    predictions = [algo.predict(str(userId), str(movieId)) for movieId in unseen_movies]

    # predictions는 Prediction()으로 하나의 객체로 되어있기 때문에 예측평점(est값)을 기준으로 정렬해야함
    # est값을 반환하는 함수부터 정의. 이것을 이용해 리스트를 정렬하는 sort()인자의 key값에 넣어주자!
    def sortkey_est(pred):
        return pred.est
    
    # sortkey_est함수로 리스트를 정렬하는 sort함수의 key인자에 넣어주자
    # 리스트 sort는 디폴트값이 inplace=True인 것처럼 정렬되어 나온다. reverse=True가 내림차순
    predictions.sort(key=sortkey_est, reverse=True)
    # 상위 n개의 예측값들만 할당
    top_predictions = predictions[:top_n]
    
    # top_predictions에서 movie id, rating, movie title 각 뽑아내기
    top_movie_ids = [int(pred.iid) for pred in top_predictions]
    top_movie_ratings = [pred.est*2 for pred in top_predictions]
    top_movie_titles = movies[movies.movie_id.isin(top_movie_ids)]['title']
    # 위 3가지를 튜플로 담기
    # zip함수를 사용해서 각 자료구조(여기선 리스트)의 똑같은 위치에있는 값들을 mapping
    # zip함수는 참고로 여러개의 문자열의 똑같은 위치들끼리 mapping도 가능!
    top_movie_preds = [(ids, rating, title) for ids, rating, title in zip(top_movie_ids, top_movie_ratings, top_movie_titles)]
    movies = pd.DataFrame(top_movie_preds, columns=['movie_id', 'score', 'title'])
    js = movies.to_json(orient = 'records', force_ascii=False)
    
    return js

if __name__ == "__main__":
    import pickle
    surprise_recommender()

    with open('movies.pickle', "rb") as f:
        movies = pickle.load(f)

    with open('ratings.pickle', "rb") as f:
        ratings = pickle.load(f)

    with open('algo.pickle', "rb") as f:
        algo = pickle.load(f)

    ### 위에서 정의한 함수를 사용해 특정 유저의 추천 영화들 출력해보기
    unseen_lst = get_unseen_surprise(ratings, movies, 'admin1234')

    with open('unseen_movies.pickle', "rb") as f:
        unseen_movies = pickle.load(f)

    top_movies_preds = recomm_movie_by_surprise(algo, 'admin1234', unseen_lst,movies, top_n=10)
    print()
    print('#'*8,'Top-10 추천영화 리스트','#'*8)

    # top_movies_preds가 여러가지의 튜플을 담고 있는 리스트이기 때문에 반복문 수행
    for top_movie in top_movies_preds:
        print('* 추천 영화 이름: ', top_movie[2])
        print('* 해당 영화의 예측평점: ', top_movie[1])
        print()

    unseen_lst = get_unseen_surprise(ratings, movies, 'iamuser')
    top_movies_preds = recomm_movie_by_surprise(algo, 'iamuser', unseen_lst, movies,
                                           top_n=10)
    print()
    print('#'*8,'Top-10 추천영화 리스트','#'*8)

    # top_movies_preds가 여러가지의 튜플을 담고 있는 리스트이기 때문에 반복문 수행
    for top_movie in top_movies_preds:
        print('* 추천 영화 이름: ', top_movie[2])
        print('* 해당 영화의 예측평점: ', top_movie[1])
        print()


