def set_movie_data(): 
    import pandas as pd
    import numpy as np
    import os
    import pymysql
    import pickle

    # 1. connect
    conn = pymysql.connect(host="127.0.0.1", port=3306, db="finalteam3", user="kdigital", password="mysql", charset="utf8")

    # 2. get command object
    cursor = conn.cursor()

    # 3. execute command
    sql = """select m.movie_id, vote_average, vote_count, popularity,title, mg.genre_id, g.name, m.posterpath, m.release_date
            from movie m
            join movie_genre mg on (m.movie_id = mg.movie_id)
            join genre g on(g.genre_id = mg.genre_id)
            where vote_count > 4"""
    cursor.execute(sql)

    rows = cursor.fetchall()
    df = pd.DataFrame(rows, columns=['movie_id', 'vote_average','vote_count', 'popularity', 'title', 'genre_id', 'genre','posterpath','release_date'] )

    # 4. close resource
    cursor.close()
    conn.close()

    data = df[['movie_id','genre','vote_count','title','vote_average','posterpath','release_date']]

    data["genre"] = data.groupby(by=["movie_id", "title", "vote_count", "vote_average","posterpath","release_date"])["genre"].transform(lambda row: ','.join(row))
    data.drop_duplicates(inplace=True)
    data['genre'] = data['genre'].map(lambda sc : ','.join(np.unique(sc.split(','))))

    m = data['vote_count'].quantile(0)
    data = data.loc[data['vote_count']>= m]

    c = data['vote_average'].mean()

    def weight_rating(x, m=m, c=c):
        v = x['vote_count']
        r = x['vote_average']
        if x['release_date'] > '2018-01-01':
            a = 10
        elif x['release_date'] > '2015-01-01':
            a = 9
        elif x['release_date'] > '2011-01-01':
            a = 8
        elif x['release_date'] > '2007-01-01':
            a = 7
        elif x['release_date'] > '2003-01-01':
            a = 6
        elif x['release_date'] > '1999-01-01':
            a = 5
        else:
            a = 0

        return( v / (v+m) *r) + (m/(v+m)*c + a)

    data['score'] = data.apply(weight_rating, axis=1)

    with open('movie_data.pickle', "wb") as f:
        pickle.dump(data, f)

def train_recommender_system(data): 
    import os
    from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer

    counter = CountVectorizer(ngram_range=(1,3))

    data['genre']=data['genre'].astype(str)

    c_count_genres = counter.fit_transform(data['genre'])
    
    tfidf_transformer = TfidfTransformer()

    c_t_genres = tfidf_transformer.fit_transform(c_count_genres)

    from sklearn.metrics.pairwise import cosine_similarity

    similarity_genre = cosine_similarity(c_t_genres, c_t_genres).argsort()[:,::-1]
    similarity_genre2 = similarity_genre.astype('int32')
    import pickle

    with open('similarity_genre2.pickle', "wb") as f:
        pickle.dump(similarity_genre2, f, protocol=4)

    

def recommend_movies(target_movie_index, similarity_genre2, data, top):
    # 특정영화 정보 뽑아내기
    
    # target_movie_index = data[data['movie_id'] == title].index.values
    # print(data['movie_id'] == title)
    
    # 타겟영화와 비슷한 코사인 유사도값
    sim_index = similarity_genre2[target_movie_index, :(top*2)].reshape(-1)
    # 본인제외
    sim_index = sim_index[sim_index != target_movie_index ]

    # 추천결과 새로운 df생성, 평균평점(score)으로 정렬
    result = data.iloc[sim_index].sort_values('score', ascending=False)[:10]
    js = result.to_json(orient = 'records')

    return js

if __name__ == "__main__":

    import os
    import pickle
    
    if not os.path.exists("similarity_genre2.pickle"):
        train_recommender_system() 

    if not os.path.exists("movie_data.pickle") :
        set_movie_data()

    with open('movie_data.pickle', "rb") as f:
        data = pickle.load(f)
        data = data.reset_index(drop=True)

    with open('similarity_genre2.pickle', "rb") as f:
        similarity_genre2 = pickle.load(f)

    target_movie_index = data[data['movie_id'] == 363088].index.values
    recommended_movies = recommend_movies(target_movie_index, similarity_genre2, data, 30)

    print(recommended_movies)
    print('success')


