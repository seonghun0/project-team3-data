<<<<<<< HEAD
<<<<<<< HEAD
def genrecommend(title,top=30):
    import pandas as pd
    import numpy as np
    import os
    import pymysql

    # %%
    # 1. connect
    conn = pymysql.connect(host="127.0.0.1", port=3306, db='finalteam3', user="mysql", password="mysql", charset="utf8")

    # 2. get command object
    cursor = conn.cursor()

    # 3. execute command
    sql = """select m.movie_id, vote_average, vote_count, popularity,title, mg.genre_id, g.name
            from movie m
            join movie_genre mg on (m.movie_id = mg.movie_id)
            join genre g on(g.genre_id = mg.genre_id)
            where vote_count > 5"""
    cursor.execute(sql)
        
        # break

    conn.commit() # confirm previous execution

    # 4. close resource
    cursor.close()
    conn.close()

    # %%
    movies = []
    movies.append(cursor.fetchall())

    # %%
    m2 = []
    for m in movies:
        for a in m:
            m2.append(a)

    # %%
    df = pd.DataFrame(m2)

    # %%
    df

    # %%
    df['id'] = df[0]
    df['genre'] = df[6]
    df['vote_count'] = df[2]
    df['title'] = df[4]
    df['vote_average'] = df[1]

    # %%
    data = df[['id','genre','vote_count','title','vote_average']]
    data.head()

    # %%
    data["genre"] = data.groupby(by=["id", "title", "vote_count", "vote_average"])["genre"].transform(lambda row: ','.join(row))
    data.drop_duplicates(inplace=True)
    data['genre'] = data['genre'].map(lambda sc : ','.join(np.unique(sc.split(','))))
    data.shape

    # %%
    m = data['vote_count'].quantile(0)
    print(m)
    data = data.loc[data['vote_count']>= m]

    # %%
    data.shape

    # %%
    c = data['vote_average'].mean()
    print(c)

    # %%
    def weight_rating(x, m=m, c=c):
        v = x['vote_count']
        r = x['vote_average']

        return( v / (v+m) *r) + (m/(v+m)*c)

    # %%
    data['score'] = data.apply(weight_rating, axis=1)

    # %%
    data.shape

    # %%
    from sklearn.feature_extraction.text import CountVectorizer

    # %%
    counter = CountVectorizer(ngram_range=(1,3))

    # %%
    data['genre']=data['genre'].astype(str)

    # %%
    c_count_genres = counter.fit_transform(data['genre'])

    # %%
    c_count_genres

    # %%
    from sklearn.metrics.pairwise import cosine_similarity

    similarity_genre = cosine_similarity(c_count_genres, c_count_genres).argsort()[:,::-1]
    print(similarity_genre)

    # %%
    similarity_genre.shape

    # %%
    # ???????????? ?????? ????????????
    target_movie_index = data[data['title'] == title].index.values

    # ??????????????? ????????? ????????? ????????????
    sim_index = similarity_genre[target_movie_index, :(top*2)].reshape(-1)
    # ????????????
    sim_index = sim_index[sim_index != target_movie_index ]

    # ???????????? ????????? df??????, ????????????(score)?????? ??????
    result = data.iloc[sim_index].sort_values('score', ascending=False)[:10]
    print(result)
    return result


if __name__ == "__main__":
    genrecommend('????????????: ???????????? 4 ????????? ??????')
    pass
=======
=======
>>>>>>> 0db9c01b98edf7effa32e93df56269acd8360e23
def genre():
    print('hello')

if __name__ == "__main__":
    import genrerun
    x = int(input('?????? :'))
    genrerun.recommend(x, data, similarity_gernre)
<<<<<<< HEAD
>>>>>>> 0db9c01b98edf7effa32e93df56269acd8360e23
=======
>>>>>>> 0db9c01b98edf7effa32e93df56269acd8360e23
