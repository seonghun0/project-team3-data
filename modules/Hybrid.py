def hybrid(userId, title):
    import pandas as pd
    import numpy as np
    import matplotlib.pyplot as plt
    import seaborn as sns
    from scipy import stats
    from ast import literal_eval
    from sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer
    from sklearn.metrics.pairwise import linear_kernel, cosine_similarity
    from nltk.stem.snowball import SnowballStemmer
    from nltk.stem.wordnet import WordNetLemmatizer
    from nltk.corpus import wordnet
    from surprise import Reader, Dataset, SVD, accuracy

    import warnings; warnings.simplefilter('ignore')

    md=pd.read_csv('data-files/recommendation/movies_metadata.csv')


    # apply()에 literal_eval 함수를 적용해 문자열을 객체로 변경
    md['genres']=md['genres'].apply(literal_eval)
    md.head(1)


    # apply lambda를 이용하여 리스트 내 여러 개의 딕셔너리의 'name' 키 찾아 리스트 객체로 변환.
    md['genres']=md['genres'].apply(lambda x : [ y['name'] for y in x])
    md[['genres']][:1]


    print('vote ::: \n', md[['vote_count', 'vote_average']].head())
    vote_counts = md[md['vote_count'].notnull()]['vote_count'].astype('int')
    vote_averages = md[md['vote_average'].notnull()]['vote_average'].astype('int')
    C = vote_averages.mean()
    C
    print(vote_counts.sort_values(ascending=False)[2273:2274])
    m = vote_counts.quantile(0.95)
    m

    print('release_date ::: \n', md['release_date'].head())

    # pd.to_datetime
    # errors : {‘ignore’, ‘raise’, ‘coerce’}, default ‘raise’
    # If ‘raise’, then invalid parsing will raise an exception
    # If ‘coerce’, then invalid parsing will be set as NaT
    # If ‘ignore’, then invalid parsing will return the input

    # 'release_date'를 split해서 year만 추출
    md['year'] = pd.to_datetime(md['release_date'], errors='coerce').apply(lambda x: str(x).split('-')[0] if x != np.nan else np.nan)

    print('year ::: \n', md['year'].head())


    # 평가 수가 상위 5%인(434보다 큰) 데이터 추출
    qualified = md[(md['vote_count'] >= m) & (md['vote_count'].notnull()) & (md['vote_average'].notnull())][['title', 'year', 'vote_count', 'vote_average', 'popularity', 'genres']]
    qualified['vote_count'] = qualified['vote_count'].astype('int')
    qualified['vote_average'] = qualified['vote_average'].astype('int')
    qualified.shape


    def weighted_rating(x):
        v = x['vote_count']
        R = x['vote_average']
        return (v/(v+m) * R) + (m/(m+v) * C)


    qualified['wr'] = qualified.apply(weighted_rating, axis=1)


    # Weighted Rating 상위 250개의 영화 
    qualified = qualified.sort_values('wr', ascending=False).head(250)


    # stack() : stack이 (위에서 아래로 길게, 높게) 쌓는 것이면, unstack은 쌓은 것을 옆으로 늘어놓는것(왼쪽에서 오른쪽으로 넓게) 라고 연상이 될 것
    # reset_index() : 기존의 행 인덱스를 제거하고 인덱스를 데이터 열로 추가
    s = md.apply(lambda x: pd.Series(x['genres']), axis=1).stack().reset_index(level=1, drop=True)
    s.name = 'genre'

    gen_md = md.drop('genres', axis=1).join(s)
    print(gen_md.head(10))


    def build_chart(genre, percentile=0.85):
        df = gen_md[gen_md['genre'] == genre]
        vote_counts = df[df['vote_count'].notnull()]['vote_count'].astype('int')
        vote_averages = df[df['vote_average'].notnull()]['vote_average'].astype('int')
        C = vote_averages.mean()
        m = vote_counts.quantile(percentile)
        
        qualified = df[(df['vote_count'] >= m) & (df['vote_count'].notnull()) & (df['vote_average'].notnull())][['title','year','vote_count','vote_average','popularity']]
        qualified['vote_count'] = qualified['vote_count'].astype('int')
        qualified['vote_average'] = qualified['vote_average'].astype('int')
        
        qualified['wr'] = qualified.apply(lambda x: (x['vote_count']/(x['vote_count']+m) * x['vote_average']) + (m/(m+x['vote_count']) * C), axis=1)
        qualified = qualified.sort_values('wr', ascending=False).head(250)
        
        return qualified

    links_small = pd.read_csv('data-files/recommendation/links_small.csv')
    links_small = links_small[links_small['tmdbId'].notnull()]['tmdbId'].astype('int')
    links_small.head()


    # Drop a row by index : 19730, 29503, 33587 행은 이상한 데이터들(md.iloc[19730], md.iloc[29503], md.iloc[33587])
    md = md.drop([19730, 29503, 35587])


    #Check EDA Notebook for how and why I got these indices.
    md['id'] = md['id'].astype('int')


    smd = md[md['id'].isin(links_small)]
    smd.shape


    smd['tagline'] = smd['tagline'].fillna('')
    smd['description'] = smd['overview'] + smd['tagline']
    smd['description'] = smd['description'].fillna('')

    smd['description'].head()


    tf = TfidfVectorizer(analyzer='word', ngram_range=(1, 2), min_df=0, stop_words='english')
    tfidf_matrix = tf.fit_transform(smd['description'])


    tfidf_matrix.shape


    # linear_kernel는 두 벡터의 dot product 이다.
    cosine_sim = linear_kernel(tfidf_matrix, tfidf_matrix)


    cosine_sim[0]


    smd = smd.reset_index()
    titles = smd['title']
    indices = pd.Series(smd.index, index=smd['title'])

    print(titles.head(), indices.head())

    def get_recommendations(title):
        idx = indices[title]
        sim_scores = list(enumerate(cosine_sim[idx]))
        sim_scores = sorted(sim_scores, key=lambda x: x[1], reverse=True)
        sim_scores = sim_scores[1:31]
        movie_indices = [i[0] for i in sim_scores]
        return titles.iloc[movie_indices]

    credits = pd.read_csv('data-files/recommendation/credits.csv')
    keywords = pd.read_csv('data-files/recommendation/keywords.csv')

    credits['crew'][0]

    keywords['id'] = keywords['id'].astype('int')
    credits['id'] = credits['id'].astype('int')
    md['id'] = md['id'].astype('int')

    md.shape

    md = md.merge(credits, on='id')
    md = md.merge(keywords, on='id')


    smd = md[md['id'].isin(links_small)]
    smd.shape


    smd['cast'] = smd['cast'].apply(literal_eval)
    smd['crew'] = smd['crew'].apply(literal_eval)
    smd['keywords'] = smd['keywords'].apply(literal_eval)
    smd['cast_size'] = smd['cast'].apply(lambda x: len(x))
    smd['crew_size'] = smd['crew'].apply(lambda x: len(x))


    def get_director(x):
        for i in x:
            if i['job'] == 'Director':
                return i['name']
        return np.nan


    smd['director'] = smd['crew'].apply(get_director)


    # 출연진 중 상위에 노출되는 3명만 추출
    smd['cast'] = smd['cast'].apply(lambda x: [i['name'] for i in x] if isinstance(x, list) else [])
    smd['cast'] = smd['cast'].apply(lambda x: x[:3] if len(x) >= 3 else x)


    smd['keywords'] = smd['keywords'].apply(lambda x: [i['name'] for i in x] if isinstance(x, list) else [])


    # 출연진의 이름에서 공백 삭제
    smd['cast'] = smd['cast'].apply(lambda x: [str.lower(i.replace(" ", "")) for i in x])


    # 감독의 이름에서 공백 삭제 및 3번 언급?
    smd['director'] = smd['director'].astype('str').apply(lambda x: str.lower(x.replace(" ", "")))
    smd['director'] = smd['director'].apply(lambda x: [x, x, x])


    s = smd.apply(lambda x: pd.Series(x['keywords']), axis=1).stack().reset_index(level=1, drop=True)
    s.name = 'keyword'


    s = s.value_counts()
    s[:5]


    # 2번 이상 등장한 키워드만 추출
    s = s[s > 1]


    # 어근 추출을 통해 동일 의미&다른 형태의 단어(dogs&dog, imaging&image 등)를 동일한 단어로 인식
    stemmer = SnowballStemmer('english')
    print("dogs의 어근 : ", stemmer.stem('dogs'))
    print("dog의 어근 : ", stemmer.stem('dog'))

    def filter_keywords(x):
        words = []
        for i in x:
            if i in s:
                words.append(i)
        return words

    # 키워드의 어근을 찾아서 공백 제거 후 세팅
    smd['keywords'] = smd['keywords'].apply(filter_keywords)
    smd['keywords'] = smd['keywords'].apply(lambda x: [stemmer.stem(i) for i in x])
    smd['keywords'] = smd['keywords'].apply(lambda x: [str.lower(i.replace(" ", "")) for i in x])

    smd['soup'] = smd['keywords'] + smd['cast'] + smd['director'] + smd['genres']
    smd['soup'] = smd['soup'].apply(lambda x: ' '.join(x))


    count = CountVectorizer(analyzer='word', ngram_range=(1,2), min_df=0, stop_words='english')
    count_matrix = count.fit_transform(smd['soup'])

    cosine_sim = cosine_similarity(count_matrix, count_matrix)

    smd = smd.reset_index()
    titles = smd['title']
    indices = pd.Series(smd.index, index=smd['title'])

    def improved_recommendations(title):
        print(title)
        idx = indices[title]
        print(idx)
        sim_scores = list(enumerate(cosine_sim[idx]))
        sim_scores = sorted(sim_scores, key=lambda x: x[1], reverse=True)
        sim_scores = sim_scores[1:26]
        movie_indices = [i[0] for i in sim_scores]
        print(movie_indices)

        movies = smd.iloc[movie_indices][['title','vote_count','vote_average','year']]
       
        vote_counts = movies[movies['vote_count'].notnull()]['vote_count'].astype('int')
        vote_averages = movies[movies['vote_average'].notnull()]['vote_average'].astype('int')
        C = vote_averages.mean()
        m = vote_counts.quantile(0.60)
        qualified = movies[(movies['vote_count'] >= m) & (movies['vote_count'].notnull())]

        qualified['vote_count'] = qualified['vote_count'].astype('int')
        qualified['wr'] = qualified.apply(weighted_rating, axis=1)
        qualified = qualified.sort_values('wr', ascending=False).head(10)
        print(qualified)
        return qualified
 ##############################################
    reader = Reader()

    ratings = pd.read_csv('data-files/recommendation/ratings_small.csv')
    ratings.head()

    data = Dataset.load_from_df(ratings[['userId', 'movieId','rating']], reader)
    # data.split(n_folds=5)

    trainset = data.build_full_trainset()
    testset = trainset.build_testset()

    svd = SVD()
    svd.fit(trainset)
    predictions = svd.test(testset)
    accuracy.rmse(predictions)

    ratings[ratings['userId'] == 1]

    svd.predict(1, 302, 3)

    def convert_int(x):
        try:
            return int(x)
        except:
            return np.nan

    id_map = pd.read_csv('data-files/recommendation/links_small.csv')[['movieId', 'tmdbId']]
    id_map['tmdbId'] = id_map['tmdbId'].apply(convert_int)
    id_map.columns = ['movieId', 'id']
    id_map = id_map.merge(smd[['title', 'id']], on='id').set_index('title')

    indices_map = id_map.set_index('id')

    def hybrid(userId, title):
        idx = indices[title]
        tmdbId = id_map.loc[title]['id']
        #print(idx)
        movie_id = id_map.loc[title]['movieId']
        
        sim_scores = list(enumerate(cosine_sim[int(idx)]))
        sim_scores = sorted(sim_scores, key=lambda x: x[1], reverse=True)
        sim_scores = sim_scores[1:26]
        movie_indices = [i[0] for i in sim_scores]
        
        movies = smd.iloc[movie_indices][['title', 'vote_count', 'vote_average', 'year', 'id']]
        movies['est'] = movies['id'].apply(lambda x: svd.predict(userId, indices_map.loc[x]['movieId']).est)
        movies = movies.sort_values('est', ascending=False)
        return movies.head(10)

if __name__ == "__main__":
   hybrid('500', 'Avatar')
   pass
