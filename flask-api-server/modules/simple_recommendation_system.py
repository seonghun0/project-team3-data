def simple(genre, percentile=0.85):

    from IPython import get_ipython
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

    md=pd.read_csv("data-files/recommendation/movies_metadata.csv")
    print(md.shape)
    md.head(1)

    md.isnull().sum()

    # 컬럼 길이 100으로 세팅
    pd.set_option('max_colwidth', 100)
    md[['genres']][:1]

    # apply()에 literal_eval 함수를 적용해 문자열을 객체로 변경
    md['genres']=md['genres'].apply(literal_eval)
    md.head(1)

    md['genres']=md['genres'].apply(lambda x : [ y['name'] for y in x])
    md[['genres']][:1]

    md[['genres']]

    print('vote ::: \n', md[['vote_count', 'vote_average']].head())
    vote_counts = md[md['vote_count'].notnull()]['vote_count'].astype('int')
    vote_averages = md[md['vote_average'].notnull()]['vote_average'].astype('int')
    C = vote_averages.mean()
    C

    # 총 45460개의 영화 중 상위 5%는 2273번째
    print(vote_counts.sort_values(ascending=False)[2273:2274])

    # quantile는 데이터를 크기대로 정렬하였을 때 분위수를 구하는 함수. quantile(0.95)는 상위 5%에 해당하는 값을 찾는 것
    m = vote_counts.quantile(0.95)
    m

    print('release_date ::: \n', md['release_date'].head())
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

    qualified = qualified.sort_values('wr', ascending=False).head(250)


    s = md.apply(lambda x: pd.Series(x['genres']), axis=1).stack().reset_index(level=1, drop=True)
    s.name = 'genre'
    print(s.head(10))

    gen_md = md.drop('genres', axis=1).join(s)
    print(gen_md.head(10))

    df = gen_md[gen_md['genre'] == genre]
    vote_counts = df[df['vote_count'].notnull()]['vote_count'].astype('int')
    vote_averages = df[df['vote_average'].notnull()]['vote_average'].astype('int')
    C = vote_averages.mean()
    m = vote_counts.quantile(percentile)
    
    qualified = df[(df['vote_count'] >= m) & (df['vote_count'].notnull()) & (df['vote_average'].notnull())][['title','year','vote_count','vote_average','popularity']]
    qualified['vote_count'] = qualified['vote_count'].astype('int')
    qualified['vote_average'] = qualified['vote_average'].astype('int')
    
    qualified['wr'] = qualified.apply(lambda x: (x['vote_count']/(x['vote_count']+m) * x['vote_average']) + (m/(m+x['vote_count']) * C), axis=1)
    result = qualified.sort_values('wr', ascending=False).head(250)
 
    print(result)
    return result

if __name__ == "__main__":
   simple('Family').head(15)
   pass