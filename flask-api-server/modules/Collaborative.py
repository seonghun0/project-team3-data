
def Collaborative(result):
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
    from surprise import Reader, Dataset, SVD, accuracy, KNNBasic
    import warnings; warnings.simplefilter('ignore')

    # surprise 라이브러리의 Reader
    reader = Reader()

    # 데이터 파일 불러와서 확인
    ratings = pd.read_csv('data-files/recommendation/ratings_small.csv')
    ratings.head()

    # 훈련데이터 나누기
    data = Dataset.load_from_df(ratings[['userId', 'movieId','rating']], reader)
    # data.split(n_folds=5)

    trainset = data.build_full_trainset()
    testset = trainset.build_testset()

    type(trainset), type(testset)

    testset[:5]

    trainset.n_users, trainset.n_items, trainset.n_ratings, trainset.n_users * trainset.n_items

    trainset.all_users(), trainset.all_items()

    [rating for rating in trainset.all_ratings()][:10]

    # 훈련하기

    svd = SVD()
    svd.fit(trainset)

    predictions = svd.test(testset)

    type(predictions)
    predictions[:5]

    testset[:5]

    print( accuracy.rmse(predictions) )
    print( accuracy.mae(predictions) )

    svd.predict('690', '431')

    ratings = pd.read_csv('data-files/recommendation/ratings1.csv')
    ratings.head()

    ratings["rating"].describe()

    ratings.to_csv('data-files/recommendation/ratings-noh.csv', header=False, index=False)

    reader = Reader(line_format="user item rating timestamp", sep=",", rating_scale=(0.5, 5))
    data2 = Dataset.load_from_file('data-files/recommendation/ratings-noh.csv', reader)

    data2

    from surprise.model_selection import train_test_split
    train_set, test_set = train_test_split(data)
    train_set2, test_set2 = train_test_split(data2, test_size=0.2, random_state=42)

    type(train_set2), type(test_set2)

    train_set2.n_users, train_set2.n_items

    svd2 = SVD(n_factors=50, random_state=42)

    svd2.fit(train_set2)

    predictions2 = svd2.test(test_set2)

    predictions2[:5]

    accuracy.rmse(predictions2), accuracy.mae(predictions2)

    svd2.predict(str(150), str(5000))

    movies = pd.read_csv('data-files/recommendation/moives.csv')
    movies.head()

    all_movies = movies["movieId"].values # 전체 영화 목록
    rated_movies = ratings[ratings["userId"] == 1]["movieId"].values # 1번 사용자가 평가한 영화 목록

    not_rated_movies = [ movie for movie in all_movies if movie not in rated_movies ] # 1번 사용자가 평가하지 않은 영화 목록

    len(all_movies), len(rated_movies), len(not_rated_movies)

    predictions3 = [ svd2.predict(str(1), str(movie)) for movie in not_rated_movies ]

    predictions3[:5]

    predictions3[0].est

    predictions3.sort(key=lambda p: p.est, reverse=True)

    top_10_rated_predictions = predictions3[:10]

    result = [ (p.est, movies[movies["movieId"] == int(p.iid)]["title"].values[0]) for p in top_10_rated_predictions ]
 
    print(result)
    return result

if __name__ == "__main__":
   Collaborative(10)
   pass
