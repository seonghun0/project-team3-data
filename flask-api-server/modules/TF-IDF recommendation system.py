def TFIDF(sim_scores):

    import pandas as pd
    import numpy as np
    import pickle
    from sklearn.feature_extraction.text import TfidfVectorizer # TF-IDF 방식으로 단어의 가중치를 조정한 BOW 인코딩 벡터를 만든다.

    movies=pd.read_csv("data-files/recommendation/total_tmdbmovielist_new.csv")
    print(movies.shape)
    movies.head(1)

    # null 값 체크
    movies.isnull().sum()

    movies1 = movies.dropna(subset=['id', 'title', 'genres', 'overview'])

    movies1.shape

    movies1.columns

    movies1 = movies1[movies1['overview'].notnull()].reset_index(drop=True)
    movies1.shape

    # 불용어 : 유의미하지 않은 단어 토큰을 제거
    tfidf = TfidfVectorizer(stop_words='english')
    # overview에 대해서 tf-idf 수행
    tfidf_matrix = tfidf.fit_transform(movies1['overview'])
    print(tfidf_matrix.shape)

    # 유사도 측정
    from sklearn.metrics.pairwise import cosine_similarity
    cosine_matrix = cosine_similarity(tfidf_matrix, tfidf_matrix)
    print(tfidf_matrix)
    print(tfidf_matrix[:1])

    np.round(cosine_matrix, 4)

    # movie title와 id를 매핑할 dictionary를 생성해줍니다. 
    movie2id = {}
    for i, c in enumerate(movies1['title']): movie2id[i] = c

    # id와 movie title를 매핑할 dictionary를 생성해줍니다. 
    id2movie = {}
    for i, c in movie2id.items(): id2movie[c] = i

    # 포레스트 검프의 id 추출 
    idx = id2movie['포레스트 검프'] # Toy Story : 0번 인덱스 
    sim_scores = [(i, c) for i, c in enumerate(cosine_matrix[idx]) if i != idx] # 자기 자신을 제외한 영화들의 유사도 및 인덱스를 추출 
    sim_scores = sorted(sim_scores, key = lambda x: x[1], reverse=True) # 유사도가 높은 순서대로 정렬 
    sim_scores[0:10] # 상위 10개의 인덱스와 유사도를 추출 

    # 인덱스를 Movie Title로 변환 
    sim_scores = [(movie2id[i], score) for i, score in sim_scores[0:10]]

    print(sim_scores)
    return sim_scores

if __name__ == "__main__":
   TFIDF("")

