def genre_recommendation_movies(output_path):

    import pandas as pd
    import numpy as np
    import warnings; warnings.filterwarnings('ignore') # 경고 메세지 숨기기
    from ast import literal_eval # 딕셔너리 형태의 문자열을 딕셔너리로 변경
    from sklearn.feature_extraction.text import CountVectorizer # 단어 들의 카운트(출현 빈도(frequency))로 여러 문서들을 벡터화
    from sklearn.metrics.pairwise import cosine_similarity # 코사인 유사도

    # crawling 된 파일 가져오기
    movies=pd.read_csv("../data-files/recommendation/total_tmdbmovielist_new.csv")
    print(movies.shape)
    movies.head(1)

    # # null 값 체크
    movies.isnull().sum()

    # 필요한 칼럼 추출
    movies1 = movies.dropna(subset=['id', 'title', 'genres', 'vote_average', 'vote_count'])

    # 유사도 측정시 데이터양이 많아서 오류가 발생하여 60만개 -> 30만개(0초과) -> 3만개(10초과) -> 만개(100초과)로 수정하여 작업진행함
    movies2=movies1[movies1['vote_count']>100]
    movies2.shape

    # 주요 컬럼 추출
    movies3_df=movies2[['id', 'title', 'genres', 'vote_average', 'vote_count']]
    movies3_df.head(5)

    # 컬럼 길이 100으로 세팅
    pd.set_option('max_colwidth', 100)
    movies3_df[['genres']][:1]

    # apply()에 literal_eval 함수를 적용해 문자열을 객체로 변경
    movies3_df['genres']=movies3_df['genres'].apply(literal_eval)
    movies3_df.head(1)

    # apply lambda를 이용하여 리스트 내 여러 개의 딕셔너리의 'name' 키 찾아 리스트 객체로 변환.
    movies3_df['genres']=movies3_df['genres'].apply(lambda x : [ y['name'] for y in x])
    movies3_df[['genres']][:1]

    # 장르 확인
    movies3_df[['genres']]

    # CountVectorizer를 적용하기 위해 공백문자로 word 단위가 구분되는 문자열로 변환.
    movies3_df['genres_literal']=movies3_df['genres'].apply(lambda x : (' ').join(x))

    # min_df는 너무 드물게로 나타나는 용어를 제거하는 데 사용. min_df = 0.01은 "문서의 1 % 미만"에 나타나는 용어를 무시한다. 
    # ngram_range는 n-그램 범위.
    count_vect=CountVectorizer(min_df=0, ngram_range=(1, 2))
    genre_mat=count_vect.fit_transform(movies3_df['genres_literal'])
    print(genre_mat.shape)

    # 유사도 측정
    genre_sim=cosine_similarity(genre_mat, genre_mat)
    print(genre_sim.shape)
    print(genre_sim[:1])

    # [:, ::-1] axis = 1 기준으로 2차원 numpy 배열 뒤집기
    genre_sim_sorted_ind=genre_sim.argsort()[:, ::-1]
    print(genre_sim_sorted_ind[:1])

    # 장르 콘텐츠 필터링 영화 추천
    movies3_df[['title', 'vote_average', 'vote_count']].sort_values('vote_average', ascending=False)[:10]

    # 가중평점 계산
    # 가중 평점(Weighted Rating) = (v/(v+m)) * R + (m/(v+m)) * C
    # v : 영화에 평가를 매긴 횟수(movie_df의 'vote_count')
    # m : 평점을 부여하기 위한 최소 평가 수(movies_df['vote_count'].quantile(0.6) - 전체 투표 수에서 상위 60%의 횟수를 기준)
    # R : 영화의 평균 평점(movie_df의 'vote_average')
    # C : 전체 영화의 평균 평점(movie_df['vote_average'].mean())

    def weighted_vote_average(record):
        v = record['vote_count']  # 영화에 평가를 매긴 횟수
        R = record['vote_average']  # 영화의 평균 평점
        return ( (v/(v+m)) * R ) + ( (m/(m+v)) * C )  # 가중 평점 계산 식

        movies3_df['weighted_vote'] = movies.apply(weighted_vote_average, axis=1)

        movies3_df[['title', 'weighted_vote', 'vote_count']].sort_values('weighted_vote', ascending=False)[:10]

    def find_sim_movie(df, sorted_ind, title_name, top_n=10):
        title_movie = df[df['title'] == title_name]
        title_index = title_movie.index.values

        # top_n의 2배에 해당하는 장르 유사성이 높은 인덱스 추출
        similar_indexes = sorted_ind[title_index, :(top_n*2)]
        # reshape(-1) 1차열 배열 반환
        similar_indexes = similar_indexes.reshape(-1)
        # 기준 영화 인덱스는 제외
        similar_indexes = similar_indexes[similar_indexes != title_index]

        # top_n의 2배에 해당하는 후보군에서 weighted_vote가 높은 순으로 top_n만큼 추출
        return df.iloc[similar_indexes].sort_values('weighted_vote', ascending=False)[:top_n]
        similar_movies=find_sim_movie(movies3_df, genre_sim_sorted_ind, '쇼생크 탈출', 241)
        similar_movies[['title', 'vote_count', 'weighted_vote']]

if __name__ == "__main__":
    pass