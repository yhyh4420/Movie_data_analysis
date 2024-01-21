import pandas as pd
import numpy as np
from datetime import datetime, timedelta

import chardet

with open('C:\\Users\\yehun chang\\OneDrive\\바탕 화면\\빅데이터 예측분석\\2_기말고사 과제\\영화\\movie_info_3weeks.csv', 'rb') as f:
    result = chardet.detect(f.read())

movie_info = pd.read_csv('C:\\Users\\yehun chang\\OneDrive\\바탕 화면\\빅데이터 예측분석\\2_기말고사 과제\\영화\\movie_info_3weeks.csv', encoding=result['encoding'])

# review_timestamp 시계열 데이터로 변환 및 movie_info의 개봉일도 시계열 데이터로 변환
sent_raw = pd.read_csv('C:\\Users\\yehun chang\\OneDrive\\바탕 화면\\빅데이터 예측분석\\2_기말고사 과제\\영화\\movie_data_sent_raw.csv')
crawling_time_raw = '2023-11-25 16:00:00'
crawling_time = pd.to_datetime(crawling_time_raw)

sent_raw['review_timestamp'] = sent_raw['review_timestamp'].apply(lambda x: crawling_time - timedelta(hours=int(x.split('시간전')[0])) if '시간전' in x else pd.to_datetime(x))
movie_info['개봉일'] = pd.to_datetime(movie_info['개봉일'], format = '%Y%m%d')

# 감정을 수치로 변환하는 함수
def sentiment_to_numeric(sentiment):
    if sentiment == "긍정적":
        return 1
    elif sentiment == "부정적":
        return -1
    else:
        return 0

# 'sentiment' 열을 수치로 변환하여 'numeric_sentiment' 열 추가
sent_raw['numeric_sentiment'] = sent_raw['sentiment'].apply(sentiment_to_numeric)

# sent_raw와 movie_info를 'movie_title'을 기준으로 병합
merged_df = pd.merge(sent_raw, movie_info, left_on='movie_title', right_on='영화명', how='left')

# 각 영화 제목별로 평점 계산(기간 전체)
ratiing_by_movie = sent_raw.groupby('movie_title')['review_rating'].mean()
rating_tot = pd.DataFrame({
    'movie_title': ratiing_by_movie.index,
    'rating_total': ratiing_by_movie.values
})

# 개봉일 이전 리뷰에 대한 평점 계산
ratiing_before_release = merged_df[merged_df['review_timestamp'] < merged_df['개봉일']].groupby('movie_title')['review_rating'].mean()
ratiing_before_release_df = pd.DataFrame({
    'movie_title': ratiing_before_release.index,
    'rating_before_release': ratiing_before_release.values
})

# 개봉일 일주일 뒤까지의 평점 계산
ratiing_one_week_after_release = merged_df[merged_df['review_timestamp'] <= (merged_df['개봉일'] + pd.Timedelta(days = 7))].groupby('movie_title')['review_rating'].mean()
ratiing_one_week_after_release_df = pd.DataFrame({
    'movie_title': ratiing_one_week_after_release.index,
    'ratiing_one_week_after_release': ratiing_one_week_after_release.values
})

# 개봉일 이주일 뒤까지의 평점 계산
ratiing_two_week_after_release = merged_df[merged_df['review_timestamp'] <= (merged_df['개봉일'] + pd.Timedelta(days = 14))].groupby('movie_title')['review_rating'].mean()
ratiing_two_week_after_release_df = pd.DataFrame({
    'movie_title': ratiing_two_week_after_release.index,
    'ratiing_two_week_after_release': ratiing_two_week_after_release.values
})

# 각 영화 제목별로 positive ratio 계산(기간 전체)
positive_ratio_by_movie = sent_raw.groupby('movie_title')['numeric_sentiment'].mean()
positive_ratio = pd.DataFrame({
    'movie_title': positive_ratio_by_movie.index,
    'positive_ratio_total': positive_ratio_by_movie.values
})


# 개봉일 이전 리뷰에 대한 positive ratio 계산
positive_ratio_before_release = merged_df[merged_df['review_timestamp'] < merged_df['개봉일']].groupby('movie_title')['numeric_sentiment'].mean()
positive_ratio_before_release_df = pd.DataFrame({
    'movie_title': positive_ratio_before_release.index,
    'positive_ratio_before_release': positive_ratio_before_release.values
})

# 개봉일 일주일 뒤까지의 positive ratio 계산
positive_ratio_one_week_after_release = merged_df[merged_df['review_timestamp'] <= (merged_df['개봉일'] + pd.Timedelta(days=7))].groupby('movie_title')['numeric_sentiment'].mean()
positive_ratio_one_week_after_release_df = pd.DataFrame({
    'movie_title': positive_ratio_one_week_after_release.index,
    'positive_ratio_one_week_after_release': positive_ratio_one_week_after_release.values
})

# 개봉일 이주일 뒤까지의 positive ratio 계산
positive_ratio_two_week_after_release = merged_df[merged_df['review_timestamp'] <= (merged_df['개봉일'] + pd.Timedelta(days=14))].groupby('movie_title')['numeric_sentiment'].mean()
positive_ratio_two_week_after_release_df = pd.DataFrame({
    'movie_title': positive_ratio_two_week_after_release.index,
    'positive_ratio_two_week_after_release': positive_ratio_two_week_after_release.values
})

# 각 주차 데이터프레임들과 movie_info 병합
movie_sent_rating_tot = pd.merge(movie_info, positive_ratio, left_on='영화명', right_on='movie_title', how='outer')
movie_sent_rating_tot = movie_sent_rating_tot.drop('movie_title', axis = 1)
movie_sent_rating_tot = pd.merge(movie_sent_rating_tot, positive_ratio_before_release_df, left_on='영화명', right_on='movie_title', how='outer')
movie_sent_rating_tot = movie_sent_rating_tot.drop('movie_title', axis = 1)
movie_sent_rating_tot = pd.merge(movie_sent_rating_tot, positive_ratio_one_week_after_release_df, left_on='영화명', right_on='movie_title', how='outer')
movie_sent_rating_tot = movie_sent_rating_tot.drop('movie_title', axis = 1)
movie_sent_rating_tot = pd.merge(movie_sent_rating_tot, positive_ratio_two_week_after_release_df, left_on='영화명', right_on='movie_title', how='outer')
movie_sent_rating_tot = movie_sent_rating_tot.drop('movie_title', axis = 1)
movie_sent_rating_tot = pd.merge(movie_sent_rating_tot, rating_tot, left_on='영화명', right_on='movie_title', how='outer')
movie_sent_rating_tot = movie_sent_rating_tot.drop('movie_title', axis = 1)
movie_sent_rating_tot = pd.merge(movie_sent_rating_tot, ratiing_before_release_df, left_on='영화명', right_on='movie_title', how='outer')
movie_sent_rating_tot = movie_sent_rating_tot.drop('movie_title', axis = 1)
movie_sent_rating_tot = pd.merge(movie_sent_rating_tot, ratiing_one_week_after_release_df, left_on='영화명', right_on='movie_title', how='outer')
movie_sent_rating_tot = movie_sent_rating_tot.drop('movie_title', axis = 1)
movie_sent_rating_tot = pd.merge(movie_sent_rating_tot, ratiing_two_week_after_release_df, left_on='영화명', right_on='movie_title', how='outer')
movie_sent_rating_tot = movie_sent_rating_tot.drop('movie_title', axis = 1)

# 파일 저장 
movie_sent_rating_tot.to_csv(r'C:\\Users\\yehun chang\\OneDrive\\바탕 화면\\빅데이터 예측분석\\2_기말고사 과제\\영화\\movie_tot.csv', index=False)
