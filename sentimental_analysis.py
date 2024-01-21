import pandas as pd

data = pd.read_csv('C:\\Users\\yehun chang\\Desktop\\빅데이터 예측분석\\2_기말고사 과제\\영화\\movie_review.csv')

from konlpy.tag import Okt
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# Okt 형태소 분석기 초기화
okt = Okt()

# VADER 감정 분석기 초기화
analyzer = SentimentIntensityAnalyzer()

def analyze_sentiment(text):
    # NaN 값이거나 문자열이 아닌 경우 빈 문자열로 처리
    if pd.isna(text) or not isinstance(text, str):
        text = ''
    
    # 한글 텍스트를 형태소 분석하여 단어로 분리
    words = okt.morphs(text)

    # 단어를 다시 문장으로 결합합니다.
    processed_text = ' '.join(words)

    # VADER 감정 분석 수행
    sentiment_scores = analyzer.polarity_scores(processed_text)

    # 감정 점수에 따라 감정 분류
    #향후 분석시 긍정 == 1, 부정 == -1, 중립 == 0으로 매치 예정
    if sentiment_scores['compound'] >= 0.05:
        sentiment = "긍정적"
    elif sentiment_scores['compound'] <= -0.05:
        sentiment = "부정적"
    else:
        sentiment = "중립적"

    return sentiment

data['sentiment'] = data['review_contents'].apply(analyze_sentiment)
data.to_csv(r'C:\\Users\\yehun chang\\Desktop\\빅데이터 예측분석\\2_기말고사 과제\\영화\\movie_data_sent_raw.csv', index=False)