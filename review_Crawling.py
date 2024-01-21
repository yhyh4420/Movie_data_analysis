#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import numpy as np
import requests
import time
import lxml.html
import re
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException
from contextlib import ExitStack


# 본 리뷰 크롤링 기본개념
# 1. 네이버 영화에서 크롤링하고 싶었지만 서비스종료로 불가하므로 다음 영화에서 가져오도록 한다.
# 2. 정적 크롤링을 통해 beautifulSoap만을 이용해 하고 싶었지만 다음 영화가 동적 크롤링이 필요하여 Selenium을 사용한다.
#    1) 영화 상세페이지 url를 먼저 크롤링한 후 movie_info에 있는 영화만 다시 선별
#    2) url 모음 딕셔너리 활용 리뷰 크롤링

# In[ ]:


import chardet

with open('C:\\Users\\yehun chang\\Desktop\\빅데이터 예측분석\\2_기말고사 과제\\영화\\movieInfo.csv', 'rb') as f:
    result = chardet.detect(f.read())

title_data = pd.read_csv('C:\\Users\\yehun chang\\Desktop\\빅데이터 예측분석\\2_기말고사 과제\\영화\\movieInfo.csv', encoding=result['encoding'])
movie_names = title_data['영화명'].tolist()


# In[ ]:


def crawl_movie_links(name):
    # 웹을 로드하지 않고 크롤링
    chrome_options = Options()
    chrome_options.add_argument('--headless')

    driver = webdriver.Chrome(options=chrome_options)
    links = []  
    
    try:
        driver.get("https://movie.daum.net/main")  # 다음 영화 메인페이지 
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.NAME, "q")))
        
        search_box = driver.find_element(By.NAME, "q")  # 검색창 찾기
        search_box.send_keys(name)
        search_box.send_keys(Keys.RETURN)  # 영화제목 검색

        # 영화 탭 검색
        movie_tab_button = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "//a[@data-search-type='movie']")))

        driver.execute_script("arguments[0].click();", movie_tab_button)

        try:
            # movie link가 하나라도 검색될때까지 대기
            movie_link = WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.XPATH, "//a[contains(@href, '/moviedb/main?movieId=')]")))
        except TimeoutException:
            # movie link가 없으면 다음 문구 출력
            print(f"TimeoutException: No movie link found for {name}")
            return None

        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')
        movie_items = soup.select('ul.list_searchresult li')
        for item in movie_items:
            title_tag = item.select_one('.tit_item a.link_tit')
            title = title_tag.text.strip() if title_tag else 'No movie title'

            link_tag = item.select_one('.tit_item a.link_tit')
            link = 'https://movie.daum.net' + link_tag['href'] if link_tag else None

            links.append({
                'title': title,
                'link': link,
            })
        print(f'크롤링 완료 : {name}')
    finally:
        driver.quit()

    return links  


# In[ ]:


start_time = time.time()  # 시작시간 기록
link_dict = []

for item in movie_names:
    links = crawl_movie_links(item)
    if links:
        link_dict.extend(links)
    print(link_dict)

end_time = time.time()  # 종료시간 기록
elapsed_time = end_time - start_time
print(f"Time taken for code execution: {elapsed_time} seconds")  # 총 소요시간 기록
print(link_dict)


# In[ ]:


link_dict_uniq = [dict(t) for t in {tuple(d.items()) for d in link_dict}]
for item in link_dict_uniq:
    item['link'] = item['link'].replace('main?', 'grade?')


# In[ ]:


filtered_movie = [movie for movie in link_dict_uniq if movie['title'] in movie_names]
df_filtered_movie = pd.DataFrame(filtered_movie)
df_filtered_movie_drop = df_filtered_movie.drop_duplicates(subset='title', keep='first')
movie_dictionary = df_filtered_movie_drop.to_dict(orient = 'records')


# In[ ]:


start_time = time.time()
link_kor = []

for item in movie_dictionary:
    url = item['link']
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36"}

    try:
        res = requests.get(url, headers=headers)
        res.raise_for_status()  # 요청이 성공적으로 이루어지지 않으면 예외 발생
        soup = BeautifulSoup(res.text, 'lxml')

        title_tag = soup.find('h3', class_='tit_movie')
        title = title_tag.find('span', class_='txt_tit').text.strip() if title_tag else "영화 제목 없음"

        country_tag = soup.find('dt', string='국가')
        country = country_tag.find_next('dd').text.strip() if country_tag else "국가 정보 없음"

        if country == '한국':
            link_kor.append({
                'title': title,
                'link': item['link']
            })
        else:
            continue

    except requests.exceptions.HTTPError as errh:
        print("HTTP Error:", errh)
    except requests.exceptions.ConnectionError as errc:
        print("Error Connecting:", errc)
    except requests.exceptions.Timeout as errt:
        print("Timeout Error:", errt)
    except requests.exceptions.RequestException as err:
        print("Error:", err)

end_time = time.time()

print(end_time - start_time)


# In[ ]:


link_kor


# In[ ]:


def crawl_movie_data(movie_info):
    driver = webdriver.Chrome()

    try:
        movie_url = movie_info['link']
        driver.get(movie_url)
        time.sleep(1)

        html_source = driver.page_source
        soup = BeautifulSoup(html_source, 'html.parser')

        for _ in range(1000):
            try:
                more_button = driver.find_element(By.XPATH, "//button[@class='link_fold #more']")
                more_button.click()
                time.sleep(1)
            except Exception as e:
                break

        html_source = driver.page_source
        soup = BeautifulSoup(html_source, 'html.parser')

        review_list = soup.find_all('li', {'id': True})

        movie_data = []
        for review in review_list:
            review_content_tag = review.find('p', {'class': 'desc_txt'})
            review_content = review_content_tag.text.strip() if review_content_tag else "리뷰 내용 없음"

            rating_tag = review.find('div', {'class': 'ratings'})
            rating = rating_tag.text.strip() if rating_tag else "평점 없음"

            timestamp_tag = review.find('span', {'class': 'txt_date'})
            timestamp = timestamp_tag.text.strip() if timestamp_tag else "작성 시간 없음"

            movie_data.append({
                'movie_title': movie_info['title'], 
                'review_contents': review_content,
                'review_rating': rating,
                'review_timestamp': timestamp
            })
        print('크롤링 완료')
        print(movie_data[:2])
        return movie_data

    except Exception as e:
        print(f"{movie_info['title']}에 대해 오류가 발생했습니다: {e}")
        return None
    finally:
        driver.quit()


# In[ ]:


# link_kor 딕셔너리를 기반으로 크롤링 함수를 호출하고 결과를 리스트에 저장
def crawl_movie_data_wrapper(movie_info):
    result = crawl_movie_data(movie_info)
    return result


# In[ ]:


# 시작 시간 기록
start_time = time.time()

# WebDriver 인스턴스 생성
driver = webdriver.Chrome()

# 병렬 크롤링을 위한 ThreadPoolExecutor 설정
max_workers = 10
with ThreadPoolExecutor(max_workers=max_workers) as executor:
    # 크롤링 함수에 필요한 인자 생성
    args_list = link_kor

    # 각 영화 페이지를 크롤링 함수에 매핑하여 병렬로 실행
    results = list(executor.map(crawl_movie_data_wrapper, args_list))

# WebDriver 인스턴스 닫기
driver.quit()

# 종료 시간 기록
end_time = time.time()

# 걸린 시간 출력
elapsed_time = end_time - start_time
print(f"코드 실행에 소요된 시간: {elapsed_time}초")

# 결과를 DataFrame으로 변환
movie_data_raw = pd.DataFrame([item for sublist in results if sublist is not None for item in sublist])
print(movie_data_raw)

