

############################################ Kobis 영화예매율 ###################################################
def Kobis_movie_ticketing(output_path):
    import requests # 웹 요청 도구
    from bs4 import BeautifulSoup # html에서 데이터 읽는 도구
    from bs4.element import NavigableString, Tag, Comment, ProcessingInstruction
    from selenium import webdriver
    import pandas as pd
    import pymysql
    import os
    import numpy as np

    browser_proxy = webdriver.Chrome('/Users/parkseonghun/kdigital-busan/tools/chromedriver')
    browser_proxy.get("https://www.kobis.or.kr/kobis/business/stat/boxs/findRealTicketList.do")
    soup = BeautifulSoup(browser_proxy.page_source)
    table = soup.select_one('.tbl_comm.th_sort')

    th_list = table.select('thead tr th')
    columns = []
    for th in th_list:
        for c in th.children:
            # print(c)
            if type(c) == NavigableString and len(c.strip()) > 0:
                columns.append(c.strip())

    tr_list = table.select('tbody tr')
    values_list = []
    for tr in tr_list:
        values = []
        for td in tr.select('td'):
            values.append(td.text.strip().replace("%", '').replace(",", ""))
        values_list.append(values)

    data = pd.DataFrame(values_list, columns=columns)
    data.groupby(by=["순위"])["순위"].count()

    # csv파일 DB(mysql)에 저장  
    # 1. connect
    conn = pymysql.connect(host="127.0.0.1", port=3306, db='finalteam3', user="kdigital", password="mysql", charset="utf8")

    # 2. get command object
    cursor = conn.cursor()
    cursor.execute("delete from movie_ticketing") #기존 데이터를 지울 때 주석을 풀어주세요 

    # 3. execute command
    sql = """insert into movie_ticketing (ranked, title, release_date, reserve_rate, reserve_revenue, accumulated_revenue, ticketing_view, accumulated_view) 
            values (%s, %s, %s, %s, %s, %s, %s, %s)"""
    for movie in data.values:
        cursor.execute(sql, list(movie))
                            
    # break

    conn.commit() # confirm previous executio

    # 4. close resource
    cursor.close()
    conn.close()

    browser_proxy.close()
    print("success")
    return True

    # browser_proxy.close() #주석 풀어서 사용하세요) 가장 마지막 크롬 하나만 종료
    # browser_proxy.quit() #주석 풀어서 사용하세요) 크롬s 모두 종료

if __name__ == "__main__":
    Kobis_movie_ticketing("../data-files/test-kobis.csv")
    pass

