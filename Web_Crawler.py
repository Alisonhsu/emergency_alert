"""
Created on 2020.08
Author: @Alison
"""

# HTTP Method: GET(), POST()
import requests as rq
from bs4 import BeautifulSoup as BS # 導入 BeautifulSoup module: 解析 HTML 語法工具

URL = 'https://www.ptt.cc/bbs/Stock/index1.html' # 將 PTT Stock 存到 URL 變數中

for round in range(3): # 使用 for 迴圈將逐筆將標籤(tags)裡的 List 印出, 這裡取3頁
    RES = rq.get(URL) # Send get request to PTT Stock
    soup = BS(RES.text, 'html.parser') # 將 HTML 網頁程式碼丟入 bs4 分析模組
    
    articles = soup.select('div.title a') # 查找所有 html 元素, 過濾出標籤名稱為'div'且 class 屬性為 title, 子標籤名稱為'a'--> 標題文章
    paging = soup.select('div.btn-group-paging a') # 呈上。取出'下一頁'元素
    
    next_URL = 'https://www.ptt.cc' + paging[2]['href'] # 將'下一頁'元素存到 next_URL 中
    URL = next_URL # for 迴圈帶入到下一頁的 URL 資訊
    
    for x in articles: # 萃取文字出來
        print(x.text, x['href']) # title, URL
