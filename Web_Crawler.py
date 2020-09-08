"""
Created on 2020.08
Author: @Alison
"""

# 導入 HTTP Method: GET(), POST()
import requests as rq

# 導入 BeautifulSoup module: 解析 HTML 語法工具
from bs4 import BeautifulSoup as BS 

# 將 PTT Stock 存到 URL 變數中
URL = 'https://www.ptt.cc/bbs/Stock/index1.html' 


# 使用 for 迴圈將逐筆將標籤(tags)裡的 List 印出, 這裡取3頁
for round in range(3): 
    
    # Send get request to PTT Stock
    RES = rq.get(URL) 
    
    # 將 HTML 網頁程式碼丟入 bs4 分析模組
    soup = BS(RES.text, 'html.parser') 
    
    # 查找所有 html 元素, 取標題文章。 過濾出標籤名稱為'div'且 class 屬性為 title, 子標籤名稱為'a'
    articles = soup.select('div.title a') 
    
    # 呈上。取出'下一頁'元素
    paging = soup.select('div.btn-group-paging a')
    
    # 將'下一頁'元素存到 next_URL 中
    next_URL = 'https://www.ptt.cc' + paging[2]['href'] 
    
    # for 迴圈帶入到下一頁的 URL 資訊
    URL = next_URL 
    
    # 萃取文字出來: title, URL
    for x in articles: 
        print(x.text, x['href'])
