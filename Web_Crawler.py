# HTTP Method: GET(), POST()
import requests as rq
from bs4 import BeautifulSoup as BS

URL = 'https://www.ptt.cc/bbs/Stock/index1.html'
for round in range(3):
    RES = rq.get(URL)
    soup = BS(RES.text, 'html.parser')
    articles = soup.select('div.title a')
    paging = soup.select('div.btn-group-paging a')
    next_URL = 'https://www.ptt.cc' + paging[2]['href']
    URL = next_URL
    for x in articles:
        print(x.text, x['href'])
