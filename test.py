# import libraries
import requests
from fake_useragent import UserAgent
from time import sleep

from bs4 import BeautifulSoup
import re

# Crawling Website
url='https://afltables.com/afl/seas/2022.html'
# Fake a user header
headers = {'User-Agent': UserAgent().random}


datas = requests.get(url=url, headers=headers)
html = datas.text

# For Loop crawling, time interval is demanded.
sleep(1)


soup = BeautifulSoup(html, 'lxml')

games = soup.find_all('table', attrs={'border':1})

games = soup.find_all('table', attrs={'style':'font: 12px Verdana;', 'width':"100%", 'border':"1"})

for game in games:
    tables = game.find_all('td')
    for table in tables:
        print(table)
        print('#########')
    print('$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$')


print('test for the github update !')