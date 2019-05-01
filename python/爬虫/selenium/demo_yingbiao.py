from selenium import webdriver
import  pprint
import time
import  socket
import sys
import os,re
import requests
import bs4
from fake_useragent import UserAgent
ua = UserAgent()
headers = {'User-Agent': ua.random}
#socket.setdefaulttimeout(1.0)

class mm131(object):
    """
    下载http://www.win4000.com这个网站的图片
    """
    def __init__(self, origin_url, key_words):
        self.chrome_browser = webdriver.Chrome()
        self.url = origin_url.format(key_words)
        print(self.url)
        self.chrome_browser.get(self.url)
        # pprint.pprint(self.chrome_browser.page_source)
        # with open("res.txt", 'w') as f:
        #     f.write(str(self.chrome_browser.page_source.encode("gbk", "ignore")))
        # 每一章小说的连接
        url_div = self.chrome_browser.find_elements_by_css_selector('phonetic-transcription')
        # print(url_div.text)
        for d in url_div:
            pprint.pprint(d.find_element_by_tag_name('span').text)
            pprint.pprint(d.find_element_by_tag_name('b').text)
        #pprint.pprint([b.get_attribute('href') for i in url_div for b in i.find_elements_by_tag_name('a')])
        # for url in url_div:
        #     # 获取下载图片的上一级目录和相应的名字
        #     print(url)
        #     url_first = [i.get_attribute('href') for i in url.find_elements_by_tag_name('a')]
        #     title_first = [i.find_element_by_tag_name('p').text for i in url.find_elements_by_tag_name('a')]
        # #pprint.pprint(url_div)
        # for i in range(len(url_first)):
        #     start = time.time()
        #     print(url_first[i])
        #     self.download(url_first[i], title_first[i])
        #     end = time.time()
        #     print("runninng time: {0}".format(end - start))
        self.chrome_browser.close()

    
if __name__ == '__main__':
    #url = sys.argv[1] if sys.argv[1] else "https://www.cdzdgw.com/6_6828/"
    #url = "http://www.win4000.com/meitu.html"
    url = "https://fanyi.baidu.com/?aldtype=16047#en/zh/{0}"
    s_obj = mm131(url, 'perfect')
    #xs_obj = meizhuo(url)
    #for i in range(5):
        #xs_obj = mm131_bs4("".join([url[:-5], '_', str(i+1), '.html']))