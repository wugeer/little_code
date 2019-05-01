from selenium import webdriver
import  pprint
import time
import sys
from selenium.common.exceptions import StaleElementReferenceException
from selenium.webdriver.support.wait import WebDriverWait  
from selenium.webdriver.support import expected_conditions as EC  
from selenium.webdriver.common.by import By  
class cdzdgw(object):
    """
    下载www.cdzdgw.com这个网站的小说，也就是笔趣阁
    """
    def __init__(self, origin_url):
        self.chrome_browser = webdriver.Chrome()
        self.chrome_browser.get(origin_url)
        # 每一章小说的连接
        time.sleep(5)
        url_div = self.chrome_browser.find_element_by_css_selector("[class='listmain']").find_elements_by_tag_name('dd')
        # 小说的名字
        title = self.chrome_browser.find_element_by_tag_name('h2').text
        #url_every = [i.find_element_by_tag_name('a').get_attribute('href') for i in url_div]
        #pprint.pprint(title)
        # 开始下载
        # pprint.pprint(url_div)
        total_url = []
        start = time.time()
        for i in url_div[12:]:
            total_url.append(i.find_element_by_tag_name('a').get_attribute('href'))
            # break
        self.downloader(total_url, title)
        end = time.time()
        print("runninng time: {0}".format(end-start))
        #pprint.pprint(url_every[12:])
        # print(chrome_browser.page_source)
        self.chrome_browser.close()
        # time.sleep(1)

    def downloader(self, total_url, title):
        """
        单线程下载传入的每一章的连接
        :param total_url: 小说的所有章节的URL
        :param title: 小说的名字
        :return: 无
        """
        len_ = len(total_url)
        total_txt = []
        for url in total_url:
            # 每一章小说的URL
            self.chrome_browser.get(url)
            WebDriverWait(self.chrome_browser,20,0.5).until(EC.presence_of_element_located((By.ID, 'content')))  
            total_txt.append(self.chrome_browser.find_element_by_css_selector("[class='content']").find_element_by_tag_name('h1').text+'\n')
            content_div = self.chrome_browser.find_element_by_css_selector("[id='content']").text
            total_txt.append("\n".join(content_div.split('\n')[:-3]))
            
        print("开始写入文本")
        with open("".join([title, '.txt']), 'w', encoding='utf-8') as f:
            f.write("".join(total_txt))
            print("写入成功")

if __name__ == '__main__':
    #url = sys.argv[1] if sys.argv[1] else "https://www.cdzdgw.com/6_6828/"
    url = "https://www.cdzdgw.com/1_1161/"
    xs_obj = cdzdgw(url)
    # t = "http://www.win4000.com/meinv170324.html"
    # print("".join([t[:-11],'1', t[-5:]]))



