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
    def __init__(self, origin_url):
        self.chrome_browser = webdriver.Chrome()
        self.chrome_browser.get(origin_url)
        # 每一章小说的连接
        url_div = self.chrome_browser.find_elements_by_css_selector("[class='clearfix']")
        #pprint.pprint([b.get_attribute('href') for i in url_div for b in i.find_elements_by_tag_name('a')])
        for url in url_div:
            # 获取下载图片的上一级目录和相应的名字
            print(url)
            url_first = [i.get_attribute('href') for i in url.find_elements_by_tag_name('a')]
            title_first = [i.find_element_by_tag_name('p').text for i in url.find_elements_by_tag_name('a')]
        #pprint.pprint(url_div)
        for i in range(len(url_first)):
            start = time.time()
            print(url_first[i])
            self.download(url_first[i], title_first[i])
            end = time.time()
            print("runninng time: {0}".format(end - start))
        self.chrome_browser.close()

    def download(self,total_url, dir):
        self.chrome_browser.get(total_url)
        # 图片的数量
        len_ = int(self.chrome_browser.find_element_by_css_selector("[class='ptitle']").find_element_by_tag_name("em").\
            text)
        url1 = self.chrome_browser.find_element_by_css_selector("[class='pic-large']").get_attribute("data-original")
        #pprint.pprint(url1)
        if not os.path.exists(dir):
            os.mkdir(dir)
        r = requests.get(url1, headers=headers)
        # 如果发送了一个错误请求(一个 4XX 客户端错误，或者
        # 5XX 服务器错误响应)，我们可以通过Response.raise_for_status() 来抛出异常：
        r.raise_for_status()
        print("开始下载{0}".format(url1))
        with open("".join([dir, '/1.jpg']), 'wb') as f:
           f.write(r.content)
        print("下载成功{0}".format(url1))
        for i in range(1, len_):
            # 每一张图片的URL
            download_url = "".join([total_url[:-5],"_" + str(i+1), total_url[-5:]])
            #print(download_url)
            self.chrome_browser.get(download_url)
            url = self.chrome_browser.find_element_by_css_selector("[class='pic-large']").get_attribute(
                "data-original")
            r = requests.get(url)
            # 如果发送了一个错误请求(一个 4XX 客户端错误，或者
            # 5XX 服务器错误响应)，我们可以通过Response.raise_for_status() 来抛出异常：
            r.raise_for_status()
            print("开始下载{0}".format(url))
            with open("".join([dir, '/', str(i+1), '.jpg']), 'wb') as f:
               f.write(r.content)
            print("下载成功{0}".format(url))

    def pre_download_bs4(self, total_url, path):
        print(total_url)
        res = requests.get(total_url, headers=headers)
        if res.status_code != 200:
            print('the unvalid url %s' % (total_url))
            return
        res.encoding = 'gb2312'
        time.sleep(1)
        soup = bs4.BeautifulSoup(res.text, 'html.parser')
        len_ = soup.select("div.ptitle > em")[0].get_text()
        if not os.path.exists(path):
            os.mkdir(path)
        for i in range(int(len_)):
            download_url = "".join([total_url[:-5], "_" + str(i + 1), total_url[-5:]])
            self.downloader_base(download_url, path, i+1)
        #print(len_)

    def downloader_base_bs4(self, base_url, path, index):
        """
        单线程下载传入的每个主题的图片
        :param total_url: 图片总的URL
        :param title: 图片的名字
        :return: 无
        """
        res = requests.get(base_url, headers=headers)
        if res.status_code != 200:
            print('the unvalid url %s' % (base_url))
            return
        res.encoding = 'gb2312'
        time.sleep(1)
        soup = bs4.BeautifulSoup(res.text, 'html.parser')
        url = soup.find('img', {'class': 'pic-large'}).get('data-original')
        #pprint.pprint(linkElems)
        #print('There are %s Picture waiting for downloading:\n' % (len(linkElems)))
        #for i in range(len(linkElems)):
        print('Downloading image {0} ...'.format(url))
        res1 = requests.get(url, headers=headers)
        if res1.status_code != 200:
            print("cann't download " + url)
            return
        with open("".join([path, '/', str(index + 1), '.jpg']), 'wb') as f:
            f.write(res1.content)
            print("写入成功")
 
if __name__ == '__main__':
    #url = sys.argv[1] if sys.argv[1] else "https://www.cdzdgw.com/6_6828/"
    #url = "http://www.win4000.com/meitu.html"
    url = "https://www.7160.com/lianglichemo/"
    s_obj = mm131(url)
    #xs_obj = meizhuo(url)
    #for i in range(5):
        #xs_obj = mm131_bs4("".join([url[:-5], '_', str(i+1), '.html']))