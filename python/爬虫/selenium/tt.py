# import requests
# import bs4
# import pprint,time
# from fake_useragent import UserAgent
# ua = UserAgent()
# headers = {'User-Agent': ua.random}
# #socket.setdefaulttimeout(1.0)
# url = 'http://www.hao312.com/?action-viewnews-itemid-53764'
# res = requests.get(url, headers=headers)
# if res.status_code != 200:
#     print('the unvalid url %s' % (url))
# res.encoding = 'gb2312'
# time.sleep(1)
# soup = bs4.BeautifulSoup(res.text, 'html.parser')
# #pprint.pprint(soup.select("div.articleBody img"))
# url = soup.select("div.articleBody img")[0].get("src")
# # pprint.pprint(linkElems)
# # print('There are %s Picture waiting for downloading:\n' % (len(linkElems)))
# # for i in range(len(linkElems)):
# print('Downloading image {0} ...'.format(url))
# #url = 'http://www.hao312.com/attachments/2017/03/23/201709172cqpwjn5vjc.jpg'
# res1 = requests.get(url, headers=headers)
# if res1.status_code != 200:
#     print("cann't download " + url)
# with open('1.jpg', 'wb') as f:
#     f.write(res1.content)
#     print("写入成功")

item = [("hd",12),(31,"dahs")]
for i in item:
    print(i[0]+str(i[1]))
