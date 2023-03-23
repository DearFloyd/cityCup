import os
import re
import time

import jieba
import pdfplumber
import pymysql
import requests

from selenium import webdriver
from selenium.common import NoSuchElementException
from selenium.webdriver import ActionChains
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from zhconv import convert


def parsepdf(pdf_address, txt_address="./NLP.txt"):
    print("-----parse pdf to txt-----")
    """获取PDF中与ESG相关的文本"""
    with pdfplumber.open(pdf_address) as pdf:
        with open(txt_address, mode='w', encoding='utf-8') as fp2:
            for i in range(len(pdf.pages)):
                page = pdf.pages[i]
                text = page.extract_text()
                fp2.write(text)
    os.remove(pdf_address)  # 删除PDF文件


def getpdf(pdf_url):
    print("-----get pdf from url-----")
    # 请求头
    headers_d = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; rv:11.0) like Gecko)'}
    # 请求参数
    url_params = {'headers': headers_d, 'data': None, 'params': None, 'proxies': None}
    response = requests.get(pdf_url, **url_params)
    # 写入临时文件再进行解析
    pdf_path = f'~temp{time.time()}~.pdf'
    with open(pdf_path, 'wb') as pdf:
        for chunk in response.iter_content(chunk_size=1024):
            if chunk:
                pdf.write(chunk)
                pdf.flush()
    # 获取该文件的绝对路径
    return os.path.abspath(pdf_path)


def ranking(txt="./NLP_1.txt"):
    print("-----rank company according to txt-----")
    text = open(txt, encoding="utf-8").read()
    if text == '':
        return 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
    words = jieba.lcut(text)
    set_e1 = len(set(words) & set(E1)) / 25.05  # 交集的大小
    set_e2 = len(set(words) & set(E2)) / 56.1
    set_e3 = len(set(words) & set(E3)) / 47.7
    set_e4 = len(set(words) & set(E4)) / 22.95
    if set_e1 > 1:
        set_e1 = 1
    if set_e2 > 1:
        set_e2 = 1
    if set_e3 > 1:
        set_e3 = 1
    if set_e4 > 1:
        set_e4 = 1
    set_s1 = len(set(words) & set(S1)) / 6
    set_s2 = len(set(words) & set(S2)) / 19.65
    set_s3 = len(set(words) & set(S3)) / 23.4
    set_s4 = len(set(words) & set(S4)) / 8.4
    if set_s1 > 1:
        set_s1 = 1
    if set_s2 > 1:
        set_s2 = 1
    if set_s3 > 1:
        set_s3 = 1
    if set_s4 > 1:
        set_s4 = 1
    set_g1 = len(set(words) & set(G1)) / 10.8
    set_g2 = len(set(words) & set(G2)) / 30
    set_g3 = len(set(words) & set(G3)) / 16.95
    if set_g1 > 1:
        set_g1 = 1
    if set_g2 > 1:
        set_g2 = 1
    if set_g3 > 1:
        set_g3 = 1
    return set_e1, set_e2, set_e3, set_e4, set_s1, set_s2, set_s3, set_s4, set_g1, set_g2, set_g3


def formating(read_txt="./NLP.txt", write_txt="./NLP_1.txt"):
    print("-----format txt-----")
    with open(write_txt, 'w', encoding="utf-8") as f:
        with open(read_txt, 'r', encoding="utf-8") as fp:
            for content in fp:
                # 将繁体转化为简体，并去除换行
                content = convert(content, 'zh-cn').strip()
                # 保存处理过的文本
                f.write(content)


# ESG报告来源
url = 'https://www1.hkexnews.hk/search/titlesearch.xhtml?lang=zh'
# 不加载图片,不缓存在硬盘(内存)
SERVICE_ARGS = ['--load-images=false', '--disk-cache=false']
s = Service(executable_path='./chromedriver.exe', log_path=os.devnull)
# 创建浏览器
browser = webdriver.Chrome(service=s, service_args=SERVICE_ARGS)
# 访问相对应链接
browser.get(url)

# 加载ESG相关的自定义词典路径 需要修改
ESG_list = ["./keyWord/G1.txt", "./keyWord/G2.txt", "./keyWord/G3.txt",
            "./keyWord/S1.txt", "./keyWord/S2.txt", "./keyWord/S3.txt", "./keyWord/S4.txt",
            "./keyWord/E1.txt", "./keyWord/E2.txt", "./keyWord/E3.txt", "./keyWord/E4.txt"]
for esg in ESG_list:
    jieba.load_userdict(esg)
G1 = [line.strip() for line in open("./G1.txt", 'r', encoding='utf-8').readlines()]
G2 = [line.strip() for line in open("./G2.txt", 'r', encoding='utf-8').readlines()]
G3 = [line.strip() for line in open("./G3.txt", 'r', encoding='utf-8').readlines()]
S1 = [line.strip() for line in open("./S1.txt", 'r', encoding='utf-8').readlines()]
S2 = [line.strip() for line in open("./S2.txt", 'r', encoding='utf-8').readlines()]
S3 = [line.strip() for line in open("./S3.txt", 'r', encoding='utf-8').readlines()]
S4 = [line.strip() for line in open("./S4.txt", 'r', encoding='utf-8').readlines()]
E1 = [line.strip() for line in open("./E1.txt", 'r', encoding='utf-8').readlines()]
E2 = [line.strip() for line in open("./E2.txt", 'r', encoding='utf-8').readlines()]
E3 = [line.strip() for line in open("./E3.txt", 'r', encoding='utf-8').readlines()]
E4 = [line.strip() for line in open("./E4.txt", 'r', encoding='utf-8').readlines()]
# 数据库连接
conn = pymysql.connect(host="82.157.145.14", port=3306, user="root", passwd="%jHUlEuIfUwW^jda7s}L", db="citycup")
cursor = conn.cursor()

# 标题类别，选择：標題類別
try:
    locator = (By.CSS_SELECTOR, "#tier1-select > div > div > a")
    WebDriverWait(browser, 2).until(EC.presence_of_element_located(locator))
    # 需要点击的位置
    button = browser.find_element(By.CSS_SELECTOR,
                                  "#hkex_news_header_section > section > div.container.search-component > div > div.filter__inputGroup > ul > li.filter__container-input.searchDocType > div > div.combobox-group-container.clearfix > div.combobox-group.searchType.filter__dropdown-js > div.combobox-boundlist")
    # 打开下拉菜单，代码的操作与鼠标的点击不完全相同
    browser.execute_script("arguments[0].setAttribute(arguments[1],arguments[2])", button, "style", "display: block")
    # 鼠标点击
    browser.find_element(By.CSS_SELECTOR,
                         "#hkex_news_header_section > section > div.container.search-component > div > div.filter__inputGroup > ul > li.filter__container-input.searchDocType > div > div.combobox-group-container.clearfix > div.combobox-group.searchType.filter__dropdown-js > div.combobox-boundlist > div > div > div > div.droplist-items > div:nth-child(2) > a").click()
except NoSuchElementException:
    print("NotFound：标题类别")

# 选择：環境、社會及管治資料/報告
try:
    locator = (By.CSS_SELECTOR, "#rbAfter2006 > div.combobox-body > div > div > a")
    WebDriverWait(browser, 2).until(EC.presence_of_element_located(locator))
    button1 = browser.find_element(By.CSS_SELECTOR, "#rbAfter2006 > div.combobox-boundlist")
    # 属性aria-expanded可以不改变
    browser.execute_script("arguments[0].setAttribute(arguments[1],arguments[2])", button1, "style", "display: block")
    # 经试验。这里要采取另一种方法
    browser.find_element(By.CSS_SELECTOR,
                         "#rbAfter2006 > div.combobox-boundlist > div > div > div > ul > li:nth-child(5) > a").click()
    # 这里用.click()无效
    ActionChains(browser).click(browser.find_element(By.CSS_SELECTOR,
                                                     "#rbAfter2006 > div.combobox-boundlist > div > div > div > ul > li:nth-child(5) > div > ul > li:nth-child(3) > a")).perform()
except NoSuchElementException:
    print("NotFound：环境、社会及管制资料")

# 开始日期
browser.find_element(By.CSS_SELECTOR, "#searchDate-From").click()
try:
    locator = (By.CSS_SELECTOR, "#date-picker > div.columns > div.clickup")
    WebDriverWait(browser, 2).until(EC.presence_of_element_located(locator))
    # 年，固定为2022
    browser.find_element(By.CSS_SELECTOR, "#date-picker > div.columns > b.year > ul > li:nth-child(2) > button").click()
    # 月
    month = 6
    pos = "#date-picker > div.columns > b.month > ul > li:nth-child({}) > button".format(month)
    browser.find_element(By.CSS_SELECTOR, pos).click()
    # 日
    day = 1
    pos = "#date-picker > div.columns > b.day > ul > li:nth-child({}) > button".format(day)
    # 双击提交结果
    ActionChains(browser).double_click(browser.find_element(By.CSS_SELECTOR, pos)).perform()
except Exception:
    print("NotFound：开始日期")
# 完结日期
browser.find_element(By.CSS_SELECTOR, "#searchDate-To").click()
try:
    locator = (By.CSS_SELECTOR, "#date-picker > div.columns > div.clickup")
    WebDriverWait(browser, 2).until(EC.presence_of_element_located(locator))
    # 年，固定为2022
    browser.find_element(By.CSS_SELECTOR, "#date-picker > div.columns > b.year > ul > li:nth-child(2) > button").click()
    # 月
    month = 7
    pos = "#date-picker > div.columns > b.month > ul > li:nth-child({}) > button".format(month)
    browser.find_element(By.CSS_SELECTOR, pos).click()
    # 日
    day = 22
    pos = "#date-picker > div.columns > b.day > ul > li:nth-child({}) > button".format(day)
    ActionChains(browser).double_click(browser.find_element(By.CSS_SELECTOR, pos)).perform()
except Exception:
    print("NotFound：完结日期")

# 点击搜索，由于实际的链接不是<a>标签，而是文本“搜尋”，
# 所以可以仅通过部分文本，以匹配所需的链接
browser.find_element(By.PARTIAL_LINK_TEXT, "搜尋").click()
locator = (By.CSS_SELECTOR, "#hkex_news_topbanner > section > div.banner__container > h1")
WebDriverWait(browser, 2).until(EC.presence_of_element_located(locator))

symbol_pos = "#titleSearchResultPanel > div > div.title-search-result.search-page-container > div.table-scroller-container > div.table-scroller > table > tbody > tr:nth-child({}) > td.text-right.stock-short-code"
name_pos = "#titleSearchResultPanel > div > div.title-search-result.search-page-container > div.table-scroller-container > div.table-scroller > table > tbody > tr:nth-child({}) > td.stock-short-name"
# 公司数，并转化为数字
totalnum = int(
    re.sub(r'\D', '', browser.find_element(By.CSS_SELECTOR, "#recordCountPanel > div > div.total-records").text))

for line in range(1, totalnum + 1):
    print("-----line {} start-----".format(line))
    # 需要查找的属性：股份代号、股份简称，爬取时只需要改变行数
    symbol = browser.find_element(By.CSS_SELECTOR, symbol_pos.format(line)).text.strip()
    symbol = symbol.split('\n')[0]
    print(symbol)
    # PDF的地址
    url_pos = "#titleSearchResultPanel > div > div.title-search-result.search-page-container > div.table-scroller-container > div.table-scroller > table > tbody > tr:nth-child({}) > td:nth-child(4) > div.doc-link > a"
    url_suffix = browser.find_element(By.CSS_SELECTOR, url_pos.format(line)).get_attribute('href')
    # 解析网络上的PDF，保存文本到本地
    addr = getpdf(url_suffix)
    parsepdf(addr)
    formating()
    results = ranking()
    updateSql = 'update split_words_score set e1={0},e2={1},e3={2},e4={3},s1={4},s2={5},s3={6},s4={7},g1={8},g2={9},g3={10} where symbol={11}'
    cursor.execute(
        updateSql.format(results[0], results[1], results[2], results[3], results[4], results[5], results[6], results[7],
                         results[8], results[9], results[10], symbol))
    conn.commit()
    print("-----line {} end-----".format(line))
    # 显示更多行
    if line % 100 == 0 and line < totalnum:
        browser.find_element(By.CSS_SELECTOR,
                             "#recordCountPanel2 > div.search-results__content-loadmore.component-loadmore.component-loadmore-no-options > div > div.component-loadmore__dropdown-container > ul > li > a").click()
        time.sleep(5)
# 关闭连接
conn.close()
cursor.close()
