# -*- coding: utf-8 -*- #
# ------------------------------------------------------------------
# File Name:        ESG
# Author:           joey
# Version:          ver0_1
# Created:          2023/3/20
# Description:      Main Function:    获取数据库的新闻数据 翻译并进行ESG与情感分类
# ------------------------------------------------------------------
import requests
from transformers import BertTokenizer, BertForSequenceClassification, pipeline
from translate import Translator
import pandas as pd
import pymysql
import sqlalchemy
import random
import hashlib
import numpy as np
import uuid
import time

# 首次运行，因为会下载FinBERT模型，耗时会比较久
senti_finbert = BertForSequenceClassification.from_pretrained('yiyanghkust/finbert-tone', num_labels=3)
esg_finbert = BertForSequenceClassification.from_pretrained('yiyanghkust/finbert-esg', num_labels=4)
senti_tokenizer_tone = BertTokenizer.from_pretrained('yiyanghkust/finbert-tone')
esg_tokenizer_esg = BertTokenizer.from_pretrained('yiyanghkust/finbert-esg')
senti_nlp_tone = pipeline("text-classification", model=senti_finbert, tokenizer=senti_tokenizer_tone)
senti_nlp_esg = pipeline("text-classification", model=esg_finbert, tokenizer=esg_tokenizer_esg)

# 使用有道付费翻译API
YOUDAO_URL = 'https://openapi.youdao.com/api'
APP_KEY = '7da0def1fe8d020e'
APP_SECRET = 'BAU2v93G3IIWi72vZSV7SzsHv9rtJ1Gw'


def txtClassification(sentence):
    senti_results1 = senti_nlp_tone(str(sentence))
    senti_results2 = senti_nlp_esg(str(sentence))
    return senti_results1[0]['label'], senti_results2[0]['label']


"""
# 弃用 免费版api有反爬虫机制 无法爬取
def translate(sentence):
    words = sentence
    url = 'http://fanyi.youdao.com/translate?smartresult=dict&smartresult=rule&smartresult=ugc&sessionFrom=null'
    data = {'i': words, 'doctype': 'json'}
    header = {'User-Agent': 'Mozilla/5.0'}
    response = requests.post(url, data=data, headers=header)
    reply = response.json()['translateResult'][0][0]['tgt']
    # print(reply)
    # 将翻译的内容打印出来并换行
    return reply
    
def get_data(content):
    r = str(round(time.time() * 1000))
    salt = r + str(random.randint(0, 9))

    data = "fanyideskweb" + content + salt + "Tbh5E8=q6U3EXe+&L[4c@"
    sign = hashlib.md5()

    sign.update(data.encode("utf-8"))

    sign = sign.hexdigest()

    return content, salt, sign

def send_request(content, salt, sign):
    url = 'http://fanyi.youdao.com/translate_o'
    headers = {
        'Cookie': 'OUTFOX_SEARCH_USER_ID=-948455480@10.169.0.83;',
        'Host': 'fanyi.youdao.com',
        'Origin': 'http://fanyi.youdao.com',
        'Referer': 'http://fanyi.youdao.com/',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.146 Safari/537.36',
    }
    data = {
        'i': str(content),
        'from': 'AUTO',
        'to': 'AUTO',
        'smartresult': 'dict',
        'client': 'fanyideskweb',
        'salt': str(salt),
        'sign': str(sign),
        'lts': '1612879546052',
        'bv': '6a1ac4a5cc37a3de2c535a36eda9e149',
        'doctype': 'json',
        'version': '2.1',
        'keyfrom': 'fanyi.web',
        'action': 'FY_BY_REALTlME',
    }
    res = requests.post(url=url, headers=headers, data=data).json()
    reply = res['translateResult'][0][0]['tgt']
    return reply
"""
"""
# pymysql读取数据库数据 返回DF格式
def get_mysql_dict(database, table_name):
    #  创建连接，指定数据库的ip地址，账号、密码、端口号、要操作的数据库、字符集
    host, user, pwd = '82.157.145.14', 'root', '%jHUlEuIfUwW^jda7s}L'
    conn = pymysql.connect(host=host, user=user, passwd=pwd, db=database, port=3306,
                           charset='utf8')  # port必须写int类型,MySQL的默认端口为3306. charset必须写utf8
    # 创建游标
    cursor = conn.cursor()
    # 执行sql语句
    sql = 'select * from %s ;' % table_name
    cursor.execute(sql)

    # 获取到sql执行的全部结果
    results = cursor.fetchall()
    cols = cursor.description
    # 处理为DF数据
    col_name = []
    for col in cols:
        col_name.append(col[0])
    data = list(map(list, results))
    dataF = pd.DataFrame(data, columns=col_name)
    # 处理为字典数据
    table_list = {}
    for r in results:
        r = list(r)  # 由于fetchall方法返回的一个元组
        print(r)
        #table_list[r[2]] = r[4]

    cursor.close()  # 关闭游标
    conn.close()  # 关闭连接

    return table_list, dataF  # 返回一个完整的列表数据
"""


# 使用pandas获取数据库数据
def getMySqlData():
    engine = sqlalchemy.create_engine('mysql+pymysql://root:%jHUlEuIfUwW^jda7s}L@82.157.145.14:3306/citycup')
    # sql = 'select NewsID,DeclareDate,Title,Symbol from news_security where NewsID in (104200255);'
    sql = 'select NewsID,DeclareDate,Title,Symbol from news_security limit 30000 offset 35104;'
    # sql = 'select NewsID,DeclareDate,Title,Symbol from news_security where NewsID in (104200255);'
    df = pd.read_sql(sql, engine)
    print(df)
    return df, engine


'-----------------------------------------------# 付费api配置-----------------------------------------------------------'


def encrypt(signStr):
    hash_algorithm = hashlib.sha256()
    hash_algorithm.update(signStr.encode('utf-8'))
    return hash_algorithm.hexdigest()


def truncate(q):
    if q is None:
        return None
    size = len(q)
    return q if size <= 20 else q[0:10] + str(size) + q[size - 10:size]


def do_request(data):
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    return requests.post(YOUDAO_URL, data=data, headers=headers)


def connect(q):
    # q = "待输入的文字"
    data = {}
    data['from'] = 'zh-CHS'
    data['to'] = 'en'
    data['signType'] = 'v3'
    curtime = str(int(time.time()))
    data['curtime'] = curtime
    salt = str(uuid.uuid1())
    signStr = APP_KEY + truncate(q) + salt + curtime + APP_SECRET
    sign = encrypt(signStr)
    data['appKey'] = APP_KEY
    data['q'] = q
    data['salt'] = salt
    data['sign'] = sign
    data['vocabId'] = "general"

    response = do_request(data).json()
    print(response)
    print(response["translation"][0])
    return response["translation"][0]


'-----------------------------------------------# 付费api配置-----------------------------------------------------------'


def translateEng(content):
    # content, salt, sign = get_data(content)
    # reply = send_request(content, salt, sign)  # 由于反爬虫 弃用
    # reply = Translator(from_lang="ZH", to_lang="EN-US").translate(f'{content}')  # 免费的翻译库 速度很慢
    reply = connect(content)
    return reply


df, engine = getMySqlData()
df['emotion'] = ''
df['esg'] = ''

# 分类
emo, esg = [], []
for i in range(df.shape[0]):
    print(f'process {i}/{df.shape[0]}')
    # 获得分类
    emo_, esg_ = txtClassification(translateEng(df['Title'][i]))

    if esg_ == 'None':
        esg_ = 'Governance'
    df['emotion'][i] = emo_
    df['esg'][i] = esg_

    new_df = df.iloc[[i], :]
    print(new_df)
    # 存入数据库
    new_df.to_sql('news_security_classification', con=engine, index=False, if_exists='append')
    # 设置访问间隔，防止报411访问频繁错误
    time.sleep(0.4)





