import time
import urllib
from datetime import date
import requests
import pandas as pd
import re
import json
import hashlib
import ccxt
import sqlite3
from sqlite3 import Error


def md5(_str):
    m = hashlib.md5()
    m.update(_str.encode("utf8"))
    return m.hexdigest()

# smsbao.com 注册申请开通短信功能
def sendsms(_symbol):
    smsapi = "http://api.smsbao.com/"
    user = '******'
    password = md5('******')
    content = '【**软件】报价提醒' + _symbol
    phone = '136*********'
    data__ = urllib.parse.urlencode({'u': user, 'p': password, 'm': phone, 'c': content})
    send_url = smsapi + 'sms?' + data__
    urllib.request.urlopen(send_url)


def get_ahr999():
    """
    获取ahr999指标
    :return:
    """
    url = 'https://m.qkl123.com/data/ahr999/btc'
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Encoding': 'gzip,deflate,br',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Host': 'm.qkl123.com',
        'Pragma': 'no-cache',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0(Linux;Android5.0;SM-G900PBuild/LRX21T)AppleWebKit/538.36(KHTML,likeGecko)Chrome/86.1.4240.198MobileSafari/538.36'
    }
    response_ = requests.get(url, headers=headers).text
    return response_


def parse_html(response_):
    """
    处理脏数据
    :param response_:
    :return:
    """
    pat = '{bottom_line:.45,data:(.*?),terminal_line:1.2,update_time'
    res = re.compile(pat, re.S).findall(response_, re.S)[0]
    res2 = res.replace('[{', '[{"').replace('}]', '"}]').replace(':', '":"').replace(',', '","').replace('}","{',
                                                                                                         '"},{"').replace(
        '$', '')
    res_data = json.loads(res2)
    return res_data


def return_ahr999():
    """
    返回ahr999指数值
    :return:
    """
    data__ = parse_html(get_ahr999())
    df = pd.DataFrame(data__)
    df.index = pd.to_datetime(df['time'].values, unit='s')
    df['candle_begin_time'] = pd.to_datetime(df['time'].values, unit='s')
    df.sort_values(by=['candle_begin_time'], inplace=True)
    df['tmp'] = df['price_usd'].apply(lambda x: 1 if x.isalpha() else 0)
    df.loc[df['tmp'] != 0, 'price_usd'] = None
    df['price_usd'].fillna(method='ffill', inplace=True)
    df['tmp2'] = df['value'].apply(lambda x: 1 if x.isalpha() else 0)
    df.loc[df['tmp2'] != 0, 'value'] = None
    df['value'].fillna(method='ffill', inplace=True)
    df['ahr999'] = df['value'].astype('float64')
    return df.iloc[-1]['ahr999']


def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    _conn = None
    try:
        _conn = sqlite3.connect(db_file)
        return _conn
    except Error as e:
        print(e)

    return conn


def select_data(_conn, _sql):
    """
    获取数据
    :param _conn:
    :param _sql:
    :return:
    """
    _cur = _conn.cursor()
    _cur.execute(_sql)
    rows = _cur.fetchall()

    _cur.close()

    return rows


def insert_data(_conn, _data):
    """
    写入数据
    :param _conn:
    :param _data:
    :return: id
    """
    sql_ = ''' INSERT INTO autobuy(id,symbol,time,amount,sumamount,price,coinnum,sumcoinnum,avgprice)
              VALUES(?,?,?,?,?,?,?,?,?) '''
    cur_ = _conn.cursor()
    cur_.execute(sql_, _data)

    _conn.commit()
    cur_.close()

    return cur_.lastrowid


def delete_all_data(_conn):
    """
    Delete all rows in the tasks table
    :param _conn: Connection to the SQLite database
    :return:
    """
    sql_ = 'DELETE FROM autobuy'
    cur_ = _conn.cursor()
    cur_.execute(sql_)
    _conn.commit()


def cal_order_price(price, order_type, ratio=0.01):
    """
    为了达到成交的目的，计算实际委托价格会向上或者向下浮动一定比例默认为1% 或 0.5%
    :param price:
    :param order_type:
    :param ratio:
    :return:
    """
    if order_type in [1, 4]:
        return price * (1 + ratio)
    elif order_type in [2, 3]:
        return price * (1 - ratio)


def retry_wrapper(func, params={}, act_name='', sleep_seconds=3, retry_times=10):
    """
    需要在出错时不断重试的函数，例如和交易所交互，可以使用本函数调用。
    :param func: 需要重试的函数名
    :param params: func的参数
    :param act_name: 本次动作的名称
    :param sleep_seconds: 报错后的sleep时间
    :param retry_times: 为最大的出错重试次数
    :return:
    """

    for _ in range(retry_times):
        try:
            result = func(params=params)
            return result
        except Exception as e:
            print(act_name, '报错，报错内容：', str(e), '程序暂停(秒)：', sleep_seconds)
            time.sleep(sleep_seconds)
    else:
        # send_dingding_and_raise_error(output_info)
        raise ValueError(act_name, '报错重试次数超过上限，程序退出。')


def place_order(_symbol, _quantity, _price):
    """
    # 下单
    :param _symbol:
    :param _quantity:
    :param _price:
    :return:
    """
    # 精度数据准备
    for _ in range(10):
        try:
            info = exchange.create_limit_buy_order(_symbol + '/USDT', _quantity, _price)
            print(info)
            if info['id']:
                return info
            else:
                raise ValueError('下单失败,45秒后重试')
        except Exception as e:
            print(e)
            time.sleep(45)

    _ = _symbol + '现货_下单多次失败，请登录服务器查询错误日志'
    # 如果遇到错误发送信息通知


# 获取ahr999
ahr999_index = float(return_ahr999())

# 定义交易所API
apiKey = 'M19SmfF***********************'
secret = '7Uid2nv***********************'

exchange = ccxt.binance({
    'apiKey': apiKey,
    'secret': secret
})

balance = exchange.fetch_balance()['USDT']['free']

# 配置sqlite数据库路径
db_path = '/mnt/binance_swap/trade_data.db'

# ---------清除定投记录----------
# conn = create_connection(db_path)
# delete_all_data(conn)
# print('数据清除完毕')

# --------查询定投记录-----------
# conn = create_connection(db_path)
# for _ in select_data(conn, "select * from autobuy"):
#     print(_)

# exit()

# 配置每次定投金额和币种 BTC、ETH、BNB、AR、UNI、DOGE
buy_amount = 100  # USDT的数量
buy_symbol = ['BTC', 'ETH', 'AR', 'UNI', 'DOGE', 'BNB']  # 定投币种

symbol_s = ''
for x in buy_symbol:
    symbol_s += ' ' + x

if ahr999_index <= 0.2:
    buy_amount *= 7
elif 0.2 < ahr999_index <= 0.4:
    buy_amount *= 3.8
elif 0.4 < ahr999_index <= 0.6:
    buy_amount *= 2.2
elif 0.6 < ahr999_index <= 0.8:
    buy_amount *= 1.4
elif 0.8 < ahr999_index <= 1.0:
    buy_amount *= 1
elif 1.0 < ahr999_index <= 1.2:
    buy_amount *= 0.8
else:
    buy_amount = 0
    print('未达到定投条件，本次跳过')

print('当前ahr999值为', ahr999_index)
print('本次定投金额', buy_amount)
print('======================')

if buy_amount > 0:
    for _ in buy_symbol:
        # 交易所买入
        price_ = float(exchange.public_get_ticker_price({'symbol': _ + 'USDT'})['price'])
        price_ = cal_order_price(price_, 1)
        _quantity = float(buy_amount / price_)
        print('币种', _, '当前价格', price_, '数量', _quantity)
        # 交易所下单
        orderinfo = place_order(_, _quantity, price_)
    
        # 开始记录数据至数据库
        conn = create_connection(db_path)
    
        id_ = 1
        sumamount = orderinfo['cost']
        sumcoinnum = orderinfo['amount']
    
        # 计算最大ID
        with conn:
            data = select_data(conn, "SELECT * FROM autobuy ORDER BY id DESC LIMIT 1 ")
            if data:
                id_ = int(data[0][0]) + 1
    
        # 查询购买总额和数量总和
        with conn:
            sql = "SELECT * FROM autobuy WHERE symbol = '" + _ + "' ORDER BY id DESC LIMIT 1 "
            for row in select_data(conn, sql):
                if row:
                    sumamount = float(row[4]) + orderinfo['cost']
                    sumcoinnum = float(row[7]) + orderinfo['amount']
    
        if id_ == 1:
            sumamount = orderinfo['cost']
            sumcoinnum = orderinfo['amount']
    
        time_ = str(date.today())
        avgprice = round(sumamount / sumcoinnum, 4)
    
        # 写入数据库
        data_ = (id_, _, time_, orderinfo['cost'], sumamount, orderinfo['average'], orderinfo['amount'], sumcoinnum, avgprice)
        insert_data(conn, data_)
    
        # 关闭数据源
        conn.close()
        
        # 休息一下
        time.sleep(1)
    
    # 发送短信通知
    sendsms(' ahr999值为：' + str(ahr999_index) + symbol_s + '定投成功' + 'U结余' + str(balance))
