# -- coding: utf-8 --**
import datetime
import json
import time
from concurrent.futures import ThreadPoolExecutor

import pymysql as py
import requests

Host = '192.168.31.197'
Port = 3306
User = 'root'
Password = 'cbj123'
DB = 'trade'
dbMap = {
    "BTC-USDT":"dbbardata_btcusdt",
    "ETH-USDT":"dbbardata_ethusdt",
    "ETH-USDT-SWAP":"dbbardata_ethusdt_swap"
}
interval = {
    "1m":60000,
    "3m":180000,
    "5m":300000,
    "15m":900000,
    "30m":1800000,
    "1H":3600000,
    "2H":7200000,
    "4H":14400000
}
class okexApi:
    def __init__(self):
        self.Host = '192.168.31.197'
        self.Port = 3306
        self.User = 'root'
        self.Password = 'cbj123'
        self.DB = 'trade'
        self.dbMap = {
            "BTC-USDT": "dbbardata_btcusdt",
            "ETH-USDT": "dbbardata_ethusdt",
            "ETH-USDT-SWAP": "dbbardata_ethusdt_swap"
        }
        self.interval = {
            "1m": 60000,
            "3m": 180000,
            "5m": 300000,
            "15m": 900000,
            "30m": 1800000,
            "1H": 3600000,
            "2H": 7200000,
            "4H": 14400000
        }
    def conn_mysql(self,instId,bar):
        db = py.connect(host=self.Host, port=self.Port, user=self.User, password=self.Password,
                        database=self.DB)
        cur = db.cursor()

        self.get_restApi(db,cur,instId,bar)

        cur.close()
        db.close()

    def get_restApi(self,db, cursor,instId,bar):
        headers = {
            "Content-Type": "application/json",
            'Connection': 'close'
        }
        # 获取数据表映射关系
        tableName = self.dbMap[instId]
        # 数据开始是闭区间，获取22年初到最新的数据，开始时间则为21年年尾
        min_1_open = 1640966340000  # 一分钟K线数据开始时间：2021-12-31
        # 每分钟间隔60000ms
        min_interval = self.interval[bar]

        # 先是看看数据库里有没有对于bar的数据，如果没有时间就从上面的min_1_open开始，如果有数据，则从数据库最新的时间点开始
        query_Sql = "select UNIX_TIMESTAMP(datetime)*1000 from "+ tableName +" where `interval` = '"+bar+"'  ORDER BY datetime desc limit 1"
        sql = "insert into " + tableName + "(`symbol`,`interval`,`datetime`,`open_price`,`high_price`,`low_price`,`close_price`,`vol`,`volCcy`)" \
              " values(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        cursor.execute(query_Sql)
        rest = cursor.fetchone()
        if rest != None:
            min_1_open = rest[0]
            print("捕获数据库时间戳！")
        while True:
            strTime = time.strftime("%Y-%m-%d %H:%M") + ":00"
            localTime = int(time.mktime(time.strptime(strTime, '%Y-%m-%d %H:%M:%S'))) * 1000
            # 判断开始时间与当前系统的分钟时间相差多少个分钟级
            min_num = (localTime - min_1_open) / min_interval
            if min_num > 100:
                min_1_end = min_1_open + (min_interval * 101)
                params = {
                    "instId": instId,
                    "bar": bar,
                    "before": min_1_open,
                    "after": min_1_end
                }
                try:
                    response = requests.session()
                    response.adapters.DEFAULT_RETRIES = 5
                    response.keep_alive = False
                    response = response.get('https://www.okx.com/api/v5/market/history-candles', headers=headers,
                                            params=params)
                    data = json.loads(str(response.text))["data"]
                    for i in range(len(data)):
                        sqltime = datetime.datetime.fromtimestamp(int(data[i][0]) / 1000)
                        cursor.execute(sql, (
                            instId, bar, sqltime, data[i][1], data[i][2], data[i][3], data[i][4], data[i][5],
                            data[i][6]))
                    min_1_open = min_1_end - min_interval
                    response.close()
                except Exception as e:
                    response.close()
                    time.sleep(10)
                    print("请求异常，重新请求：",e)
                    self.get_restApi(db, cursor,instId,bar)

            elif min_num > 0:
                min_1_end = min_1_open + (min_interval * (int(min_num)))
                params = {
                    "instId": instId,
                    "bar": bar,
                    "before": int(min_1_open),
                    "after": int(min_1_end)
                }
                try:
                    response = requests.session()
                    response.adapters.DEFAULT_RETRIES = 5
                    response.keep_alive = False
                    response = response.get('https://www.okx.com/api/v5/market/candles', headers=headers, params=params)
                    data = json.loads(str(response.text))["data"]
                    for i in range(len(data)):
                        sqltime = datetime.datetime.fromtimestamp(int(data[i][0]) / 1000)
                        cursor.execute(sql, (
                            instId, bar, sqltime, data[i][1], data[i][2], data[i][3], data[i][4], data[i][5],
                            data[i][6]))
                    min_1_open = min_1_end - min_interval
                    response.close()
                except Exception as e:
                    response.close()
                    time.sleep(10)
                    print("请求异常，重新请求：", e)
                    self.get_restApi(db, cursor, instId, bar)
            db.commit()

            print("进行请求：", bar)
            time.sleep(10)

if __name__ == '__main__':
    okex = okexApi()
    okex.conn_mysql("ETH-USDT", "1m")