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
barInfo = {
    "1m":{
        "interval":60000,
        "openTime":1640966340000
    },
    "3m":{
        "interval":180000,
        "openTime":1640966220000
    },
    "5m":{
        "interval":300000,
        "openTime":1640966100000
    },
    "15m":{
        "interval":900000,
        "openTime":1640965500000
    },
    "30m":{
        "interval":1800000,
        "openTime":1640964600000
    },
    "1H":{
        "interval":3600000,
        "openTime":1640962800000
    },
    "2H":{
        "interval":7200000,
        "openTime":1640959200000
    },
    "4H":{
        "interval":14400000,
        "openTime":1640952000000
    }
}

def conn_mysql(instId,bar):
    db = py.connect(host=Host, port=Port, user=User, password=Password,
                    database=DB)
    cur = db.cursor()
    get_restApi(db,cur,instId,bar)

    cur.close()
    db.close()

def get_restApi(db, cursor,instId,bar):
    headers = {
        "Content-Type": "application/json"
    }
    # 获取数据表映射关系
    tableName = dbMap[instId]
    # 数据开始是闭区间，获取22年初到最新的数据，开始时间则为21年年尾
    openTime = barInfo[bar]["openTime"]  # 一分钟K线数据开始时间：2021-12-31
    # 每分钟间隔60000ms
    interval_ms = barInfo[bar]["interval"]
    # 先是看看数据库里有没有对于bar的数据，如果没有时间就从上面的openTime开始，如果有数据，则从数据库最新的时间点开始
    query_Sql = "select UNIX_TIMESTAMP(datetime)*1000 from "+ tableName +" where `interval` = '"+bar+"'  ORDER BY datetime desc limit 1"
    sql = "insert into " + tableName + "(`symbol`,`interval`,`datetime`,`open_price`,`high_price`,`low_price`,`close_price`,`vol`,`volCcy`)" \
          " values(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    cursor.execute(query_Sql)
    rest = cursor.fetchone()
    if rest != None:
        openTime = rest[0]
        print("捕获数据库时间戳！")
    # 获取当前时间
    str_time = time.strftime("%Y-%m-%d %H:%M", time.localtime()) + ":00"
    localTime = int(time.mktime(time.strptime(str_time, "%Y-%m-%d %H:%M:%S"))) * 1000
    pdTime = localTime + interval_ms
    while True:
        # 判断是否满足条件进行请求
        while_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        while_time2 = int(time.mktime(time.strptime(while_time, "%Y-%m-%d %H:%M:%S"))) * 1000
        # print("满足请求条件，进行请求！", "当前时间 = ", while_time2, "，判断时间=", pdTime)
        try:
            strTime = time.strftime("%Y-%m-%d %H:%M") + ":00"
            localTime = int(time.mktime(time.strptime(strTime, '%Y-%m-%d %H:%M:%S'))) * 1000
            # 判断开始时间与当前系统的分钟时间相差多少个分钟级
            min_num = (localTime - openTime) / interval_ms
            if min_num > 100:
                endTime = openTime + (interval_ms * 101)
                params = {
                    "instId": instId,
                    "bar": bar,
                    "before": openTime,
                    "after": endTime
                }
                try:
                    response = requests.session()
                    response.adapters.DEFAULT_RETRIES = 5
                    response.keep_alive = False
                    rp = response.get('https://www.okx.com/api/v5/market/history-candles', headers=headers,
                                            params=params)
                    print("进行请求：", instId," bar=", bar, "  optime=", openTime, "  endtime=", endTime)
                    data = json.loads(str(rp.text))["data"]
                    while len(data) == 0:
                        time.sleep(1)
                        rp_1 = response.get('https://www.okx.com/api/v5/market/history-candles',
                                                headers=headers,
                                                params=params)
                        print("之前请求过早，服务器尚未有数据传输，先隔一秒再次进行请求：", instId," bar=",bar, "  optime=", openTime, "  endtime=", endTime,
                              "url=https://www.okx.com/api/v5/market/history-candles?instId=",instId,"&bar=",bar,"&before=",openTime,"&after=",endTime)
                        data = json.loads(str(rp_1.text))["data"]
                    for i in range(len(data)):
                        sqltime = datetime.datetime.fromtimestamp(int(data[i][0]) / 1000)
                        cursor.execute(sql, (
                            instId, bar, sqltime, data[i][1], data[i][2], data[i][3], data[i][4], data[i][5],
                            data[i][6]))
                    openTime = endTime - interval_ms
                    response.close()
                except Exception as e:
                    response.close()
                    time.sleep(10)
                    print("请求异常，重新请求：", e)
                    cursor.close()
                    db.close()
                    conn_mysql(instId, bar)
            elif while_time2 > pdTime:
                params = {
                    "instId": instId,
                    "bar": bar,
                    "before": int(openTime)
                    # "after": int(endTime)
                }
                try:
                    response = requests.session()
                    response.adapters.DEFAULT_RETRIES = 5
                    response.keep_alive = False
                    rp = response.get('https://www.okx.com/api/v5/market/candles', headers=headers,
                                            params=params)
                    print("进行请求：", instId," bar=", bar, "  optime=", openTime, "  endtime=none")
                    data = json.loads(str(rp.text))["data"]
                    while len(data) == 0:
                        time.sleep(1)
                        rp_1 = response.get('https://www.okx.com/api/v5/market/candles',
                                                headers=headers,
                                                params=params)
                        print("之前请求过早，服务器尚未有数据传输，先隔一秒再次进行请求：", instId, " bar=", bar, "  optime=", openTime,"  endtime=none",
                              "url=https://www.okx.com/api/v5/market/candles?instId=", instId, "&bar=", bar,"&before=", openTime)
                        data = json.loads(str(rp_1.text))["data"]
                    tmp_endTime = 0
                    for i in range(len(data)):
                        sqltime = datetime.datetime.fromtimestamp(int(data[i][0]) / 1000)
                        cursor.execute(sql, (
                            instId, bar, sqltime, data[i][1], data[i][2], data[i][3], data[i][4], data[i][5],
                            data[i][6]))
                        if tmp_endTime == 0:
                            tmp_endTime = int(data[i][0])
                        elif tmp_endTime < int(data[i][0]):
                            tmp_endTime = int(data[i][0])
                    pdTime = pdTime + interval_ms
                    openTime = tmp_endTime
                    response.close()
                except Exception as e:
                    response.close()
                    time.sleep(10)
                    print("请求异常，重新请求--", instId,"--",bar,"--",e)
                    cursor.close()
                    db.close()
                    conn_mysql(instId, bar)

            db.commit()
        except Exception as e:
            print("出现异常：",e)

        time.sleep(1)



if __name__ == '__main__':
    threadpool = ThreadPoolExecutor(20)
    threadpool.submit(conn_mysql, "ETH-USDT", "1m")
    threadpool.submit(conn_mysql, "ETH-USDT", "3m")
    threadpool.submit(conn_mysql, "ETH-USDT", "5m")
    threadpool.submit(conn_mysql, "ETH-USDT", "15m")
    threadpool.submit(conn_mysql, "ETH-USDT", "30m")
    threadpool.submit(conn_mysql, "BTC-USDT", "1m")
    threadpool.submit(conn_mysql, "BTC-USDT", "3m")
    threadpool.submit(conn_mysql, "BTC-USDT", "5m")
    threadpool.submit(conn_mysql, "BTC-USDT", "15m")
    threadpool.submit(conn_mysql, "BTC-USDT", "30m")