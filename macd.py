import pymysql as py
import pandas as pd
Host = '192.168.31.197'
Port = 3306
User = 'root'
Password = 'cbj123'
DB = 'trade'
currency = {
    "BTC": {
        "USD": {
            "optime": "2011-08-18 00:00:00",
            "tableName": "bitstamp_btcusd"
        }
    },
    "ETH": {
        "USD": {
            "optime": "2017-08-17 00:00:00",
            "tableName": "bitstamp_ethusd"
        }
    }
}
barInfo = {
    60: "1m",
    180: "3m",
    300: "5m",
    900: "15m",
    1800: "30m",
    3600: "1h",
    7200: "2h",
    14400: "4h",
    21600: "6h",
    43200: "12h",
    86400: "1d"
}

def conn_mysql(trad1, trad2, bar):
    db = py.connect(host=Host, port=Port, user=User, password=Password,
                    database=DB)
    cur = db.cursor()
    calculateMACD(trad1, trad2, bar,db,cur)
    cur.close()
    db.close()

def calculateMACD(trad1, trad2, bar,db,cur):
    interval = barInfo[bar]
    # 先是看看数据库里有没有对于bar的数据，如果没有时间就从上面的openTime开始，如果有数据，则从数据库最新的时间点开始
    tableName = currency[trad1][trad2]["tableName"]
    query_Sql = 'select * from ' + tableName + ' where `interval` = "' + interval + '" ORDER BY sysdatetime'
    cur.execute(query_Sql)
    rest = cur.fetchall()
    df = pd.DataFrame(list(rest))
    df.columns = ["id","symbol","sysdatetime","cndatetime","interval","volume","open_price","high_price","low_price","close_price"]

    paras = []
    for i in range(1, 251):
        paras.append(i)
    for para in paras:
        df['EMA_' + str(para)] = pd.DataFrame.ewm(df['close_price'], span=para).mean()
    print(df)
if __name__ == '__main__':
    conn_mysql("ETH", "USD", 300)