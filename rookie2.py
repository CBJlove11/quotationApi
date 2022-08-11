import pymysql as py
import pandas as pd
import pymysql
from sqlalchemy import create_engine

Host = '192.168.0.187'
Port = 3306
User = 'root'
Password = '123456'
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
proxies = {'https': 'http://127.0.0.1:7890'}
mysql_connect = create_engine('mysql+pymysql://root:123456@192.168.0.187:3306/trade?charset=utf8')


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
    df['EMA_' + '6'] = pd.DataFrame.ewm(df['close_price'], span=6).mean()
    df['EMA_' + '12'] = pd.DataFrame.ewm(df['close_price'], span=12).mean()
    df['EMA_' + '13'] = pd.DataFrame.ewm(df['close_price'], span=13).mean()
    df['EMA_' + '24'] = pd.DataFrame.ewm(df['close_price'], span=24).mean()
    df['EMA_' + '26'] = pd.DataFrame.ewm(df['close_price'], span=26).mean()
    df['EMA_' + '52'] = pd.DataFrame.ewm(df['close_price'], span=52).mean()
    df['EMA_' + '78'] = pd.DataFrame.ewm(df['close_price'], span=78).mean()
    df['EMA_' + '104'] = pd.DataFrame.ewm(df['close_price'], span=104).mean()

    print(df)
    df['DIFF'] = df['EMA_12'] - df['EMA_26']
    df['DEA'] = pd.DataFrame.ewm(df['DIFF'], span=9).mean()
    df['MACD'] = 2*(df['DIFF']  - df['DEA'])
    print(df)

    pd.io.sql.to_sql(df, 'macd5', mysql_connect,  if_exists='replace', index=False)

if __name__ == '__main__':
    conn_mysql("BTC", "USD", 300)