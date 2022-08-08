import time
import datetime
# strTime = time.strftime("%Y-%m-%d %H:%M") + ":00"
# print(strTime)
testTime = '2018-08-27 10:44:17'
# print(time.strftime(testTime))

# nowData = (datetime.datetime.now()+datetime.timedelta(days=1)).strftime("%Y-%m-%d %H:%M") + ":00"
# nowData = datetime.datetime.strptime(testTime,"%Y-%m-%d %H:%M:%S")
# print(nowData)
# nowq = datetime.datetime.now().strftime("%Y-%m-%d %H:%M") + ":00"
# nowData2 = (nowData+datetime.timedelta(minutes=1))
# print(nowData2)
# nowData2 = (nowData2+datetime.timedelta(minutes=1))
# print(nowData2)
# nowq = datetime.datetime.now().strftime("%Y-%m-%d %H:%M") + ":00"

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

times = 1659236400
sqltime = datetime.datetime.fromtimestamp(int(times))
num = barInfo[3600].replace("h","")
sqltime2 = (sqltime+datetime.timedelta(hours=int(num)))
print(sqltime)
# if nowq > nowData:
#     print("AA")

