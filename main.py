import base64
import hashlib
import hmac
import time
from hashlib import md5

import requests
import websocket
import json

class okexWebSocke:
    def __init__(self):
        self.wsData = {}
    def signature(self,timestamp, method, request_path, secret_key):
        body = ''
        message = str(timestamp) + str.upper(method) + request_path + str(body)
        mac = hmac.new(bytes(secret_key, encoding='utf8'), bytes(message, encoding='utf-8'), digestmod='sha256')
        d = mac.digest()
        return base64.b64encode(d)

    def on_open_public(self,ws_public):
        data = {
            "op": "subscribe",
            "args": [{
                "channel": "tickers",
                "instId": "ETH-USDT"
            }]
        }
        # "instId": "ETH-USDT"
        ws_public.send(json.dumps(data))

    def on_open_private(self,ws_private):

        times = int(time.time())
        SecretKey = "4848F5CA1D1F21DB91D16E842212876A"
        datad = self.signature(times, 'GET', '/users/self/verify', SecretKey)
        print(datad)

        data = {
            "op": "login",
            "args":
                [
                    {
                        "apiKey": "375d7c4f-d439-4ad0-aeb1-1fe4df1052e3",
                        "passphrase": "1998Chen..",
                        "timestamp": times,
                        "sign": str(datad.decode())
                    }
                ]
        }

        ws_private.send(json.dumps(data))

    def on_message(self,ws, msg):
        print(msg)
        # self.sinkData(msg)

    def on_error(self,ws, error):
        print(f"on error:{error}")

    # 公有频道
    def public_ws(self):
        d_base_url = "wss://wsaws.okx.com:8443/ws/v5/public"
        # wss://ws.okx.com:8443/ws/v5/private
        ws_public = websocket.WebSocketApp(d_base_url,
                                    on_message=self.on_message,
                                    on_error=self.on_error
                                    )
        ws_public.on_open = self.on_open_public
        ws_public.run_forever()

    # 私有频道
    def private_ws(self):
        d_base_url = "wss://ws.okx.com:8443/ws/v5/private"
        ws_private = websocket.WebSocketApp(d_base_url,
                                    on_message=self.on_message,
                                    on_error=self.on_error
                                    )
        ws_private.on_open = self.on_open_private
        ws_private.run_forever()

    # 存储数据
    def sinkData(self,msg):
        msgJson = json.loads(msg)
        if "data" in msgJson:
            if str(msgJson["data"][0][0]) in self.wsData:
                self.wsData[str(msgJson["data"][0][0])] = {
                    "o": msgJson["data"][0][1],
                    "h": msgJson["data"][0][2],
                    "l": msgJson["data"][0][3],
                    "c": msgJson["data"][0][4],
                    "vol": msgJson["data"][0][5],
                    "volCcy": msgJson["data"][0][6],
                    "type":"false"
                }
            else:
                self.wsData[str(msgJson["data"][0][0])] = {
                    "o": msgJson["data"][0][1],
                    "h": msgJson["data"][0][2],
                    "l": msgJson["data"][0][3],
                    "c": msgJson["data"][0][4],
                    "vol": msgJson["data"][0][5],
                    "volCcy": msgJson["data"][0][6],
                    "type":"false"
                }
                for key in self.wsData:
                    if key not in str(msgJson["data"][0][0]):
                        self.wsData[key]["type"] = "true"

        else:
            return

        print(self.wsData)
        if len(self.wsData) > 1:
            sinkKey = []
            for key in self.wsData:
                if self.wsData[key]["type"] == "true":
                    sinkKey.append(key)
                    print("存进数据库，并且准备删除字典里标记好的数据")
                    # 存进数据库
            for key in sinkKey:
                del self.wsData[key]
                print("删除key："+key+" 的数据")

if __name__ == '__main__':
    # private_ws()
    okexWs = okexWebSocke()
    okexWs.public_ws()
