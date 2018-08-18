import json

from apis.redis_api import RedisAPI
from common.utils import convert_date_string_to_timestamp
from ws_apis.bitmex_websocket import BitMEXWebSocket
import time


class DataFetcher(object):
    def __init__(self):
        self.source = "BitMEX"
        self.end_point = "wss://www.bitmex.com/"
        self.symbols = ["XBTUSD"]

    def process_candle(self, candle):
        event_datetime = candle['timestamp']
        event_timestamp = convert_date_string_to_timestamp(event_datetime)
        RedisAPI.set_candle(self.source, candle['symbol'], event_timestamp, candle['open'], candle['close'],
                            candle['low'], candle['high'], candle['volume'])

    def process_candles(self, ws, message):
        message = json.loads(message)
        if "data" not in message:
            return
        candles = message["data"]
        for candle in candles:
            self.process_candle(candle)

    def run(self):
        BitMEXWebSocket(self.end_point, self.symbols, self.process_candles)
        while True:
            time.sleep(100)


if __name__ == '__main__':
    data_fetcher = DataFetcher()
    data_fetcher.run()
