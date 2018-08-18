import redis

from common.utils import get_formatted_exchange_result

MAX_CANDLES = 48000


class RedisAPI(object):
    conn = redis.Redis(charset="utf-8", decode_responses=True)

    @classmethod
    def set_candle(cls, exchange, symbol, timestamp, open, close, low, high, volume):
        key_name = "%s:%s:candles" % (exchange, symbol)
        cls.conn.lpush(key_name, get_formatted_exchange_result(timestamp, open, high, low, close, volume))
        cls.conn.ltrim(key_name, 0, MAX_CANDLES - 1)
