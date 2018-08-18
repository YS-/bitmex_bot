import redis

from common.utils import get_formatted_exchange_result, get_logger

MAX_CANDLES = 48000

logger = get_logger(__name__)


class RedisAPI(object):
    conn = redis.Redis(charset="utf-8", decode_responses=True)
    timeframe_to_minutes = {"1m": 1, "5m": 5, "1h": 60, "1d": 60 * 24}

    @classmethod
    def set_candle(cls, exchange, symbol, timestamp, _open, close, low, high, volume):
        key_name = "%s:%s:candles" % (exchange, symbol)
        cls.conn.lpush(key_name, get_formatted_exchange_result(timestamp, _open, high, low, close, volume))
        cls.conn.ltrim(key_name, 0, MAX_CANDLES - 1)

    @classmethod
    def fetch_ohlcv(cls, exchange, symbol, timeframe='1m', since=None, limit=None):
        """
        :param limit:
        :param exchange:
        :param symbol:
        :param timeframe: support only 1m for now
        :param since: timestamp in milliseconds
        """
        if timeframe != '1m':
            raise NotImplemented("timeframe != 1m is not implemented")
        key_name = "%s:%s:candles" % (exchange, symbol)
        candles = cls.conn.lrange(key_name, 0, -1)
        relevant_candles = list()
        for candle in candles:
            timestamp, _open, high, low, close, volume = candle.split("~")
            if float(timestamp) > since:
                relevant_candles.append([timestamp, _open, high, low, close, volume])
                if len(relevant_candles) >= limit:
                    break
        return relevant_candles
