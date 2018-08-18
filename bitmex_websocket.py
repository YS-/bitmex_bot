import urllib

from common.utils import get_logger
from ws_apis.base_websocket import BaseWebSocket

logger = get_logger(__name__)


class BitMEXWebSocket(BaseWebSocket):
    def __init__(self, endpoint, symbols, callback_method):
        super().__init__(endpoint, symbols, callback_method)

    def get_url(self):
        subscriptions = ['trade:' + symbol for symbol in self.symbols]

        url_parts = list(urllib.parse.urlparse(self.endpoint))
        url_parts[0] = url_parts[0].replace('http', 'ws')
        url_parts[2] = "/realtime?subscribe={}".format(','.join(subscriptions))
        return urllib.parse.urlunparse(url_parts)
