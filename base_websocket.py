import threading
from time import sleep

import websocket
from retrying import retry

from common.utils import get_logger

logger = get_logger(__name__)


class BaseWebSocket(object):
    def __init__(self, endpoint, symbols, callback_method):
        logger.debug("Initializing WebSocket.")

        self.endpoint = endpoint
        self.symbols = symbols
        self.callback_method = callback_method

        self.exited = False
        self.wst = None

        ws_url = self.get_url()
        self.connect(ws_url)

    def exit(self, ws):
        self.exited = True
        ws.close()

    def get_ws(self, ws_url):
        return websocket.WebSocketApp(ws_url,
                                      on_message=self.callback_method,
                                      on_close=self.on_close,
                                      on_open=self.on_open,
                                      on_error=self.on_error)

    def validate_socket(self, ws):
        # Wait for connect before continuing
        conn_timeout = 5
        while not ws.sock or not ws.sock.connected and conn_timeout:
            sleep(1)
            conn_timeout -= 1
        if not conn_timeout:
            logger.error("Couldn't connect to WS! Exiting.")
            self.exit(ws)
            raise websocket.WebSocketTimeoutException('Couldn\'t connect to WS! Exiting.')

    @retry(wait_exponential_multiplier=1000, wait_exponential_max=60000)
    def connect(self, ws_url):
        logger.debug("Starting thread")

        ws = self.get_ws(ws_url)

        self.wst = threading.Thread(target=lambda: ws.run_forever())
        self.wst.daemon = True
        self.wst.start()
        logger.debug("Started thread")

        self.validate_socket(ws)

    def get_url(self):
        return self.endpoint

    @retry(wait_exponential_multiplier=1000, wait_exponential_max=60000)
    def on_error(self, ws, error):
        if not self.exited:
            logger.error("Error : %s" % error)
            ws.close()
            ws = self.get_ws(self.get_url())
            ws.run_forever()

    @staticmethod
    def on_open(ws):
        logger.debug("Websocket Opened.")

    @staticmethod
    def on_close(ws):
        logger.info('Websocket Closed')
