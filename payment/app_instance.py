import os

from quart import Quart

GATEWAY_URL = os.environ['GATEWAY_URL']
app = Quart("payment-service")