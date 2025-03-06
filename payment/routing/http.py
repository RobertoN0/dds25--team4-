from quart import abort, jsonify, Response, Quart

import payment.payment_logic as logic
from payment.payment_logic import DBError, NotEnoughCreditError

# class HTTP:
#     def __init__(self, quart_app: Quart, logger):
#         self.app = quart_app
#         self.logger = logger
#
# app: Quart | None = None
#
#
#
#
# def init(quart_app: Quart):
#     global app
#     app = quart_app
