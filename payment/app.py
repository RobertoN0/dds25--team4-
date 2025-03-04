import logging

from quart import jsonify, Response

import payment.routing.kafka as kafka
import payment.routing.http as http
import payment.redis_db as db
from payment.app_instance import app
from common.otlp_grcp_config import configure_telemetry

configure_telemetry('payment-service')


@app.before_serving
async def startup():
    app.logger.info("Starting Payment Service")
    http.init()
    await kafka.init()
    db.init()


@app.after_serving
async def shutdown():
    app.logger.info("Stopping Payment Service")
    await kafka.close()
    await db.close()


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
    app.logger.setLevel(logging.INFO)
else:
    hypercorn_logger = logging.getLogger('hypercorn.error')
    app.logger.handlers = hypercorn_logger.handlers
    app.logger.setLevel(hypercorn_logger.level)
