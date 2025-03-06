import logging

from quart import abort, jsonify, Response, Quart

from payment.payment_logic import DBError, NotEnoughCreditError
from payment.routing.kafka import Kafka
from payment.redis_db import ReddisDB
from common.otlp_grcp_config import configure_telemetry
from payment.payment_logic import PaymentLogic


configure_telemetry('payment-service')
app = Quart("payment-service")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)

logic: PaymentLogic | None = None
kafka: Kafka | None = None
db: ReddisDB | None = None

DB_ERROR_STR = 'DB error'
REQ_ERROR_STR = 'Requests error'


@app.post('/create_user')
async def create_user():
    uuid, err = await logic.create_user()

    if err:
        return abort(400, DB_ERROR_STR)

    return jsonify({'user_id': uuid})


@app.post('/batch_init/<n>/<starting_money>')
async def batch_init_users(n: int, starting_money: int):
    n = int(n)
    starting_money = int(starting_money)
    if n < 0 or starting_money < 0:
        return abort(400, REQ_ERROR_STR)

    msg, err = await logic.multi_set_user(n, starting_money)

    if err:
        return abort(400, DB_ERROR_STR)

    return jsonify({'msg': 'Batch init for users successful'})


@app.get('/find_user/<user_id>')
async def find_user(user_id: str):
    user_entry, err = await logic.get_user(user_id)

    if err:
        abort(400, DB_ERROR_STR)

    return jsonify(
        {
            'user_id': user_id,
            'credit': user_entry.credit
        }
    )


@app.post('/add_funds/<user_id>/<int:amount>')
async def add_credit(user_id: str, amount: int):
    credit, err = await logic.add_credit(user_id, amount)

    if err:
        return abort(400, DB_ERROR_STR)

    return Response(f"User: {user_id} credit updated to: {credit}", status=200)


@app.post('/pay/<user_id>/<int:amount>')
async def remove_credit(user_id: str, amount: int):
    credit, err = await logic.remove_credit(user_id, amount)

    if isinstance(err, NotEnoughCreditError):
        return abort(400, f"User: {user_id} credit cannot get reduced below zero!")
    elif isinstance(err, DBError):
        return abort(400, DB_ERROR_STR)

    return Response(f"User: {user_id} credit updated to: {credit}", status=200)


@app.before_serving
async def startup():
    app.logger.info("Starting Payment Service")
    global db
    db = ReddisDB(app.logger)
    global logic
    logic = PaymentLogic(app.logger, db)
    global kafka
    kafka = Kafka(app.logger, logic)
    await kafka.init()


@app.after_serving
async def shutdown():
    app.logger.info("Stopping Payment Service")
    await kafka.close()
    await db.close()


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
    app.logger.setLevel(logging.INFO)
    hypercorn_logger = logging.getLogger('hypercorn.error')
    app.logger.handlers = hypercorn_logger.handlers
    app.logger.setLevel(hypercorn_logger.level)
else:
    hypercorn_logger = logging.getLogger('hypercorn.error')
    app.logger.handlers = hypercorn_logger.handlers
    app.logger.setLevel(hypercorn_logger.level)

