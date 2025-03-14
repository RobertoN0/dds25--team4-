import logging
import os
import uuid

from msgspec import msgpack, Struct
from quart import abort, jsonify, Response, Quart, json
from redis import RedisError
from redis.asyncio import Redis

from common.otlp_grcp_config import configure_telemetry
from common.kafka.kafkaConsumer import KafkaConsumerSingleton
from common.kafka.kafkaProducer import KafkaProducerSingleton

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPICS = ["payment-operations"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)

db = Redis(
            host=os.environ['REDIS_HOST'],
            port=int(os.environ['REDIS_PORT']),
            password=os.environ['REDIS_PASSWORD'],
            db=int(os.environ['REDIS_DB'])
        )


configure_telemetry('payment-service')

app = Quart("payment-service")

async def close_db_connection():
    await db.close()

DB_ERROR_STR = 'DB error'
REQ_ERROR_STR = 'Requests error'

class UserValue(Struct):
    credit: int


async def get_user_from_db(user_id: str) -> UserValue | None:
    try:
        # get serialized data
        entry: bytes = await db.get(user_id)
    except RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: UserValue | None = msgpack.decode(entry, type=UserValue) if entry else None
    if entry is None:
        # if user does not exist in the database; abort
        abort(400, f"User: {user_id} not found!")
    return entry


@app.post('/create_user')
async def create_user():
    key = str(uuid.uuid4())
    value = msgpack.encode(UserValue(credit=0))
    try:
        await db.set(key, value)
    except RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'user_id': key})


@app.post('/batch_init/<n>/<starting_money>')
async def batch_init_users(n: int, starting_money: int):
    n = int(n)
    starting_money = int(starting_money)
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(UserValue(credit=starting_money))
                                  for i in range(n)}
    try:
        await db.mset(kv_pairs)
    except RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for users successful"})


@app.get('/find_user/<user_id>')
async def find_user(user_id: str):
    user_entry: UserValue = await get_user_from_db(user_id)
    return jsonify(
        {
            "user_id": user_id,
            "credit": user_entry.credit
        }
    )


@app.post('/add_funds/<user_id>/<amount>')
async def add_credit(user_id: str, amount: int):
    user_entry: UserValue = await get_user_from_db(user_id)
    # update credit, serialize and update database
    user_entry.credit += int(amount)
    try:
        await db.set(user_id, msgpack.encode(user_entry))
    except RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)


@app.post('/pay/<user_id>/<amount>')
async def remove_credit(user_id: str, amount: int):
    app.logger.debug(f"Removing {amount} credit from user: {user_id}")
    user_entry: UserValue = await get_user_from_db(user_id)
    # update credit, serialize and update database
    user_entry.credit -= int(amount)
    if user_entry.credit < 0:
        abort(400, f"User: {user_id} credit cannot get reduced below zero!")
    try:
        await db.set(user_id, msgpack.encode(user_entry))
    except RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)


async def handle_event(event):
    logging.info(f"Received event: {event}")
    type = event["type"]
    if type == "pay":
        logging.info(f"Received pay event: {event}")
        await handle_pay_event(event)
    elif type == "refund":
        logging.info(f"Received refund event: {event}")
        await handle_refund_event(event)
    else:
        logging.info(f"Event type not implemented: {type}")


async def handle_refund_event(event):
    logging.info(f"Handling refund event: {event}")
    user_id = event["uuid"]
    try:
        user_entry_bytes = await db.get(user_id)
    except RedisError:
        event["error"] = DB_ERROR_STR
        return KafkaProducerSingleton.send_event("payment-responses", "refund-error", event)
    if user_entry_bytes is None:
        logging.info(f"User not found in DB: {user_id}")
        event["error"] = "USER NOT FOUND"
        return KafkaProducerSingleton.send_event("payment-responses", "refund-error", event)
    
    user_entry = msgpack.decode(user_entry_bytes, type=UserValue) #Need to decode the user_entry from bytes to UserValue
    # update credit, serialize and update database
    user_entry.credit += int(event["amount"])
    try:
        await db.set(user_id, msgpack.encode(user_entry))
    except RedisError:
        event["error"] = DB_ERROR_STR
        return KafkaProducerSingleton.send_event("payment-responses", "refund-error", event)

    event["credit"] = user_entry.credit
    await KafkaProducerSingleton.send_event("payment-responses", "refund-success", event)



async def handle_pay_event(event):
    logging.info(f"Handling pay event: {event}")

    user_id = event["uuid"]
    try:
        user_entry_bytes = await db.get(user_id)
    except RedisError:
        logging.info(f"Unable to retrieve user from DB: {user_id}")
        event["error"] = DB_ERROR_STR
        return KafkaProducerSingleton.send_event("payment-responses", "refund-error", event)
    if user_entry_bytes is None:
        logging.info(f"User not found in DB: {user_id}")
        event["error"] = "USER NOT FOUND"
        return KafkaProducerSingleton.send_event("payment-responses", "refund-error", event)
    user_entry = msgpack.decode(user_entry_bytes, type=UserValue) #Need to decode the user_entry from bytes to UserValue
    # update credit, serialize and update database
    user_entry.credit -= int(event["amount"])
    if user_entry.credit < 0:
        logging.info(f"User: {user_id} credit cannot get reduced below zero!")
        event["error"] = f"User: {user_id} credit cannot get reduced below zero!"
        return await KafkaProducerSingleton.send_event("payment-responses", "payment-error", event)
    try:
        await db.set(user_id, msgpack.encode(user_entry))
    except RedisError:
        event["error"] = DB_ERROR_STR
        return await KafkaProducerSingleton.send_event("payment-responses", "payment-error", event)

    logging.info(f"User: {user_id} credit updated to: {user_entry.credit}")
    event["credit"] = user_entry.credit
    await KafkaProducerSingleton.send_event("payment-responses", "payment-success", event)


@app.before_serving
async def startup():
    logging.info("Starting Payment Service")
    logging.info("Initializing Kafka")
    await KafkaProducerSingleton.get_instance(KAFKA_BOOTSTRAP_SERVERS)
    await KafkaConsumerSingleton.get_instance(
        TOPICS,
        KAFKA_BOOTSTRAP_SERVERS,
        "stock-group",
        handle_event
    )


@app.after_serving
async def shutdown():
    app.logger.info("Stopping Payment Service")
    await KafkaProducerSingleton.close()
    await KafkaConsumerSingleton.close()
    await close_db_connection()


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
