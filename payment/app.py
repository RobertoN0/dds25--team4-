import asyncio
import logging
import os
import uuid

from msgspec import msgpack, Struct
from quart import abort, jsonify, Response, Quart, json
from redis.exceptions import WatchError, RedisError, ConnectionError, TimeoutError
from redis.asyncio import Redis
from redis.asyncio.sentinel import Sentinel

from common.db.util import retry_db_call
from common.kafka.kafkaConsumer import KafkaConsumerSingleton
from common.kafka.kafkaProducer import KafkaProducerSingleton
from common.kafka.topics_config import PAYMENT_TOPIC
from common.kafka.events_config import *
from redis.sentinel import MasterNotFoundError

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)

sentinel = Sentinel(
    [
         (host.strip(), int(os.environ['REDIS_SENTINEL_PORT']))
        for host in os.environ['REDIS_SENTINEL_HOSTS'].split(',')
    ],
    password=os.environ['REDIS_PASSWORD']
)

master_db = sentinel.master_for(
    service_name=os.environ['REDIS_SERVICE_NAME'],
    password=os.environ['REDIS_PASSWORD'],
    db=int(os.environ['REDIS_DB'])
)

app = Quart("payment-service")

async def close_db_connection():
    await master_db.close()

DB_ERROR_STR = 'DB error'
REQ_ERROR_STR = 'Requests error'

class UserValue(Struct):
    credit: int

async def get_user_from_db(user_id: str) -> UserValue | None:
    try:
        entry = await retry_db_call(master_db.get, user_id)
    except (ConnectionError, TimeoutError, MasterNotFoundError, RedisError) as e:
        return abort(400, DB_ERROR_STR)
    entry: UserValue | None = msgpack.decode(entry, type=UserValue) if entry else None
    if entry is None:
        abort(400, f"User: {user_id} not found!")
    return entry


@app.post('/create_user')
async def create_user():
    key = str(uuid.uuid4())
    value = msgpack.encode(UserValue(credit=0))
    try:
        await retry_db_call(master_db.set, key, value)
    except (ConnectionError, TimeoutError, MasterNotFoundError, RedisError) as e:
        return abort(400, DB_ERROR_STR)
    return jsonify({'user_id': key})

@app.post('/batch_init/<n>/<starting_money>')
async def batch_init_users(n: int, starting_money: int):
    n = int(n)
    starting_money = int(starting_money)
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(UserValue(credit=starting_money))
                                  for i in range(n)}
    try:
        await retry_db_call(master_db.mset, kv_pairs)
    except (ConnectionError, TimeoutError, MasterNotFoundError, RedisError) as e:
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
        await retry_db_call(master_db.set, user_id, msgpack.encode(user_entry))
    except (ConnectionError, TimeoutError, MasterNotFoundError, RedisError) as e:
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
        await retry_db_call(master_db.set, user_id, msgpack.encode(user_entry))
    except (ConnectionError, TimeoutError, MasterNotFoundError, RedisError) as e:
        return abort(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)


async def handle_event(event):
    event_type = event["type"]
    idempotency_key = f"{event_type}:{event['correlation_id']}"
    already_processed_event = await retry_db_call(master_db.get, idempotency_key)
    if already_processed_event:
        already_processed_event = msgpack.decode(already_processed_event)
        app.logger.info(f"Event already processed: {already_processed_event}")
        await KafkaProducerSingleton.send_event(PAYMENT_TOPIC[1], already_processed_event["correlation_id"], already_processed_event)
        return
    if event_type == EVENT_PAY:
        await handle_pay_event(event, idempotency_key)
    elif event_type == EVENT_REFUND:
        await handle_refund_event(event, idempotency_key)


async def handle_refund_event(event, idempotency_key):
    user_id = event["user_id"]
    max_retries = 5
    attempt = 0
    while attempt < max_retries:
        try:
            async with master_db.pipeline() as pipe:
                await pipe.watch(user_id)
                user_entry_bytes = await pipe.get(user_id)
                if user_entry_bytes is None:
                    event["error"] = "USER NOT FOUND"
                    event["type"] = EVENT_REFUND_ERROR
                    await retry_db_call(master_db.set, idempotency_key, msgpack.encode(event), ex=3600)
                    return await KafkaProducerSingleton.send_event(PAYMENT_TOPIC[1], event["correlation_id"], event)
                user_entry = msgpack.decode(user_entry_bytes, type=UserValue)
                user_entry.credit += int(event["amount"])

                event["credit"] = user_entry.credit
                event["type"] = EVENT_REFUND_SUCCESS
                pipe.multi()
                await pipe.set(user_id, msgpack.encode(user_entry))
                await pipe.set(idempotency_key, msgpack.encode(event), ex=3600)
                await pipe.execute()
            return await KafkaProducerSingleton.send_event(PAYMENT_TOPIC[1], event["correlation_id"], event)
        except WatchError:
            # If a watched key has been modified, the transaction is aborted
            app.logger.error("Concurrency conflict detected. Transaction aborted.")
            continue
        except (ConnectionError, TimeoutError, MasterNotFoundError, RedisError) as e:
            app.logger.error(f"Error during refund: {e}")
            await asyncio.sleep(0.5)
            continue


async def handle_pay_event(event, idempotency_key):
    user_id = event["user_id"]
    max_retries = 5
    attempt = 0
    while attempt < max_retries:
        try:
            async with master_db.pipeline() as pipe:
                await pipe.watch(user_id)
                user_entry_bytes = await pipe.get(user_id)
                if user_entry_bytes is None:
                    event["error"] = "USER NOT FOUND"
                    event["type"] = EVENT_PAYMENT_ERROR
                    await master_db.set(idempotency_key, msgpack.encode(event), ex=3600)
                    return await KafkaProducerSingleton.send_event(PAYMENT_TOPIC[1], event["correlation_id"], event)
                
                user_entry = msgpack.decode(user_entry_bytes, type=UserValue)
                user_entry.credit -= int(event["amount"])
                if user_entry.credit < 0:
                    event["error"] = "INSUFFICIENT FUNDS"
                    event["type"] = EVENT_PAYMENT_ERROR
                    await retry_db_call(master_db.set, idempotency_key, msgpack.encode(event), ex=3600)
                    return await KafkaProducerSingleton.send_event(PAYMENT_TOPIC[1], event["correlation_id"], event)
                
                event["credit"] = user_entry.credit
                event["type"] = EVENT_PAYMENT_SUCCESS
                # update credit, serialize and update database
                pipe.multi()
                await pipe.set(user_id, msgpack.encode(user_entry))
                await pipe.set(idempotency_key, msgpack.encode(event), ex=3600)
                await pipe.execute()
            return await KafkaProducerSingleton.send_event(PAYMENT_TOPIC[1], event["correlation_id"], event)
        except WatchError:
            logging.error("Concurrency conflict detected. Transaction aborted.")
            continue
        except (ConnectionError, TimeoutError, MasterNotFoundError, RedisError) as e:
            attempt += 1
            if attempt >= max_retries:
                event["error"] = DB_ERROR_STR + str(e)
                event["type"] = EVENT_PAYMENT_ERROR
                await retry_db_call(master_db.set, idempotency_key, msgpack.encode(event), ex=3600)
                return await KafkaProducerSingleton.send_event(PAYMENT_TOPIC[1], event["correlation_id"], event)
            await asyncio.sleep(0.5)   


@app.before_serving
async def startup():
    app.logger.info("Starting Payment Service")
    await KafkaProducerSingleton.get_instance(KAFKA_BOOTSTRAP_SERVERS)
    await KafkaConsumerSingleton.get_instance(
        [PAYMENT_TOPIC[0]],
        KAFKA_BOOTSTRAP_SERVERS,
        "payment-group",
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
else:
    hypercorn_logger = logging.getLogger('hypercorn.error')
    app.logger.handlers = hypercorn_logger.handlers
    app.logger.setLevel(hypercorn_logger.level)
