import asyncio
import json
import logging
import os
import atexit
import uuid

from aiokafka import AIOKafkaConsumer
import redis.asyncio as redis
from redis.asyncio import Sentinel
from redis.exceptions import WatchError
from redis.exceptions import RedisError
from msgspec import msgpack, Struct
from quart import Quart, jsonify, abort, Response
from common.db.util import retry_db_call
from common.kafka.kafkaConsumer import KafkaConsumerSingleton
from opentelemetry import trace, metrics

from common.kafka.kafkaProducer import KafkaProducerSingleton
from common.otlp_grcp_config import configure_telemetry
from common.kafka.topics_config import STOCK_TOPIC
from common.kafka.events_config import *
from redis.exceptions import ConnectionError, TimeoutError
from redis.sentinel import MasterNotFoundError

logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler() 
    ]
)

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")

DB_ERROR_STR = "DB error"

app = Quart("stock-service")

sentinel = Sentinel(
    [
        (host.split(':')[0], int(host.split(':')[1]))
        for host in os.environ['REDIS_SENTINEL_HOSTS'].split(',')
    ],
    password=os.environ['REDIS_PASSWORD']
)

master_db = sentinel.master_for(
    service_name=os.environ['REDIS_SERVICE_NAME'],
    password=os.environ['REDIS_PASSWORD'],
    db=int(os.environ['REDIS_DB'])
)

configure_telemetry('stock-service')

async def close_db_connection():
    await master_db.close()
    


class StockValue(Struct):
    stock: int
    price: int


async def get_item_from_db(item_id: str) -> StockValue | None:
    try:
        entry: bytes = await retry_db_call(master_db.get, item_id)
    except RedisError:
        return abort(400, DB_ERROR_STR)
    entry: StockValue | None = msgpack.decode(entry, type=StockValue) if entry else None
    if entry is None:
        abort(400, f"Item: {item_id} not found!")
    return entry


@app.post('/item/create/<price>')
async def create_item(price: int):
    key = str(uuid.uuid4())
    app.logger.debug(f"Item: {key} created")
    value = msgpack.encode(StockValue(stock=0, price=int(price)))
    try:
        await retry_db_call(master_db.set, key, value)
    except RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'item_id': key})


@app.post('/batch_init/<n>/<starting_stock>/<item_price>')
async def batch_init_users(n: int, starting_stock: int, item_price: int):
    n = int(n)
    starting_stock = int(starting_stock)
    item_price = int(item_price)
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(StockValue(stock=starting_stock, price=item_price))
                                  for i in range(n)}
    try:
        await retry_db_call(master_db.mset, kv_pairs)
    except RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for stock successful"})


@app.get('/find/<item_id>')
async def find_item(item_id: str):
    item_entry: StockValue = await get_item_from_db(item_id)
    return jsonify(
        {
            "stock": item_entry.stock,
            "price": item_entry.price
        }
    )


@app.post('/add/<item_id>/<amount>')
async def add_stock(item_id: str, amount: int):
    item_entry: StockValue = await get_item_from_db(item_id)
    item_entry.stock += int(amount)
    try:
        await retry_db_call(master_db.set, item_id, msgpack.encode(item_entry))
    except RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)

@app.post('/subtract/<item_id>/<amount>')
async def remove_stock(item_id: str, amount: int):
    item_entry: StockValue = await get_item_from_db(item_id)
    # update stock, serialize and update database
    item_entry.stock -= int(amount)
    if item_entry.stock < 0:
        abort(400, f"Item: {item_id} stock cannot get reduced below zero!")
    try:
        await retry_db_call(master_db.set, item_id, msgpack.encode(item_entry))
    except RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)


async def add_stock_event(event, idempotency_key):
    items = event.get("items")
    max_retries = 3
    attempt = 0
    while attempt < max_retries:
        try:
            async with master_db.pipeline() as pipe:
                # Watch all items for changes (concurrency control)
                for item_id, amount in items:
                    await pipe.watch(item_id)

                new_items = {}
                for item_id, amount in items:
                    raw_value = await pipe.get(item_id)
                    if not raw_value:
                        event["error"] = f"ITEM {item_id} NOT FOUND"
                        event["type"] = EVENT_STOCK_COMPENSATION_FAILED
                    item_entry = msgpack.decode(raw_value, type=StockValue)
                    item_entry.stock += int(amount)
                    new_items[item_id] = item_entry
                event["type"] = EVENT_STOCK_COMPENSATED
                # Start the transaction
                pipe.multi()
                for item_id, item_entry in new_items.items():
                    pipe.set(item_id, msgpack.encode(item_entry))
                # Execute the transaction
                pipe.set(idempotency_key, msgpack.encode(event), ex=3600)
                await pipe.execute()
            return await KafkaProducerSingleton.send_event(STOCK_TOPIC[1], event["correlation_id"], event)
        except WatchError:
            # If a watched key has been modified, the transaction is aborted
            logging.error("Concurrency conflict detected. Transaction aborted.")
            continue
        except RedisError:
            logging.error(f"Redis error while adding stock")
            event["error"] = DB_ERROR_STR
            event["type"] = EVENT_STOCK_COMPENSATION_FAILED
            await KafkaProducerSingleton.send_event(STOCK_TOPIC[1], event["correlation_id"], event)
        except (ConnectionError, TimeoutError, MasterNotFoundError) as e:
            attempt += 1
            if attempt >= max_retries:
                event["error"] = DB_ERROR_STR
                event["type"] = EVENT_STOCK_COMPENSATION_FAILED
                return await KafkaProducerSingleton.send_event(STOCK_TOPIC[1], event["correlation_id"], event)
            await asyncio.sleep(0.5)
            
            
async def remove_stock_event(event, idempotency_key):
    items = event.get("items")
    max_retries = 3
    attempt = 0
    while attempt < max_retries:
        try:
            async with master_db.pipeline() as pipe:
                # Watch all items for changes (concurrency control)
                for item_id, amount in items:
                    await pipe.watch(item_id)
                new_items = {}
                for item_id, amount in items:
                    raw_value = await pipe.get(item_id)
                    if not raw_value:
                        event["error"] = f"ITEM {item_id} NOT FOUND"
                        event["type"] = EVENT_STOCK_ERROR
                        return await KafkaProducerSingleton.send_event(STOCK_TOPIC[1], event["correlation_id"], event)
                    item_entry = msgpack.decode(raw_value, type=StockValue)
                    new_stock = item_entry.stock - int(amount)
                    if new_stock < 0:
                        event["error"] = f"ITEM {item_id} STOCK CANNOT GET REDUCED BELOW ZERO"
                        event["type"] = EVENT_STOCK_ERROR
                        return await KafkaProducerSingleton.send_event(STOCK_TOPIC[1], event["correlation_id"], event)
                    item_entry.stock = new_stock
                    new_items[item_id] = item_entry
                # Start the transaction
                event["type"] = EVENT_STOCK_SUBTRACTED
                pipe.multi()
                for item_id, item_entry in new_items.items():
                    pipe.set(item_id, msgpack.encode(item_entry), ex=3600)
                pipe.set(idempotency_key, msgpack.encode(event), ex=3600)
                # Execute the transaction
                await pipe.execute()
            return await KafkaProducerSingleton.send_event(STOCK_TOPIC[1], event["correlation_id"], event)
        except WatchError:
            logging.error("Concurrency conflict detected. Transaction aborted.")
            continue
        except RedisError:
            event["error"] = DB_ERROR_STR
            event["type"] = EVENT_STOCK_ERROR
            await KafkaProducerSingleton.send_event(STOCK_TOPIC[1], event["correlation_id"], event)
        except (ConnectionError, TimeoutError, MasterNotFoundError) as e:
            attempt += 1
            if attempt >= max_retries:
                event["error"] = DB_ERROR_STR
                event["type"] = EVENT_STOCK_ERROR
                return await KafkaProducerSingleton.send_event(STOCK_TOPIC[1], event["correlation_id"], event)
            await asyncio.sleep(0.5)


async def handle_events(event):
    event_type = event.get("type")
    if event_type == EVENT_FIND_ITEM:
        return await find_item_event(event)
    idempotency_key = f"{event_type}:{event["correlation_id"]}"
    already_processed_event = await retry_db_call(master_db.get, idempotency_key)
    if already_processed_event:
        already_processed_event = msgpack.decode(already_processed_event)
        app.logger.info(f"Event already processed: {already_processed_event}")
        return await KafkaProducerSingleton.send_event(STOCK_TOPIC[1], event["correlation_id"], already_processed_event)
    elif event_type == EVENT_ADD_STOCK:
        await add_stock_event(event, idempotency_key)
    elif event_type == EVENT_SUBTRACT_STOCK:
        await remove_stock_event(event, idempotency_key)


async def find_item_event(event):
    item_id = event.get("item_id")
    try:
        item_entry: StockValue = await get_item_from_db(item_id)
        event["type"] = EVENT_ITEM_FOUND
        event["stock"] = item_entry.stock
        event["price"] = item_entry.price
        await KafkaProducerSingleton.send_event(STOCK_TOPIC[1], event["correlation_id"], event)
    except Exception as e:
        logging.error(f"Error while processing FindItem event: {e}")
        event["type"] = EVENT_ITEM_NOT_FOUND
        await KafkaProducerSingleton.send_event(STOCK_TOPIC[1], event["correlation_id"], event)


@app.before_serving
async def startup():
    app.logger.info("Starting Stock Service")
    await KafkaProducerSingleton.get_instance(KAFKA_BOOTSTRAP_SERVERS)
    await KafkaConsumerSingleton.get_instance(
        [STOCK_TOPIC[0]],
        KAFKA_BOOTSTRAP_SERVERS,
        "stock-group",
        handle_events
    )

@app.after_serving
async def shutdown():
    app.logger.info("Stopping Stock Service")
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
