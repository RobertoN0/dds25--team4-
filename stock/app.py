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
from msgspec import msgpack, Struct
from quart import Quart, jsonify, abort, Response
from common.db.util import retry_db_call
from common.kafka.kafkaConsumer import KafkaConsumerSingleton
from opentelemetry import trace, metrics

from common.kafka.kafkaProducer import KafkaProducerSingleton
from common.otlp_grcp_config import configure_telemetry
from common.kafka.topics_config import STOCK_TOPIC
from common.kafka.events_config import *



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
    [(
        os.environ['REDIS_SENTINEL_HOST'], int(os.environ['REDIS_SENTINEL_PORT'])
    )],
    password = os.environ['REDIS_PASSWORD']
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
    except redis.exceptions.RedisError:
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
    except redis.exceptions.RedisError:
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
    except redis.exceptions.RedisError:
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
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)


async def add_stock_event(items: list[tuple[str, int]]):
    while True:
        try:
            async with master_db.pipeline() as pipe:
                # Watch all items for changes (concurrency control)
                items_new_amount = []
                for item_id, amount in items:
                    await pipe.watch(item_id)

                items_from_db = {}
                for item_id, amount in items:
                    raw_value = await pipe.get(item_id)
                    if not raw_value:
                        raise ValueError(f"Item not found : {item_id}")
                    item_entry = msgpack.decode(raw_value, type=StockValue)
                    item_entry.stock += int(amount)
                    app.logger.info(f"Item: {item_id} stock updated to: {item_entry.stock}")
                    items_from_db[item_id] = item_entry
                    items_new_amount.append((item_id, item_entry.stock))
                # Start the transaction
                pipe.multi()
                for item_id, item_entry in items_from_db.items():
                    pipe.set(item_id, msgpack.encode(item_entry))
                # Execute the transaction
                await pipe.execute()
            return items_new_amount
        except WatchError:
            # If a watched key has been modified, the transaction is aborted
            logging.error("Concurrency conflict detected. Transaction aborted.")
            continue
        except redis.RedisError:
            logging.error(f"Redis error while adding stock")
            raise ValueError("Error while adding stock.")


@app.post('/subtract/<item_id>/<amount>')
async def remove_stock(item_id: str, amount: int):
    item_entry: StockValue = await get_item_from_db(item_id)
    # update stock, serialize and update database
    item_entry.stock -= int(amount)
    app.logger.debug(f"Item: {item_id} stock updated to: {item_entry.stock}")
    if item_entry.stock < 0:
        abort(400, f"Item: {item_id} stock cannot get reduced below zero!")
    try:
        await retry_db_call(master_db.set, item_id, msgpack.encode(item_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)

async def remove_stock_event(items: list[tuple[str, int]]):
    while True:
        try:
            async with master_db.pipeline() as pipe:
                # Watch all items for changes (concurrency control)
                items_new_amount = []
                for item_id, amount in items:
                    await pipe.watch(item_id)
                items_from_db = {}
                for item_id, amount in items:
                    raw_value = await pipe.get(item_id)
                    if not raw_value:
                        raise ValueError(f"Item not found : {item_id}")
                    item_entry = msgpack.decode(raw_value, type=StockValue)
                    new_stock = item_entry.stock - int(amount)
                    if new_stock < 0:
                        raise ValueError(f"Insufficient stock for item: {item_id}")
                    item_entry.stock = new_stock
                    app.logger.info(f"Item: {item_id} stock updated to: {item_entry.stock}")
                    items_from_db[item_id] = item_entry
                    items_new_amount.append((item_id, item_entry.stock))
                # Start the transaction
                pipe.multi()
                for item_id, item_entry in items_from_db.items():
                    pipe.set(item_id, msgpack.encode(item_entry))
                # Execute the transaction
                await pipe.execute()
            return items_new_amount
        except WatchError:
            logging.error("Concurrency conflict detected. Transaction aborted.")
            continue
        except redis.RedisError as e:
            logging.error(f"Redis error: {e}")
            raise ValueError("Error while removing stock.")
        except ValueError as ve:
            logging.error(f"Validation error: {ve}")
            raise


async def handle_events(event):
    app.logger.info(f"Received: {event}")
    event_type = event.get("type")
    if event_type == EVENT_FIND_ITEM:
        await handle_event_find_item(event)
    elif event_type == EVENT_ADD_STOCK:
        await handle_event_add_stock(event)
    elif event_type == EVENT_SUBTRACT_STOCK:
        await handle_event_remove_stock(event)


async def handle_event_find_item(event):
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

async def handle_event_add_stock(event):
    items = event.get("items")
    try:
        success_items = await add_stock_event(items)
        event["type"] = EVENT_STOCK_COMPENSATED
        #event["items"] = success_items
        await KafkaProducerSingleton.send_event(STOCK_TOPIC[1], event["correlation_id"], event)
    except Exception as e:
        logging.error(f"Error while processing AddStock event: {e}")
        event["type"] = EVENT_STOCK_COMPENSATION_FAILED
        event["error"] = str(e)
        await KafkaProducerSingleton.send_event(STOCK_TOPIC[1], event["correlation_id"], event)


async def handle_event_remove_stock(event):
    items = event.get("items")
    try:
        success_items = await remove_stock_event(items)
        event["type"] = EVENT_STOCK_SUBTRACTED
        #event["items"] = success_items
        await KafkaProducerSingleton.send_event(STOCK_TOPIC[1], event["correlation_id"], event)
    except Exception as e:
        logging.error(f"Error while processing RemoveStock event: {e}")
        event["type"] = EVENT_STOCK_ERROR
        event["error"] = str(e)
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
    hypercorn_logger = logging.getLogger('hypercorn.error')
    app.logger.handlers = hypercorn_logger.handlers
    app.logger.setLevel(hypercorn_logger.level)
else:
    hypercorn_logger = logging.getLogger('hypercorn.error')
    app.logger.handlers = hypercorn_logger.handlers
    app.logger.setLevel(hypercorn_logger.level)
