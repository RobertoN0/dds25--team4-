import asyncio
import json
import logging
import os
import atexit
import uuid

from aiokafka import AIOKafkaConsumer
import redis.asyncio as redis 
from msgspec import msgpack, Struct
from quart import Quart, jsonify, abort, Response
from common.kafka.kafkaConsumer import KafkaConsumerSingleton
from opentelemetry import trace, metrics

from common.kafka.kakfaProducer import KafkaProducerSingleton
from common.otlp_grcp_config import configure_telemetry




logging.basicConfig(
    level=logging.INFO,  # Forza a mostrare anche i log INFO
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler()  # Stampa su stdout
    ]
)


KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPICS = ["stock-operations"]


DB_ERROR_STR = "DB error"

app = Quart("stock-service")

db = redis.Redis(
    host=os.environ['REDIS_HOST'],
    port=int(os.environ['REDIS_PORT']),
    password=os.environ['REDIS_PASSWORD'],
    db=int(os.environ['REDIS_DB'])
)

configure_telemetry('stock-service')

async def close_db_connection():
    await db.close()


class StockValue(Struct):
    stock: int
    price: int


async def get_item_from_db(item_id: str) -> StockValue | None:
    try:
        entry: bytes = await db.get(item_id)
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
        await db.set(key, value)
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
        await db.mset(kv_pairs)
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
        await db.set(item_id, msgpack.encode(item_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)


@app.post('/subtract/<item_id>/<amount>')
async def remove_stock(item_id: str, amount: int):
    item_entry: StockValue = await get_item_from_db(item_id)
    # update stock, serialize and update database
    item_entry.stock -= int(amount)
    app.logger.debug(f"Item: {item_id} stock updated to: {item_entry.stock}")
    if item_entry.stock < 0:
        abort(400, f"Item: {item_id} stock cannot get reduced below zero!")
    try:
        await db.set(item_id, msgpack.encode(item_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)


async def handle_events(event):
    event_type = event.get("type")
    if event_type == "order":
        app.logger.info(f"Received order event: {event}")
    if event_type == "app-event":
        app.logger.info(f"Received app event: {event}")
    if event_type == "FindItem":
        item_id = event.get("item_id")
        try:
            item_entry: StockValue = await get_item_from_db(item_id)
            responseEvent = {
                "correlation_id": event.get("correlation_id"),
                "type": "ItemFound",
                "item_id": item_id,
                "stock": item_entry.stock,
                "price": item_entry.price
            }
            await KafkaProducerSingleton.send_event("stock-responses", "item-found", responseEvent)
            app.logger.info(f"Item found event sent: {event}")
        except Exception as e:
            logging.error(f"Error while processing FindItem event: {e}")
            responseEvent = {
                "correlation_id": event.get("correlation_id"),
                "type": "ItemNotFound",
                "item_id": item_id
            }
            await KafkaProducerSingleton.send_event("stock-responses", "item-not-found", responseEvent)
            app.logger.info(f"Item not found event sent: {event}")



@app.before_serving
async def startup():
    app.logger.info("Starting Stock Service")
    await KafkaProducerSingleton.get_instance(KAFKA_BOOTSTRAP_SERVERS)
    await KafkaConsumerSingleton.get_instance(
        TOPICS,
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
