import asyncio
import logging
import os
import random
import uuid
import requests
import redis.asyncio as redis
from redis.exceptions import RedisError
from msgspec import msgpack, Struct
from quart import Quart, jsonify, abort, Response
from common.kafka.kafkaProducer import KafkaProducerSingleton
from common.kafka.kafkaConsumer import KafkaConsumerSingleton
from common.kafka.topics_config import ORDER_TOPIC, STOCK_TOPIC
from common.kafka.events_config import *

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

from common.otlp_grcp_config import configure_telemetry


DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"

GATEWAY_URL = os.environ['GATEWAY_URL']

app = Quart("order-service")

db = redis.Redis(
    host=os.environ['REDIS_HOST'],
    port=int(os.environ['REDIS_PORT']),
    password=os.environ['REDIS_PASSWORD'],
    db=int(os.environ['REDIS_DB'])
)

configure_telemetry('order-service')

async def close_db_connection():
    await db.close()

class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int

def update_items(items: list[tuple[str, int]], item_id: str, quantity: int) -> list[tuple[str, int]]:
    for i, (existing_item_id, existing_quantity) in enumerate(items):
        if existing_item_id == item_id:
            app.logger.info(f"Item: {item_id} updating")
            items[i] = (item_id, existing_quantity + quantity) 
            return items
    items.append((item_id, quantity)) 
    return items

async def get_order_from_db(order_id: str) -> OrderValue | None:
    try:
        entry: bytes = await db.get(order_id)
    except RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: OrderValue | None = msgpack.decode(entry, type=OrderValue) if entry else None
    if entry is None:
        # if order does not exist in the database; abort
        abort(400, f"Order: {order_id} not found!")
    return entry


@app.post('/create/<user_id>')
async def create_order(user_id: str):
    key = str(uuid.uuid4())
    value = msgpack.encode(OrderValue(paid=False, items=[], user_id=user_id, total_cost=0))
    try:
        await db.set(key, value)
    except RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'order_id': key})


@app.post('/batch_init/<n>/<n_items>/<n_users>/<item_price>')
async def batch_init_users(n: int, n_items: int, n_users: int, item_price: int):

    n = int(n)
    n_items = int(n_items)
    n_users = int(n_users)
    item_price = int(item_price)

    def generate_entry() -> OrderValue:
        user_id = random.randint(0, n_users - 1)
        item1_id = random.randint(0, n_items - 1)
        item2_id = random.randint(0, n_items - 1)
        value = OrderValue(paid=False,
                           items=[(f"{item1_id}", 1), (f"{item2_id}", 1)],
                           user_id=f"{user_id}",
                           total_cost=2*item_price)
        return value

    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(generate_entry()) for i in range(n)}
    try:
        await db.mset(kv_pairs)
    except RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for orders successful"})


@app.get('/find/<order_id>')
async def find_order(order_id: str):
    order_entry: OrderValue = await get_order_from_db(order_id)
    return jsonify(
        {
            "order_id": order_id,
            "paid": order_entry.paid,
            "items": order_entry.items,
            "user_id": order_entry.user_id,
            "total_cost": order_entry.total_cost
        }
    )


def send_post_request(url: str):
    try:
        response = requests.post(url)
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
    else:
        return response


def send_get_request(url: str):
    try:
        response = requests.get(url)
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
    else:
        return response
    

@app.post('/addItem/<order_id>/<item_id>/<quantity>')
async def add_item(order_id: str, item_id: str, quantity: int):
    order_entry: OrderValue = await get_order_from_db(order_id)
    correlation_id = str(uuid.uuid4())
    stream_name = f"order_response:{correlation_id}"
    event = {
        "type": EVENT_FIND_ITEM,
        "item_id": item_id,
        "correlation_id": correlation_id,
        "quantity": quantity,
        "order_id": order_id
    }
    await KafkaProducerSingleton.send_event(STOCK_TOPIC[0], correlation_id, event)
    app.logger.debug("Waiting for checkout response")
    timeout_ms = 30000  
    try:
        result = await db.xread({stream_name: '0-0'}, block=timeout_ms, count=1)
    except RedisError as e:
        app.logger.error(f"Error while reading stream {stream_name}", exc_info=True)
        return abort(400, "error while reading stream: " + str(e))
    if not result:
        return abort(408, "Timeout error")
    for _, messages in result:
        for _, fields in messages:
            data_bytes = fields.get(b"data")
            if data_bytes is not None:
                responseEvent = msgpack.decode(data_bytes)
    await db.delete(stream_name)
    if responseEvent.get("type") == EVENT_ITEM_NOT_FOUND:
        return abort(400, f"Item: {item_id} does not exist!")
    return Response(f"Item: {item_id} added to: {order_id} price updated to: {responseEvent["total_cost"]}",
                    status=200)


@app.post('/checkout/<order_id>')
async def checkout(order_id: str):
    app.logger.debug(f"Checking out {order_id}")
    order_entry: OrderValue = await get_order_from_db(order_id)
    correlation_id = str(uuid.uuid4())
    stream_name = f"order_response:{correlation_id}"
    event = {
        "type": EVENT_CHECKOUT_REQUESTED,
        "correlation_id": correlation_id,
        "order_id": order_id,
        "user_id": order_entry.user_id,
        "items": order_entry.items,
        "amount": order_entry.total_cost
    }
    await KafkaProducerSingleton.send_event(ORDER_TOPIC[0], correlation_id, event)
    app.logger.debug("Waiting for checkout response")
    timeout_ms = 30000  
    try:
        result = await db.xread({stream_name: '0-0'}, block=timeout_ms, count=1)
    except RedisError as e:
        app.logger.error(f"Error while reading stream {stream_name}", exc_info=True)
        return abort(400, "error while reading stream: " + str(e))
    if not result:
        return abort(408, "Timeout error")
    await db.delete(stream_name)
    for _, messages in result:
        for _, fields in messages:
            data_bytes = fields.get(b"data")
            if data_bytes is not None:
                responseEvent = msgpack.decode(data_bytes)
    if responseEvent.get("type") == EVENT_CHECKOUT_FAILED:
        return abort(400, "Checkout failed")
    return Response("Checkout successful", status=200)

async def handle_response_event(event):
    correlation_id = event.get("correlation_id")
    if event["type"] not in {EVENT_ITEM_FOUND, EVENT_ITEM_NOT_FOUND, EVENT_CHECKOUT_SUCCESS, EVENT_CHECKOUT_FAILED}:
        return 
    stream_name = f"order_response:{correlation_id}"
    idempotency_key = f"{event['type']}:{correlation_id}"
    already_processed_event = await db.get(idempotency_key)
    if already_processed_event:
        app.logger.info(f"Event already processed: {already_processed_event}")
        return
    if event["type"] in {EVENT_ITEM_FOUND, EVENT_ITEM_NOT_FOUND}:
        await handle_find_item_event(event, idempotency_key, stream_name)
    if event["type"] in {EVENT_CHECKOUT_SUCCESS, EVENT_CHECKOUT_FAILED}:
        await handle_checkout_event(event, idempotency_key, stream_name)

async def handle_find_item_event(event, idempotency_key, stream_name):
    try:
        async with db.pipeline() as pipe:
            if event["type"] == EVENT_ITEM_NOT_FOUND:
                pipe.multi()
                await pipe.set(idempotency_key, msgpack.encode(event), ex=3600)
                await pipe.xadd(stream_name, {"data" : msgpack.encode(event)})
                await pipe.execute()
            if event["type"] == EVENT_ITEM_FOUND:
                order_entry_bytes = await db.get(event["order_id"])
                if order_entry_bytes is None:
                    app.logger.error(f"Order not found in DB: {event['order_id']}")
                    return
                order_entry = msgpack.decode(order_entry_bytes, type=OrderValue)
                order_entry.items = update_items(order_entry.items, event["item_id"], int(event["quantity"]))
                order_entry.total_cost += int(event["quantity"]) * int(event["price"])
                event["total_cost"] = order_entry.total_cost
                pipe.multi()
                await pipe.set(idempotency_key, msgpack.encode(event), ex=3600)
                await pipe.xadd(stream_name, {"data" : msgpack.encode(event)})
                await pipe.set(event["order_id"] , msgpack.encode(order_entry))
                await pipe.execute()
    except RedisError:
        app.logger.error(f"Error while processing found item event")
        
            
async def handle_checkout_event(event, idempotency_key, stream_name):
    try:
            async with db.pipeline() as pipe:
                if event["type"] == EVENT_CHECKOUT_FAILED:
                    pipe.multi()
                    await pipe.set(idempotency_key, msgpack.encode(event), ex=3600)
                    await pipe.xadd(stream_name, {"data" : msgpack.encode(event)})
                    await pipe.execute()
                if event["type"] == EVENT_CHECKOUT_SUCCESS:
                    order_entry_bytes = await db.get(event["order_id"])
                    if order_entry_bytes is None:
                        app.logger.error(f"Order not found in DB: {event['order_id']}")
                        return
                    order_entry = msgpack.decode(order_entry_bytes, type=OrderValue)
                    order_entry.paid = True
                    pipe.multi()
                    await pipe.set(event["order_id"], msgpack.encode(order_entry))
                    await pipe.xadd(stream_name, {"data" : msgpack.encode(event)})
                    await pipe.set(idempotency_key, msgpack.encode(event), ex=3600)
                    await pipe.execute()
    except RedisError:
            app.logger.error(f"Error during checkout processing")

@app.before_serving
async def startup():
    app.logger.info("Starting Order Service")
    await KafkaProducerSingleton.get_instance(KAFKA_BOOTSTRAP_SERVERS)
    await KafkaConsumerSingleton.get_instance(
        topics=[STOCK_TOPIC[1], ORDER_TOPIC[1]],
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="order-service-group",
        callback=handle_response_event
    )

@app.after_serving
async def shutdown():
    app.logger.info("Stopping Order Service")
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
