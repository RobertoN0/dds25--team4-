import asyncio
import logging
import os
import atexit
import random
import uuid
from collections import defaultdict

import requests
import redis.asyncio as redis
from msgspec import msgpack, Struct
from quart import Quart, jsonify, abort, Response
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
import json

from common.kafka.kafkaProducer import KafkaProducerSingleton
from common.kafka.kafkaConsumer import KafkaConsumerSingleton


pending_requests = {}
TOPICS = ["stock-responses"]
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


async def get_order_from_db(order_id: str) -> OrderValue | None:
    try:
        entry: bytes = await db.get(order_id)
    except redis.exceptions.RedisError:
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
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'order_id': key})


# Created for testing purposes
#@app.post('/create/<user_id>')
#async def create_order(user_id: str):
#    key = str(uuid.uuid4())
#    order = OrderValue(paid=False, items=[], user_id=user_id, total_cost=0)
#    encoded_order = msgpack.encode(order)
#    try:
#        await db.set(key, encoded_order)
#    except redis.exceptions.RedisError:
#        return abort(400, DB_ERROR_STR)
#    
#    # Costruisci l'evento da inviare
#    event = {
#        "type": "OrderCreated",
#        "order_id": key,
#        "user_id": user_id,
#        "paid": False,
#        "items": [],
#        "total_cost": 0
#    }
#    await KafkaProducerSingleton.send_event("orders", "order-created", event)
#    
#    return jsonify({'order_id': key})

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

    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(generate_entry())
                                  for i in range(n)}
    try:
        await db.mset(kv_pairs)
    except redis.exceptions.RedisError:
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
    

# Rest vesion of add_item
#@app.post('/addItem/<order_id>/<item_id>/<quantity>')
#async def add_item(order_id: str, item_id: str, quantity: int):
#    order_entry: OrderValue = await get_order_from_db(order_id)
#    item_reply = send_get_request(f"{GATEWAY_URL}/stock/find/{item_id}")
#    if item_reply.status_code != 200:
#        # Request failed because item does not exist
#        abort(400, f"Item: {item_id} does not exist!")
#    item_json: dict = item_reply.json()
#    order_entry.items.append((item_id, int(quantity)))
#    order_entry.total_cost += int(quantity) * item_json["price"]
#    try:
#        await db.set(order_id, msgpack.encode(order_entry))
#    except redis.exceptions.RedisError:
#        return abort(400, DB_ERROR_STR)
#    return Response(f"Item: {item_id} added to: {order_id} price updated to: {order_entry.total_cost}",
#                    status=200)
#

@app.post('/addItem/<order_id>/<item_id>/<quantity>')
async def add_item(order_id: str, item_id: str, quantity: int):
    order_entry: OrderValue = await get_order_from_db(order_id)
    correlation_id = str(uuid.uuid4())
    loop = asyncio.get_event_loop()
    future = loop.create_future()
    pending_requests[correlation_id] = future
    event = {
        "type": "FindItem",
        "item_id": item_id,
        "correlation_id": correlation_id
    }
    await KafkaProducerSingleton.send_event("stock-operations", "find-item", event)
    try:
        responseEvent = await asyncio.wait_for(future, timeout=10)
    except asyncio.TimeoutError:
        pending_requests.pop(correlation_id, None)
        return abort(408, "Timeout error")
    if responseEvent.get("type") != "ItemFound":
        return abort(400, f"Item: {item_id} does not exist!")
    order_entry.items.append((item_id, int(quantity)))
    order_entry.total_cost += int(quantity) * responseEvent.get("price")
    try:
        await db.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} added to: {order_id} price updated to: {order_entry.total_cost}",
                    status=200)


#should not be needed anymore
def rollback_stock(removed_items: list[tuple[str, int]]):
    for item_id, quantity in removed_items:
        send_post_request(f"{GATEWAY_URL}/stock/add/{item_id}/{quantity}")

# Rest version of checkout
#@app.post('/checkout/<order_id>')
#async def checkout(order_id: str):
#    app.logger.debug(f"Checking out {order_id}")
#    order_entry: OrderValue = await get_order_from_db(order_id)
#    # get the quantity per item
#    items_quantities: dict[str, int] = defaultdict(int)
#    for item_id, quantity in order_entry.items:
#        items_quantities[item_id] += quantity
#    # The removed items will contain the items that we already have successfully subtracted stock from
#    # for rollback purposes.
#    removed_items: list[tuple[str, int]] = []
#    for item_id, quantity in items_quantities.items():
#        stock_reply = send_post_request(f"{GATEWAY_URL}/stock/subtract/{item_id}/{quantity}")
#        if stock_reply.status_code != 200:
#            # If one item does not have enough stock we need to rollback
#            rollback_stock(removed_items)
#            abort(400, f'Out of stock on item_id: {item_id}')
#        removed_items.append((item_id, quantity))
#    user_reply = send_post_request(f"{GATEWAY_URL}/payment/pay/{order_entry.user_id}/{order_entry.total_cost}")
#    if user_reply.status_code != 200:
#        # If the user does not have enough credit we need to rollback all the item stock subtractions
#        rollback_stock(removed_items)
#        abort(400, "User out of credit")
#    order_entry.paid = True
#    try:
#        await db.set(order_id, msgpack.encode(order_entry))
#    except redis.exceptions.RedisError:
#        return abort(400, DB_ERROR_STR)
#    app.logger.debug("Checkout successful")
#    return Response("Checkout successful", status=200)

@app.post('/checkout/<order_id>')
async def checkout(order_id: str):
    app.logger.debug(f"Checking out {order_id}")
    order_entry: OrderValue = await get_order_from_db(order_id)
    correlation_id = str(uuid.uuid4())
    loop = asyncio.get_event_loop()
    future = loop.create_future()
    pending_requests[correlation_id] = future
    event = {
        "type": "CheckoutRequested",
        "correlation_id": correlation_id,
        "order_id": order_id,
        "user_id": order_entry.user_id,
        "items": order_entry.items,
        "total_cost": order_entry.total_cost
    }
    await KafkaProducerSingleton.send_event("orchestator-operations", "checkout-requested", event)
    app.logger.debug("Waiting for checkout response")
    try:
        responseEvent = await asyncio.wait_for(future, timeout=10)
    except asyncio.TimeoutError:
        pending_requests.pop(correlation_id, None)
        return abort(408, "Timeout error")
    if responseEvent.get("type") != "CheckoutSuccess":
        return abort(400, "Checkout failed")
    order_entry.paid = True
    try:
        await db.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    app.logger.debug("Checkout successful")
    return Response("Checkout successful", status=200)

async def handle_response_event(event):
    correlation_id = event.get("correlation_id")
    if not correlation_id:
        return 
    future = pending_requests.pop(correlation_id, None) 
    if future and not future.done():
        future.set_result(event)  

@app.before_serving
async def startup():
    app.logger.info("Starting Order Service")
    await KafkaProducerSingleton.get_instance(KAFKA_BOOTSTRAP_SERVERS)
    await KafkaConsumerSingleton.get_instance(
        topics=TOPICS,
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
