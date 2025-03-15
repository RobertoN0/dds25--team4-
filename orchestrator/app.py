import asyncio
import logging
import os
from quart import Quart, jsonify, abort, Response

from common.kafka.kafkaProducer import KafkaProducerSingleton
from common.kafka.kafkaConsumer import KafkaConsumerSingleton
from common.saga.saga import SagaManager, Saga, SagaError
from common.kafka.topics_config import *
from common.kafka.events_config import *

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

SAGA_MANAGER = SagaManager()

# Event mappings
CHECKOUT_EVENT_MAPPING = {
    "CorrectEvents": [EVENT_STOCK_SUBTRACTED, EVENT_PAYMENT_SUCCESS],
    "ErrorEvents": [EVENT_STOCK_ERROR, EVENT_PAYMENT_ERROR],
    "CommitEvent": [EVENT_CHECKOUT_SUCCESS],
    "AbortEvent": [EVENT_CHECKOUT_FAILED]
}

app = Quart("orchestrator-service")

logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler() 
    ]
)


async def subtract_item_transaction(event):
    event["type"] = EVENT_SUBTRACT_STOCK
    await KafkaProducerSingleton.send_event(STOCK_TOPIC[0], "subtract-stock", event)
    

async def process_payment_transaction(event):
    event["type"] = EVENT_PAY
    await KafkaProducerSingleton.send_event(PAYMENT_TOPIC[0], "process-payment", event)
    

async def compensate_stock(event):
    event["type"] = EVENT_ADD_STOCK
    await KafkaProducerSingleton.send_event(STOCK_TOPIC[0], "compensate-stock", event)


async def compensate_payment(event):    
    event["type"] = EVENT_REFUND
    await KafkaProducerSingleton.send_event(PAYMENT_TOPIC[0], "compensate-payment", event)

async def commit_checkout(event, *args, **kwargs):
    await KafkaProducerSingleton.send_event(ORDER_TOPIC[1], "checkout-response", event)

async def abort_checkout(event, *args, **kwargs):
    await KafkaProducerSingleton.send_event(ORDER_TOPIC[1], "checkout-response", event)


async def handle_response(event):
    if event["type"] == EVENT_CHECKOUT_REQUESTED: # This event will start the Checkout Distributed Transaciton       
        try:
            built_saga = SAGA_MANAGER.build_distributed_transaction(
                event["correlation_id"], 
                CHECKOUT_EVENT_MAPPING, 
                [subtract_item_transaction, process_payment_transaction], 
                [compensate_stock, compensate_payment], 
                commit_checkout, 
                abort_checkout)
    
            await built_saga.next_transaction(event=event)
        
        except SagaError as e:
            app.logger.error(f"SAGA execution failed [correlation_id: {e.correlation_id}]: {str(e)}")
            event["type"] = EVENT_CHECKOUT_FAILED
            event["error"] = str(e)
            await KafkaProducerSingleton.send_event(ORDER_TOPIC[1], "checkout-response", event)
    elif any(event["type"] in values for values in CHECKOUT_EVENT_MAPPING.values()): # Discard any other event
        try:
            await SAGA_MANAGER.event_handling(event=event)
        except SagaError as e:
            app.logger.error(f"SAGA execution failed [correlation_id: {e.correlation_id}]: {str(e)}")


@app.before_serving
async def startup():
    app.logger.info("Starting Orchestrator Service")

    await KafkaProducerSingleton.get_instance(KAFKA_BOOTSTRAP_SERVERS)


    await KafkaConsumerSingleton.get_instance(
        topics=[ORDER_TOPIC[0], STOCK_TOPIC[1], PAYMENT_TOPIC[1]],
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="orchestrator-service-group",
        callback=handle_response
    )

@app.after_serving
async def shutdown():
    app.logger.info("Stopping Orchestrator Service")
    
    await KafkaProducerSingleton.close()
    await KafkaConsumerSingleton.close()


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
    app.logger.setLevel(logging.INFO)
else:
    hypercorn_logger = logging.getLogger('hypercorn.error')
    app.logger.handlers = hypercorn_logger.handlers
    app.logger.setLevel(hypercorn_logger.level)