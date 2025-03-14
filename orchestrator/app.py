import asyncio
import logging
import os
from quart import Quart, jsonify, abort, Response

from common.kafka.kafkaProducer import KafkaProducerSingleton
from common.kafka.kafkaConsumer import KafkaConsumerSingleton
from common.saga.saga import SagaManager, Saga, SagaError

#TOPIC   PRODUCE TO || CONSUMING FROM
#
#Stock: stock-operations || stock-responses
#Payment: payment-operations || payment-responses
#Order: orchestrator-responses || order-operations


# Topics Configurations
STOCK_TOPIC = ["stock-operations", "stock-responses"]
PAYMENT_TOPIC = ["payment-operations", "payment-responses"]
ORDER_TOPIC = ["order-operations", "orchestrator-responses"]

TOPICS = [*STOCK_TOPIC, *PAYMENT_TOPIC, *ORDER_TOPIC]

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

SAGA_MANAGER = SagaManager()

# Event mappings
CHECKOUT_EVENT_MAPPING = {
    "CorrectEvents": ["StockSubtracted", "PaymentProcessed"],
    "ErrorEvents": ["StockError", "PaymentError"],
    "CommitEvent": ["CheckoutCompleted"],
    "AbortEvent": ["CheckoutFailed"]
}

app = Quart("orchestrator-service")

logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


def subtract_item_transaction(event):
    event = {
        "type": "SubtractStock",
        "order_id": event["order_id"],
        "items": event["items"],
        "correlation_id": event["correlation_id"]
    }
    asyncio.create_task(KafkaProducerSingleton.send_event(STOCK_TOPIC[0], "subtract-stock", event))
    

def process_payment_transaction(event):
    event = {
        "type": "pay",
        "order_id": event["order_id"],
        "user_id": event["user_id"],
        "amount": event["amount"],
        "correlation_id": event["correlation_id"]
    }
    asyncio.create_task(KafkaProducerSingleton.send_event(PAYMENT_TOPIC[0], "process-payment", event))
    

def compensate_stock(event):
    event = {
        "type": "AddStock",
        "order_id": event["order_id"],
        "items": event["items"],
        "correlation_id": event["correlation_id"]
    }
    asyncio.create_task(KafkaProducerSingleton.send_event(STOCK_TOPIC[0], "compensate-stock", event))


def compensate_payment(event):    
    event = {
        "type": "refund",
        "order_id": event["order_id"],
        "user_id": event["user_id"],
        "amount": event["amount"],
        "correlation_id": event["correlation_id"]
    }
    asyncio.create_task(KafkaProducerSingleton.send_event(PAYMENT_TOPIC[0], "compensate-payment", event))

def commit_checkout(event, *args, **kwargs):
    asyncio.create_task(KafkaProducerSingleton.send_event(ORDER_TOPIC[0], "checkout-response", event))

def abort_checkout(event, *args, **kwargs):
    asyncio.create_task(KafkaProducerSingleton.send_event(ORDER_TOPIC[0], "checkout-response", event))


async def handle_response(event):
    app.logger.info(f"Received event: {event}")

    if event["type"] == "CheckoutRequested": # This event will start the Checkout Distributed Transaciton       
        try:
            built_saga = SAGA_MANAGER.build_distributed_transaction(
                event["correlation_id"], 
                CHECKOUT_EVENT_MAPPING, 
                [subtract_item_transaction, process_payment_transaction], 
                [compensate_stock, compensate_payment], 
                commit_checkout, 
                abort_checkout)
    
            built_saga.next_transaction(event=event)
        
        except SagaError as e:
            app.logger.error(f"SAGA execution failed [correlation_id: {e.correlation_id}]: {str(e)}")
            #await KafkaProducerSingleton.send_event(ORDER_TOPIC[0], "checkout-response", jsonify({"status": "error", "message": str(e)}))
    else:
        try:
            SAGA_MANAGER.event_handling(event=event)
        except SagaError as e:
            app.logger.error(f"SAGA execution failed [correlation_id: {e.correlation_id}]: {str(e)}")


@app.before_serving
async def startup():
    app.logger.info("Starting Orchestrator Service")

    await KafkaProducerSingleton.get_instance(KAFKA_BOOTSTRAP_SERVERS)
    
    RESPONSE_TOPIC = "orchestrator-request"

    await KafkaConsumerSingleton.get_instance(
        topics=[RESPONSE_TOPIC],
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