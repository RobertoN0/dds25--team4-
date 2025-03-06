import asyncio
import logging
import os
from quart import Quart, jsonify, abort, Response

from common.kafka.kakfaProducer import KafkaProducerSingleton
from common.kafka.kafkaConsumer import KafkaConsumerSingleton
from common.saga.saga import SagaManager, Saga, SagaError

# Configurations
STOCK_TOPIC = "stock-operations"
PAYMENT_TOPIC = "payment-operations"
RESPONSE_TOPIC = "orchestator-operations"
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

SAGA_MANAGER = SagaManager()

# Event mappings
CHECKOUT_EVENT_MAPPING = {
    "CorrectEvents": ["StockSubtracted", "PaymentProcessed"],
    "ErrorEvents": ["StockError", "PaymentError"]
}

app = Quart("orchestrator-service")



def subtract_item_transaction(event):
    event = {
        "type": "SubtractStock",
        "order_id": event["order_id"],
        "items": event["items"],
        "correlation_id": event["correlation_id"]
    }
    KafkaProducerSingleton.send_event(STOCK_TOPIC, "subtract-stock", event)
    

def process_payment_transaction(event):
    event = {
        "type": "ProcessPayment",
        "order_id": event["order_id"],
        "user_id": event["user_id"],
        "amount": event["amount"],
        "correlation_id": event["correlation_id"]
    }
    KafkaProducerSingleton.send_event(PAYMENT_TOPIC, "process-payment", event)
    

def compensate_stock(event):
    event = {
        "type": "CompensateStock",
        "order_id": event["order_id"],
        "items": event["items"],
        "correlation_id": event["correlation_id"]
    }
    KafkaProducerSingleton.send_event(STOCK_TOPIC, "compensate-stock", event)


def compensate_payment(event):    
    event = {
        "type": "CompensatePayment",
        "order_id": event["order_id"],
        "user_id": event["user_id"],
        "amount": event["amount"],
        "correlation_id": event["correlation_id"]
    }
    KafkaProducerSingleton.send_event(PAYMENT_TOPIC, "compensate-payment", event)



async def handle_response(event: dict):
    if event["type"] == "CheckoutRequested": # This event will start the Checkout Distributed Transaciton       
        try:
            SAGA_MANAGER.build_distributed_transaction(CHECKOUT_EVENT_MAPPING, [], [])
            KafkaProducerSingleton.send_event(RESPONSE_TOPIC, "checkout-response", None)
        
        except SagaError as e:
            app.logger.error(f"SAGA execution failed [correlation_id: {e.correlation_id}]: {str(e)}")
            KafkaProducerSingleton.send_event(RESPONSE_TOPIC, "checkout-response", jsonify({"status": "error", "message": str(e)}))
    else:
        SAGA_MANAGER.event_handling(event["type"], event["correlation_id"], event=event)



@app.before_serving
async def startup():
    app.logger.info("Starting Order Service")

    await KafkaProducerSingleton.get_instance(KAFKA_BOOTSTRAP_SERVERS)

    await KafkaConsumerSingleton.get_instance(
        topics=[RESPONSE_TOPIC],
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="order-service-group",
        callback=handle_response
    )

@app.after_serving
async def shutdown():
    app.logger.info("Stopping Order Service")
    
    await KafkaProducerSingleton.close()
    await KafkaConsumerSingleton.close()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)