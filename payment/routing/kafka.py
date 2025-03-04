import json
import logging
import os

from payment.payment_logic import add_credit, remove_credit

from common.kafka.kafkaConsumer import KafkaConsumerSingleton as KafkaConsumer
from common.kafka.kafkaProducer import KafkaProducerSingleton as KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPICS = ["payment", "app-events"]


async def handle_event(event):
    event = json.loads(event)

    if event["type"] == "pay":
        logging.info(f"Received pay event: {event}")
        await handle_pay_event(event)
    elif event["type"] == "refund":
        logging.info(f"Received refund event: {event}")
        await handle_refund_event(event)
    elif event["type"] == "app-event":
        logging.info(f"Received app event: {event}")


async def handle_refund_event(event):
    credit, err = await add_credit(event["uuid"], event["amount"])

    if err:
        event["error"] = str(err)
        return await KafkaProducer.send_event("payment", "refund-error", json.dumps(event))

    event["credit"] = credit
    await KafkaProducer.send_event("payment", "refund-success", json.dumps(event))


async def handle_pay_event(event):
    credit, err = await remove_credit(event["uuid"], event["amount"])

    if err:
        event["error"] = str(err)
        return await KafkaProducer.send_event("payment", "payment-error", json.dumps(event))

    event["credit"] = credit
    await KafkaProducer.send_event("payment", "payment-success", json.dumps(event))


async def init():
    await KafkaConsumer.get_instance(
        TOPICS,
        KAFKA_BOOTSTRAP_SERVERS,
        "payment-group",
        handle_event
    )
    await KafkaProducer.get_instance(KAFKA_BOOTSTRAP_SERVERS)
    startup_event = {
        "type": "AppStarted",
        "service": "payment-service",
        "message": "Payment Service is up and running!"
    }
    await KafkaProducer.send_event("app-events", "payment-startup", json.dumps(startup_event))


async def close():
    await KafkaProducer.close()
    await KafkaConsumer.close()