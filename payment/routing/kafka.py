import logging
import os

from payment.payment_logic import add_credit, remove_credit

from common.kafka.kafkaConsumer import KafkaConsumerSingleton as KafkaConsumer
from common.kafka.kafkaProducer import KafkaProducerSingleton as KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPICS = ["payment", "app-events"]


def handle_events(event):
    event_type = event.get("type")
    if event_type == "pay":
        handle_pay_event(event)
    elif event_type == "refund":
        handle_refund_event(event)
    elif event_type == "app-event":
        logging.info(f"Received app event: {event}")


async def handle_refund_event(event):
    uuid = event.get("uuid")
    amount = int(event.get("amount"))
    credit, err = await add_credit(uuid, amount)

    if err:
        event["error"] = str(err)
        return await KafkaProducer.send_event("payment", "refund-error", event)

    event["credit"] = credit
    await KafkaProducer.send_event("payment", "refund-success", event)


async def handle_pay_event(event):
    uuid = event.get("uuid")
    amount = int(event.get("amount"))
    credit, err = await remove_credit(uuid, amount)

    if err:
        event["error"] = str(err)
        return await KafkaProducer.send_event("payment", "payment-error", event)

    event["credit"] = credit
    await KafkaProducer.send_event("payment", "payment-success", event)


async def init():
    await KafkaConsumer.get_instance(
        TOPICS,
        KAFKA_BOOTSTRAP_SERVERS,
        "payment-group",
        handle_events
    )
    await KafkaProducer.get_instance(KAFKA_BOOTSTRAP_SERVERS)
    startup_event = {
        "type": "AppStarted",
        "service": "payment-service",
        "message": "Payment Service is up and running!"
    }
    await KafkaProducer.send_event("app-events", "payment-startup", startup_event)


async def close():
    await KafkaProducer.close()
    await KafkaConsumer.close()