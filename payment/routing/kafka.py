import json
import os

from common.kafka.kafkaConsumer import KafkaConsumerSingleton as KafkaConsumer
from common.kafka.kafkaProducer import KafkaProducerSingleton as KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPICS = ["payment", "app-events"]


class Kafka:
    def __init__(self, logger, logic) -> None:
        self.logger = logger
        self.logic = logic

    async def handle_event(self, event):
        self.logger.info(f"Received event: {event}")
        # event = json.loads(event)
        if event["type"] == "pay":
            self.logger.info(f"Received pay event: {event}")
            await self.handle_pay_event(event)
        elif event["type"] == "refund":
            self.logger.info(f"Received refund event: {event}")
            await self.handle_refund_event(event)
        elif event["type"] == "app-event":
            self.logger.info(f"Received app event: {event}")


    async def handle_refund_event(self, event):
        self.logger.info(f"Handling refund event: {event}")
        credit, err = await self.logic.add_credit(event["uuid"], event["amount"])

        if err:
            event["error"] = str(err)
            return await KafkaProducer.send_event("payment", "refund-error", event)

        event["credit"] = credit
        await KafkaProducer.send_event("payment", "refund-success", event)


    async def handle_pay_event(self, event):
        self.logger.info(f"Handling pay event: {event}")
        credit, err = await self.logic.remove_credit(event["uuid"], event["amount"])

        if err:
            event["error"] = str(err)
            return await KafkaProducer.send_event("payment", "payment-error", event)

        event["credit"] = credit
        await KafkaProducer.send_event("payment", "payment-success", event)


    async def init(self):
        self.logger.info("Initializing Kafka")
        await KafkaConsumer.get_instance(
            TOPICS,
            KAFKA_BOOTSTRAP_SERVERS,
            "payment-group",
            self.handle_event
        )
        await KafkaProducer.get_instance(KAFKA_BOOTSTRAP_SERVERS)
        startup_event = {
            "type": "AppStarted",
            "service": "payment-service",
            "message": "Payment Service is up and running!"
        }
        await KafkaProducer.send_event("app-events", "payment-startup", json.dumps(startup_event))


    async def close(self):
        self.logger.info("Closing Kafka")
        await KafkaProducer.close()
        await KafkaConsumer.close()




