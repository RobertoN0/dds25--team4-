from aiokafka import AIOKafkaProducer
import json
import logging

class KafkaProducerSingleton:
    _instance = None
    _bootstrap_servers = None
    @classmethod
    async def get_instance(cls, bootstrap_servers):
        if cls._instance is None:
            cls._bootstrap_servers = bootstrap_servers
            cls._instance = AIOKafkaProducer(
                bootstrap_servers=bootstrap_servers
            )
            await cls._instance.start()
            logging.info("Kafka Producer started")
        return cls._instance

    @classmethod
    async def send_event(cls, topic, event_key, event_value):
        producer = await cls.get_instance(cls._bootstrap_servers)
        message = json.dumps(event_value).encode('utf-8')
        await producer.send_and_wait(topic, key=event_key.encode('utf-8'), value=message)

    @classmethod
    async def close(cls):
        if cls._instance:
            await cls._instance.stop()
            logging.info("Kafka Producer stopped")
            cls._instance = None
