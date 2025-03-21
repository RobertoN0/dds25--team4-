from aiokafka import AIOKafkaConsumer
import asyncio
import json
import logging

class KafkaConsumerSingleton:
    _instance = None 
    _task = None 

    @classmethod
    async def get_instance(cls, topics, bootstrap_servers, group_id, callback):
        if cls._instance is None:
            cls._instance = AIOKafkaConsumer(
                *topics,  
                bootstrap_servers=bootstrap_servers,
                group_id=group_id,
                enable_auto_commit= False,
                session_timeout_ms=25000,
                max_poll_interval_ms=300000
            )
            await cls._instance.start()
            logging.info(f"Kafka Consumer Started on topics: {topics}")
            cls._task = asyncio.create_task(cls._consume_events(callback))
        return cls._instance

    @classmethod
    async def _consume_events(cls, callback):
        try:
            async for message in cls._instance:
                event = json.loads(message.value.decode('utf-8'))
                logging.info(f"Event received: {event}")
                await callback(event)
                await cls._instance.commit()
        except Exception as e:
            logging.error(f"Error during event consuming: {e}")
        finally:
            await cls._instance.stop()

    @classmethod
    async def close(cls):
        if cls._instance:
            await cls._instance.stop()
            logging.info("Kafka Consumer stopped")
            cls._instance = None
            if cls._task:
                cls._task.cancel()
                try:
                    await cls._task
                except asyncio.CancelledError:
                    logging.info("Consumer task cancelled")
