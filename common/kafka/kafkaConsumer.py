from aiokafka import AIOKafkaConsumer, ConsumerRebalanceListener
import asyncio
import json
import logging

class KafkaConsumerSingleton:
    _instance = None 
    _task = None 
    _rebalance_lock = asyncio.Lock()


    class SafeRebalanceListener(ConsumerRebalanceListener):
        def __init__(self, consumer):
            self.consumer = consumer

        async def on_partitions_revoked(self, revoked):
            logging.info(f"[REBALANCE] Revoking partitions: {revoked}")
            async with KafkaConsumerSingleton._rebalance_lock:
                pass 

        async def on_partitions_assigned(self, assigned):
            logging.info(f"[REBALANCE] Assigned new partitions: {assigned}")

    @classmethod
    async def get_instance(cls, topics, bootstrap_servers, group_id, callback):
        if cls._instance is None:
            cls._instance = AIOKafkaConsumer(
                *topics,  
                bootstrap_servers=bootstrap_servers,
                group_id=group_id,
                enable_auto_commit= False,
                auto_offset_reset="earliest",
            )     
            listener = cls.SafeRebalanceListener(cls._instance)
            cls._instance.subscribe(topics, listener=listener)
            await cls._instance.start()
            logging.info(f"Kafka Consumer Started on topics: {topics}")
            cls._task = asyncio.create_task(cls._consume_events(callback))
        return cls._instance

    @classmethod
    async def _consume_events(cls, callback):
        while True:
            try:
                async for message in cls._instance:
                    event = json.loads(message.value.decode('utf-8'))
                    async with cls._rebalance_lock:
                        await callback(event)
                        await cls._instance.commit()
            except Exception as e:
                logging.error(f"Error during event consuming: {e}")
                await asyncio.sleep(1)
                continue
            

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
