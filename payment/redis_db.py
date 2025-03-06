import os

from redis.asyncio import Redis

class ReddisDB:
    def __init__(self, logger):
        self.logger = logger
        self.db = Redis(
            host=os.environ['REDIS_HOST'],
            port=int(os.environ['REDIS_PORT']),
            password=os.environ['REDIS_PASSWORD'],
            db=int(os.environ['REDIS_DB'])
        )

    async def get(self, key: str):
        return await self.db.get(key)

    async def set(self, key: str, value: bytes):
        await self.db.set(key, value)

    async def multi_set(self, kv_pairs: dict[str, bytes]):
        await self.db.mset(kv_pairs)

    async def close(self):
        await self.db.close()