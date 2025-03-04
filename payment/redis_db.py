import os

from redis.asyncio import Redis

db: Redis


async def get(key: str):
    return await db.get(key)


async def set(key: str, value: bytes):
    await db.set(key, value)


async def multi_set(kv_pairs: dict[str, bytes]):
    await db.mset(kv_pairs)


def init():
    global db
    db = Redis(
        host=os.environ['REDIS_HOST'],
        port=int(os.environ['REDIS_PORT']),
        password=os.environ['REDIS_PASSWORD'],
        db=int(os.environ['REDIS_DB'])
    )


async def close():
    await db.close()