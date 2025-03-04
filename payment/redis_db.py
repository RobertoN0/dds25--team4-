import os

from redis.asyncio import Redis

db: Redis | None = None


async def get(key: str):
    if db is None:
        raise RuntimeError("The Redis client has not been initialized. Call init() first.")

    return await db.get(key)


async def set(key: str, value: bytes):
    if db is None:
        raise RuntimeError("The Redis client has not been initialized. Call init() first.")

    await db.set(key, value)


async def multi_set(kv_pairs: dict[str, bytes]):
    if db is None:
        raise RuntimeError("The Redis client has not been initialized. Call init() first.")

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
    if db is not None:
        await db.close()