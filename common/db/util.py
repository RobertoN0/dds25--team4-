import asyncio
from redis.exceptions import RedisError, ConnectionError

async def retry_db_call(func, *args, retries=3, **kwargs):
    for attempt in range(retries):
        try:
            return await func(*args, **kwargs)
        except (RedisError, ConnectionError) as e:
            if attempt < retries - 1:
                await asyncio.sleep(0.5)
                continue
            else:
                raise e