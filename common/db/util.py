import asyncio
import logging
from redis.exceptions import ConnectionError, TimeoutError, RedisError
from redis.sentinel import MasterNotFoundError

async def retry_db_call(func, *args, retries=5, **kwargs):
    for attempt in range(retries):
        try:
            return await func(*args, **kwargs)
        except (MasterNotFoundError, ConnectionError, TimeoutError, RedisError) as e:
            logging.info(f"Attempt {attempt + 1} failed: {e},  {type(e).__name__}:")
            if attempt < retries - 1:
                await asyncio.sleep(0.5)
                continue
            else:
                raise e