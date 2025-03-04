import uuid
from typing import TypeVar, Tuple, Optional

from msgspec import msgpack, Struct
from redis import RedisError

import payment.redis_db as db


class UserValue(Struct):
    credit: int


T = TypeVar("T")
E = TypeVar("E", bound=Exception)

Result = Tuple[T, Optional[E]]
class DBError(Exception):
    pass
class UserNotFoundError(Exception):
    pass
class NotEnoughCreditError(Exception):
    pass

async def set_user(user_id: str, value: bytes) -> Result[str, DBError]:
    try:
        await db.set(user_id, value)
    except RedisError as e:
        return "", DBError(e.args[0])

    return user_id, None


async def multi_set_user(n: int, starting_money: int) -> Result[str, DBError]:
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(UserValue(credit=starting_money))
                                  for i in range(n)}
    try:
        await db.multi_set(kv_pairs)
    except RedisError as e:
        return "", DBError(e.args[0])

    return "Bach init for users successful", None


async def get_user(user_id: str) -> Result[UserValue | None, DBError | UserNotFoundError]:
    try:
        entry: bytes = await db.get(user_id)
    except RedisError as e:
        return None, DBError(e.args[0])

    if not entry:
        return None, UserNotFoundError(user_id)

    entry: UserValue = msgpack.decode(entry, type=UserValue)
    return entry, None


async def create_user() -> Result[str, DBError]:
    key = str(uuid.uuid4())
    value = msgpack.encode(UserValue(credit=0))
    return await set_user(key, value)


async def add_credit(user_id: str, amount: int) -> Result[int, DBError]:
    value, err = await get_user(user_id)
    if err:
        return 0, err
    value.credit += amount

    _, err = await set_user(user_id, msgpack.encode(value))
    if err:
        return 0, err

    return value.credit, None


async def remove_credit(user_id: str, amount: int) -> Result[int, DBError | NotEnoughCreditError]:
    value, err = await get_user(user_id)
    if err:
        return 0, err

    value.credit -= amount
    if value.credit < 0:
        return 0, NotEnoughCreditError()

    _, err = await set_user(user_id, msgpack.encode(value))
    if err:
        return 0, err

    return value.credit, None
