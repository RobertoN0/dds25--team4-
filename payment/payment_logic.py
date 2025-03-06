import uuid
from typing import TypeVar, Tuple, Optional

from msgspec import msgpack, Struct
from redis import RedisError

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


class PaymentLogic:
    def __init__(self, logger, db):
        self.logger = logger
        self.db = db

    async def set_user(self, user_id: str, value: bytes) -> Result[str, DBError]:
        try:
            await self.db.set(user_id, value)
        except RedisError as e:
            return "", DBError(e.args[0])

        return user_id, None


    async def multi_set_user(self, n: int, starting_money: int) -> Result[str, DBError]:
        kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(UserValue(credit=starting_money))
                                      for i in range(n)}
        try:
            await self.db.multi_set(kv_pairs)
        except RedisError as e:
            return "", DBError(e.args[0])

        return "Bach init for users successful", None


    async def get_user(self, user_id: str) -> Result[UserValue | None, DBError | UserNotFoundError]:
        try:
            entry: bytes = await self.db.get(user_id)
        except RedisError as e:
            return None, DBError(e.args[0])

        if not entry:
            return None, UserNotFoundError(user_id)

        entry: UserValue = msgpack.decode(entry, type=UserValue)
        return entry, None


    async def create_user(self) -> Result[str, DBError]:
        key = str(uuid.uuid4())
        value = msgpack.encode(UserValue(credit=0))
        return await self.set_user(key, value)


    async def add_credit(self, user_id: str, amount: int) -> Result[int, DBError]:
        value, err = await self.get_user(user_id)
        if err:
            return 0, err
        value.credit += amount

        _, err = await self.set_user(user_id, msgpack.encode(value))
        if err:
            return 0, err

        return value.credit, None


    async def remove_credit(self, user_id: str, amount: int) -> Result[int, DBError | NotEnoughCreditError]:
        value, err = await self.get_user(user_id)
        if err:
            return 0, err

        value.credit -= amount
        if value.credit < 0:
            return 0, NotEnoughCreditError()

        _, err = await self.set_user(user_id, msgpack.encode(value))
        if err:
            return 0, err

        return value.credit, None
