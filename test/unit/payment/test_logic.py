import unittest
from unittest.mock import ANY, AsyncMock

from mock import patch
from msgspec import msgpack, Struct

import payment.payment_logic as logic
from payment.payment_logic import DBError

class UserValue(Struct):
    credit: int

class TestPaymentLogic(unittest.TestCase):

    @patch('payment.redis_db')
    async def test_set_user_success(self, mock_db):
        mock_db.set = AsyncMock(return_value=None)
        user_id = "test_user"
        value = msgpack.encode(UserValue(credit=100))

        result, err = await logic.set_user(user_id, value)

        self.assertEqual(result, user_id)
        self.assertIsNone(err)
        mock_db.set.assert_called_with(user_id, value)


    @patch('payment.redis_db')
    async def test_set_user_failure(self, mock_db):
        mock_db.set = AsyncMock(side_effect=logic.RedisError("DB error"))
        user_id = "test_user"
        value = msgpack.encode(UserValue(credit=100))

        result, err = await logic.set_user(user_id, value)

        self.assertEqual(result, "")
        self.assertIsInstance(err, DBError)
        self.assertEqual(str(err), "DB error")


    @patch('payment.redis_db')
    async def test_multi_set_user_success(self, mock_db):
        mock_db.multi_set = AsyncMock(return_value=None)
        result, err = await logic.multi_set_user(10, 100)

        self.assertEqual(result, "Bach init for users successful")
        self.assertIsNone(err)
        self.assertEqual(len(mock_db.multi_set.call_args[0][0]), 10)


    @patch('payment.redis_db')
    async def test_multi_set_user_failure(self, mock_db):
        mock_db.multi_set = AsyncMock(side_effect=logic.RedisError("DB error"))
        result, err = await logic.multi_set_user(10, 100)

        self.assertEqual(result, "")
        self.assertIsInstance(err, DBError)
        self.assertEqual(str(err), "DB error")


    @patch('payment.redis_db')
    async def test_get_user_success(self, mock_db):
        user_id = '123456789'
        expected_value = UserValue(credit=10)
        mock_db.get = AsyncMock(return_value=msgpack.encode(expected_value))

        actual, err = await logic.get_user(user_id)

        self.assertEqual(expected_value, actual)
        self.assertIsNone(err)
        mock_db.get.assert_called_with(user_id)


    @patch('payment.redis_db')
    async def test_get_user_not_found(self, mock_db):
        user_id = 'non_existent_user'
        mock_db.get = AsyncMock(return_value=None)

        result, err = await logic.get_user(user_id)

        self.assertIsNone(result)
        self.assertIsInstance(err, logic.UserNotFoundError)
        self.assertEqual(str(err), user_id)


    @patch('payment.redis_db')
    async def test_get_user_failure(self, mock_db):
        user_id = 'test_user'
        mock_db.get = AsyncMock(side_effect=logic.RedisError("DB error"))

        result, err = await logic.get_user(user_id)

        self.assertIsNone(result)
        self.assertIsInstance(err, DBError)
        self.assertEqual(str(err), "DB error")


    @patch('payment.redis_db')
    async def test_create_user_success(self, mock_db):
        mock_db.set = AsyncMock(return_value=None)
        uuid, err = await logic.create_user()

        self.assertIsNotNone(uuid)
        self.assertIsNone(err)
        mock_db.set.assert_called_with(uuid, ANY)


    @patch('payment.redis_db')
    async def test_create_user_failure(self, mock_db):
        mock_db.set = AsyncMock(side_effect=logic.RedisError("DB error"))
        uuid, err = await logic.create_user()

        self.assertEqual(uuid, "")
        self.assertIsInstance(err, DBError)


    @patch('payment.redis_db')
    async def test_add_credit_success(self, mock_db):
        user_id = "test_user"
        initial_value = UserValue(credit=50)
        updated_value = UserValue(credit=80)

        mock_db.get = AsyncMock(return_value=msgpack.encode(initial_value))
        mock_db.set = AsyncMock(return_value=None)

        result, err = await logic.add_credit(user_id, 30)

        self.assertEqual(result, user_id)
        self.assertIsNone(err)
        mock_db.set.assert_called_with(user_id, msgpack.encode(updated_value))


    @patch('payment.redis_db')
    async def test_add_credit_user_not_found(self, mock_db):
        user_id = "non_existent_user"
        mock_db.get = AsyncMock(return_value=None)

        result, err = await logic.add_credit(user_id, 30)

        self.assertEqual(result, "")
        self.assertIsInstance(err, logic.UserNotFoundError)


    @patch('payment.redis_db')
    async def test_remove_credit_success(self, mock_db):
        user_id = "test_user"
        initial_value = UserValue(credit=50)
        updated_value = UserValue(credit=20)

        mock_db.get = AsyncMock(return_value=msgpack.encode(initial_value))
        mock_db.set = AsyncMock(return_value=None)

        result, err = await logic.remove_credit(user_id, 30)

        self.assertEqual(result, user_id)
        self.assertIsNone(err)
        mock_db.set.assert_called_with(user_id, msgpack.encode(updated_value))


    @patch('payment.redis_db')
    async def test_remove_credit_insufficient_funds(self, mock_db):
        user_id = "test_user"
        initial_value = UserValue(credit=20)

        mock_db.get = AsyncMock(return_value=msgpack.encode(initial_value))

        result, err = await logic.remove_credit(user_id, 30)

        self.assertEqual(result, "")
        self.assertIsInstance(err, logic.NotEnoughCreditError)


    @patch('payment.redis_db')
    async def test_remove_credit_user_not_found(self, mock_db):
        user_id = "non_existent_user"
        mock_db.get = AsyncMock(return_value=None)

        result, err = await logic.remove_credit(user_id, 30)

        self.assertEqual(result, "")
        self.assertIsInstance(err, logic.UserNotFoundError)


    @patch('payment.redis_db')
    async def test_remove_credit_failure(self, mock_db):
        user_id = "test_user"
        initial_value = UserValue(credit=50)

        mock_db.get = AsyncMock(return_value=msgpack.encode(initial_value))
        mock_db.set = AsyncMock(side_effect=logic.RedisError("DB error"))

        result, err = await logic.remove_credit(user_id, 30)

        self.assertEqual(result, "")
        self.assertIsInstance(err, DBError)
        self.assertEqual(str(err), "DB error")


if __name__ == '__main__':
    unittest.main()