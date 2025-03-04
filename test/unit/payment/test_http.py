import unittest
from unittest.mock import patch, AsyncMock

from quart.testing import QuartClient
from payment.app_instance import app
import payment.routing.http as http
from payment.payment_logic import DBError, UserValue, NotEnoughCreditError
from payment.routing.http import DB_ERROR_STR, REQ_ERROR_STR


class TestHttp(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        """Set up a Quart test client before each test."""
        self.test_client: QuartClient = app.test_client()
        http.init()

    async def test_create_user(self):
        """Test that creating a user returns a valid user_id."""
        uuid = "user123"

        with patch("payment.routing.http.logic", new_callable=AsyncMock) as mock_logic:
            mock_logic.create_user.return_value = (uuid, None)

            response = await self.test_client.post("/create_user")
            data = await response.get_json()

            self.assertIn("user_id", data)
            self.assertIsInstance(data["user_id"], str)
            self.assertEqual(data["user_id"], uuid)
            mock_logic.create_user.assert_called_once()


    async def test_add_funds_success(self):
        """Test successful adding funds to a user."""
        uuid = "user123"
        initial_credit = 20
        amount = 100

        with patch("payment.routing.http.logic", new_callable=AsyncMock) as mock_logic:
            mock_logic.add_credit.return_value = (initial_credit + amount, None)

            response = await self.test_client.post(f"/add_funds/{uuid}/{amount}")
            data = await response.get_data(as_text=True)

            self.assertEqual(response.status_code, 200)
            self.assertEqual(data, f"User: {uuid} credit updated to: {initial_credit + amount}")
            mock_logic.add_credit.assert_called_once_with(uuid, amount)


    async def test_add_funds_failure(self):
        """Test successful adding funds to a user."""
        uuid = "user123"
        amount = 100

        with patch("payment.routing.http.logic", new_callable=AsyncMock) as mock_logic:
            mock_logic.add_credit.return_value = (0, DBError())

            response = await self.test_client.post(f"/add_funds/{uuid}/{amount}")

            self.assertEqual(response.status_code, 400)



    async def test_find_user_success(self):
        """Test retrieving user information."""
        uuid = "user123"
        credit = 50

        with patch("payment.routing.http.logic", new_callable=AsyncMock) as mock_logic:
            mock_logic.get_user.return_value = (UserValue(credit=credit), None)

            response = await self.test_client.get(f"/find_user/{uuid}")
            data = await response.get_json()

            self.assertEqual(data["user_id"], uuid)
            self.assertIsInstance(data["credit"], int)
            self.assertEqual(data["credit"], credit)
            mock_logic.get_user.assert_called_once_with(uuid)


    async def test_find_user_failure(self):
        """Test retrieving user information when the user does not exist."""
        uuid = "user123"

        with patch("payment.routing.http.logic", new_callable=AsyncMock) as mock_logic:
            mock_logic.get_user.return_value = (None, DBError)

            response = await self.test_client.get(f"/find_user/{uuid}")
            data = await response.get_data(as_text=True)

            self.assertEqual(response.status_code, 400)
            self.assertIn(DB_ERROR_STR, data)


    async def test_pay_success(self):
        """Test successful payment when the user has enough credit."""
        uuid = "user123"
        credit = 50
        amount = 25

        with patch("payment.routing.http.logic", new_callable=AsyncMock) as mock_logic:
            mock_logic.remove_credit.return_value = (credit - amount, None)

            response = await self.test_client.post(f"/pay/{uuid}/{amount}")
            data = await response.get_data(as_text=True)

            self.assertEqual(response.status_code, 200)
            self.assertEqual(data, f"User: {uuid} credit updated to: {credit - amount}")
            mock_logic.remove_credit.assert_called_once_with(uuid, amount)


    async def test_pay_failure_not_enough_credit(self):
        """Test payment failure when the user does not have enough credit."""
        uuid = "user123"
        amount = 75

        with patch("payment.routing.http.logic", new_callable=AsyncMock) as mock_logic:
            mock_logic.remove_credit.return_value = (0, NotEnoughCreditError())

            response = await self.test_client.post(f"/pay/{uuid}/{amount}")
            data = await response.get_data(as_text=True)

            self.assertEqual(response.status_code, 400)
            self.assertIn(f"User: {uuid} credit cannot get reduced below zero", data)


    async def test_pay_failure_not_db_error(self):
        """Test payment failure when the user does not have enough credit."""
        uuid = "user123"
        amount = 75

        with patch("payment.routing.http.logic", new_callable=AsyncMock) as mock_logic:
            mock_logic.remove_credit.return_value = (0, DBError())

            response = await self.test_client.post(f"/pay/{uuid}/{amount}")
            data = await response.get_data(as_text=True)

            self.assertEqual(response.status_code, 400)
            self.assertIn(DB_ERROR_STR, data)


    async def test_batch_init_users_success(self):
        """Test successful batch initialization."""
        n = 10
        starting_money = 100

        with patch("payment.routing.http.logic", new_callable=AsyncMock) as mock_logic:
            mock_logic.multi_set_user.return_value = ("Success", None)

            response = await self.test_client.post(f"/batch_init/{n}/{starting_money}")
            data = await response.get_json()

            self.assertEqual(response.status_code, 200)
            self.assertDictEqual(data, {"msg": "Batch init for users successful"})
            mock_logic.multi_set_user.assert_awaited_once_with(n, starting_money)

    async def test_batch_init_users_db_error(self):
        """Test when the logic function returns a database error."""
        n = 10
        starting_money = 100

        with patch("payment.routing.http.logic", new_callable=AsyncMock) as mock_logic:
            mock_logic.multi_set_user.return_value = (None, "Database error")

            response = await self.test_client.post(f"/batch_init/{n}/{starting_money}")
            data = await response.get_data(as_text=True)

            self.assertEqual(response.status_code, 400)
            self.assertIn(DB_ERROR_STR, data)


    async def test_batch_init_users_invalid_starting_money(self):
        """Test when invalid inputs are provided."""
        n = 5
        starting_money = -100

        with patch("payment.routing.http.logic", new_callable=AsyncMock) as mock_logic:
            mock_logic.multi_set_user.return_value = (None, "Input Error")

            response = await self.test_client.post(f"/batch_init/{n}/{starting_money}")
            data = await response.get_data(as_text=True)

            self.assertEqual(response.status_code, 400)
            self.assertIn(REQ_ERROR_STR, data)


    async def test_batch_init_users_invalid_number_of_users(self):
        """Test when invalid inputs are provided."""
        n = -5
        starting_money = 100

        with patch("payment.routing.http.logic", new_callable=AsyncMock) as mock_logic:
            mock_logic.multi_set_user.return_value = (None, "Input Error")

            response = await self.test_client.post(f"/batch_init/{n}/{starting_money}")
            data = await response.get_data(as_text=True)

            self.assertEqual(response.status_code, 400)
            self.assertIn(REQ_ERROR_STR, data)


if __name__ == '__main__':
    unittest.main()