import os
if "GATEWAY_URL" not in os.environ:
    os.environ["GATEWAY_URL"] = "https://mock-gateway-url.com"

import unittest
from quart.testing import QuartClient
from payment.app_instance import app


class TestHttp(unittest.TestCase):

    async def asyncSetUp(self):
        """Set up a Quart test client before each test."""
        self.test_client: QuartClient = app.test_client()


    async def test_create_user(self):
        """Test that creating a user returns a valid user_id."""
        response = await self.test_client.post("/payment/create_user")
        self.assertEqual(response.status_code, 200)
        data = await response.get_json()
        self.assertIn("user_id", data)
        self.assertIsInstance(data["user_id"], str)


    async def test_add_funds(self):
        """Test adding funds to a user."""
        # Step 1: Create a user
        create_response = await self.test_client.post("/payment/create_user")
        user_data = await create_response.get_json()
        user_id = user_data["user_id"]

        # Step 2: Add funds
        response = await self.test_client.post(f"/payment/add_funds/{user_id}/100")
        self.assertEqual(response.status_code, 200)
        data = await response.get_json()
        self.assertIn("done", data)
        self.assertTrue(data["done"])


    async def test_find_user(self):
        """Test retrieving user information."""
        # Step 1: Create a user
        create_response = await self.test_client.post("/payment/create_user")
        user_data = await create_response.get_json()
        user_id = user_data["user_id"]

        # Step 2: Fetch user details
        response = await self.test_client.get(f"/payment/find_user/{user_id}")
        self.assertEqual(response.status_code, 200)
        data = await response.get_json()
        self.assertEqual(data["user_id"], user_id)
        self.assertIsInstance(data["credit"], int)


    async def test_pay_success(self):
        """Test successful payment when the user has enough credit."""
        # Step 1: Create a user
        create_response = await self.test_client.post("/payment/create_user")
        user_data = await create_response.get_json()
        user_id = user_data["user_id"]

        # Step 2: Add funds to the user
        await self.test_client.post(f"/payment/add_funds/{user_id}/100")

        # Step 3: Make a payment
        response = await self.test_client.post(f"/payment/pay/{user_id}/order123/50")
        self.assertEqual(response.status_code, 200)
        data = await response.get_json()
        self.assertIn("done", data)
        self.assertTrue(data["done"])


    async def test_pay_failure_not_enough_credit(self):
        """Test payment failure when the user does not have enough credit."""
        # Step 1: Create a user
        create_response = await self.test_client.post("/payment/create_user")
        user_data = await create_response.get_json()
        user_id = user_data["user_id"]

        # Step 2: Try to make a payment without adding funds
        response = await self.test_client.post(f"/payment/pay/{user_id}/order123/50")
        self.assertEqual(response.status_code, 400)
        error_message = await response.get_data(as_text=True)
        self.assertIn("credit cannot get reduced below zero", error_message)


if __name__ == '__main__':
    unittest.main()