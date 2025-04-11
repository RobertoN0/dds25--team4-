import os.path
import random
import json
import time
from locust import HttpUser, SequentialTaskSet, task, between, events

# Load URLs from urls.json
with open(os.path.join('..', 'urls.json')) as f:
    urls = json.load(f)
    ORDER_URL = urls['ORDER_URL']
    PAYMENT_URL = urls['PAYMENT_URL']
    STOCK_URL = urls['STOCK_URL']

# Configuration
NUM_ITEMS = 100_000
NUM_USERS = 100_000
NUM_ORDERS = 100_000

# Global stats
failed_operations = 0
successful_operations = 0

@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    print("Starting Fault Tolerance Stress Test")

@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
    print("=== Fault Tolerance Test Results ===")
    print(f"Successful operations: {successful_operations}")
    print(f"Failed operations: {failed_operations}")
    print(f"Success rate: {successful_operations/(successful_operations+failed_operations)*100:.2f}%")

class CheckoutOrderTaskSet(SequentialTaskSet):
    """Task set for checking out random orders."""
    
    @task
    def checkout_random_order(self):
        global successful_operations, failed_operations
        
        order_id = random.randint(0, NUM_ORDERS - 1)
        with self.client.post(
            f"{ORDER_URL}/orders/checkout/{order_id}", 
            name="/orders/checkout/[order_id]",
            catch_response=True
        ) as response:
            if 400 <= response.status_code < 500:
                response.failure(f"Checkout failed: {response.text}")
                failed_operations += 1
            else:
                response.success()
                successful_operations += 1
                
class CreateAndCheckoutOrderTaskSet(SequentialTaskSet):
    """Task set for creating and checking out orders."""
    
    @task
    def create_and_checkout_order(self):
        global successful_operations, failed_operations
        
        # Step 1: Create an order
        user_id = random.randint(0, NUM_USERS - 1)
        with self.client.post(
            f"{ORDER_URL}/orders/create/{user_id}", 
            name="/orders/create/[user_id]",
            catch_response=True
        ) as response:
            if response.status_code != 200:
                response.failure("Failed to create order")
                failed_operations += 1
                return
                
            try:
                order_id = response.json()["order_id"]
            except (ValueError, KeyError):
                response.failure("Invalid JSON response or missing order_id")
                failed_operations += 1
                return
        
        # Step 2: Add an item to the order
        item_id = random.randint(0, NUM_ITEMS - 1)
        quantity = 1
        with self.client.post(
            f"{ORDER_URL}/orders/addItem/{order_id}/{item_id}/{quantity}",
            name="/orders/addItem/[order_id]/[item_id]/[quantity]",
            catch_response=True
        ) as response:
            if response.status_code != 200:
                response.failure("Failed to add item to order")
                failed_operations += 1
                return
        
        # Step 3: Checkout the order
        with self.client.post(
            f"{ORDER_URL}/orders/checkout/{order_id}",
            name="/orders/checkout/[order_id]",
            catch_response=True
        ) as response:
            if 400 <= response.status_code < 500:
                response.failure(f"Checkout failed: {response.text}")
                failed_operations += 1
            else:
                response.success()
                successful_operations += 1

class QueryStatusTaskSet(SequentialTaskSet):
    """Task set for querying system status."""
    
    @task
    def query_random_user(self):
        user_id = random.randint(0, NUM_USERS - 1)
        with self.client.get(
            f"{PAYMENT_URL}/payment/find_user/{user_id}",
            name="/payment/find_user/[user_id]",
            catch_response=True
        ) as response:
            if response.status_code != 200:
                response.failure("Failed to query user")
            else:
                response.success()
    
    @task
    def query_random_item(self):
        item_id = random.randint(0, NUM_ITEMS - 1)
        with self.client.get(
            f"{STOCK_URL}/stock/find/{item_id}",
            name="/stock/find/[item_id]",
            catch_response=True
        ) as response:
            if response.status_code != 200:
                response.failure("Failed to query item")
            else:
                response.success()

class MixedOperationsUser(HttpUser):
    """User class that executes mixed operations with fault tolerance awareness."""
    wait_time = between(1, 5)  # Wait between 1-5 seconds between tasks
    
    tasks = {
        CheckoutOrderTaskSet: 60,       # 60% weight - just checkout existing orders
        CreateAndCheckoutOrderTaskSet: 30,  # 30% weight - create and checkout new orders
        QueryStatusTaskSet: 10          # 10% weight - query system status
    }
    
    def on_start(self):
        # Add a small random delay to avoid all users starting simultaneously
        time.sleep(random.random())
