import os.path
import random
import json
import time
from locust import HttpUser, SequentialTaskSet, task, between, events

# Import the dynamic service killer
from dynamic_service_killer import DynamicServiceKiller, DockerServiceManager

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
current_users = 0
request_count = 0
start_time = None
kill_config = None
service_killer = None

@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    """Initialize the dynamic service killer when the test starts."""
    global start_time, kill_config, service_killer
    
    print("Starting Fault Tolerance Stress Test with Dynamic Service Killing")
    start_time = time.time()
    
    # Load kill configuration
    try:
        with open('kill_config.json', 'r') as f:
            kill_config = json.load(f)
        print(f"Loaded kill configuration with {len(kill_config.get('kill_triggers', []))} triggers")
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Error loading kill configuration: {e}")
        print("Using default kill configuration")
        
        # Default configuration: kill services during the ramp-up phase
        kill_config = {
            "kill_triggers": [
                {
                    "type": "users",
                    "threshold": int(environment.runner.target_user_count * 0.3),  # At 30% of target users
                    "services": ["order-service"],
                    "count": 1,
                    "recovery_time": 30
                },
                {
                    "type": "users",
                    "threshold": int(environment.runner.target_user_count * 0.6),  # At 60% of target users
                    "services": ["payment-service"],
                    "count": 1,
                    "recovery_time": 0
                },
                {
                    "type": "users",
                    "threshold": int(environment.runner.target_user_count * 0.8),  # At 80% of target users
                    "services": ["stock-service"],
                    "count": 1,
                    "recovery_time": 60
                }
            ]
        }
    
    # Create the service killer
    docker_manager = DockerServiceManager()
    service_killer = DynamicServiceKiller(
        kill_triggers=kill_config.get("kill_triggers", []),
        manager=docker_manager
    )
    
    # Start monitoring
    service_killer.start_monitoring(stats_callback=get_current_stats)
    print("Dynamic service killer started")

@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
    """Stop the dynamic service killer when the test ends."""
    global service_killer
    
    print("=== Fault Tolerance Test Results ===")
    print(f"Successful operations: {successful_operations}")
    print(f"Failed operations: {failed_operations}")
    if successful_operations + failed_operations > 0:
        print(f"Success rate: {successful_operations/(successful_operations+failed_operations)*100:.2f}%")
    
    # Stop the service killer
    if service_killer:
        service_killer.stop()
        print("Dynamic service killer stopped")

# Utilizziamo spawn_complete al posto di user_count_changed
@events.spawning_complete.add_listener
def on_spawn_complete(user_count, **kwargs):
    """Track when users have finished spawning."""
    global current_users
    current_users = user_count
    print(f"Spawning complete. Active users: {user_count}")

# In aggiunta, monitoriamo gli utenti durante la fase di spawn
@events.spawning_complete.add_listener
def on_spawning_complete(user_count, **kwargs):
    """Track when spawning is complete."""
    global current_users
    current_users = user_count
    print(f"All {user_count} users spawned")

# Aggiorniamo il conteggio utenti durante l'esecuzione
def update_user_count(environment):
    """Update the current user count based on the runner."""
    global current_users
    if environment and environment.runner:
        current_users = environment.runner.user_count

@events.request.add_listener
def on_request(request_type, name, response_time, response_length, exception, **kwargs):
    """Track requests and update user count."""
    global request_count
    request_count += 1
    
    # Aggiorniamo periodicamente il conteggio utenti (ogni 100 richieste)
    if request_count % 100 == 0 and 'environment' in kwargs:
        update_user_count(kwargs['environment'])

def get_current_stats():
    """Get current test statistics for the dynamic service killer."""
    global start_time, current_users, request_count
    
    elapsed = time.time() - start_time if start_time else 0
    rps = request_count / elapsed if elapsed > 0 else 0
    
    return {
        'elapsed_time': elapsed,
        'user_count': current_users,
        'rps': rps
    }

class CheckoutOrderTaskSet(SequentialTaskSet):
    """Task set for checking out random orders."""
    
    @task
    def checkout_random_order(self):
        global successful_operations, failed_operations, request_count
        
        order_id = random.randint(0, NUM_ORDERS - 1)
        with self.client.post(
            f"{ORDER_URL}/orders/checkout/{order_id}", 
            name="/orders/checkout/[order_id]",
            catch_response=True
        ) as response:
            request_count += 1
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
        global successful_operations, failed_operations, request_count
        
        # Step 1: Create an order
        user_id = random.randint(0, NUM_USERS - 1)
        with self.client.post(
            f"{ORDER_URL}/orders/create/{user_id}", 
            name="/orders/create/[user_id]",
            catch_response=True
        ) as response:
            request_count += 1
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
            request_count += 1
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
            request_count += 1
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
        global request_count
        
        user_id = random.randint(0, NUM_USERS - 1)
        with self.client.get(
            f"{PAYMENT_URL}/payment/find_user/{user_id}",
            name="/payment/find_user/[user_id]",
            catch_response=True
        ) as response:
            request_count += 1
            if response.status_code != 200:
                response.failure("Failed to query user")
            else:
                response.success()
    
    @task
    def query_random_item(self):
        global request_count
        
        item_id = random.randint(0, NUM_ITEMS - 1)
        with self.client.get(
            f"{STOCK_URL}/stock/find/{item_id}",
            name="/stock/find/[item_id]",
            catch_response=True
        ) as response:
            request_count += 1
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