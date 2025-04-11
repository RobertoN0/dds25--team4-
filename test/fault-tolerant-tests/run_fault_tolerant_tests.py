import argparse
import asyncio
import json
import logging
import os
import random
import shutil
import subprocess
import sys
import time
from tempfile import gettempdir
from typing import List, Optional, Tuple

import aiohttp

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s - %(asctime)s - %(name)s - %(message)s',
    datefmt='%I:%M:%S'
)
logger = logging.getLogger("Fault-Tolerance-Test")

# Constants for consistency test
CONSISTENCY_ITEMS = 1
CONSISTENCY_ITEM_STOCK = 100
CONSISTENCY_ITEM_PRICE = 1
CONSISTENCY_USERS = 1000
CONSISTENCY_USER_CREDIT = 1
CONSISTENCY_ORDERS = 1000

# Constants for stress test
STRESS_ITEMS = 100_000
STRESS_ITEM_STOCK = 1_000_000
STRESS_ITEM_PRICE = 1
STRESS_USERS = 100_000
STRESS_USER_CREDIT = 1_000_000
STRESS_ORDERS = 100_000

TEST_TYPE = "consistency"  # Default to consistency test

# Load URLs
with open(os.path.join('..', 'urls.json')) as f:
    urls = json.load(f)
    ORDER_URL = urls['ORDER_URL']
    PAYMENT_URL = urls['PAYMENT_URL']
    STOCK_URL = urls['STOCK_URL']


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Run combined stress and consistency tests with fault tolerance testing')
    parser.add_argument('--test-type', choices=['consistency', 'stress', 'both'], default='consistency',
                        help='Type of test to run (default: consistency)')
    parser.add_argument('--kill-services', nargs='+', 
                        choices=['order-service', 'stock-service', 'payment-service', 'orchestrator-service', 
                                 'order-db', 'stock-db', 'payment-db', 'kafka' ,'none'],
                        default=['none'],
                        help='Services to kill before testing (default: none)')
    parser.add_argument('--kill-count', type=int, default=1,
                        help='Number of instances to kill for each service (default: 1)')
    parser.add_argument('--recovery-time', type=int, default=0,
                        help='Time to wait in seconds before restarting killed services (default: 0, no restart)')
    
    return parser.parse_args()


async def post_and_get_status(session, url):
    """Send a POST request and return the status code."""
    async with session.post(url) as resp:
        return resp.status


async def post_and_get_field(session, url, field):
    """Send a POST request and return a specific field from the JSON response."""
    async with session.post(url) as resp:
        jsn = await resp.json()
        return jsn[field]


async def get_and_get_field(session, url, field, key):
    """Send a GET request and return a specific field from the JSON response, paired with the provided key."""
    async with session.get(url) as resp:
        jsn = await resp.json()
        return key, jsn[field]


async def create_items(session, number_of_items: int, stock: int, price: int) -> List[str]:
    """Create items with stock in the stock service."""
    tasks = []
    # Create items
    for _ in range(number_of_items):
        create_item_url = f"{STOCK_URL}/stock/item/create/{price}"
        tasks.append(asyncio.ensure_future(post_and_get_field(session, create_item_url, 'item_id')))
    item_ids: List[str] = list(await asyncio.gather(*tasks))
    
    tasks = []
    # Add stock
    for item_id in item_ids:
        create_item_url = f"{STOCK_URL}/stock/add/{item_id}/{stock}"
        tasks.append(asyncio.ensure_future(post_and_get_status(session, create_item_url)))
    await asyncio.gather(*tasks)
    return item_ids


async def create_users(session, number_of_users: int, credit: int) -> List[str]:
    """Create users with credit in the payment service."""
    tasks = []
    # Create users
    for _ in range(number_of_users):
        create_user_url = f"{PAYMENT_URL}/payment/create_user"
        tasks.append(asyncio.ensure_future(post_and_get_field(session, create_user_url, 'user_id')))
    user_ids: List[str] = list(await asyncio.gather(*tasks))
    
    tasks = []
    # Add funds
    for user_id in user_ids:
        add_funds_url = f"{PAYMENT_URL}/payment/add_funds/{user_id}/{credit}"
        tasks.append(asyncio.ensure_future(post_and_get_status(session, add_funds_url)))
    await asyncio.gather(*tasks)
    return user_ids


async def batch_init_databases(session, test_type: str):
    """Initialize databases using batch initialization endpoints based on test type."""
    if test_type == "consistency":
        items = CONSISTENCY_ITEMS
        item_stock = CONSISTENCY_ITEM_STOCK
        item_price = CONSISTENCY_ITEM_PRICE
        users = CONSISTENCY_USERS
        user_credit = CONSISTENCY_USER_CREDIT
        orders = CONSISTENCY_ORDERS
    else:  # stress test
        items = STRESS_ITEMS
        item_stock = STRESS_ITEM_STOCK
        item_price = STRESS_ITEM_PRICE
        users = STRESS_USERS
        user_credit = STRESS_USER_CREDIT
        orders = STRESS_ORDERS
    
    logger.info(f"Batch creating users for {test_type} test...")
    url = f"{PAYMENT_URL}/payment/batch_init/{users}/{user_credit}"
    async with session.post(url) as resp:
        await resp.json()
    logger.info("Users created")
    
    logger.info(f"Batch creating items for {test_type} test...")
    url = f"{STOCK_URL}/stock/batch_init/{items}/{item_stock}/{item_price}"
    async with session.post(url) as resp:
        await resp.json()
    logger.info("Items created")
    
    logger.info(f"Batch creating orders for {test_type} test...")
    url = f"{ORDER_URL}/orders/batch_init/{orders}/{items}/{users}/{item_price}"
    async with session.post(url) as resp:
        await resp.json()
    logger.info("Orders created")

    # Return minimal item and user IDs for verification
    return [str(i) for i in range(items)], [str(i) for i in range(users)]


async def create_orders(session, item_ids, user_ids, number_of_orders):
    """Create orders and add items to them."""
    tasks = []
    # Create orders
    orders_user_id = []
    for _ in range(number_of_orders):
        user_id = random.choice(user_ids)
        orders_user_id.append(user_id)
        create_order_url = f"{ORDER_URL}/orders/create/{user_id}"
        tasks.append(asyncio.ensure_future(post_and_get_field(session, create_order_url, 'order_id')))
    order_ids = list(await asyncio.gather(*tasks))
    
    tasks = []
    # Add items
    for order_id in order_ids:
        item_id = random.choice(item_ids)
        create_item_url = f"{ORDER_URL}/orders/addItem/{order_id}/{item_id}/1"
        tasks.append(asyncio.ensure_future(post_and_get_status(session, create_item_url)))
    await asyncio.gather(*tasks)
    return order_ids, orders_user_id


async def perform_checkouts(session, order_ids, orders_user_id, log_file):
    """Perform checkouts for all orders concurrently and log the results."""
    tasks = []
    for i, order_id in enumerate(order_ids):
        url = f"{ORDER_URL}/orders/checkout/{order_id}"
        tasks.append(asyncio.ensure_future(
            post_and_get_status_with_log(session, url, checkout=(order_id, orders_user_id[i], log_file))
        ))
    order_responses = await asyncio.gather(*tasks)
    return order_responses


async def post_and_get_status_with_log(session, url, checkout=None):
    """Send a POST request, log the result, and return the status code."""
    async with session.post(url) as resp:
        if checkout:
            if 400 <= resp.status < 500:
                log = f"CHECKOUT | ORDER: {checkout[0]} USER: {checkout[1]} FAIL __OUR_LOG__\n"
                checkout[2].write(log)
            else:
                log = f"CHECKOUT | ORDER: {checkout[0]} USER: {checkout[1]} SUCCESS __OUR_LOG__\n"
                checkout[2].write(log)
        return resp.status


async def get_user_credit_dict(session, user_id_list: List[str]):
    """Get the credit for all users."""
    tasks = []
    for user_id in user_id_list:
        url = f"{PAYMENT_URL}/payment/find_user/{user_id}"
        tasks.append(asyncio.ensure_future(get_and_get_field(session, url, 'credit', user_id)))
    user_id_credit = list(await asyncio.gather(*tasks))
    return dict(user_id_credit)


async def get_item_stock_dict(session, item_id_list):
    """Get the stock for all items."""
    tasks = []
    for item_id in item_id_list:
        url = f"{STOCK_URL}/stock/find/{item_id}"
        tasks.append(asyncio.ensure_future(get_and_get_field(session, url, 'stock', item_id)))
    item_id_stock = list(await asyncio.gather(*tasks))
    return dict(item_id_stock)


def get_prior_user_state(user_ids, test_type):
    """Get the expected initial user state based on the test type."""
    user_state = dict()
    credit = CONSISTENCY_USER_CREDIT if test_type == "consistency" else STRESS_USER_CREDIT
    for user_id in user_ids:
        user_state[str(user_id)] = credit
    return user_state


def parse_log(tmp_dir, prior_user_state, test_type):
    """Parse the test log to determine the expected state after testing."""
    i = 0
    price = CONSISTENCY_ITEM_PRICE if test_type == "consistency" else STRESS_ITEM_PRICE
    with open(f'{tmp_dir}/fault-tolerance-test.log', 'r') as log_file:
        log_file = log_file.readlines()
        for log in log_file:
            if log.endswith('__OUR_LOG__\n'):
                m = re.search('ORDER: (.*) USER: (.*) (.*) __OUR_LOG__', log)
                user_id = str(m.group(2))
                status = m.group(3)
                if status == 'SUCCESS':
                    i += 1
                    prior_user_state[user_id] = prior_user_state[user_id] - price
    
    if test_type == "consistency":
        items = CONSISTENCY_ITEMS
        item_stock = CONSISTENCY_ITEM_STOCK
        logger.info(f"Stock service inconsistencies in the logs: {i - (items * item_stock)}")
    else:  # For stress test, just log the successful checkouts
        logger.info(f"Successful checkouts: {i}")
    
    return prior_user_state


async def verify_consistency(tmp_dir, item_ids, user_ids, test_type):
    """Verify the consistency of the system state after testing."""
    import re
    
    if test_type == "consistency":
        items = CONSISTENCY_ITEMS
        item_stock = CONSISTENCY_ITEM_STOCK
        item_price = CONSISTENCY_ITEM_PRICE
        users = CONSISTENCY_USERS
        user_credit = CONSISTENCY_USER_CREDIT
    else:  # stress test
        items = STRESS_ITEMS
        item_stock = STRESS_ITEM_STOCK
        item_price = STRESS_ITEM_PRICE
        users = STRESS_USERS
        user_credit = STRESS_USER_CREDIT
    
    # Calculate the expected final state
    correct_user_state = (users * user_credit) - (items * item_stock * item_price)
    
    # Parse logs to get the expected state based on successful operations
    pus = parse_log(tmp_dir, get_prior_user_state(user_ids, test_type), test_type)
    
    # Get actual state from services
    async with aiohttp.ClientSession() as session:
        uic = await get_user_credit_dict(session, user_ids)
        iis = await get_item_stock_dict(session, item_ids)
    
    # Calculate inconsistencies
    server_side_items_bought = (items * item_stock) - sum(iis.values()) // len(iis)
    logger.info(f"Stock service inconsistencies in the database: "
                f"{server_side_items_bought - (items * item_stock)}")
    
    logged_user_credit = sum(pus.values())
    logger.info(f"Payment service inconsistencies in the logs: {abs(correct_user_state - logged_user_credit)}")
    
    server_side_user_credit = sum(uic.values())
    logger.info(f"Payment service inconsistencies in the database: {abs(correct_user_state - server_side_user_credit)}")
    
    return {
        "log_stock_inconsistencies": server_side_items_bought - (items * item_stock),
        "log_payment_inconsistencies": abs(correct_user_state - logged_user_credit),
        "db_stock_inconsistencies": server_side_items_bought - (items * item_stock),
        "db_payment_inconsistencies": abs(correct_user_state - server_side_user_credit)
    }


def kill_services(services_to_kill, kill_count):
    """Kill specified service containers."""
    if "none" in services_to_kill:
        logger.info("No services will be killed")
        return []
    
    killed_containers = []
    
    for service in services_to_kill:
        logger.info(f"Attempting to kill {kill_count} instances of {service}...")
        
        # List all containers for this service
        result = subprocess.run(
            ["docker", "ps", "--filter", f"name={service}", "--format", "{{.Names}}"],
            capture_output=True, text=True
        )
        containers = result.stdout.strip().split('\n')
        containers = [c for c in containers if c]  # Remove empty strings
        
        if not containers:
            logger.warning(f"No containers found for service {service}")
            continue
        
        # Kill the specified number of containers (or all if fewer exist)
        for container in containers[:min(kill_count, len(containers))]:
            logger.info(f"Killing container {container}...")
            subprocess.run(["docker", "stop", container], capture_output=True)
            killed_containers.append(container)
    
    logger.info(f"Killed containers: {killed_containers}")
    return killed_containers


def restart_services(containers):
    """Restart the killed service containers."""
    if not containers:
        return
    
    logger.info(f"Restarting killed containers: {containers}")
    for container in containers:
        logger.info(f"Restarting container {container}...")
        subprocess.run(["docker", "start", container], capture_output=True)
    
    # Give services some time to restart fully
    logger.info("Waiting for services to restart fully...")
    time.sleep(10)


async def run_consistency_test(tmp_dir, kill_services_list, kill_count, recovery_time):
    """Run the consistency test with fault tolerance testing."""
    global TEST_TYPE
    TEST_TYPE = "consistency"
    
    # Create users and items
    logger.info("Populating databases for consistency test...")
    async with aiohttp.ClientSession() as session:
        item_ids, user_ids = await batch_init_databases(session, TEST_TYPE)
    logger.info("Databases populated")
    
    # Kill specified services
    killed_containers = kill_services(kill_services_list, kill_count)
    
    # Wait a bit to ensure the services are down
    if killed_containers:
        logger.info("Waiting for services to fully stop...")
        time.sleep(5)
    
    # Start the recovery process in a separate thread if needed
    if recovery_time > 0 and killed_containers:
        import threading
        recovery_thread = threading.Timer(recovery_time, restart_services, [killed_containers])
        recovery_thread.daemon = True
        recovery_thread.start()
        logger.info(f"Scheduled service recovery in {recovery_time} seconds")
    
    # Run the consistency test
    logger.info("Starting the consistency test...")
    async with aiohttp.ClientSession() as session:
        # Create orders
        order_ids, orders_user_id = await create_orders(session, item_ids, user_ids, CONSISTENCY_ORDERS)
        
        # Run concurrent checkouts
        with open(f"{tmp_dir}/fault-tolerance-test.log", "w") as log_file:
            await perform_checkouts(session, order_ids, orders_user_id, log_file)
    logger.info("Consistency test completed")
    
    # Ensure all services are back up before verification
    if recovery_time <= 0 and killed_containers:
        logger.info("Restarting services for verification...")
        restart_services(killed_containers)
    
    # Verify consistency
    logger.info("Starting consistency verification...")
    results = await verify_consistency(tmp_dir, item_ids, user_ids, TEST_TYPE)
    logger.info("Consistency verification completed")
    
    return results


async def run_stress_test(tmp_dir, kill_services_list, kill_count, recovery_time):
    """Run the stress test with fault tolerance testing."""
    global TEST_TYPE
    TEST_TYPE = "stress"
    
    # Initialize databases
    logger.info("Initializing databases for stress test...")
    async with aiohttp.ClientSession() as session:
        item_ids, user_ids = await batch_init_databases(session, TEST_TYPE)
    logger.info("Databases initialized")
    
    # Kill specified services
    killed_containers = kill_services(kill_services_list, kill_count)
    
    # Wait a bit to ensure the services are down
    if killed_containers:
        logger.info("Waiting for services to fully stop...")
        time.sleep(5)
    
    # Start the recovery process in a separate thread if needed
    if recovery_time > 0 and killed_containers:
        import threading
        recovery_thread = threading.Timer(recovery_time, restart_services, [killed_containers])
        recovery_thread.daemon = True
        recovery_thread.start()
        logger.info(f"Scheduled service recovery in {recovery_time} seconds")
    
    # Run locust for the stress test
    logger.info("Starting the stress test with Locust in headless mode...")
    
    # Run locust in headless mode with subprocess
    locust_cmd = [
        "locust",
        "-f", "locustfile.py",
        "--host", ORDER_URL,
        "--headless",
        "-u", "1000",  # Number of users
        "-r", "50",    # Spawn rate
        "-t", "60s"    # Run time
    ]
    
    try:
        process = subprocess.run(locust_cmd, capture_output=True, text=True)
        logger.info("Locust output:")
        logger.info(process.stdout)
        if process.stderr:
            logger.error("Locust errors:")
            logger.error(process.stderr)
    except Exception as e:
        logger.error(f"Error running Locust: {e}")
    
    logger.info("Stress test completed")
    
    # Ensure all services are back up before verification
    if recovery_time <= 0 and killed_containers:
        logger.info("Restarting services for verification...")
        restart_services(killed_containers)
    
    # Verify system state
    logger.info("Verifying system state after stress test...")
    results = await verify_consistency(tmp_dir, item_ids, user_ids, TEST_TYPE)
    logger.info("Verification completed")
    
    return results


async def main():
    """Main function to run the fault tolerance tests."""
    args = parse_arguments()
    
    # This creates a temporary directory for the test logs
    tmp_dir = os.path.join(gettempdir(), 'wdm_fault_tolerance_test')
    if os.path.isdir(tmp_dir):
        shutil.rmtree(tmp_dir)
    os.mkdir(tmp_dir)
    logger.info(f"Created temporary directory at {tmp_dir}")
    
    try:
        results = {}
        
        if args.test_type in ["consistency", "both"]:
            logger.info("=== RUNNING CONSISTENCY TEST WITH FAULT TOLERANCE ===")
            consistency_results = await run_consistency_test(
                tmp_dir, args.kill_services, args.kill_count, args.recovery_time
            )
            results["consistency"] = consistency_results
        
        if args.test_type in ["stress", "both"]:
            logger.info("=== RUNNING STRESS TEST WITH FAULT TOLERANCE ===")
            stress_results = await run_stress_test(
                tmp_dir, args.kill_services, args.kill_count, args.recovery_time
            )
            results["stress"] = stress_results
        
        # Summary
        logger.info("=== TEST SUMMARY ===")
        for test_type, test_results in results.items():
            logger.info(f"{test_type.upper()} TEST RESULTS:")
            for key, value in test_results.items():
                logger.info(f"  {key}: {value}")
        
    finally:
        if os.path.isdir(tmp_dir):
            shutil.rmtree(tmp_dir)
            logger.info(f"Cleaned up temporary directory {tmp_dir}")


if __name__ == "__main__":
    import re
    asyncio.run(main())
