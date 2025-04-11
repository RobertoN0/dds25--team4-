#!/usr/bin/env python3
import argparse
import asyncio
import json
import logging
import os
import subprocess
import sys
import time
from datetime import datetime
from tempfile import gettempdir

sys.path.append(os.path.join('..', 'stress-test'))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s - %(asctime)s - %(name)s - %(message)s',
    datefmt='%I:%M:%S'
)
logger = logging.getLogger("Dynamic-Stress-Test")

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Run dynamic stress test with service failures during ramp-up')
    parser.add_argument('--output-dir', default='results', help='Directory to store results')
    parser.add_argument('--init', action='store_true', help='Initialize databases before testing')
    parser.add_argument('--users', type=int, default=1000, help='Target number of users for the stress test')
    parser.add_argument('--spawn-rate', type=int, default=50, help='Users to spawn per second')
    parser.add_argument('--runtime', type=int, default=300, help='Test duration in seconds')
    parser.add_argument('--kill-config', default='kill_config.json', help='Path to kill configuration file')
    
    return parser.parse_args()

async def initialize_databases():
    """Initialize the databases for the stress test."""
    from init_orders import populate_databases
    
    logger.info("Initializing databases for stress test...")
    try:
        await populate_databases()
        logger.info("Databases initialized successfully")
        return True
    except Exception as e:
        logger.error(f"Error initializing databases: {e}")
        return False

def run_locust_test(users, spawn_rate, runtime, output_dir):
    """Run the Locust stress test with dynamic service killer."""
    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate unique output filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_prefix = os.path.join(output_dir, f"stress_test_{timestamp}")
    
    # Build the Locust command
    locust_cmd = [
        "locust",
        "-f", "locustfile_with_dynamic_killer.py",  # Use the modified Locust file with dynamic killing
        "--host", "http://localhost:8000",  # This should match your ORDER_URL
        "--headless",
        "-u", str(users),     # Number of users
        "-r", str(spawn_rate), # Spawn rate
        "-t", f"{runtime}s",   # Test runtime
        "--csv", csv_prefix,   # Save results to CSV
        "--logfile", f"{output_dir}/locust_{timestamp}.log"
    ]
    
    logger.info(f"Starting Locust with {users} users, spawn rate {spawn_rate}/sec, runtime {runtime}s")
    logger.info(f"Results will be saved to {csv_prefix}_*.csv")
    
    # Run Locust
    try:
        process = subprocess.run(locust_cmd, check=True)
        logger.info("Locust test completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Locust test failed: {e}")
        return False

def verify_system_state():
    """Verify the system state after the test."""
    logger.info("Verifying system state...")
    
    # Run the docker service manager to check service status
    try:
        result = subprocess.run(
            ["python", "docker_service_manager.py", "status"],
            capture_output=True, text=True, check=True
        )
        logger.info("Service status after test:")
        logger.info(result.stdout)
        
        # Check if any services are not fully running
        if "0/" in result.stdout:
            logger.warning("Some services are not running!")
            return False
    except subprocess.CalledProcessError as e:
        logger.error(f"Error checking service status: {e}")
        return False
    
    return True

def generate_test_report(args, output_dir, test_successful):
    """Generate a report for the test."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_file = os.path.join(output_dir, f"dynamic_stress_test_report_{timestamp}.txt")
    
    try:
        with open(args.kill_config, 'r') as f:
            kill_config = json.load(f)
    except:
        kill_config = {"kill_triggers": []}
    
    with open(report_file, 'w') as f:
        f.write("# Dynamic Stress Test Report\n")
        f.write(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        f.write("## Test Configuration\n")
        f.write(f"Target users: {args.users}\n")
        f.write(f"Spawn rate: {args.spawn_rate} users/sec\n")
        f.write(f"Test duration: {args.runtime} seconds\n")
        f.write(f"Kill configuration: {args.kill_config}\n\n")
        
        f.write("## Service Kill Triggers\n")
        for i, trigger in enumerate(kill_config.get("kill_triggers", [])):
            f.write(f"Trigger {i+1}: ")
            f.write(f"Kill {trigger.get('services')} when {trigger.get('type')} reaches {trigger.get('threshold')}, ")
            f.write(f"recovery after {trigger.get('recovery_time')} seconds\n")
        
        f.write("\n## Test Result\n")
        f.write(f"Overall result: {'PASSED' if test_successful else 'FAILED'}\n")
        
        f.write("\nRefer to the CSV files and Locust logs for detailed metrics.\n")
    
    logger.info(f"Test report generated: {report_file}")
    return report_file

async def main():
    """Main function."""
    args = parse_arguments()
    
    # Create output directory
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Initialize databases if requested
    if args.init:
        db_init_success = await initialize_databases()
        if not db_init_success:
            logger.error("Database initialization failed. Aborting test.")
            return 1
    
    # Copy the kill configuration to the current directory if needed
    if args.kill_config != 'kill_config.json' and os.path.exists(args.kill_config):
        with open(args.kill_config, 'r') as src:
            with open('kill_config.json', 'w') as dst:
                dst.write(src.read())
        logger.info(f"Copied kill configuration from {args.kill_config}")
    
    # Run the Locust test
    test_success = run_locust_test(args.users, args.spawn_rate, args.runtime, args.output_dir)
    
    # Wait a bit for services to stabilize before verification
    logger.info("Waiting for services to stabilize...")
    time.sleep(10)
    
    # Verify system state
    system_ok = verify_system_state()
    
    # Generate test report
    report_file = generate_test_report(args, args.output_dir, test_success and system_ok)
    
    if not system_ok:
        logger.warning("System verification failed. Some services may need to be restarted.")
    
    if test_success and system_ok:
        logger.info("Dynamic stress test completed successfully")
        return 0
    else:
        logger.error("Dynamic stress test failed")
        return 1

if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
