#!/usr/bin/env python3
import os
import subprocess
import logging
import time
import argparse
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s - %(asctime)s - %(name)s - %(message)s',
    datefmt='%I:%M:%S'
)
logger = logging.getLogger("Test-Suite")

# Define the test configurations to run
TEST_CONFIGS = [
    {
        "name": "baseline",
        "description": "Baseline test - No services killed",
        "command": ["python", "run_fault_tolerant_tests.py", "--test-type", "consistency", "--kill-services", "none"]
    },
    {
        "name": "order_service_failure",
        "description": "Order Service Failure",
        "command": ["python", "run_fault_tolerant_tests.py", "--test-type", "consistency", "--kill-services", "order-service"]
    },
    {
        "name": "payment_service_failure",
        "description": "Payment Service Failure",
        "command": ["python", "run_fault_tolerant_tests.py", "--test-type", "consistency", "--kill-services", "payment-service"]
    },
    {
        "name": "stock_service_failure",
        "description": "Stock Service Failure",
        "command": ["python", "run_fault_tolerant_tests.py", "--test-type", "consistency", "--kill-services", "stock-service"]
    },
    {
        "name": "multiple_services_failure",
        "description": "Multiple Services Failure",
        "command": ["python", "run_fault_tolerant_tests.py", "--test-type", "consistency", "--kill-services", "order-service", "payment-service"]
    },
    {
        "name": "order_service_with_recovery",
        "description": "Order Service Failure with Recovery",
        "command": ["python", "run_fault_tolerant_tests.py", "--test-type", "consistency", "--kill-services", "order-service", "--recovery-time", "30"]
    },
    {
        "name": "dynamic_stress_test",
        "description": "Dynamic Stress Test",
        "command": ["python", "run_dynamic_stress_test.py", "--users", "100", "--spawn-rate", "10", "--runtime", "60"]
    }
]

def parse_arguments():
    parser = argparse.ArgumentParser(description='Run a suite of fault tolerance tests')
    parser.add_argument('--configs', nargs='+', help='Specific test configurations to run (by name)')
    parser.add_argument('--results-dir', default='results', help='Directory to store test results')
    parser.add_argument('--visualize', action='store_true', help='Generate visualizations after tests')
    parser.add_argument('--skip-tests', action='store_true', help='Skip running tests, just visualize existing results')
    return parser.parse_args()

def run_test(config, results_dir):
    """Run a single test configuration."""
    logger.info(f"Running test: {config['description']}")
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(results_dir, f"{config['name']}_{timestamp}.log")
    
    # (IMPORTATN) Create a new subprocess with the command
    try:
        with open(log_file, 'w') as f:
            process = subprocess.run(
                config['command'],
                stdout=f,
                stderr=subprocess.STDOUT,
                check=True
            )
        logger.info(f"Test completed successfully. Log saved to {log_file}")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Test failed with return code {e.returncode}")
        return False

def generate_visualizations(results_dir):
    """Generate visualizations based on test results."""
    logger.info("Generating visualizations and report...")
    
    visualizations_dir = "visualizations"
    if not os.path.exists(visualizations_dir):
        os.makedirs(visualizations_dir)
    
    try:
        subprocess.run(
            ["python", "visualize_results.py", "--results-dir", results_dir, "--output-dir", visualizations_dir, "--report"],
            check=True
        )
        logger.info(f"Visualizations saved to {visualizations_dir}")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Visualization generation failed with return code {e.returncode}")
        return False

def main():
    args = parse_arguments()
    
    if not os.path.exists(args.results_dir):
        os.makedirs(args.results_dir)
    
    configs_to_run = TEST_CONFIGS
    if args.configs:
        configs_to_run = [c for c in TEST_CONFIGS if c['name'] in args.configs]
        if not configs_to_run:
            logger.error(f"No matching configurations found. Available: {[c['name'] for c in TEST_CONFIGS]}")
            return 1
    
    if not args.skip_tests:
        for i, config in enumerate(configs_to_run):
            logger.info(f"Running test {i+1}/{len(configs_to_run)}: {config['name']}")
            success = run_test(config, args.results_dir)
            
            if not success:
                logger.warning(f"Test {config['name']} failed, but continuing with next test")
            
            # Wait a bit between tests to let the system stabilize
            if i < len(configs_to_run) - 1:
                logger.info("Waiting for system to stabilize before next test...")
                time.sleep(20)
    else:
        logger.info("Skipping tests as requested")
    
    #  visualizations if requested (--visualize flag)
    if args.visualize:
        generate_visualizations(args.results_dir)
    
    logger.info("Test suite completed")
    return 0

if __name__ == "__main__":
    exit(main())