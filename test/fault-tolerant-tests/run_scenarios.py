import subprocess
import logging
import time
import argparse
import os
import json
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s - %(asctime)s - %(name)s - %(message)s',
    datefmt='%I:%M:%S'
)
logger = logging.getLogger("Test-Scenarios")

# Define test scenarios
SCENARIOS = [
    {
        "name": "Baseline - No Failures",
        "test_type": "both",
        "kill_services": ["none"],
        "kill_count": 0,
        "recovery_time": 0
    },
    {
        "name": "Single Order Service Failure",
        "test_type": "both",
        "kill_services": ["order-service"],
        "kill_count": 1,
        "recovery_time": 0
    },
    {
        "name": "Single Payment Service Failure",
        "test_type": "both",
        "kill_services": ["payment-service"],
        "kill_count": 1,
        "recovery_time": 0
    },
    {
        "name": "Single Stock Service Failure",
        "test_type": "both",
        "kill_services": ["stock-service"],
        "kill_count": 1,
        "recovery_time": 0
    },
    {
        "name": "Multiple Service Failures",
        "test_type": "both",
        "kill_services": ["order-service", "stock-service"],
        "kill_count": 1,
        "recovery_time": 0
    },
    {
        "name": "Service Recovery Test",
        "test_type": "both",
        "kill_services": ["order-service", "payment-service", "stock-service"],
        "kill_count": 1,
        "recovery_time": 30
    },
    {
        "name": "Cascading Failures",
        "test_type": "consistency",
        "kill_services": ["order-service", "payment-service", "stock-service"],
        "kill_count": 2,  # Kill more instances
        "recovery_time": 60
    }
]

def parse_arguments():
    parser = argparse.ArgumentParser(description='Run multiple fault tolerance test scenarios')
    parser.add_argument('--scenarios', nargs='+', type=int, help='Specific scenario indexes to run (default: all)')
    parser.add_argument('--output-dir', default='results', help='Directory to store results')
    return parser.parse_args()

def run_scenario(scenario, output_dir):
    scenario_name = scenario["name"].lower().replace(" ", "_")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(output_dir, f"{scenario_name}_{timestamp}.log")
    
    logger.info(f"Running scenario: {scenario['name']}")
    logger.info(f"Test type: {scenario['test_type']}")
    logger.info(f"Services to kill: {', '.join(scenario['kill_services'])}")
    logger.info(f"Kill count: {scenario['kill_count']}")
    logger.info(f"Recovery time: {scenario['recovery_time']} seconds")
    
    cmd = [
        "python", "run_fault_tolerant_tests.py",
        "--test-type", scenario["test_type"],
        "--kill-services"
    ] + scenario["kill_services"] + [
        "--kill-count", str(scenario["kill_count"]),
        "--recovery-time", str(scenario["recovery_time"])
    ]
    
    with open(log_file, 'w') as f:
        process = subprocess.run(cmd, stdout=f, stderr=subprocess.STDOUT)
    
    logger.info(f"Scenario completed with return code {process.returncode}")
    logger.info(f"Log file: {log_file}")
    
    logger.info("Waiting for system to stabilize before next test...")
    time.sleep(60)
    
    return process.returncode

def main():
    args = parse_arguments()
    
    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)
    
    if args.scenarios:
        scenarios_to_run = [SCENARIOS[i] for i in args.scenarios if 0 <= i < len(SCENARIOS)]
    else:
        scenarios_to_run = SCENARIOS
    
    results = []
    for i, scenario in enumerate(scenarios_to_run):
        logger.info(f"Running scenario {i+1}/{len(scenarios_to_run)}: {scenario['name']}")
        return_code = run_scenario(scenario, args.output_dir)
        results.append({
            "scenario": scenario["name"],
            "success": return_code == 0
        })
    
    logger.info("=== Test Scenarios Summary ===")
    for result in results:
        status = "SUCCESS" if result["success"] else "FAILED"
        logger.info(f"{result['scenario']}: {status}")

if __name__ == "__main__":
    main()
