# Stress and Consistency Tests with Dynamic Fault Tolerance

This testing framework allow us to evaluate the fault tolerance and the system consistency during sress loads, with the possibility to kill services dynamically during the test's ramp-up phase (you can also set the recovery times for each killed service).

## Principal Characteristics

1. **Consistency Tests** – Ensures the system maintains data consistency even if services fail.
2. **Dynamic Fault Tolerance** – Kills services at controlled intervals during high-load scenarios.
3. **Automatic Recovery** – Restarts services after a configurable downtime to simulate real-world fault recovery.
4. **Results Visualization** – Analyzes and displays test outcomes for easy interpretation.

## Setup

### Requirements

* Python >= 3.8
* Docker
* Microservises must be running in Docker containers

### Intallation

1. install requirements
   ```bash
   pip install -r requirements.txt
   ```

2. Make sure your services are running in Docker containers.

3. Check that the URLs in `urls.json` are correct for your environment.

## Available Tests

### Stress Testing with Dynamic Fault Tolerance

This test allows you to kill services during the load growth phase:

``bash
python run_dynamic_stress_test.py --users 1000 --spawn-rate 50 --runtime 300
```

This command:
- Starts a stress test with 1000 users
- Adds 50 new users per second
- Runs the test for 300 seconds
- Uses the default kill configuration in `kill_config.json`.

### 2. Consistency Test with Fault Tolerance

The consistency test verifies that the system maintains data consistency when services fail:

```bash
python run_fault_tolerant_tests.py --test-type consistency --kill-services order-service
```

### 3. Combined Test with Static Fault Tolerance

Performs both consistency and stress tests:

```bash
python run_fault_tolerant_tests.py --test-type both --kill-services stock-service
```

## Dynamic Fault Configuration

The `kill_config.json` file defines when and which services will be terminated during a test:

```json
{
  "kill_triggers": [
    {
      "type": "users",
      "threshold": 200,
      "services": ["order-service"],
      "count": 1,
      "recovery_time": 30
    },
    {
      "type": "rps",
      "threshold": 100,
      "services": ["payment-service"],
      "count": 1,
      "recovery_time": 0
    }
  ]
}
```


Available trigger types:
- `users`: Triggered when a certain number of simulated users is reached
- `time`: Triggered after a certain number of seconds from the start of the test
- rps`: On when a certain number of requests per second is reached

## Advanced Options

### Dynamic Test with Database Initialisation

To initialise the database before running the test:

```bash
python run_dynamic_stress_test.py --init --users 2000 --spawn-rate 100
```

### Customising the Fault Configuration

You can specify a custom configuration file:

```bash
python run_dynamic_stress_test.py --kill-config my_custom_kill_config.json
```

### Manual Management of Docker Services

``bash
# List all services
python docker_service_manager.py list

# Kill specific services
python docker_service_manager.py kill order-service --count 2

# Restart services
python docker_service_manager.py restart container_name1 container_name2

# Get the status of services
python docker_service_manager.py status
```

## View Results

Displays test results:

```bash
python visualize_results.py --results-dir results --output-dir visualizations --report
```

This will generate:
- Graphs comparing inconsistencies between different scenarios
- Graphs showing checkout success rates
- A complete test report

## Interpretation of Results

The test results will show:

1. **Inconsistencies in Stock Service**: Differences between expected and actual stock of items
2. **Inconsistencies in Payment Service**: Differences between expected and actual user credit
3. **Checkout Success Rate**: Percentage of successfully completed checkouts
4. **Service Recovery Metrics**: How well the system recovers after service recovery

A high number of inconsistencies indicates potential problems with the fault tolerance of the system.

## Workflow Example

1. Preparing the sest for dynamic stress:
   ```bash
   mkdir -p results visualizations
   
   # Modify kill_config.json (if needed)
   ```

2. Execute the sest with dynamic kill during ramp-up phase:
   ```bash
   python run_dynamic_stress_test.py --init --users 1000 --spawn-rate 50 --runtime 300
   ```

3. Results visualization (comiing soon):
   ```bash
   python visualize_results.py --report
   ```

## How Dynamic Service Killing Works

The dynamic kill mechanissm works indipendently during the Locust test file execution 

1. `DynamicServiceKiller` starts with the test

2. A background thread monitors metrics (users, time, RPS)

3. On threshold breach, it kills specified services

4. If recovery time is set, services are restarted automatically

This approach simulates realistic failure scenarios during high-load growth phases, helping test system resilience under pressure.