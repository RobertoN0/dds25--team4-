import argparse
import json
import logging
import os
import subprocess
import sys
import threading
import time
from typing import List, Dict, Optional, Callable

from docker_service_manager import DockerServiceManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s - %(asctime)s - %(name)s - %(message)s',
    datefmt='%I:%M:%S'
)
logger = logging.getLogger("Dynamic-Service-Killer")

class DynamicServiceKiller:
    """Class to kill services at specific points during a stress test."""
    
    def __init__(self, 
                 kill_triggers: List[Dict],
                 manager: Optional[DockerServiceManager] = None):
        """Initialize the dynamic service killer.
        
        Args:
            kill_triggers: List of dictionaries defining when to kill services
                Each trigger should have:
                - 'type': 'time', 'users', or 'rps' (requests per second)
                - 'threshold': Value at which to trigger the kill
                - 'services': List of services to kill
                - 'count': Number of instances to kill per service
                - 'recovery_time': Time in seconds before restarting services (0 = no restart)
            manager: Optional DockerServiceManager instance
        """
        self.kill_triggers = kill_triggers
        self.manager = manager or DockerServiceManager()
        self.killed_containers = []
        self.active_triggers = []
        self.monitoring_thread = None
        self.stop_monitoring = threading.Event()
    
    def start_monitoring(self, stats_callback: Callable[[], Dict] = None):
        """Start monitoring for kill triggers.
        
        Args:
            stats_callback: Function that returns current test statistics
                Should return a dict with at least these keys:
                - 'elapsed_time': Seconds since test started
                - 'user_count': Current number of users
                - 'rps': Current requests per second
        """
        if self.monitoring_thread and self.monitoring_thread.is_alive():
            logger.warning("Monitoring thread is already running")
            return
        
        self.stop_monitoring.clear()
        self.active_triggers = self.kill_triggers.copy()
        
        def _monitor_loop():
            start_time = time.time()
            while not self.stop_monitoring.is_set() and self.active_triggers:
                current_stats = {
                    'elapsed_time': time.time() - start_time,
                    'user_count': 0,
                    'rps': 0
                }
                
                # Update stats if callback is provided
                if stats_callback:
                    current_stats.update(stats_callback())
                
                # Check each active trigger
                triggers_to_remove = []
                for i, trigger in enumerate(self.active_triggers):
                    triggered = False
                    
                    if trigger['type'] == 'time' and current_stats['elapsed_time'] >= trigger['threshold']:
                        triggered = True
                    elif trigger['type'] == 'users' and current_stats['user_count'] >= trigger['threshold']:
                        triggered = True
                    elif trigger['type'] == 'rps' and current_stats['rps'] >= trigger['threshold']:
                        triggered = True
                    
                    if triggered:
                        logger.info(f"Trigger activated: {trigger['type']} reached {trigger['threshold']}")
                        self._execute_trigger(trigger)
                        triggers_to_remove.append(i)
                
                # Remove processed triggers (in reverse order to avoid index issues)
                for i in sorted(triggers_to_remove, reverse=True):
                    self.active_triggers.pop(i)
                
                # Sleep briefly to avoid high CPU usage
                time.sleep(0.5)
            
            logger.info("Monitoring stopped")
        
        self.monitoring_thread = threading.Thread(target=_monitor_loop)
        self.monitoring_thread.daemon = True
        self.monitoring_thread.start()
        logger.info("Started monitoring for kill triggers")
    
    def stop(self):
        """Stop monitoring and clean up."""
        self.stop_monitoring.set()
        if self.monitoring_thread:
            self.monitoring_thread.join(timeout=5)
        
        # Make sure all killed services are restarted if needed
        for trigger_info in self.killed_containers:
            if time.time() >= trigger_info['restart_time'] and not trigger_info['restarted']:
                self._restart_services(trigger_info)
    
    def _execute_trigger(self, trigger: Dict):
        """Execute a trigger by killing specified services."""
        services = trigger.get('services', [])
        count = trigger.get('count', 1)
        recovery_time = trigger.get('recovery_time', 0)
        
        if not services:
            logger.warning("No services specified in trigger")
            return
        
        logger.info(f"Killing services: {services}, count: {count}")
        killed = self.manager.kill_services(services, count)
        
        if killed:
            trigger_info = {
                'containers': killed,
                'restart_time': time.time() + recovery_time if recovery_time > 0 else float('inf'),
                'restarted': False
            }
            self.killed_containers.append(trigger_info)
            
            # Schedule service recovery if needed
            if recovery_time > 0:
                recovery_thread = threading.Timer(
                    recovery_time, 
                    self._restart_services, 
                    [trigger_info]
                )
                recovery_thread.daemon = True
                recovery_thread.start()
                logger.info(f"Scheduled service recovery in {recovery_time} seconds")
    
    def _restart_services(self, trigger_info: Dict):
        """Restart services that were killed."""
        if trigger_info['restarted']:
            return
        
        containers = trigger_info['containers']
        logger.info(f"Restarting services: {containers}")
        self.manager.restart_services(containers)
        trigger_info['restarted'] = True

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Dynamic Service Killer for Fault Tolerance Testing')
    parser.add_argument('--config', default='kill_config.json', help='Path to kill configuration file')
    parser.add_argument('--dry-run', action='store_true', help='Print actions without executing them')
    
    return parser.parse_args()

def main():
    """Main function for standalone operation."""
    args = parse_arguments()
    
    # Load configuration file
    try:
        with open(args.config, 'r') as f:
            config = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logger.error(f"Error loading configuration: {e}")
        logger.info("Using sample configuration instead")
        
        # Sample configuration
        config = {
            "kill_triggers": [
                {
                    "type": "time",
                    "threshold": 30,  # 30 seconds after start
                    "services": ["order-service"],
                    "count": 1,
                    "recovery_time": 60
                },
                {
                    "type": "time",
                    "threshold": 90,  # 90 seconds after start
                    "services": ["payment-service", "stock-service"],
                    "count": 1,
                    "recovery_time": 0
                }
            ]
        }
    
    # Create Docker service manager
    manager = DockerServiceManager()
    
    # Create and run the dynamic service killer
    killer = DynamicServiceKiller(
        kill_triggers=config.get("kill_triggers", []),
        manager=manager
    )
    
    if args.dry_run:
        logger.info("DRY RUN MODE - Actions will be printed but not executed")
        logger.info(f"Loaded {len(killer.kill_triggers)} kill triggers:")
        for i, trigger in enumerate(killer.kill_triggers):
            logger.info(f"  Trigger {i+1}: Kill {trigger.get('services')} when {trigger.get('type')} reaches {trigger.get('threshold')}")
        return 0
    
    # Mock stats callback for standalone operation
    def mock_stats_callback():
        return {
            'user_count': 0,
            'rps': 0
        }
    
    try:
        # Start monitoring
        killer.start_monitoring(mock_stats_callback)
        
        # Keep running until all triggers are processed
        logger.info("Press Ctrl+C to stop...")
        while killer.monitoring_thread and killer.monitoring_thread.is_alive():
            time.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("Stopping...")
    finally:
        killer.stop()
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
