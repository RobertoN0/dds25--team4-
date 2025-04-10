import argparse
import json
import logging
import os
import subprocess
import sys
import time
from typing import List, Dict, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s - %(asctime)s - %(name)s - %(message)s',
    datefmt='%I:%M:%S'
)
logger = logging.getLogger("Docker-Service-Manager")

class DockerServiceManager:
    """Class to manage Docker services for fault tolerance testing."""
    
    def __init__(self, project_name: str = ""):
        """Initialize the Docker service manager.
        
        Args:
            project_name: Optional Docker Compose project name. Leave empty to detect from docker-compose.yml.
        """
        self.project_name = project_name or self._detect_project_name()
        self._validate_environment()
    
    def _detect_project_name(self) -> str:
        """Detect the Docker Compose project name from running containers."""
        try:
            result = subprocess.run(
                ["docker", "ps", "--format", "{{.Labels}}"],
                capture_output=True, text=True, check=True
            )
            for line in result.stdout.splitlines():
                if "com.docker.compose.project=" in line:
                    for label in line.split(','):
                        if "com.docker.compose.project=" in label:
                            return label.split('=')[1].strip()
        except subprocess.SubprocessError as e:
            logger.error(f"Error detecting project name: {e}")
        
        # Default to directory name if no project is found
        return os.path.basename(os.path.abspath(".."))
    
    def _validate_environment(self) -> bool:
        """Validate that Docker is running and the services are available."""
        try:
            # Check Docker is running
            subprocess.run(["docker", "info"], capture_output=True, check=True)
            
            # Check our services are running
            services = self.list_services()
            if not services:
                logger.warning("No services found for project. Make sure your Docker Compose services are running")
                return False
            
            logger.info(f"Found {len(services)} services for project '{self.project_name}'")
            return True
            
        except subprocess.SubprocessError as e:
            logger.error(f"Error validating Docker environment: {e}")
            return False
    
    def list_services(self) -> List[Dict[str, str]]:
        """List all running services for this project."""
        try:
            result = subprocess.run(
                ["docker", "ps", "--filter", f"label=com.docker.compose.project={self.project_name}",
                 "--format", "{{.Names}}\t{{.Status}}\t{{.Image}}"],
                capture_output=True, text=True, check=True
            )
            
            services = []
            for line in result.stdout.splitlines():
                if not line.strip():
                    continue
                    
                parts = line.split('\t')
                if len(parts) >= 3:
                    service_name = parts[0]
                    # Extract the base service name without project prefix and instance number
                    base_name = service_name.split('_')[-2] if '_' in service_name else service_name
                    services.append({
                        "container_name": service_name,
                        "base_name": base_name,
                        "status": parts[1],
                        "image": parts[2]
                    })
            
            return services
            
        except subprocess.SubprocessError as e:
            logger.error(f"Error listing services: {e}")
            return []
    
    def kill_services(self, service_names: List[str], count: int = 1) -> List[str]:
        """Kill specified services.
        
        Args:
            service_names: List of base service names to kill (e.g., 'order-service')
            count: Number of instances to kill for each service
            
        Returns:
            List of container IDs that were killed
        """
        killed_containers = []
        services = self.list_services()
        
        for service_name in service_names:
            # Find containers for this service
            matching_containers = [s["container_name"] for s in services if s["base_name"] == service_name]
            
            if not matching_containers:
                logger.warning(f"No containers found for service '{service_name}'")
                continue
            
            logger.info(f"Found {len(matching_containers)} containers for service '{service_name}'")
            
            # Kill the specified number of containers (or all if fewer exist)
            for container in matching_containers[:min(count, len(matching_containers))]:
                logger.info(f"Killing container {container}...")
                try:
                    subprocess.run(["docker", "stop", container], capture_output=True, check=True)
                    killed_containers.append(container)
                except subprocess.SubprocessError as e:
                    logger.error(f"Error killing container {container}: {e}")
        
        return killed_containers
    
    def restart_services(self, container_names: List[str]) -> bool:
        """Restart previously killed services.
        
        Args:
            container_names: List of container names to restart
            
        Returns:
            True if all services were restarted successfully, False otherwise
        """
        success = True
        
        for container in container_names:
            logger.info(f"Restarting container {container}...")
            try:
                subprocess.run(["docker", "start", container], capture_output=True, check=True)
            except subprocess.SubprocessError as e:
                logger.error(f"Error restarting container {container}: {e}")
                success = False
        
        # Wait for services to be ready
        if container_names:
            logger.info("Waiting for services to become ready...")
            time.sleep(5)
        
        return success
    
    def get_logs(self, container_name: str, lines: int = 50) -> str:
        """Get logs for a specific container.
        
        Args:
            container_name: Name of the container
            lines: Number of log lines to retrieve
            
        Returns:
            Container logs as string
        """
        try:
            result = subprocess.run(
                ["docker", "logs", "--tail", str(lines), container_name],
                capture_output=True, text=True, check=True
            )
            return result.stdout
        except subprocess.SubprocessError as e:
            logger.error(f"Error getting logs for container {container_name}: {e}")
            return f"Error: {str(e)}"
    
    def get_service_status(self) -> Dict[str, Dict]:
        """Get the status of all services, grouped by service name.
        
        Returns:
            Dictionary with service information
        """
        services = self.list_services()
        status = {}
        
        for service in services:
            base_name = service["base_name"]
            if base_name not in status:
                status[base_name] = {
                    "total": 0,
                    "running": 0,
                    "instances": []
                }
            
            status[base_name]["total"] += 1
            if "Up" in service["status"]:
                status[base_name]["running"] += 1
            
            status[base_name]["instances"].append({
                "name": service["container_name"],
                "status": service["status"],
                "image": service["image"]
            })
        
        return status

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Docker Service Manager for Fault Tolerance Testing')
    parser.add_argument('--project', help='Docker Compose project name')
    
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
    # List services
    list_parser = subparsers.add_parser('list', help='List services')
    
    # Kill services
    kill_parser = subparsers.add_parser('kill', help='Kill services')
    kill_parser.add_argument('services', nargs='+', help='Services to kill')
    kill_parser.add_argument('--count', type=int, default=1, help='Number of instances to kill')
    
    # Restart services
    restart_parser = subparsers.add_parser('restart', help='Restart services')
    restart_parser.add_argument('containers', nargs='+', help='Containers to restart')
    
    # Get logs
    logs_parser = subparsers.add_parser('logs', help='Get service logs')
    logs_parser.add_argument('container', help='Container name')
    logs_parser.add_argument('--lines', type=int, default=50, help='Number of log lines')
    
    # Get status
    status_parser = subparsers.add_parser('status', help='Get service status')
    
    return parser.parse_args()

def main():
    """Main function."""
    args = parse_arguments()
    
    manager = DockerServiceManager(args.project)
    
    if args.command == 'list':
        services = manager.list_services()
        print(f"Found {len(services)} services:")
        for service in services:
            print(f"- {service['container_name']} ({service['status']})")
    
    elif args.command == 'kill':
        killed = manager.kill_services(args.services, args.count)
        print(f"Killed {len(killed)} containers: {', '.join(killed)}")
    
    elif args.command == 'restart':
        success = manager.restart_services(args.containers)
        print(f"Restart {'successful' if success else 'failed'}")
    
    elif args.command == 'logs':
        logs = manager.get_logs(args.container, args.lines)
        print(logs)
    
    elif args.command == 'status':
        status = manager.get_service_status()
        print(f"Service Status for project '{manager.project_name}':")
        for service_name, info in status.items():
            print(f"- {service_name}: {info['running']}/{info['total']} running")
            for instance in info['instances']:
                print(f"  - {instance['name']}: {instance['status']}")
    
    else:
        services = manager.list_services()
        if services:
            print(f"Found {len(services)} services for project '{manager.project_name}':")
            for service in services:
                print(f"- {service['base_name']}: {service['container_name']} ({service['status']})")
        else:
            print("No services found or Docker is not running.")
            return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
