#!/usr/bin/env python3
"""
Stop the MySQL Cluster for LineairDB Replication.

This script stops Docker secondary containers.
Remote MySQL hosts are NOT stopped (managed externally).

Usage:
    python3 stop_cluster.py
"""

import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from utils import (
    ClusterConfig,
    docker_compose_down,
    get_repl_dir,
    load_config_if_exists,
    print_header,
    print_step,
    print_success,
    print_warning,
    run_command_with_sudo,
)


def stop_docker_containers(config: ClusterConfig) -> None:
    """Stop Docker containers."""
    repl_dir = get_repl_dir()
    compose_file = repl_dir / "docker-compose-secondaries.yml"
    
    if config.num_docker_containers > 0:
        if compose_file.exists():
            print_step("Stopping Docker containers via docker-compose...")
            docker_compose_down(compose_file)
        else:
            # Stop containers directly by name
            print_step("Stopping Docker containers directly...")
            for sec in config.secondaries:
                if sec.node_type == "docker_container":
                    print(f"  Stopping {sec.container_name}...")
                    run_command_with_sudo(["docker", "stop", sec.container_name])
        
        print_success("Docker containers stopped")
    else:
        print("No Docker containers configured.")


def report_remote_hosts(config: ClusterConfig) -> None:
    """Report remote hosts that were not stopped."""
    if config.num_remote_hosts > 0:
        print(f"\nRemote hosts ({config.num_remote_hosts}):")
        for sec in config.secondaries:
            if sec.node_type == "remote_host":
                print(f"  - {sec.host}:{sec.port} (not stopped - managed externally)")
        
        print()
        print_warning("Remote MySQL servers must be stopped manually if needed.")


def main():
    print_header("Stopping LineairDB Cluster")
    
    # Load configuration
    config = load_config_if_exists()
    
    if config is None:
        print_warning("No cluster configuration found")
        print("Nothing to stop.")
        return
    
    # Stop Docker containers
    stop_docker_containers(config)
    
    # Report remote hosts
    report_remote_hosts(config)
    
    print()
    print_header("Cluster Stopped!")
    print()
    print("Note: Local MySQL (primary) is managed by systemctl and was not stopped.")
    print("To stop primary: sudo systemctl stop mysql")
    print()


if __name__ == "__main__":
    main()

