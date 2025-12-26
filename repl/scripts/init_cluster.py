#!/usr/bin/env python3
"""
Initialize MySQL Cluster for LineairDB Replication.

This script sets up the cluster configuration by generating a config JSON file
that describes the primary node and secondary nodes (Docker or remote hosts).

Usage:
    python3 init_cluster.py <num_secondaries> [--remote host1 --remote host2 ...]

Examples:
    python3 init_cluster.py 2                                    # 2 Docker secondaries
    python3 init_cluster.py 3 --remote 192.168.1.10 --remote 192.168.1.11 --remote 192.168.1.12
    python3 init_cluster.py 5 --remote 192.168.1.10 --remote 192.168.1.11  # 2 remote + 3 Docker

Remote host format: host[:port[:ssh_user]]
    192.168.1.10          -> port=3306, ssh_user=root
    192.168.1.10:3307     -> port=3307, ssh_user=root
    192.168.1.10:3306:ubuntu -> port=3306, ssh_user=ubuntu
"""

import argparse
import json
import socket
import sys
from pathlib import Path
from typing import List

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from utils import (
    MYSQL_PASSWORD,
    get_repl_dir,
    parse_remote_host,
    print_header,
    print_step,
    print_success,
    print_error,
)


def generate_cluster_config(
    num_secondaries: int,
    output_path: Path,
    remote_hosts: List[str] = None,
) -> dict:
    """
    Generate cluster configuration.
    
    Args:
        num_secondaries: Total number of secondary nodes
        output_path: Path to save configuration
        remote_hosts: List of remote host specifications
        
    Returns:
        Configuration dictionary
    """
    remote_hosts = remote_hosts or []
    
    # Validate
    if len(remote_hosts) > num_secondaries:
        raise ValueError(
            f"Too many remote hosts ({len(remote_hosts)}) for {num_secondaries} secondaries"
        )
    
    num_docker = num_secondaries - len(remote_hosts)
    
    # Network configuration (for Docker containers)
    docker_network_name = "mysql-cluster-net"
    docker_network_subnet = "172.20.0.0/16"
    docker_base_ip = "172.20.0"
    
    # MySQL configuration
    mysql_root_password = MYSQL_PASSWORD
    mysql_database = "testdb"
    mysql_user = "clusteruser"
    mysql_user_password = MYSQL_PASSWORD
    
    # Primary node (local MySQL)
    primary = {
        "node_id": 1,
        "hostname": socket.gethostname(),
        "role": "primary",
        "node_type": "local_systemctl",
        "host": "127.0.0.1",
        "port": 3306,
        "mysql_root_password": mysql_root_password,
        "server_id": 1,
    }
    
    secondaries = []
    node_id = 2  # Primary is 1, secondaries start at 2
    
    # Add remote hosts first
    for host_spec in remote_hosts:
        host, port, ssh_user = parse_remote_host(host_spec)
        
        secondary = {
            "node_id": node_id,
            "hostname": host,
            "role": "secondary",
            "node_type": "remote_host",
            "host": host,
            "port": port,
            "mysql_root_password": mysql_root_password,
            "ssh_user": ssh_user,
            "server_id": node_id,
        }
        secondaries.append(secondary)
        node_id += 1
    
    # Add Docker containers for remaining slots
    docker_index = 1
    for _ in range(num_docker):
        container_name = f"mysql-secondary-{docker_index}"
        docker_ip = f"{docker_base_ip}.{10 + node_id}"
        host_port = 33061 + docker_index  # 33062, 33063, ...
        
        secondary = {
            "node_id": node_id,
            "hostname": container_name,
            "role": "secondary",
            "node_type": "docker_container",
            "host": "127.0.0.1",
            "port": host_port,
            "mysql_root_password": mysql_root_password,
            "container_name": container_name,
            "docker_network": docker_network_name,
            "docker_ip": docker_ip,
            "server_id": node_id,
        }
        secondaries.append(secondary)
        node_id += 1
        docker_index += 1
    
    # Full configuration
    config = {
        "cluster_name": "lineairdb_cluster",
        "mysql_version": "8.0.43",
        "mysql_root_password": mysql_root_password,
        "mysql_database": mysql_database,
        "mysql_user": mysql_user,
        "mysql_user_password": mysql_user_password,
        "docker_network_name": docker_network_name,
        "docker_network_subnet": docker_network_subnet,
        "docker_base_ip": docker_base_ip,
        "docker_image": "mysql-lineairdb-ubuntu:8.0.43",
        "use_custom_image": True,
        "primary": primary,
        "secondaries": secondaries,
        "num_remote_hosts": len(remote_hosts),
        "num_docker_containers": num_docker,
    }
    
    # Save configuration
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(config, f, indent=2)
    
    return config


def main():
    parser = argparse.ArgumentParser(
        description="Initialize MySQL Cluster for LineairDB Replication",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s 2                                    # 2 Docker secondaries
  %(prog)s 3 --remote 192.168.1.10 --remote 192.168.1.11 --remote 192.168.1.12
  %(prog)s 5 --remote 192.168.1.10 --remote 192.168.1.11  # 2 remote + 3 Docker

Remote host format: host[:port[:ssh_user]]
  192.168.1.10          -> port=3306, ssh_user=root
  192.168.1.10:3307     -> port=3307, ssh_user=root
  192.168.1.10:3306:ubuntu -> port=3306, ssh_user=ubuntu
        """
    )
    parser.add_argument(
        'num_secondaries',
        type=int,
        help='Number of secondary nodes (1-10)'
    )
    parser.add_argument(
        '--remote',
        action='append',
        default=[],
        metavar='HOST',
        help='Remote host (can specify multiple times, e.g., --remote host1 --remote host2)'
    )
    
    args = parser.parse_args()
    
    # Validate
    if args.num_secondaries < 1 or args.num_secondaries > 10:
        print_error("num_secondaries must be between 1 and 10")
        sys.exit(1)
    
    if len(args.remote) > args.num_secondaries:
        print_error(
            f"Too many remote hosts ({len(args.remote)}) for {args.num_secondaries} secondaries"
        )
        sys.exit(1)
    
    repl_dir = get_repl_dir()
    root_dir = repl_dir.parent
    
    num_docker = args.num_secondaries - len(args.remote)
    
    print_header("LineairDB Cluster Initialization")
    
    print("Configuration:")
    print(f"  Root directory: {root_dir}")
    print(f"  Repl directory: {repl_dir}")
    print(f"  Total secondary nodes: {args.num_secondaries}")
    print(f"  Remote hosts: {len(args.remote)}")
    print(f"  Docker containers: {num_docker}")
    
    if args.remote:
        print("  Remote host list:")
        for host in args.remote:
            print(f"    - {host}")
    print()
    
    # Create config directory
    print_step("Creating configuration directory...")
    config_dir = repl_dir / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    print_success(f"Created {config_dir}")
    
    # Generate configuration
    print()
    print_step("Generating cluster configuration...")
    output_path = config_dir / "cluster_config.json"
    
    try:
        config = generate_cluster_config(
            args.num_secondaries,
            output_path,
            args.remote
        )
        
        print_success("Cluster configuration created")
        print(f"  Config saved to: {output_path}")
        print(f"  Primary: {config['primary']['host']}:{config['primary']['port']}")
        
        for i, sec in enumerate(config['secondaries'], 1):
            node_type = sec['node_type']
            if node_type == 'remote_host':
                print(f"  Secondary {i}: {sec['host']}:{sec['port']} (remote)")
            else:
                print(f"  Secondary {i}: {sec['host']}:{sec['port']} (docker: {sec['container_name']})")
        
        print()
        print(f"  Remote hosts: {config['num_remote_hosts']}")
        print(f"  Docker containers: {config['num_docker_containers']}")
        
    except Exception as e:
        print_error(f"Failed to generate configuration: {e}")
        sys.exit(1)
    
    print()
    print_header("Initialization Complete!")
    print()
    print("Next steps:")
    print("  1. Start the cluster:     python3 start_cluster.py")
    print("  2. Check status:          python3 status.py")
    print("  3. Install LineairDB:     python3 install_plugin.py")
    print()


if __name__ == "__main__":
    main()

