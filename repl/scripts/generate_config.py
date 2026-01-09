#!/usr/bin/env python3
"""
Generate cluster configuration for LineairDB Group Replication.

This script creates cluster_config.json with primary and secondary node
configurations for MySQL Group Replication.

Supports:
- Docker containers (local)
- Remote hosts (SSH-accessible MySQL servers)

Usage:
    python3 generate_config.py <num_secondaries> <output_path> [remote_hosts...]
    
Examples:
    # 2 Docker secondary nodes
    python3 generate_config.py 2 ../config/cluster_config.json
    
    # 3 remote hosts (no Docker)
    python3 generate_config.py 3 ../config/cluster_config.json 192.168.1.10 192.168.1.11 192.168.1.12
    
    # 5 total: 2 remote hosts + 3 Docker containers
    python3 generate_config.py 5 ../config/cluster_config.json 192.168.1.10 192.168.1.11
"""

import json
import socket
import sys
from pathlib import Path


def parse_remote_host(host_spec: str, node_id: int, mysql_root_password: str) -> dict:
    """
    Parse a remote host specification.
    
    Format: host[:port[:user]]
    Examples:
        192.168.1.10          -> host=192.168.1.10, port=3306
        192.168.1.10:3307     -> host=192.168.1.10, port=3307
        192.168.1.10:3306:root -> host=192.168.1.10, port=3306, ssh_user=root
    
    Args:
        host_spec: Host specification string
        node_id: Node ID for this secondary
        mysql_root_password: MySQL root password
        
    Returns:
        Secondary node configuration dictionary
    """
    parts = host_spec.split(":")
    host = parts[0]
    port = int(parts[1]) if len(parts) > 1 else 3306
    ssh_user = parts[2] if len(parts) > 2 else "root"
    
    return {
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


def generate_cluster_config(num_secondaries: int, output_path: str, remote_hosts: list = None) -> dict:
    """
    Generate cluster configuration.
    
    Args:
        num_secondaries: Total number of secondary nodes to configure
        output_path: Path to save the configuration file
        remote_hosts: List of remote host specifications (optional)
        
    Returns:
        Configuration dictionary
    """
    remote_hosts = remote_hosts or []
    
    # Validate
    if len(remote_hosts) > num_secondaries:
        raise ValueError(f"Too many remote hosts ({len(remote_hosts)}) for {num_secondaries} secondaries")
    
    num_docker = num_secondaries - len(remote_hosts)
    
    # Network configuration (for Docker containers)
    docker_network_name = "mysql-cluster-net"
    docker_network_subnet = "172.20.0.0/16"
    docker_base_ip = "172.20.0"
    
    # MySQL configuration
    mysql_root_password = "kamo"
    mysql_database = "testdb"
    mysql_user = "clusteruser"
    mysql_user_password = "kamo"
    
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
        secondary = parse_remote_host(host_spec, node_id, mysql_root_password)
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
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_file, "w") as f:
        json.dump(config, f, indent=2)
    
    return config


def main():
    if len(sys.argv) < 3:
        print("Usage: python3 generate_config.py <num_secondaries> <output_path> [remote_hosts...]")
        print("")
        print("Examples:")
        print("  # 2 Docker secondary nodes")
        print("  python3 generate_config.py 2 ../config/cluster_config.json")
        print("")
        print("  # 3 remote hosts (no Docker)")
        print("  python3 generate_config.py 3 config.json 192.168.1.10 192.168.1.11 192.168.1.12")
        print("")
        print("  # 5 total: 2 remote + 3 Docker")
        print("  python3 generate_config.py 5 config.json 192.168.1.10 192.168.1.11")
        print("")
        print("Remote host format: host[:port[:ssh_user]]")
        print("  192.168.1.10          -> port=3306, ssh_user=root")
        print("  192.168.1.10:3307     -> port=3307, ssh_user=root")
        print("  192.168.1.10:3306:ubuntu -> port=3306, ssh_user=ubuntu")
        sys.exit(1)
    
    num_secondaries = int(sys.argv[1])
    output_path = sys.argv[2]
    remote_hosts = sys.argv[3:] if len(sys.argv) > 3 else []
    
    config = generate_cluster_config(num_secondaries, output_path, remote_hosts)
    
    print(f"  ✓ Cluster configuration created")
    print(f"  Config saved to: {output_path}")
    print(f"  Primary: {config['primary']['host']}:{config['primary']['port']}")
    
    for i, sec in enumerate(config['secondaries'], 1):
        node_type = sec['node_type']
        if node_type == 'remote_host':
            print(f"  Secondary {i}: {sec['host']}:{sec['port']} (remote)")
        else:
            print(f"  Secondary {i}: {sec['host']}:{sec['port']} (docker: {sec['container_name']})")
    
    print(f"")
    print(f"  Remote hosts: {config['num_remote_hosts']}")
    print(f"  Docker containers: {config['num_docker_containers']}")


if __name__ == "__main__":
    main()
