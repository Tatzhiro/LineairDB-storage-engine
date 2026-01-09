#!/usr/bin/env python3
"""
Generate docker-compose.yml for LineairDB Group Replication secondary nodes.

This script reads cluster_config.json and generates a docker-compose file
for the secondary Docker containers.

Usage:
    python3 generate_compose.py <config_path> <output_path> <data_dir>
    
Example:
    python3 generate_compose.py ../config/cluster_config.json ./docker-compose-secondaries.yml ./data
"""

import json
import sys
from pathlib import Path

# Try to import yaml, fall back to manual generation if not available
try:
    import yaml
    HAS_YAML = True
except ImportError:
    HAS_YAML = False


def generate_docker_compose(config_path: str, output_path: str, data_base_dir: str) -> dict:
    """
    Generate docker-compose configuration for secondary nodes.
    
    Args:
        config_path: Path to cluster_config.json
        output_path: Path to save docker-compose.yml
        data_base_dir: Base directory for data volumes
        
    Returns:
        Docker compose configuration dictionary
    """
    # Load cluster config
    with open(config_path) as f:
        config = json.load(f)
    
    # Extract configuration
    docker_network_name = config.get("docker_network_name", "mysql-cluster-net")
    docker_network_subnet = config.get("docker_network_subnet", "172.20.0.0/16")
    docker_image = config.get("docker_image", "mysql-lineairdb-ubuntu:8.0.43")
    mysql_root_password = config.get("mysql_root_password", "kamo")
    mysql_database = config.get("mysql_database", "testdb")
    mysql_user = config.get("mysql_user", "clusteruser")
    mysql_user_password = config.get("mysql_user_password", "kamo")
    secondaries = config.get("secondaries", [])
    
    # Filter to only Docker container nodes
    docker_nodes = [s for s in secondaries if s.get("node_type") == "docker_container"]
    
    if not docker_nodes:
        print("No Docker secondary nodes configured")
        return {}
    
    services = {}
    
    for node in docker_nodes:
        container_name = node["container_name"]
        server_id = node.get("server_id", node["node_id"])
        host_port = node["port"]
        docker_ip = node.get("docker_ip", f"172.20.0.{10 + node['node_id']}")
        secondary_num = node["node_id"] - 1  # For config file naming
        
        service = {
            "image": docker_image,
            "container_name": container_name,
            "hostname": container_name,
            "environment": {
                "MYSQL_ROOT_PASSWORD": mysql_root_password,
                "MYSQL_DATABASE": mysql_database,
                "MYSQL_USER": mysql_user,
                "MYSQL_PASSWORD": mysql_user_password,
                "MYSQL_SERVER_ID": str(server_id),
            },
            "ports": [f"{host_port}:3306"],
            "volumes": [
                f"./config/secondary{secondary_num}.cnf:/etc/mysql/conf.d/custom.cnf",
                f"./data/secondary{secondary_num}:/var/lib/mysql",
            ],
            "networks": {
                docker_network_name: {
                    "ipv4_address": docker_ip,
                }
            },
            "command": (
                f"mysqld --server-id={server_id} "
                "--log-bin=mysql-bin "
                "--gtid-mode=ON "
                "--enforce-gtid-consistency=ON "
                "--binlog-format=ROW "
                "--relay-log=mysql-relay-bin"
            ),
            "restart": "unless-stopped",
            "healthcheck": {
                "test": [
                    "CMD", "mysqladmin", "ping", "-h", "localhost",
                    "-u", "root", f"-p{mysql_root_password}"
                ],
                "interval": "10s",
                "timeout": "5s",
                "retries": 5,
            },
        }
        
        services[container_name] = service
    
    compose_config = {
        "version": "3.3",
        "services": services,
        "networks": {
            docker_network_name: {
                "driver": "bridge",
                "ipam": {
                    "config": [
                        {"subnet": docker_network_subnet}
                    ]
                }
            }
        }
    }
    
    # Write docker-compose file
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    if HAS_YAML:
        with open(output_file, "w") as f:
            yaml.dump(compose_config, f, default_flow_style=False)
    else:
        # Manual YAML generation for simple structure
        with open(output_file, "w") as f:
            f.write(f"version: '3.3'\n")
            f.write(f"networks:\n")
            f.write(f"  {docker_network_name}:\n")
            f.write(f"    driver: bridge\n")
            f.write(f"    ipam:\n")
            f.write(f"      config:\n")
            f.write(f"      - subnet: {docker_network_subnet}\n")
            f.write(f"services:\n")
            
            for name, svc in services.items():
                f.write(f"  {name}:\n")
                f.write(f"    command: {svc['command']}\n")
                f.write(f"    container_name: {svc['container_name']}\n")
                f.write(f"    environment:\n")
                for k, v in svc['environment'].items():
                    f.write(f"      {k}: '{v}'\n")
                f.write(f"    healthcheck:\n")
                f.write(f"      interval: {svc['healthcheck']['interval']}\n")
                f.write(f"      retries: {svc['healthcheck']['retries']}\n")
                f.write(f"      test:\n")
                for item in svc['healthcheck']['test']:
                    f.write(f"      - {item}\n")
                f.write(f"      timeout: {svc['healthcheck']['timeout']}\n")
                f.write(f"    hostname: {svc['hostname']}\n")
                f.write(f"    image: {svc['image']}\n")
                f.write(f"    networks:\n")
                for net_name, net_conf in svc['networks'].items():
                    f.write(f"      {net_name}:\n")
                    f.write(f"        ipv4_address: {net_conf['ipv4_address']}\n")
                f.write(f"    ports:\n")
                for port in svc['ports']:
                    f.write(f"    - {port}\n")
                f.write(f"    restart: {svc['restart']}\n")
                f.write(f"    volumes:\n")
                for vol in svc['volumes']:
                    f.write(f"    - {vol}\n")
    
    return compose_config


def generate_secondary_configs(config_path: str, config_dir: str) -> list:
    """
    Generate MySQL configuration files for secondary nodes.
    
    Args:
        config_path: Path to cluster_config.json
        config_dir: Directory to save config files
        
    Returns:
        List of paths to generated config files
    """
    # Load cluster config
    with open(config_path) as f:
        config = json.load(f)
    
    secondaries = config.get("secondaries", [])
    config_paths = []
    config_dir_path = Path(config_dir)
    config_dir_path.mkdir(parents=True, exist_ok=True)
    
    for node in secondaries:
        server_id = node.get("server_id", node["node_id"])
        secondary_num = node["node_id"] - 1
        
        config_content = f"""[mysqld]
# Server Configuration
server-id = {server_id}
bind-address = 0.0.0.0
port = 3306

# Replication Configuration
log-bin = mysql-bin
binlog-format = ROW
gtid-mode = ON
enforce-gtid-consistency = ON
relay-log = mysql-relay-bin
relay-log-recovery = 1

# Group Replication Configuration
# Note: Group Replication settings will be configured via MySQL Shell

# Storage Engine Configuration
default-storage-engine = InnoDB

# Performance Tuning
innodb_buffer_pool_size = 1G
innodb_log_file_size = 256M
max_connections = 200
max_allowed_packet = 256M

# LineairDB Support (if plugin is available)
# plugin-load-add = ha_lineairdb.so
"""
        
        config_file = config_dir_path / f"secondary{secondary_num}.cnf"
        with open(config_file, "w") as f:
            f.write(config_content)
        
        config_paths.append(str(config_file))
    
    return config_paths


def main():
    if len(sys.argv) < 4:
        print("Usage: python3 generate_compose.py <config_path> <output_path> <data_dir>")
        print("Example: python3 generate_compose.py ../config/cluster_config.json ./docker-compose-secondaries.yml ./data")
        sys.exit(1)
    
    config_path = sys.argv[1]
    output_path = sys.argv[2]
    data_dir = sys.argv[3]
    
    # Generate secondary MySQL configs
    config_dir = str(Path(config_path).parent)
    config_files = generate_secondary_configs(config_path, config_dir)
    print(f"  ✓ Generated {len(config_files)} secondary config files")
    
    # Generate docker-compose
    compose = generate_docker_compose(config_path, output_path, data_dir)
    if compose:
        print(f"  ✓ Docker compose file generated: {output_path}")
    else:
        print("  ⚠ No Docker nodes to generate")


if __name__ == "__main__":
    main()

