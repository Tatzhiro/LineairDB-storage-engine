#!/usr/bin/env python3
"""
Check the status of LineairDB MySQL Cluster.

Shows:
- Node status (primary + secondaries: Docker containers and remote hosts)
- Group Replication status
- LineairDB plugin status

Usage:
    python3 status.py
"""

import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from utils import (
    ClusterConfig,
    Colors,
    MYSQL_PASSWORD,
    MYSQL_USER,
    docker_get_health,
    docker_is_running,
    get_cluster_status,
    get_config_path,
    load_config_if_exists,
    mysql_is_running,
    mysql_query,
    mysqlsh_execute,
    ping_host,
    print_error,
    print_header,
    print_success,
    print_warning,
    systemctl_is_active,
)


def check_lineairdb_status(host: str, port: int) -> str:
    """Check LineairDB plugin status on a node."""
    result = mysql_query(
        "SELECT PLUGIN_STATUS FROM INFORMATION_SCHEMA.PLUGINS WHERE PLUGIN_NAME = 'LINEAIRDB';",
        host=host,
        port=port,
    )
    return result if result else "NOT_INSTALLED"


def check_primary_status(config: ClusterConfig) -> None:
    """Check and display primary node status."""
    print("Primary Node (local):")
    print(f"  Host: {config.primary_host}:{config.primary_port}")
    
    # Check if binbench mode (local build MySQL, not systemctl-managed)
    binbench_marker = get_config_path().parent / ".binbench_mode"
    is_binbench = binbench_marker.exists()
    
    # In binbench mode, check TCP connection directly; otherwise check systemctl
    mysql_running = False
    if is_binbench:
        # Try both empty password (fresh init) and normal password
        mysql_running = (
            mysql_is_running(config.primary_host, config.primary_port, password="") or
            mysql_is_running(config.primary_host, config.primary_port)
        )
        if mysql_running:
            print(f"  Running: {Colors.GREEN}✓{Colors.NC} (binbench mode)")
    else:
        mysql_running = systemctl_is_active("mysql")
        if mysql_running:
            print(f"  Running: {Colors.GREEN}✓{Colors.NC}")
    
    if mysql_running:
        if mysql_is_running(config.primary_host, config.primary_port) or \
           mysql_is_running(config.primary_host, config.primary_port, password=""):
            print(f"  Reachable: {Colors.GREEN}✓{Colors.NC}")
            
            # Check LineairDB
            status = check_lineairdb_status(config.primary_host, config.primary_port)
            if status == "ACTIVE":
                print(f"  LineairDB: {Colors.GREEN}✓ ACTIVE{Colors.NC}")
            else:
                print(f"  LineairDB: {Colors.RED}✗ {status}{Colors.NC}")
        else:
            print(f"  Reachable: {Colors.RED}✗{Colors.NC}")
    else:
        print(f"  Running: {Colors.RED}✗{Colors.NC}")


def check_secondary_status(config: ClusterConfig) -> None:
    """Check and display secondary nodes status."""
    print(f"\nSecondary Nodes ({len(config.secondaries)} total: "
          f"{config.num_docker_containers} Docker, {config.num_remote_hosts} Remote):")
    
    for sec in config.secondaries:
        if sec.node_type == "docker_container":
            print(f"  {sec.container_name} (Docker, port {sec.port}):")
            
            if docker_is_running(sec.container_name):
                print(f"    Running: {Colors.GREEN}✓{Colors.NC}")
                
                # Check health status
                health = docker_get_health(sec.container_name)
                print(f"    Health: {health}")
                
                # Check MySQL connectivity
                if mysql_is_running(sec.host, sec.port):
                    print(f"    Reachable: {Colors.GREEN}✓{Colors.NC}")
                    
                    # Check LineairDB
                    status = check_lineairdb_status(sec.host, sec.port)
                    if status == "ACTIVE":
                        print(f"    LineairDB: {Colors.GREEN}✓ ACTIVE{Colors.NC}")
                    else:
                        print(f"    LineairDB: {Colors.RED}✗ {status}{Colors.NC}")
                else:
                    print(f"    Reachable: {Colors.RED}✗{Colors.NC}")
            else:
                print(f"    Running: {Colors.RED}✗{Colors.NC}")
        
        else:  # remote_host
            print(f"  {sec.hostname} (Remote, {sec.host}:{sec.port}):")
            
            # Check network connectivity
            if ping_host(sec.host):
                print(f"    Network: {Colors.GREEN}✓{Colors.NC}")
                
                # Check MySQL connectivity
                if mysql_is_running(sec.host, sec.port):
                    print(f"    MySQL: {Colors.GREEN}✓{Colors.NC}")
                    
                    # Check LineairDB
                    status = check_lineairdb_status(sec.host, sec.port)
                    if status == "ACTIVE":
                        print(f"    LineairDB: {Colors.GREEN}✓ ACTIVE{Colors.NC}")
                    else:
                        print(f"    LineairDB: {Colors.RED}✗ {status}{Colors.NC}")
                else:
                    print(f"    MySQL: {Colors.RED}✗ (port {sec.port}){Colors.NC}")
            else:
                print(f"    Network: {Colors.RED}✗ (unreachable){Colors.NC}")


def check_group_replication_status() -> None:
    """Check and display Group Replication status."""
    print("\n=== Group Replication Status ===\n")
    
    js_code = """
    shell.options.useWizards = false;
    try {
        var cluster = dba.getCluster();
        var status = cluster.status();
        print('Cluster: ' + status.clusterName);
        print('Status: ' + status.defaultReplicaSet.status);
        print('Primary: ' + status.defaultReplicaSet.primary);
        print('');
        print('Topology:');
        for (var member in status.defaultReplicaSet.topology) {
            var m = status.defaultReplicaSet.topology[member];
            print('  ' + member + ': ' + m.memberRole + ' (' + m.mode + ') - ' + m.status);
        }
    } catch(e) {
        print('InnoDB Cluster not configured or not reachable');
    }
    """
    
    success, output = mysqlsh_execute(js_code)
    if output:
        for line in output.split('\n'):
            if line.strip():
                print(f"  {line}")
    else:
        print("  InnoDB Cluster not configured or not reachable")


def main():
    print_header("LineairDB Cluster Status")
    
    # Load configuration
    config = load_config_if_exists()
    
    if config is None:
        print_warning("No cluster configuration found")
        print("Run 'python3 init_cluster.py <num_secondaries>' to initialize")
        return
    
    print("=== Node Status ===\n")
    
    # Check primary
    check_primary_status(config)
    
    # Check secondaries
    check_secondary_status(config)
    
    # Check Group Replication
    check_group_replication_status()
    
    print()


if __name__ == "__main__":
    main()

