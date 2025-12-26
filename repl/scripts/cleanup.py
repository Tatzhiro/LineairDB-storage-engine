#!/usr/bin/env python3
"""
Clean up LineairDB cluster resources.

This script removes:
- Benchmark databases
- InnoDB Cluster configuration
- Docker containers and volumes
- /etc/hosts entries
- Data directories (with --all flag)

Usage:
    python3 cleanup.py [--all]

Options:
    --all, -a    Remove containers, volumes, data, and cluster
    -h, --help   Show this help message
"""

import argparse
import sys
import time
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from utils import (
    ClusterConfig,
    Colors,
    CLUSTER_NAME,
    MYSQL_PASSWORD,
    MYSQL_USER,
    docker_compose_down,
    get_repl_dir,
    load_config_if_exists,
    mysql_execute,
    mysql_query,
    mysqlsh_execute,
    print_error,
    print_header,
    print_step,
    print_success,
    run_command,
    run_command_with_sudo,
    systemctl_restart,
    systemctl_start,
    wait_for_mysql,
)


def drop_benchmark_databases() -> None:
    """Drop all benchmark databases."""
    print_step("Cleaning up benchmark databases...")
    
    # Get list of benchmark databases
    query = """
    SELECT SCHEMA_NAME FROM information_schema.SCHEMATA 
    WHERE SCHEMA_NAME LIKE 'benchbase%' 
       OR SCHEMA_NAME LIKE 'bench\\_%'
       OR SCHEMA_NAME = 'gr_test'
       OR SCHEMA_NAME = 'repl_test';
    """
    
    result = mysql_query(query)
    if not result:
        print("  No benchmark databases found")
        return
    
    databases = [db.strip() for db in result.split('\n') if db.strip()]
    
    for db in databases:
        print(f"  Dropping database: {db}")
        
        # First drop tables (needed for LineairDB)
        tables_query = f"SELECT table_name FROM information_schema.tables WHERE table_schema='{db}';"
        tables_result = mysql_query(tables_query)
        
        if tables_result:
            tables = [t.strip() for t in tables_result.split('\n') if t.strip()]
            for table in tables:
                mysql_execute(f"DROP TABLE IF EXISTS `{db}`.`{table}`;")
        
        # Drop the database
        mysql_execute(f"DROP DATABASE IF EXISTS `{db}`;")


def dissolve_cluster() -> None:
    """Dissolve the InnoDB Cluster if it exists."""
    print_step("Dissolving InnoDB Cluster (if exists)...")
    
    js_code = f"""
    shell.options.useWizards = false;
    try {{
        var cluster = dba.getCluster('{CLUSTER_NAME}');
        cluster.dissolve({{force: true}});
        print('Cluster dissolved');
    }} catch(e) {{
        if (e.message.includes('standalone') || e.message.includes('GR is not active') || e.message.includes('51314')) {{
            // GR not active but metadata exists - drop the metadata schema
            print('GR not active, dropping metadata schema...');
            try {{
                dba.dropMetadataSchema({{force: true}});
                print('Metadata schema dropped');
            }} catch(e2) {{
                print('No metadata to drop');
            }}
        }} else {{
            print('No cluster to dissolve');
        }}
    }}
    """
    
    success, output = mysqlsh_execute(js_code)
    for line in output.split('\n'):
        if line.strip():
            print(f"  {line}")
    
    # Reset read_only mode on primary after dissolving cluster
    print_step("Resetting read_only mode on primary...")
    mysql_execute("SET GLOBAL super_read_only = 0; SET GLOBAL read_only = 0;")


def stop_docker_containers() -> None:
    """Stop and remove Docker containers."""
    repl_dir = get_repl_dir()
    compose_file = repl_dir / "docker-compose-secondaries.yml"
    
    if compose_file.exists():
        print_step("Stopping and removing containers...")
        docker_compose_down(compose_file)
    
    # Remove any remaining mysql-secondary containers
    print_step("Removing any remaining containers...")
    result = run_command_with_sudo([
        "docker", "ps", "-a", "--format", "{{.Names}}"
    ])
    
    if result.returncode == 0 and result.stdout:
        for container in result.stdout.split('\n'):
            if 'mysql-secondary' in container:
                run_command_with_sudo(["docker", "rm", "-f", container])
    
    # Remove networks
    print_step("Removing network...")
    run_command_with_sudo(["docker", "network", "rm", "mysql-cluster-net"])
    run_command_with_sudo(["docker", "network", "rm", "mysql-cluster_mysql-cluster-net"])


def cleanup_etc_hosts() -> None:
    """Clean up /etc/hosts entries."""
    print_step("Cleaning up /etc/hosts...")
    
    # Read current /etc/hosts
    try:
        with open('/etc/hosts', 'r') as f:
            lines = f.readlines()
    except Exception:
        print_error("Could not read /etc/hosts")
        return
    
    # Filter out our entries
    new_lines = [
        line for line in lines
        if 'mysql-secondary' not in line and '# MySQL Cluster' not in line
    ]
    
    if len(new_lines) != len(lines):
        # Write back using sudo
        content = ''.join(new_lines)
        run_command(["sudo", "bash", "-c", f"echo '{content}' > /etc/hosts"])


def remove_volumes_and_data() -> None:
    """Remove Docker volumes and data directories."""
    repl_dir = get_repl_dir()
    
    print_step("Removing volumes...")
    result = run_command_with_sudo(["docker", "volume", "ls", "-q"])
    if result.returncode == 0 and result.stdout:
        for vol in result.stdout.split('\n'):
            if 'secondary' in vol or 'mysql-cluster' in vol:
                run_command_with_sudo(["docker", "volume", "rm", vol])
    
    print_step("Removing configuration...")
    config_dir = repl_dir / "config"
    if config_dir.exists():
        for f in config_dir.iterdir():
            try:
                f.unlink()
            except Exception:
                pass
    
    print_step("Removing data directories...")
    data_dir = repl_dir / "data"
    if data_dir.exists():
        run_command_with_sudo(["rm", "-rf", str(data_dir)])
    
    # Clean up LineairDB internal logs
    print_step("Removing LineairDB logs...")
    run_command_with_sudo(["rm", "-rf", "/tmp/lineairdb_logs"])
    run_command_with_sudo(["rm", "-rf", "/var/lib/mysql/lineairdb_logs"])


def restart_mysql_and_wait() -> None:
    """Restart MySQL and wait for it to be ready."""
    repl_dir = get_repl_dir()
    marker_file = repl_dir / "config" / ".binbench_mode"
    
    # Check if we were in binbench mode
    if marker_file.exists():
        print_step("Stopping local build MySQL (binbench mode)...")
        run_command_with_sudo(["pkill", "-9", "mysqld"])
        time.sleep(2)
        marker_file.unlink()
        print_step("Starting system MySQL...")
        systemctl_start("mysql")
    else:
        print_step("Restarting MySQL...")
        systemctl_restart("mysql")
    
    print_step("Waiting for MySQL to be ready...")
    if wait_for_mysql(timeout=30):
        print_success("MySQL is ready")
    else:
        print_error("MySQL did not become ready within timeout")
    
    # Reset read_only mode again after restart
    print_step("Ensuring read_only is disabled...")
    mysql_execute("SET GLOBAL super_read_only = OFF; SET GLOBAL read_only = OFF;")


def main():
    parser = argparse.ArgumentParser(
        description="Clean up LineairDB cluster resources"
    )
    parser.add_argument(
        '--all', '-a',
        action='store_true',
        help="Remove containers, volumes, data, and cluster"
    )
    args = parser.parse_args()
    
    print_header("LineairDB Cluster Cleanup")
    
    # Step 0: Drop benchmark databases
    drop_benchmark_databases()
    
    # Step 1: Dissolve InnoDB Cluster (if --all)
    if args.all:
        dissolve_cluster()
    
    # Step 2: Stop Docker Containers
    stop_docker_containers()
    
    # Step 3: Clean up /etc/hosts
    cleanup_etc_hosts()
    
    # Step 4: Remove volumes, config, data (if --all)
    if args.all:
        remove_volumes_and_data()
        restart_mysql_and_wait()
    
    print()
    print_header("Cleanup Complete!")


if __name__ == "__main__":
    main()

