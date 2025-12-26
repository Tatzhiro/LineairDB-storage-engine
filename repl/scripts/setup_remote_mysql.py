#!/usr/bin/env python3
"""
Setup MySQL on remote hosts for LineairDB Group Replication.

This script connects to remote hosts via SSH and installs/configures MySQL
for use as secondary nodes in the cluster.

Usage:
    python3 setup_remote_mysql.py <host1> [host2] [host3] ...

Host format: host[:ssh_user]
    192.168.1.10         -> ssh_user=root
    192.168.1.10:ubuntu  -> ssh_user=ubuntu

Prerequisites:
    - SSH key-based authentication to remote hosts
    - sudo access on remote hosts

Examples:
    python3 setup_remote_mysql.py 192.168.1.10 192.168.1.11 192.168.1.12
    python3 setup_remote_mysql.py 192.168.1.10:ubuntu 192.168.1.11:ubuntu
"""

import argparse
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from utils import (
    MYSQL_PASSWORD,
    ping_host,
    print_error,
    print_header,
    print_success,
    print_warning,
    run_command,
    ssh_execute,
    ssh_is_reachable,
)


# MySQL setup script to run on remote host
SETUP_SCRIPT_TEMPLATE = '''
#!/bin/bash
set -e

MYSQL_PASSWORD="{mysql_password}"

echo "=== Installing MySQL Server ==="

# Detect OS
if [ -f /etc/debian_version ]; then
    export DEBIAN_FRONTEND=noninteractive
    sudo apt-get update
    sudo apt-get install -y mysql-server mysql-client
elif [ -f /etc/redhat-release ]; then
    sudo yum install -y mysql-server mysql
    sudo systemctl enable mysqld
else
    echo "Unsupported OS"
    exit 1
fi

echo "=== Configuring MySQL ==="

# Configure bind-address for remote connections
if [ -f /etc/mysql/mysql.conf.d/mysqld.cnf ]; then
    sudo sed -i "s/^bind-address.*/bind-address = 0.0.0.0/" /etc/mysql/mysql.conf.d/mysqld.cnf
    sudo sed -i "s/^mysqlx-bind-address.*/mysqlx-bind-address = 0.0.0.0/" /etc/mysql/mysql.conf.d/mysqld.cnf
elif [ -f /etc/my.cnf ]; then
    if ! grep -q "bind-address" /etc/my.cnf; then
        echo "bind-address = 0.0.0.0" | sudo tee -a /etc/my.cnf
    else
        sudo sed -i "s/^bind-address.*/bind-address = 0.0.0.0/" /etc/my.cnf
    fi
fi

# Get the server's own IP address for report_host
MY_IP=$(hostname -I | awk "{{print \\$1}}")

# Add Group Replication configuration
MYSQL_CNF="/etc/mysql/mysql.conf.d/mysqld.cnf"
if [ ! -f "$MYSQL_CNF" ]; then
    MYSQL_CNF="/etc/my.cnf"
fi

# Remove old GR settings if present (to allow reconfiguration)
sudo sed -i "/# Group Replication settings/,/log-slave-updates/d" "$MYSQL_CNF" 2>/dev/null || true
sudo sed -i "/report_host/d" "$MYSQL_CNF" 2>/dev/null || true

# Add fresh Group Replication configuration
SERVER_ID=$((RANDOM % 900 + 100))
sudo tee -a "$MYSQL_CNF" > /dev/null << EOF

# Group Replication settings
server-id = $SERVER_ID
report_host = $MY_IP
log-bin = mysql-bin
gtid-mode = ON
enforce-gtid-consistency = ON
binlog-format = ROW
relay-log = mysql-relay-bin
log-slave-updates = ON
EOF
echo "  report_host set to: $MY_IP"

# Restart MySQL
sudo systemctl restart mysql || sudo systemctl restart mysqld

# Wait for MySQL to be ready
for i in {{1..30}}; do
    if sudo mysqladmin ping &>/dev/null; then
        break
    fi
    sleep 1
done

echo "=== Setting up MySQL users ==="

# Setup root user with password
sudo mysql -e "ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY '$MYSQL_PASSWORD';" 2>/dev/null || true

# Create root@% for remote connections
sudo mysql -u root -p"$MYSQL_PASSWORD" -e "
CREATE USER IF NOT EXISTS 'root'@'%' IDENTIFIED BY '$MYSQL_PASSWORD';
GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION;
FLUSH PRIVILEGES;
" 2>/dev/null || true

# Open firewall if ufw is active
if command -v ufw &>/dev/null && sudo ufw status | grep -q "active"; then
    sudo ufw allow 3306/tcp
    sudo ufw allow 33060/tcp
    sudo ufw allow 33061/tcp
fi

# Open firewall if firewalld is active
if command -v firewall-cmd &>/dev/null && sudo firewall-cmd --state 2>/dev/null | grep -q "running"; then
    sudo firewall-cmd --permanent --add-port=3306/tcp
    sudo firewall-cmd --permanent --add-port=33060/tcp
    sudo firewall-cmd --permanent --add-port=33061/tcp
    sudo firewall-cmd --reload
fi

echo "=== MySQL setup complete ==="
'''


def parse_host_spec(host_spec: str) -> tuple:
    """
    Parse host specification.
    
    Format: host[:ssh_user]
    
    Returns:
        Tuple of (host, ssh_user)
    """
    parts = host_spec.split(":")
    host = parts[0]
    ssh_user = parts[1] if len(parts) > 1 else "root"
    return host, ssh_user


def setup_mysql_on_host(host: str, ssh_user: str) -> bool:
    """
    Setup MySQL on a remote host.
    
    Returns:
        True if successful
    """
    print(f"\nSetting up MySQL on {host} (user: {ssh_user})...")
    print("-" * 40)
    
    # Check network connectivity
    if not ping_host(host):
        print_error(f"Host {host} is not reachable on the network")
        return False
    
    # Check SSH connectivity
    if not ssh_is_reachable(host, user=ssh_user):
        print_error(f"Cannot SSH to {ssh_user}@{host} (ensure key-based auth is set up)")
        return False
    
    # Generate setup script
    setup_script = SETUP_SCRIPT_TEMPLATE.format(mysql_password=MYSQL_PASSWORD)
    
    # Run setup script on remote host
    print("  (You may be prompted for sudo password on the remote host)")
    
    # Use ssh -t for TTY to allow interactive sudo
    result = run_command([
        "ssh", "-t",
        "-o", "StrictHostKeyChecking=no",
        f"{ssh_user}@{host}",
        setup_script
    ], capture_output=False, timeout=300)
    
    if result.returncode == 0:
        print_success(f"MySQL setup completed on {host}")
        return True
    else:
        print_error(f"MySQL setup failed on {host}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Setup MySQL on remote hosts for LineairDB Group Replication",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Host format: host[:ssh_user]
    192.168.1.10         -> ssh_user=root
    192.168.1.10:ubuntu  -> ssh_user=ubuntu

Prerequisites:
    - SSH key-based authentication to remote hosts
    - sudo access on remote hosts

Examples:
    %(prog)s 192.168.1.10 192.168.1.11 192.168.1.12
    %(prog)s 192.168.1.10:ubuntu 192.168.1.11:ubuntu
        """
    )
    parser.add_argument(
        'hosts',
        nargs='+',
        metavar='HOST',
        help='Remote hosts to setup (format: host[:ssh_user])'
    )
    
    args = parser.parse_args()
    
    print_header("Setup MySQL on Remote Hosts")
    
    success_hosts = []
    failed_hosts = []
    
    for host_spec in args.hosts:
        host, ssh_user = parse_host_spec(host_spec)
        
        if setup_mysql_on_host(host, ssh_user):
            success_hosts.append(host)
        else:
            failed_hosts.append(f"{host} (setup failed)")
    
    print()
    print_header("Summary")
    
    if success_hosts:
        print()
        print_success(f"Successfully configured: {len(success_hosts)} host(s)")
        for h in success_hosts:
            print(f"  - {h}")
    
    if failed_hosts:
        print()
        print_error(f"Failed: {len(failed_hosts)} host(s)")
        for h in failed_hosts:
            print(f"  - {h}")
        sys.exit(1)
    
    print()
    print("Next step: Run the benchmark with these remote hosts:")
    print(f"  python3 run_all.py {len(success_hosts)} lineairdb --remote {' '.join(success_hosts)}")
    print()


if __name__ == "__main__":
    main()

