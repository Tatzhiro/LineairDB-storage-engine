#!/usr/bin/env python3
"""
Common utilities for LineairDB cluster management scripts.

This module provides shared functionality for:
- MySQL connections and queries
- Docker operations
- SSH remote execution
- Configuration management
- Console output formatting
"""

import json
import os
import re
import shutil
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


# =============================================================================
# Path Configuration
# =============================================================================

def get_script_dir() -> Path:
    """Get the directory containing this script."""
    return Path(__file__).parent.resolve()


def get_repl_dir() -> Path:
    """Get the repl directory (parent of python-scripts)."""
    return get_script_dir().parent


def get_root_dir() -> Path:
    """Get the project root directory."""
    return get_repl_dir().parent


def get_config_path() -> Path:
    """Get the path to cluster_config.json."""
    return get_repl_dir() / "config" / "cluster_config.json"


# =============================================================================
# Console Output Formatting
# =============================================================================

class Colors:
    """ANSI color codes for terminal output."""
    RED = '\033[0;31m'
    GREEN = '\033[0;32m'
    YELLOW = '\033[1;33m'
    BLUE = '\033[0;34m'
    CYAN = '\033[0;36m'
    MAGENTA = '\033[0;35m'
    BOLD = '\033[1m'
    NC = '\033[0m'  # No Color


def print_header(message: str) -> None:
    """Print a header message with decorative border."""
    print()
    print(f"{Colors.BLUE}╔════════════════════════════════════════════════════════════╗{Colors.NC}")
    print(f"{Colors.BLUE}║{Colors.NC} {message:<58} {Colors.BLUE}║{Colors.NC}")
    print(f"{Colors.BLUE}╚════════════════════════════════════════════════════════════╝{Colors.NC}")
    print()


def print_step(message: str) -> None:
    """Print a step message."""
    print(f"{Colors.YELLOW}▶ {message}{Colors.NC}")


def print_success(message: str) -> None:
    """Print a success message."""
    print(f"{Colors.GREEN}✓ {message}{Colors.NC}")


def print_error(message: str) -> None:
    """Print an error message."""
    print(f"{Colors.RED}✗ {message}{Colors.NC}")


def print_warning(message: str) -> None:
    """Print a warning message."""
    print(f"{Colors.YELLOW}⚠ {message}{Colors.NC}")


def print_info(message: str) -> None:
    """Print an info message."""
    print(f"{Colors.CYAN}ℹ {message}{Colors.NC}")


# =============================================================================
# Configuration Management
# =============================================================================

# Default MySQL credentials
MYSQL_USER = "root"
MYSQL_PASSWORD = "kamo"
CLUSTER_NAME = "lineairdb_cluster"


@dataclass
class SecondaryNode:
    """Represents a secondary node in the cluster."""
    node_id: int
    hostname: str
    node_type: str  # 'docker_container' or 'remote_host'
    host: str
    port: int
    ssh_user: str = "root"
    container_name: Optional[str] = None
    
    @classmethod
    def from_dict(cls, data: dict) -> 'SecondaryNode':
        """Create a SecondaryNode from a dictionary."""
        return cls(
            node_id=data['node_id'],
            hostname=data['hostname'],
            node_type=data['node_type'],
            host=data['host'],
            port=data['port'],
            ssh_user=data.get('ssh_user', 'root'),
            container_name=data.get('container_name', data.get('hostname')),
        )


@dataclass
class ClusterConfig:
    """Represents the cluster configuration."""
    cluster_name: str
    mysql_root_password: str
    primary_host: str
    primary_port: int
    secondaries: List[SecondaryNode]
    num_docker_containers: int
    num_remote_hosts: int
    
    @classmethod
    def load(cls, config_path: Optional[Path] = None) -> 'ClusterConfig':
        """Load cluster configuration from JSON file."""
        if config_path is None:
            config_path = get_config_path()
        
        if not config_path.exists():
            raise FileNotFoundError(f"Cluster configuration not found: {config_path}")
        
        with open(config_path) as f:
            data = json.load(f)
        
        primary = data.get('primary', {})
        secondaries = [
            SecondaryNode.from_dict(s) 
            for s in data.get('secondaries', [])
        ]
        
        return cls(
            cluster_name=data.get('cluster_name', CLUSTER_NAME),
            mysql_root_password=data.get('mysql_root_password', MYSQL_PASSWORD),
            primary_host=primary.get('host', '127.0.0.1'),
            primary_port=primary.get('port', 3306),
            secondaries=secondaries,
            num_docker_containers=data.get('num_docker_containers', 0),
            num_remote_hosts=data.get('num_remote_hosts', 0),
        )
    
    def get_secondary(self, index: int) -> Optional[SecondaryNode]:
        """Get secondary node by index (0-based)."""
        if 0 <= index < len(self.secondaries):
            return self.secondaries[index]
        return None


def load_config_if_exists() -> Optional[ClusterConfig]:
    """Load cluster config if it exists, return None otherwise."""
    try:
        return ClusterConfig.load()
    except FileNotFoundError:
        return None


# =============================================================================
# Command Execution
# =============================================================================

def run_command(
    cmd: List[str],
    capture_output: bool = True,
    check: bool = False,
    timeout: Optional[int] = None,
    cwd: Optional[Path] = None,
    env: Optional[Dict[str, str]] = None,
) -> subprocess.CompletedProcess:
    """
    Run a command and return the result.
    
    Args:
        cmd: Command and arguments as a list
        capture_output: Whether to capture stdout/stderr
        check: Whether to raise exception on non-zero exit
        timeout: Command timeout in seconds
        cwd: Working directory for the command
        env: Environment variables to set
        
    Returns:
        CompletedProcess object
    """
    try:
        result = subprocess.run(
            cmd,
            capture_output=capture_output,
            text=True,
            check=check,
            timeout=timeout,
            cwd=cwd,
            env={**os.environ, **(env or {})},
        )
        return result
    except subprocess.TimeoutExpired:
        return subprocess.CompletedProcess(cmd, 124, "", "Command timed out")
    except FileNotFoundError:
        return subprocess.CompletedProcess(cmd, 127, "", f"Command not found: {cmd[0]}")


def run_command_with_sudo(
    cmd: List[str],
    **kwargs
) -> subprocess.CompletedProcess:
    """Run a command with sudo prefix."""
    return run_command(["sudo"] + cmd, **kwargs)


# =============================================================================
# MySQL Operations
# =============================================================================

def mysql_execute(
    query: str,
    host: str = "127.0.0.1",
    port: int = 3306,
    user: str = MYSQL_USER,
    password: str = MYSQL_PASSWORD,
    database: Optional[str] = None,
    return_output: bool = False,
    silent: bool = True,
) -> Tuple[bool, str]:
    """
    Execute a MySQL query.
    
    Args:
        query: SQL query to execute
        host: MySQL host
        port: MySQL port
        user: MySQL user
        password: MySQL password
        database: Database name (optional)
        return_output: Whether to return query output
        silent: Whether to suppress warnings
        
    Returns:
        Tuple of (success, output)
    """
    cmd = [
        "mysql",
        f"-h{host}",
        f"-P{port}",
        f"-u{user}",
    ]
    # Only add password flag if password is not empty
    # Empty -p causes interactive prompt!
    if password:
        cmd.append(f"-p{password}")
    else:
        cmd.append("--skip-password")
    
    if return_output:
        cmd.append("-N")  # No column headers
    
    if database:
        cmd.append(database)
    
    cmd.extend(["-e", query])
    
    result = run_command(cmd, timeout=30)
    
    output = result.stdout.strip() if result.stdout else ""
    if result.returncode != 0 and not silent:
        print_error(f"MySQL error: {result.stderr}")
    
    return result.returncode == 0, output


def mysql_query(
    query: str,
    host: str = "127.0.0.1",
    port: int = 3306,
    **kwargs
) -> Optional[str]:
    """
    Execute a MySQL query and return the result.
    
    Returns:
        Query result as string, or None on error
    """
    success, output = mysql_execute(
        query, host=host, port=port, return_output=True, **kwargs
    )
    return output if success else None


def mysql_is_running(
    host: str = "127.0.0.1",
    port: int = 3306,
    user: str = MYSQL_USER,
    password: str = MYSQL_PASSWORD,
) -> bool:
    """Check if MySQL is running and accessible."""
    success, _ = mysql_execute(
        "SELECT 1",
        host=host,
        port=port,
        user=user,
        password=password,
    )
    return success


def wait_for_mysql(
    host: str = "127.0.0.1",
    port: int = 3306,
    user: str = MYSQL_USER,
    password: str = MYSQL_PASSWORD,
    timeout: int = 30,
    interval: float = 1.0,
) -> bool:
    """
    Wait for MySQL to become available.
    
    Args:
        host: MySQL host
        port: MySQL port
        user: MySQL user
        password: MySQL password
        timeout: Maximum time to wait in seconds
        interval: Check interval in seconds
        
    Returns:
        True if MySQL became available, False if timeout
    """
    start_time = time.time()
    while time.time() - start_time < timeout:
        if mysql_is_running(host, port, user, password):
            return True
        time.sleep(interval)
    return False


# =============================================================================
# MySQL Shell Operations
# =============================================================================

def mysqlsh_execute(
    js_code: str,
    host: str = "127.0.0.1",
    port: int = 3306,
    user: str = MYSQL_USER,
    password: str = MYSQL_PASSWORD,
    capture_output: bool = True,
) -> Tuple[bool, str]:
    """
    Execute JavaScript code in MySQL Shell.
    
    Args:
        js_code: JavaScript code to execute
        host: MySQL host
        port: MySQL port
        user: MySQL user
        password: MySQL password
        capture_output: Whether to capture output
        
    Returns:
        Tuple of (success, output)
    """
    uri = f"{user}:{password}@{host}:{port}"
    cmd = [
        "mysqlsh",
        "--uri", uri,
        "--js",
        "-e", js_code,
    ]
    
    result = run_command(cmd, capture_output=capture_output, timeout=60)
    
    # Filter out warnings from output
    output = result.stdout if result.stdout else ""
    output = "\n".join(
        line for line in output.split("\n")
        if "WARNING" not in line
    )
    
    return result.returncode == 0, output.strip()


def get_cluster_status() -> Optional[dict]:
    """
    Get InnoDB Cluster status.
    
    Returns:
        Cluster status as dict, or None if no cluster
    """
    js_code = """
    shell.options.useWizards = false;
    try {
        var cluster = dba.getCluster();
        var status = cluster.status();
        print(JSON.stringify(status));
    } catch(e) {
        print('null');
    }
    """
    
    success, output = mysqlsh_execute(js_code)
    if success and output and output != "null":
        try:
            return json.loads(output)
        except json.JSONDecodeError:
            pass
    return None


# =============================================================================
# Docker Operations
# =============================================================================

def docker_is_running(container_name: str) -> bool:
    """Check if a Docker container is running."""
    result = run_command_with_sudo([
        "docker", "ps", "--format", "{{.Names}}"
    ])
    if result.returncode == 0 and result.stdout:
        return container_name in result.stdout.split('\n')
    return False


def docker_get_health(container_name: str) -> str:
    """Get health status of a Docker container."""
    result = run_command_with_sudo([
        "docker", "inspect",
        "--format", "{{.State.Health.Status}}",
        container_name
    ])
    return result.stdout.strip() if result.returncode == 0 else "unknown"


def docker_get_ip(container_name: str) -> Optional[str]:
    """Get IP address of a Docker container."""
    result = run_command_with_sudo([
        "docker", "inspect",
        "-f", "{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
        container_name
    ])
    if result.returncode == 0 and result.stdout.strip():
        return result.stdout.strip()
    return None


def docker_exec(
    container_name: str,
    cmd: List[str],
    **kwargs
) -> subprocess.CompletedProcess:
    """Execute a command in a Docker container."""
    return run_command_with_sudo(
        ["docker", "exec", container_name] + cmd,
        **kwargs
    )


def docker_cp(
    source: str,
    dest: str,
) -> bool:
    """Copy file to/from Docker container."""
    result = run_command_with_sudo(["docker", "cp", source, dest])
    return result.returncode == 0


def docker_compose_up(compose_file: Path, detach: bool = True) -> bool:
    """Start containers with docker-compose."""
    cmd = ["docker-compose", "-f", str(compose_file), "up"]
    if detach:
        cmd.append("-d")
    result = run_command_with_sudo(cmd, cwd=compose_file.parent)
    return result.returncode == 0


def docker_compose_down(compose_file: Path) -> bool:
    """Stop containers with docker-compose."""
    result = run_command_with_sudo(
        ["docker-compose", "-f", str(compose_file), "down"],
        cwd=compose_file.parent
    )
    return result.returncode == 0


# =============================================================================
# SSH Operations
# =============================================================================

def ssh_execute(
    host: str,
    command: str,
    user: str = "root",
    timeout: int = 30,
) -> Tuple[bool, str]:
    """
    Execute a command on a remote host via SSH.
    
    Args:
        host: Remote host
        command: Command to execute
        user: SSH user
        timeout: Timeout in seconds
        
    Returns:
        Tuple of (success, output)
    """
    cmd = [
        "ssh",
        "-o", "BatchMode=yes",
        "-o", f"ConnectTimeout={min(timeout, 10)}",
        "-o", "StrictHostKeyChecking=no",
        f"{user}@{host}",
        command
    ]
    
    result = run_command(cmd, timeout=timeout)
    return result.returncode == 0, result.stdout.strip() if result.stdout else ""


def ssh_is_reachable(
    host: str,
    user: str = "root",
    timeout: int = 5,
) -> bool:
    """Check if SSH is available on remote host."""
    success, _ = ssh_execute(host, "echo ok", user=user, timeout=timeout)
    return success


def scp_copy(
    source: str,
    dest: str,
    timeout: int = 30,
) -> bool:
    """
    Copy file via SCP.
    
    Args:
        source: Source path (local or remote user@host:path)
        dest: Destination path
        timeout: Timeout in seconds
        
    Returns:
        True if successful
    """
    result = run_command([
        "scp",
        "-o", "StrictHostKeyChecking=no",
        source, dest
    ], timeout=timeout)
    return result.returncode == 0


# =============================================================================
# Network Operations
# =============================================================================

def ping_host(host: str, timeout: int = 2) -> bool:
    """Check if a host is reachable via ping."""
    result = run_command([
        "ping", "-c", "1", "-W", str(timeout), host
    ])
    return result.returncode == 0


def get_local_ip() -> str:
    """Get the local machine's IP address."""
    try:
        result = run_command(["hostname", "-I"])
        if result.returncode == 0 and result.stdout:
            return result.stdout.split()[0]
    except Exception:
        pass
    return "127.0.0.1"


def get_hostname() -> str:
    """Get the local machine's hostname."""
    return socket.gethostname()


# =============================================================================
# Systemctl Operations
# =============================================================================

def systemctl_is_active(service: str) -> bool:
    """Check if a systemctl service is active."""
    result = run_command(["systemctl", "is-active", "--quiet", service])
    return result.returncode == 0


def systemctl_start(service: str) -> bool:
    """Start a systemctl service."""
    result = run_command_with_sudo(["systemctl", "start", service])
    return result.returncode == 0


def systemctl_stop(service: str) -> bool:
    """Stop a systemctl service."""
    result = run_command_with_sudo(["systemctl", "stop", service])
    return result.returncode == 0


def systemctl_restart(service: str) -> bool:
    """Restart a systemctl service."""
    result = run_command_with_sudo(["systemctl", "restart", service])
    return result.returncode == 0


# =============================================================================
# File Operations
# =============================================================================

def read_file(path: Path) -> Optional[str]:
    """Read file contents."""
    try:
        return path.read_text()
    except Exception:
        return None


def write_file(path: Path, content: str) -> bool:
    """Write content to file."""
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
        return True
    except Exception:
        return False


def sudo_write_file(path: Path, content: str) -> bool:
    """Write content to file with sudo (for system files)."""
    result = run_command_with_sudo(
        ["tee", str(path)],
        capture_output=True
    )
    if result.returncode != 0:
        # Try via echo
        result = run_command(
            ["sudo", "bash", "-c", f"echo '{content}' > {path}"]
        )
    return result.returncode == 0


# =============================================================================
# Plugin Operations
# =============================================================================

def get_plugin_path(build_type: str = "release") -> Optional[Path]:
    """
    Get the path to the LineairDB plugin.
    
    Args:
        build_type: 'release' or 'debug'
        
    Returns:
        Path to plugin .so file, or None if not found
    """
    root_dir = get_root_dir()
    
    if build_type == "release":
        plugin_path = root_dir / "release" / "plugin_output_directory" / "ha_lineairdb_storage_engine.so"
    else:
        plugin_path = root_dir / "build" / "plugin_output_directory" / "ha_lineairdb_storage_engine.so"
    
    return plugin_path if plugin_path.exists() else None


def get_fence_value_from_source() -> Optional[str]:
    """Read the current FENCE value from ha_lineairdb.cc source file."""
    source_file = get_root_dir() / "ha_lineairdb.cc"
    if not source_file.exists():
        return None
    
    content = read_file(source_file)
    if content:
        match = re.search(r'#define FENCE (true|false)', content)
        if match:
            return match.group(1)
    return None


def set_fence_value_in_source(value: str) -> bool:
    """Set the FENCE value in ha_lineairdb.cc source file."""
    source_file = get_root_dir() / "ha_lineairdb.cc"
    if not source_file.exists():
        return False
    
    content = read_file(source_file)
    if content:
        new_content = re.sub(
            r'#define FENCE (true|false)',
            f'#define FENCE {value}',
            content
        )
        return write_file(source_file, new_content)
    return False


# =============================================================================
# Argument Parsing Helpers
# =============================================================================

def parse_remote_host(host_spec: str) -> Tuple[str, int, str]:
    """
    Parse a remote host specification.
    
    Format: host[:port[:user]]
    
    Returns:
        Tuple of (host, port, ssh_user)
    """
    parts = host_spec.split(":")
    host = parts[0]
    port = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 3306
    ssh_user = parts[2] if len(parts) > 2 else "root"
    
    # Handle case where port is missing but user is present
    if len(parts) == 2 and not parts[1].isdigit():
        port = 3306
        ssh_user = parts[1]
    
    return host, port, ssh_user


# =============================================================================
# Engine Helpers
# =============================================================================

VALID_ENGINES = ["lineairdb", "fence", "innodb"]


def is_lineairdb_engine(engine: str) -> bool:
    """Check if engine is a LineairDB variant."""
    return engine in ("lineairdb", "fence")


def get_engine_description(engine: str) -> str:
    """Get human-readable description of an engine."""
    descriptions = {
        "lineairdb": "LineairDB (FENCE=off, async commits)",
        "fence": "LineairDB-Fence (FENCE=on, sync commits)",
        "innodb": "InnoDB (MySQL default)",
    }
    return descriptions.get(engine, engine)


def get_mysql_engine_name(engine: str) -> str:
    """Get the MySQL engine name for use in SQL."""
    if engine in ("lineairdb", "fence"):
        return "lineairdb"
    return engine

