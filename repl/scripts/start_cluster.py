#!/usr/bin/env python3
"""
Start the MySQL InnoDB Cluster for LineairDB Replication.

This script:
1. Starts primary node (local MySQL or local build MySQL with --binbench)
2. Starts secondary nodes (Docker containers OR remote hosts)
3. Installs LineairDB plugin on ALL nodes
4. Sets up InnoDB Cluster with Group Replication

Usage:
    python3 start_cluster.py [--no-replication] [--no-plugin] [--binbench]

Options:
    --no-replication    Start without setting up InnoDB Cluster
    --no-plugin         Don't install LineairDB plugin
    --binbench          Use local build MySQL (build/bin/mysqld) as primary
"""

import argparse
import json
import os
import subprocess
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
    docker_compose_up,
    docker_cp,
    docker_exec,
    docker_get_health,
    docker_get_ip,
    docker_is_running,
    get_local_ip,
    get_hostname,
    get_plugin_path,
    get_repl_dir,
    get_root_dir,
    load_config_if_exists,
    mysql_execute,
    mysql_is_running,
    mysql_query,
    mysqlsh_execute,
    ping_host,
    print_error,
    print_header,
    print_info,
    print_step,
    print_success,
    print_warning,
    run_command,
    run_command_with_sudo,
    scp_copy,
    ssh_execute,
    systemctl_is_active,
    systemctl_restart,
    systemctl_start,
    wait_for_mysql,
    write_file,
)


def check_docker_image() -> bool:
    """Check if Docker image exists."""
    result = run_command_with_sudo(["docker", "images"])
    return "mysql-lineairdb-ubuntu" in (result.stdout or "")


def build_docker_image() -> None:
    """Build the Docker image if needed."""
    docker_dir = get_repl_dir() / "docker"
    build_script = docker_dir / "build-image.sh"
    if build_script.exists():
        run_command(["bash", str(build_script)], capture_output=False)


def start_binbench_primary(primary_ip: str) -> bool:
    """
    Start local build MySQL as primary for binbench mode.
    
    Uses my.cnf from project root for configuration.
    
    Returns:
        True if successful
    """
    root_dir = get_root_dir()
    build_dir = root_dir / "build"
    mysql_bin = build_dir / "bin" / "mysqld"
    data_dir = build_dir / "data"
    cnf_file = root_dir / "my.cnf"
    log_file = build_dir / "mysqld.log"
    
    print_step("Starting local build MySQL as primary (binbench mode)...")
    
    # Validate paths
    if not mysql_bin.exists():
        print_error(f"Local MySQL binary not found: {mysql_bin}")
        print("binbench requires MySQL built from source in the build/ directory.")
        print("To build MySQL:")
        print("  mkdir -p build && cd build")
        print("  cmake ../third_party/mysql-server -DCMAKE_BUILD_TYPE=Debug -G Ninja ...")
        print("  ninja mysqld")
        return False
    
    if not cnf_file.exists():
        print_error(f"my.cnf not found: {cnf_file}")
        print("Please create my.cnf in project root with basedir and datadir set")
        return False
    
    print(f"  Using config: {cnf_file}")
    
    # Stop system MySQL and any running mysqld
    print("  Stopping system MySQL and any running mysqld...")
    run_command_with_sudo(["systemctl", "stop", "mysql"])
    run_command_with_sudo(["pkill", "-9", "mysqld"])
    time.sleep(2)
    
    # Clean up any lock files that might cause issues
    for lock_file in ["/tmp/mysqlx.sock.lock", "/tmp/mysql.sock.lock"]:
        if Path(lock_file).exists():
            run_command_with_sudo(["rm", "-f", lock_file])
    
    # Always reinitialize for binbench mode to avoid redo log corruption
    print("  Cleaning up data directory (fresh start)...")
    if data_dir.exists():
        run_command_with_sudo(["rm", "-rf", str(data_dir)])
    data_dir.mkdir(parents=True, exist_ok=True)
    run_command_with_sudo(["chmod", "755", str(data_dir)])
    
    # Also clean up log file for fresh start
    if log_file.exists():
        log_file.unlink()
    
    print("  Initializing MySQL data directory...")
    init_result = run_command(
        [
            str(mysql_bin),
            f"--defaults-file={cnf_file}",
            "--initialize-insecure",
            f"--log-error={log_file}",
        ],
        capture_output=True,
    )
    if init_result.returncode != 0:
        print_error(f"Failed to initialize MySQL")
        if log_file.exists():
            print(f"  Log file ({log_file}):")
            try:
                log_content = log_file.read_text()
                for line in log_content.strip().split('\n')[-20:]:
                    print(f"    {line}")
            except Exception as e:
                print(f"    Could not read log: {e}")
        if init_result.stderr:
            print(f"  stderr: {init_result.stderr}")
        return False
    
    # Start local build MySQL using my.cnf
    # Preload jemalloc to avoid TLS block allocation error when loading plugins
    print("  Starting mysqld (with LD_PRELOAD for jemalloc)...")
    
    # Find jemalloc library
    jemalloc_paths = [
        "/lib/x86_64-linux-gnu/libjemalloc.so.2",
        "/usr/lib/x86_64-linux-gnu/libjemalloc.so.2",
        "/usr/lib/libjemalloc.so.2",
    ]
    jemalloc_lib = None
    for path in jemalloc_paths:
        if Path(path).exists():
            jemalloc_lib = path
            break
    
    mysqld_args = [
        str(mysql_bin),
        f"--defaults-file={cnf_file}",
        f"--log-error={log_file}",
        f"--lc-messages-dir={build_dir}/share",
        f"--socket={build_dir}/mysql.sock",
        f"--pid-file={build_dir}/mysqld.pid",
        "--port=3306",
        "--bind-address=0.0.0.0",
        f"--report-host={primary_ip}",
        "--daemonize",
    ]
    
    # Set up environment with LD_PRELOAD if jemalloc found
    env = os.environ.copy()
    if jemalloc_lib:
        print(f"    Preloading: {jemalloc_lib}")
        env["LD_PRELOAD"] = jemalloc_lib
    
    result = run_command(mysqld_args, capture_output=True, env=env)
    if result.returncode != 0:
        print_error(f"Failed to start local MySQL: {result.stderr}")
        if log_file.exists():
            print(f"  Log file ({log_file}):")
            try:
                log_content = log_file.read_text()
                for line in log_content.strip().split('\n')[-10:]:
                    print(f"    {line}")
            except Exception:
                pass
        return False
    
    time.sleep(3)
    
    # Wait for MySQL to be ready (use empty password since --initialize-insecure)
    print("  Waiting for MySQL to be ready...")
    for _ in range(30):
        # After --initialize-insecure, root has no password initially
        if mysql_is_running("127.0.0.1", 3306, password=""):
            break
        # Also try with the expected password (in case data dir was reused)
        if mysql_is_running("127.0.0.1", 3306, password=MYSQL_PASSWORD):
            break
        time.sleep(1)
    else:
        print_error("MySQL did not start in time")
        return False
    
    # Create root@'%' user and set password
    # Try with empty password first (fresh init), then with set password (reused data)
    success, _ = mysql_execute(f"""
        CREATE USER IF NOT EXISTS 'root'@'%' IDENTIFIED BY '{MYSQL_PASSWORD}';
        GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION;
        ALTER USER 'root'@'localhost' IDENTIFIED BY '{MYSQL_PASSWORD}';
        FLUSH PRIVILEGES;
    """, password="")
    if not success:
        # Data dir was reused and password already set
        mysql_execute(f"""
            CREATE USER IF NOT EXISTS 'root'@'%' IDENTIFIED BY '{MYSQL_PASSWORD}';
            GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION;
            FLUSH PRIVILEGES;
        """, password=MYSQL_PASSWORD)
    
    # Write marker file so cleanup knows to stop local build MySQL
    marker_file = get_repl_dir() / "config" / ".binbench_mode"
    marker_file.write_text("1")
    
    print_success("Local build MySQL started as primary (binbench mode)")
    return True


def configure_primary_mysql(primary_ip: str, binbench: bool = False) -> bool:
    """
    Configure primary MySQL for cluster.
    
    Args:
        primary_ip: IP address of the primary node
        binbench: If True, use local build MySQL instead of system MySQL
        
    Returns:
        True if successful
    """
    if binbench:
        return start_binbench_primary(primary_ip)
    
    print_step("Configuring primary MySQL...")
    
    # Check if MySQL is running
    if not systemctl_is_active("mysql"):
        print("  Starting MySQL...")
        systemctl_start("mysql")
        time.sleep(3)
    
    # Ensure read_only mode is disabled
    mysql_execute("SET GLOBAL super_read_only = OFF; SET GLOBAL read_only = OFF;")
    
    # Check bind-address
    bind_addr = mysql_query("SELECT @@bind_address;")
    if bind_addr == "127.0.0.1":
        print("  Updating bind-address to 0.0.0.0...")
        run_command_with_sudo([
            "sed", "-i",
            "s/bind-address.*=.*127.0.0.1/bind-address = 0.0.0.0/",
            "/etc/mysql/mysql.conf.d/mysqld.cnf"
        ])
        run_command_with_sudo([
            "sed", "-i",
            "s/mysqlx-bind-address.*=.*127.0.0.1/mysqlx-bind-address = 0.0.0.0/",
            "/etc/mysql/mysql.conf.d/mysqld.cnf"
        ])
        systemctl_restart("mysql")
        time.sleep(3)
    
    # Create root@'%' user
    mysql_execute(f"""
        CREATE USER IF NOT EXISTS 'root'@'%' IDENTIFIED BY '{MYSQL_PASSWORD}';
        GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION;
        FLUSH PRIVILEGES;
    """)
    
    print_success("Primary MySQL configured")
    return True


def generate_docker_compose(config: ClusterConfig) -> Path:
    """Generate docker-compose file from configuration."""
    repl_dir = get_repl_dir()
    config_path = repl_dir / "config" / "cluster_config.json"
    output_path = repl_dir / "docker-compose-secondaries.yml"
    
    # Import and use the generate_compose module (in same directory)
    from generate_compose import generate_docker_compose as gen_compose, generate_secondary_configs
    
    # Generate configs
    config_dir = str(config_path.parent)
    generate_secondary_configs(str(config_path), config_dir)
    gen_compose(str(config_path), str(output_path), str(repl_dir / "data"))
    
    return output_path


def start_docker_containers(config: ClusterConfig) -> None:
    """Start Docker containers for secondary nodes."""
    repl_dir = get_repl_dir()
    
    # Generate docker-compose file
    compose_file = generate_docker_compose(config)
    
    # Create data directories
    data_dir = repl_dir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    
    for sec in config.secondaries:
        if sec.node_type == "docker_container":
            container_num = sec.container_name.split("-")[-1]
            (data_dir / f"secondary{container_num}").mkdir(exist_ok=True)
    
    # Start with docker-compose
    if compose_file.exists():
        docker_compose_up(compose_file)
        
        # Wait for containers to be healthy
        print("  Waiting for Docker containers to be healthy...")
        for sec in config.secondaries:
            if sec.node_type == "docker_container":
                print(f"    {sec.container_name}: ", end="", flush=True)
                for _ in range(60):
                    health = docker_get_health(sec.container_name)
                    if health == "healthy":
                        print(f"{Colors.GREEN}✓ healthy{Colors.NC}")
                        break
                    print(".", end="", flush=True)
                    time.sleep(2)
                else:
                    print(f" (current: {health})")


def verify_remote_hosts(config: ClusterConfig) -> bool:
    """
    Verify remote hosts are accessible and properly configured.
    
    Returns:
        True if all hosts are accessible
    """
    network_unreachable = []
    mysql_unreachable = []
    config_issues = []
    
    for sec in config.secondaries:
        if sec.node_type == "remote_host":
            print(f"    {sec.host}: ", end="", flush=True)
            
            # Check network
            if not ping_host(sec.host):
                print(f"{Colors.RED}✗ Network unreachable{Colors.NC}")
                network_unreachable.append(sec.host)
                continue
            print("network ✓, ", end="", flush=True)
            
            # Check MySQL
            if mysql_is_running(sec.host, sec.port):
                print(f"{Colors.GREEN}MySQL ✓{Colors.NC}", end="")
            else:
                print(f"{Colors.RED}MySQL ✗ (port {sec.port}){Colors.NC}")
                mysql_unreachable.append(f"{sec.host}:{sec.port}")
                continue
            
            # Check report_host configuration (critical for Group Replication)
            report_host = mysql_query(
                "SELECT @@report_host;",
                host=sec.host, port=sec.port
            )
            if report_host and report_host == sec.host:
                print(f", report_host ✓")
            elif report_host:
                print(f"{Colors.YELLOW}, report_host={report_host} (should be {sec.host}){Colors.NC}")
                config_issues.append(f"{sec.host}: report_host is '{report_host}', should be '{sec.host}'")
            else:
                print(f"{Colors.YELLOW}, report_host not set{Colors.NC}")
                config_issues.append(f"{sec.host}: report_host not set (should be '{sec.host}')")
            
            # Check SSH access (for plugin installation)
            from utils import ssh_is_reachable
            if ssh_is_reachable(sec.host, user=sec.ssh_user, timeout=5):
                print(f"      SSH ({sec.ssh_user}@{sec.host}) ✓")
            else:
                print(f"      {Colors.YELLOW}SSH ({sec.ssh_user}@{sec.host}) ✗ - plugin copy will fail{Colors.NC}")
    
    if network_unreachable:
        print()
        print_error("The following hosts are not reachable on the network:")
        for h in network_unreachable:
            print(f"  - {h}")
        return False
    
    if mysql_unreachable:
        print()
        print_error("The following hosts have MySQL not accessible:")
        for h in mysql_unreachable:
            print(f"  - {h}")
        return False
    
    if config_issues:
        print()
        print_warning("Configuration issues detected (may cause Group Replication to fail):")
        for issue in config_issues:
            print(f"  - {issue}")
        print()
        print("  To fix report_host on the remote MySQL server:")
        print("    1. Add to /etc/mysql/mysql.conf.d/mysqld.cnf:")
        print("       report_host = <REMOTE_IP>")
        print("    2. Restart MySQL: sudo systemctl restart mysql")
        print()
    
    return True


def setup_network_resolution(config: ClusterConfig, primary_ip: str, primary_hostname: str) -> None:
    """Set up network resolution for containers."""
    print_step("Setting up network resolution...")
    
    # Remove old entries from /etc/hosts
    try:
        with open('/etc/hosts', 'r') as f:
            lines = f.readlines()
        
        new_lines = [
            line for line in lines
            if 'mysql-secondary' not in line and '# MySQL Cluster' not in line
        ]
    except Exception:
        new_lines = []
    
    # Add container hostnames
    docker_entries = []
    for sec in config.secondaries:
        if sec.node_type == "docker_container":
            container_ip = docker_get_ip(sec.container_name)
            if container_ip:
                docker_entries.append(f"{container_ip} {sec.container_name}")
                print(f"  Added: {container_ip} {sec.container_name}")
    
    if docker_entries:
        new_lines.append("# MySQL Cluster Secondaries\n")
        for entry in docker_entries:
            new_lines.append(f"{entry}\n")
        
        content = ''.join(new_lines)
        run_command_with_sudo(["bash", "-c", f"echo '{content}' > /etc/hosts"])
    
    # Add primary hostname to Docker containers
    for sec in config.secondaries:
        if sec.node_type == "docker_container":
            docker_exec(
                sec.container_name,
                ["bash", "-c", f"grep -q '{primary_hostname}' /etc/hosts || echo '{primary_ip} {primary_hostname}' >> /etc/hosts"]
            )
    
    print_success("Network resolution configured")


def configure_secondary_mysql_users(config: ClusterConfig) -> None:
    """Configure MySQL users on secondary nodes."""
    print_step("Configuring secondary MySQL users...")
    
    for sec in config.secondaries:
        name = sec.container_name or sec.hostname
        print(f"  Waiting for {name} ({sec.host}:{sec.port})...", end="", flush=True)
        
        for _ in range(30):
            if mysql_is_running(sec.host, sec.port):
                print(" ready")
                break
            time.sleep(1)
            print(".", end="", flush=True)
        else:
            print()
            print_warning(f"{name} not reachable, skipping")
            continue
        
        # Configure user
        success, _ = mysql_execute(f"""
            CREATE USER IF NOT EXISTS 'root'@'%' IDENTIFIED BY '{MYSQL_PASSWORD}';
            GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION;
            FLUSH PRIVILEGES;
        """, host=sec.host, port=sec.port)
        
        if success:
            print_success(f"{name} configured")
        else:
            print_warning(f"{name} configuration failed")


def install_plugin_on_all_nodes(config: ClusterConfig, primary_ip: str) -> None:
    """Install LineairDB plugin on all nodes."""
    print_step("Installing LineairDB plugin on all nodes...")
    
    # Ensure read_only is disabled
    mysql_execute("SET GLOBAL super_read_only = OFF; SET GLOBAL read_only = OFF;")
    
    # Find plugin
    plugin_path = get_plugin_path("release")
    if not plugin_path:
        plugin_path = get_plugin_path("debug")
    
    if not plugin_path:
        print_warning("LineairDB plugin not found, skipping installation")
        return
    
    print(f"  Plugin: {plugin_path}")
    
    # Install on primary
    print("  Installing on primary...")
    
    # Check if binbench mode
    binbench_marker = get_repl_dir() / "config" / ".binbench_mode"
    is_binbench = binbench_marker.exists()
    
    # Check if plugin is already active
    status = mysql_query(
        "SELECT PLUGIN_STATUS FROM INFORMATION_SCHEMA.PLUGINS WHERE PLUGIN_NAME = 'LINEAIRDB';"
    )
    
    if status == "ACTIVE":
        print_success("Primary: LineairDB already ACTIVE")
    else:
        if is_binbench:
            # In binbench mode, plugin_dir in my.cnf points directly to the release plugin dir
            # Just need to install without copying
            print("    (binbench mode: using plugin from release dir)")
        else:
            # System MySQL: copy plugin to system plugin directory
            run_command_with_sudo([
                "cp", str(plugin_path),
                "/usr/lib/mysql/plugin/ha_lineairdb_storage_engine.so"
            ])
            run_command_with_sudo([
                "chmod", "644",
                "/usr/lib/mysql/plugin/ha_lineairdb_storage_engine.so"
            ])
        
        # Uninstall if exists (ignore errors), then install
        mysql_execute("UNINSTALL PLUGIN lineairdb;")
        time.sleep(1)
        
        # Try to install plugin and capture full output
        install_cmd = [
            "mysql", "-h127.0.0.1", "-P3306", "-uroot", f"-p{MYSQL_PASSWORD}",
            "-e", "INSTALL PLUGIN lineairdb SONAME 'ha_lineairdb_storage_engine.so';"
        ]
        result = run_command(install_cmd, capture_output=True)
        success = result.returncode == 0
        
        if not success:
            print_warning("    Plugin install command failed!")
            if result.stderr:
                # Filter out password warning
                errors = [l for l in result.stderr.strip().split('\n') 
                         if 'password on the command line' not in l.lower()]
                for err in errors:
                    print(f"    MySQL Error: {err}")
        
        # Verify installation
        time.sleep(1)
        status = mysql_query(
            "SELECT PLUGIN_STATUS FROM INFORMATION_SCHEMA.PLUGINS WHERE PLUGIN_NAME = 'LINEAIRDB';"
        )
        
        if status == "ACTIVE":
            print_success("Primary: LineairDB installed and ACTIVE")
        else:
            print_warning(f"Primary: Plugin install attempted (status: {status})")
            if is_binbench:
                # Show more debug info
                plugin_dir = mysql_query("SHOW VARIABLES LIKE 'plugin_dir';")
                print(f"    plugin_dir: {plugin_dir}")
    
    # Install on secondaries
    for sec in config.secondaries:
        name = sec.container_name or sec.hostname
        
        if sec.node_type == "docker_container":
            docker_cp(
                str(plugin_path),
                f"{sec.container_name}:/usr/lib64/mysql/plugin/ha_lineairdb_storage_engine.so"
            )
        else:
            print(f"    Copying plugin to {sec.host} via scp ({sec.ssh_user}@{sec.host})...")
            scp_success = scp_copy(
                str(plugin_path),
                f"{sec.ssh_user}@{sec.host}:/tmp/ha_lineairdb_storage_engine.so"
            )
            if not scp_success:
                print_warning(f"    SCP failed for {sec.host} - check SSH key authentication for {sec.ssh_user}@{sec.host}")
                continue
            
            ssh_success, ssh_output = ssh_execute(
                sec.host,
                "sudo cp /tmp/ha_lineairdb_storage_engine.so /usr/lib/mysql/plugin/ && "
                "sudo chmod 644 /usr/lib/mysql/plugin/ha_lineairdb_storage_engine.so",
                user=sec.ssh_user,
            )
            if not ssh_success:
                print_warning(f"    SSH command failed on {sec.host}: {ssh_output}")
        
        # Install plugin on secondary
        if mysql_is_running(sec.host, sec.port):
            # Check if already active
            sec_status = mysql_query(
                "SELECT PLUGIN_STATUS FROM INFORMATION_SCHEMA.PLUGINS WHERE PLUGIN_NAME = 'LINEAIRDB';",
                host=sec.host, port=sec.port
            )
            if sec_status == "ACTIVE":
                print_success(f"{name}: LineairDB already ACTIVE")
            else:
                mysql_execute("UNINSTALL PLUGIN lineairdb;", host=sec.host, port=sec.port)
                time.sleep(1)
                mysql_execute(
                    "INSTALL PLUGIN lineairdb SONAME 'ha_lineairdb_storage_engine.so';",
                    host=sec.host, port=sec.port
                )
                print_success(f"{name}: LineairDB installed")
        else:
            print_warning(f"{name}: Not reachable, skipping plugin install")


def setup_innodb_cluster(config: ClusterConfig, primary_ip: str, binbench: bool = False) -> None:
    """Set up InnoDB Cluster with Group Replication."""
    print_step("Setting up InnoDB Cluster...")
    
    # Ensure read_only is disabled
    mysql_execute("SET GLOBAL super_read_only = OFF; SET GLOBAL read_only = OFF;")
    
    # Build IP allowlist
    print("  Building IP allowlist for Group Replication...")
    ip_allowlist = [primary_ip, "127.0.0.1"]
    
    for sec in config.secondaries:
        if sec.node_type == "docker_container":
            ip_allowlist.append("172.20.0.0/16")
        else:
            ip_allowlist.append(sec.host)
    
    ip_allowlist = list(set(ip_allowlist))  # Remove duplicates
    ip_allowlist_str = ",".join(ip_allowlist)
    print(f"  IP allowlist: {ip_allowlist_str}")
    
    # Set IP allowlist on primary
    mysql_execute(f"SET GLOBAL group_replication_ip_allowlist = '{ip_allowlist_str}';")
    
    # Configure primary for GR
    # In binbench mode, we don't restart because local build MySQL isn't managed by systemctl
    print("  Configuring primary for Group Replication...")
    restart_option = "false" if binbench else "true"
    mysqlsh_execute(f"""
    shell.options.useWizards = false;
    try {{
        dba.configureInstance('{MYSQL_USER}:{MYSQL_PASSWORD}@127.0.0.1:3306', {{
            clusterAdmin: 'root@%',
            restart: {restart_option}
        }});
    }} catch(e) {{
        if (!e.message.includes('already prepared') && !e.message.includes('already configured')) {{
            throw e;
        }}
    }}
    """)
    
    time.sleep(3)
    
    # Configure secondaries
    print("  Configuring secondaries for Group Replication...")
    for sec in config.secondaries:
        name = sec.container_name or sec.hostname
        print(f"    Configuring {name}...", end="", flush=True)
        
        # Set IP allowlist
        mysql_execute(
            f"SET GLOBAL group_replication_ip_allowlist = '{ip_allowlist_str}';",
            host=sec.host, port=sec.port
        )
        
        mysqlsh_execute(f"""
        shell.options.useWizards = false;
        try {{
            dba.configureInstance('{MYSQL_USER}:{MYSQL_PASSWORD}@{sec.host}:{sec.port}', {{
                clusterAdmin: 'root@%'
            }});
        }} catch(e) {{
            if (!e.message.includes('already prepared') && !e.message.includes('already configured')) {{
                print('Error: ' + e.message);
            }}
        }}
        """, host=sec.host, port=sec.port)
        print(" done")
        time.sleep(2)
    
    time.sleep(3)
    
    # Clean up LineairDB persistent data
    print("  Cleaning up LineairDB persistent data...")
    run_command_with_sudo(["rm", "-rf", "/var/lib/mysql/lineairdb_logs"])
    if binbench:
        # Also clean up local build MySQL's lineairdb_logs
        build_data_dir = get_root_dir() / "build" / "data" / "lineairdb_logs"
        if build_data_dir.exists():
            run_command_with_sudo(["rm", "-rf", str(build_data_dir)])
    
    # Create or get cluster
    print("  Creating InnoDB Cluster...")
    mysqlsh_execute(f"""
    shell.options.useWizards = false;
    var cluster;
    var ipAllowlist = '{ip_allowlist_str}';
    try {{
        cluster = dba.getCluster('{CLUSTER_NAME}');
        print('Cluster already exists: ' + cluster.getName());
    }} catch(e) {{
        if (e.message.includes('standalone') || e.message.includes('GR is not active') || e.message.includes('51314')) {{
            print('GR not active, trying to reboot cluster...');
            try {{
                cluster = dba.rebootClusterFromCompleteOutage('{CLUSTER_NAME}', {{force: true}});
                print('Cluster rebooted: ' + cluster.getName());
            }} catch(e2) {{
                print('Reboot failed, dropping metadata and creating new cluster...');
                try {{
                    dba.dropMetadataSchema({{force: true}});
                }} catch(e3) {{}}
                cluster = dba.createCluster('{CLUSTER_NAME}', {{
                    communicationStack: 'XCOM',
                    gtidSetIsComplete: true,
                    ipAllowlist: ipAllowlist
                }});
                print('Cluster created: ' + cluster.getName());
            }}
        }} else if (e.message.includes('not found') || e.message.includes('does not exist')) {{
            print('Creating new cluster...');
            cluster = dba.createCluster('{CLUSTER_NAME}', {{
                communicationStack: 'XCOM',
                gtidSetIsComplete: true,
                ipAllowlist: ipAllowlist
            }});
            print('Cluster created: ' + cluster.getName());
        }} else {{
            throw e;
        }}
    }}
    """)
    
    # Add secondaries to cluster
    print("  Adding secondary nodes to cluster...")
    for sec in config.secondaries:
        name = sec.container_name or sec.hostname
        
        if sec.node_type == "docker_container":
            add_uri = f"{MYSQL_USER}:{MYSQL_PASSWORD}@{sec.container_name}:3306"
        else:
            add_uri = f"{MYSQL_USER}:{MYSQL_PASSWORD}@{sec.host}:{sec.port}"
        
        print(f"    Adding {name}...")
        # Use longer timeout (5 min) for addInstance with clone - it involves data transfer and restart
        success, output = mysqlsh_execute(f"""
        shell.options.useWizards = false;
        var cluster = dba.getCluster('{CLUSTER_NAME}');
        var ipAllowlist = '{ip_allowlist_str}';
        
        // First, try to remove any existing instance (handles MISSING/errant GTID cases)
        try {{
            cluster.removeInstance('{add_uri}', {{force: true}});
            print('Removed stale instance entry');
        }} catch(e) {{
            // Ignore errors - instance may not exist in cluster
        }}
        
        // Now add the instance with clone recovery
        try {{
            cluster.addInstance('{add_uri}', {{
                recoveryMethod: 'clone',
                ipAllowlist: ipAllowlist
            }});
            print('Added successfully');
        }} catch(e) {{
            if (e.message.includes('already a member') || e.message.includes('already in the cluster')) {{
                print('Already in cluster');
            }} else {{
                print('ERROR: ' + e.message);
            }}
        }}
        """, timeout=300)
        # Show output to help debug failures
        if output:
            for line in output.strip().split('\n'):
                if line.strip():
                    if 'ERROR' in line:
                        print_error(f"      {line}")
                    elif 'Added successfully' in line or 'Already in cluster' in line:
                        print_success(f"      {line}")
                    else:
                        print(f"      {line}")
    
    print_success("InnoDB Cluster setup complete")


def print_final_status(config: ClusterConfig, primary_ip: str, setup_replication: bool) -> None:
    """Print final cluster status."""
    print()
    print_header("Cluster Started!")
    print()
    print(f"Primary (local):      127.0.0.1:3306")
    
    for i, sec in enumerate(config.secondaries):
        if sec.node_type == "docker_container":
            print(f"Secondary {i+1} (Docker): {sec.host}:{sec.port} ({sec.container_name})")
        else:
            print(f"Secondary {i+1} (Remote): {sec.host}:{sec.port}")
    print()
    
    if setup_replication:
        print("InnoDB Cluster Status:")
        success, output = mysqlsh_execute(f"""
        var cluster = dba.getCluster();
        var status = cluster.status();
        print('  Cluster: ' + status.clusterName);
        print('  Status: ' + status.defaultReplicaSet.status);
        for (var member in status.defaultReplicaSet.topology) {{
            var m = status.defaultReplicaSet.topology[member];
            print('    ' + member + ': ' + m.memberRole + ' (' + m.mode + ')');
        }}
        """)
        if output:
            for line in output.split('\n'):
                if line.strip():
                    print(line)
        print()
    
    print("Commands:")
    print("  Check cluster: mysqlsh --uri root:kamo@127.0.0.1:3306 -e \"dba.getCluster().status()\"")
    print("  Check status:  python3 status.py")
    print()


def main():
    parser = argparse.ArgumentParser(
        description="Start the MySQL InnoDB Cluster for LineairDB Replication"
    )
    parser.add_argument(
        '--no-replication',
        action='store_true',
        help="Start without setting up InnoDB Cluster"
    )
    parser.add_argument(
        '--no-plugin',
        action='store_true',
        help="Don't install LineairDB plugin"
    )
    parser.add_argument(
        '--binbench',
        action='store_true',
        help="Use local build MySQL (build/bin/mysqld) as primary instead of system MySQL"
    )
    
    args = parser.parse_args()
    
    setup_replication = not args.no_replication
    install_plugin = not args.no_plugin
    binbench = args.binbench
    
    primary_ip = get_local_ip()
    primary_hostname = get_hostname()
    
    print_header("Starting LineairDB InnoDB Cluster")
    print()
    print(f"Primary: {primary_hostname} ({primary_ip}:3306)")
    if binbench:
        print(f"Mode: binbench (local build MySQL)")
    else:
        print(f"Mode: benchbase (system MySQL)")
    print(f"Setup Replication: {setup_replication}")
    print(f"Install Plugin: {install_plugin}")
    print()
    
    # Load configuration
    config = load_config_if_exists()
    if config is None:
        print_error("Cluster configuration not found")
        print("Please run: python3 init_cluster.py")
        sys.exit(1)
    
    print(f"Secondary nodes: {len(config.secondaries)} "
          f"(Docker: {config.num_docker_containers}, Remote: {config.num_remote_hosts})")
    print()
    
    # Step 1: Check/Build Docker Image
    if config.num_docker_containers > 0:
        print_step("Checking Docker image...")
        if not check_docker_image():
            print("  Docker image not found. Building it first...")
            build_docker_image()
        else:
            print_success("mysql-lineairdb-ubuntu image found")
    else:
        print("Step 1: No Docker nodes configured, skipping Docker image check")
    
    # Step 2: Configure Primary MySQL
    print()
    if not configure_primary_mysql(primary_ip, binbench=binbench):
        print_error("Failed to configure primary MySQL")
        sys.exit(1)
    
    # Step 3: Start/Verify Secondary Nodes
    print()
    print_step("Starting/verifying secondary nodes...")
    
    if config.num_docker_containers > 0:
        start_docker_containers(config)
    
    if config.num_remote_hosts > 0:
        print("  Verifying remote hosts (all must be accessible)...")
        if not verify_remote_hosts(config):
            print_error("Aborting cluster setup.")
            sys.exit(1)
        print_success("All remote hosts verified")
    
    # Step 4: Setup Network Resolution
    print()
    setup_network_resolution(config, primary_ip, primary_hostname)
    
    # Step 5: Configure MySQL Users on Secondaries
    print()
    configure_secondary_mysql_users(config)
    
    # Step 6: Install LineairDB Plugin
    if install_plugin:
        print()
        install_plugin_on_all_nodes(config, primary_ip)
    
    # Step 7: Setup InnoDB Cluster
    if setup_replication:
        print()
        setup_innodb_cluster(config, primary_ip, binbench=binbench)
    
    # Final Status
    print_final_status(config, primary_ip, setup_replication)


if __name__ == "__main__":
    main()

