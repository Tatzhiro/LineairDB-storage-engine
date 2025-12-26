#!/usr/bin/env python3
"""
Install LineairDB Storage Engine Plugin on all cluster nodes.

This script handles building and installing the LineairDB plugin with
different FENCE configurations.

Usage:
    python3 install_plugin.py [--fence|--lineairdb] [--debug|--release] [--rebuild] [--build-only]

Options:
    --fence       Build and install with FENCE=true (synchronous commits)
    --lineairdb   Build and install with FENCE=false (async commits, faster) [default]
    --debug       Use debug build from build/ directory
    --release     Use release build from release/ directory [default]
    --rebuild     Force rebuild even if plugin exists
    --build-only  Only build the plugin, don't install or restart MySQL
    --path PATH   Install from custom path (skips build)
"""

import argparse
import sys
import time
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from utils import (
    ClusterConfig,
    MYSQL_PASSWORD,
    MYSQL_USER,
    docker_cp,
    get_fence_value_from_source,
    get_plugin_path,
    get_repl_dir,
    get_root_dir,
    load_config_if_exists,
    mysql_execute,
    mysql_query,
    print_error,
    print_header,
    print_info,
    print_step,
    print_success,
    print_warning,
    run_command,
    run_command_with_sudo,
    scp_copy,
    set_fence_value_in_source,
    ssh_execute,
    systemctl_restart,
    wait_for_mysql,
)


def build_plugin(
    build_type: str,
    fence_value: str,
    force_rebuild: bool = False,
) -> Path:
    """
    Build the LineairDB plugin.
    
    Args:
        build_type: 'release' or 'debug'
        fence_value: 'true' or 'false'
        force_rebuild: Force rebuild even if plugin exists
        
    Returns:
        Path to the built plugin
    """
    root_dir = get_root_dir()
    
    if build_type == "release":
        build_dir = root_dir / "release"
    else:
        build_dir = root_dir / "build"
    
    plugin_path = build_dir / "plugin_output_directory" / "ha_lineairdb_storage_engine.so"
    
    # Check current FENCE setting in source
    current_fence = get_fence_value_from_source()
    
    print(f"Current FENCE in source: {current_fence}")
    print(f"Required FENCE:          {fence_value}")
    print()
    
    # Determine if we need to rebuild
    need_rebuild = False
    
    if force_rebuild:
        print("Force rebuild requested.")
        need_rebuild = True
    elif not plugin_path.exists():
        print(f"Plugin not found at {plugin_path}")
        need_rebuild = True
    elif current_fence != fence_value:
        print("FENCE value mismatch - rebuild required.")
        need_rebuild = True
    
    if need_rebuild:
        print()
        print(f"=== Building Plugin with FENCE={fence_value} ===")
        print()
        
        # Update FENCE in source
        print(f"Setting FENCE={fence_value} in ha_lineairdb.cc...")
        if not set_fence_value_in_source(fence_value):
            raise RuntimeError("Failed to update FENCE value in source")
        
        # Verify the change
        new_fence = get_fence_value_from_source()
        print(f"FENCE is now: {new_fence}")
        
        # Check if build directory exists and has CMakeCache
        cmake_cache = build_dir / "CMakeCache.txt"
        if not cmake_cache.exists():
            print()
            print(f"Error: Build directory not configured at {build_dir}")
            print("Please run cmake first:")
            if build_type == "release":
                print("  mkdir -p release && cd release")
                print("  cmake ../third_party/mysql-server -DCMAKE_BUILD_TYPE=Release -G Ninja ...")
            else:
                print("  mkdir -p build && cd build")
                print("  cmake ../third_party/mysql-server -DCMAKE_BUILD_TYPE=Debug -G Ninja ...")
            sys.exit(1)
        
        # Force clean rebuild by removing old plugin binary
        print("Removing old plugin binary to force rebuild...")
        if plugin_path.exists():
            plugin_path.unlink()
        
        # Touch the source file to ensure ninja sees the change
        source_file = root_dir / "ha_lineairdb.cc"
        source_file.touch()
        
        # Build the plugin
        print()
        print("Building plugin...")
        
        import multiprocessing
        num_jobs = multiprocessing.cpu_count()
        
        result = run_command(
            ["ninja", "lineairdb_storage_engine", f"-j{num_jobs}"],
            cwd=build_dir,
            capture_output=False,
        )
        
        if result.returncode != 0:
            raise RuntimeError("Failed to build plugin")
        
        print()
        print_success("Plugin built successfully")
    else:
        print("Plugin already exists with correct FENCE setting.")
    
    return plugin_path


def uninstall_plugin(host: str, port: int) -> None:
    """Uninstall LineairDB plugin from a node."""
    # Check super_read_only
    super_ro = mysql_query(
        "SELECT @@super_read_only;",
        host=host,
        port=port,
    )
    
    if super_ro == "1":
        mysql_execute("""
            SET GLOBAL super_read_only = 0;
            UNINSTALL PLUGIN lineairdb;
            SET GLOBAL super_read_only = 1;
        """, host=host, port=port)
    else:
        mysql_execute("UNINSTALL PLUGIN lineairdb;", host=host, port=port)


def install_plugin_on_node(
    host: str,
    port: int,
    plugin_name: str = "ha_lineairdb_storage_engine.so",
) -> bool:
    """
    Install LineairDB plugin on a node.
    
    Returns:
        True if successful
    """
    # Check super_read_only
    super_ro = mysql_query(
        "SELECT @@super_read_only;",
        host=host,
        port=port,
    )
    
    if super_ro == "1":
        success, output = mysql_execute(f"""
            SET GLOBAL super_read_only = 0;
            INSTALL PLUGIN lineairdb SONAME '{plugin_name}';
            SET GLOBAL super_read_only = 1;
        """, host=host, port=port, silent=False)
    else:
        success, output = mysql_execute(
            f"INSTALL PLUGIN lineairdb SONAME '{plugin_name}';",
            host=host,
            port=port,
            silent=False,
        )
    
    if not success:
        print(f"    Install error on {host}:{port}")
    
    # Verify
    status = mysql_query(
        "SELECT PLUGIN_STATUS FROM INFORMATION_SCHEMA.PLUGINS WHERE PLUGIN_NAME = 'LINEAIRDB';",
        host=host,
        port=port,
    )
    
    return status == "ACTIVE"


def main():
    parser = argparse.ArgumentParser(
        description="Install LineairDB Storage Engine Plugin",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
FENCE Mode Options:
  --lineairdb   Build with FENCE=false (async commits, faster) [default]
  --fence       Build with FENCE=true (synchronous commits)

Build Options:
  --debug       Use debug build directory (build/)
  --release     Use release build directory (release/) [default]
  --rebuild     Force rebuild even if plugin exists
  --build-only  Only build the plugin, don't install or restart MySQL
  --path PATH   Install from custom path (skips build)

Examples:
  %(prog)s --lineairdb --release   # Fast LineairDB (FENCE=false, Release)
  %(prog)s --fence --release       # Safe LineairDB (FENCE=true, Release)
  %(prog)s --fence --debug         # Debug with FENCE=true
  %(prog)s --fence --build-only    # Build only, don't install
        """
    )
    
    fence_group = parser.add_mutually_exclusive_group()
    fence_group.add_argument('--fence', action='store_true', help='FENCE=true (sync)')
    fence_group.add_argument('--lineairdb', action='store_true', help='FENCE=false (async) [default]')
    
    build_group = parser.add_mutually_exclusive_group()
    build_group.add_argument('--debug', action='store_true', help='Use debug build')
    build_group.add_argument('--release', action='store_true', help='Use release build [default]')
    
    parser.add_argument('--rebuild', action='store_true', help='Force rebuild')
    parser.add_argument('--build-only', action='store_true', help='Build only, no install')
    parser.add_argument('--path', type=str, help='Custom plugin path (skips build)')
    
    args = parser.parse_args()
    
    # Determine settings
    fence_mode = "fence" if args.fence else "lineairdb"
    fence_value = "true" if args.fence else "false"
    build_type = "debug" if args.debug else "release"
    
    print_header("Installing LineairDB Storage Engine Plugin")
    
    print(f"FENCE mode:  {fence_mode} (FENCE={fence_value}, {'sync' if fence_value == 'true' else 'async'})")
    print(f"Build type:  {build_type}")
    print()
    
    # Determine plugin path
    if args.path:
        plugin_path = Path(args.path)
        if not plugin_path.exists():
            print_error(f"Plugin not found at {plugin_path}")
            sys.exit(1)
    else:
        plugin_path = build_plugin(
            build_type=build_type,
            fence_value=fence_value,
            force_rebuild=args.rebuild,
        )
        
        if args.build_only:
            print()
            print_header(f"Build Only Mode - Plugin ready at: {plugin_path}")
            return
        
        # MySQL needs a restart to load new plugin binary
        print()
        print_step("Restarting MySQL to load new plugin binary...")
        systemctl_restart("mysql")
        
        if wait_for_mysql(timeout=30):
            time.sleep(2)
            print_success("MySQL restarted")
        else:
            print_error("MySQL failed to restart")
            sys.exit(1)
    
    print()
    print(f"Plugin path: {plugin_path}")
    
    if not plugin_path.exists():
        print_error(f"Plugin not found at {plugin_path}")
        sys.exit(1)
    
    # Load cluster config
    config = load_config_if_exists()
    
    # === Uninstall existing plugin first ===
    print()
    print("=== Uninstalling existing plugin (if any) ===")
    
    # Uninstall from primary
    uninstall_plugin("127.0.0.1", 3306)
    print("  Primary: Uninstalled")
    
    # Uninstall from secondaries
    if config:
        for sec in config.secondaries:
            uninstall_plugin(sec.host, sec.port)
            print(f"  {sec.container_name or sec.hostname}: Uninstalled")
    
    # === Install on Primary ===
    print()
    print("=== Installing on Primary (local MySQL) ===")
    
    # Copy plugin
    run_command_with_sudo([
        "cp", str(plugin_path),
        "/usr/lib/mysql/plugin/ha_lineairdb_storage_engine.so"
    ])
    run_command_with_sudo([
        "chmod", "644",
        "/usr/lib/mysql/plugin/ha_lineairdb_storage_engine.so"
    ])
    
    # Ensure read_only is off on primary
    mysql_execute("SET GLOBAL super_read_only = 0; SET GLOBAL read_only = 0;")
    
    # Install plugin
    if install_plugin_on_node("127.0.0.1", 3306):
        print_success(f"Primary: Plugin installed and ACTIVE (FENCE={fence_value})")
    else:
        print_error("Primary: Plugin installation failed")
    
    # === Install on Secondary Nodes ===
    if config and config.secondaries:
        print()
        print("=== Installing on Secondary Nodes ===")
        
        for sec in config.secondaries:
            name = sec.container_name or sec.hostname
            print(f"Installing on {name} ({sec.node_type})...")
            
            if sec.node_type == "docker_container":
                # Docker container: use docker cp
                docker_cp(
                    str(plugin_path),
                    f"{sec.container_name}:/usr/lib64/mysql/plugin/ha_lineairdb_storage_engine.so"
                )
            else:
                # Remote host: use scp
                print(f"    Copying plugin to {sec.host} via scp...")
                if not scp_copy(
                    str(plugin_path),
                    f"{sec.ssh_user}@{sec.host}:/tmp/ha_lineairdb_storage_engine.so"
                ):
                    print_error(f"{name}: Failed to copy plugin via scp")
                    continue
                
                ssh_execute(
                    sec.host,
                    "sudo cp /tmp/ha_lineairdb_storage_engine.so /usr/lib/mysql/plugin/ && "
                    "sudo chmod 644 /usr/lib/mysql/plugin/ha_lineairdb_storage_engine.so",
                    user=sec.ssh_user,
                )
            
            # Install plugin
            if install_plugin_on_node(sec.host, sec.port):
                print_success(f"{name}: Plugin installed and ACTIVE (FENCE={fence_value})")
            else:
                print_error(f"{name}: Plugin installation failed")
    
    print()
    print_header("Plugin Installation Complete!")
    print()
    print(f"Mode: {fence_mode} (FENCE={fence_value})")
    print()


if __name__ == "__main__":
    main()

