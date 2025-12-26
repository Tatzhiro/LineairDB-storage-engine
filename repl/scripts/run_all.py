#!/usr/bin/env python3
"""
LineairDB Group Replication - Master Script

This script runs the complete flow:
    1. Clean up any existing cluster
    2. Initialize cluster with specified number of secondaries
    3. Start cluster with Group Replication
    4. Run benchmarks for each specified engine
    5. Verify replication is working

Usage:
    python3 run_all.py <num_secondaries> <engine1> [engine2] ... [options]

Required Arguments:
    num_secondaries: Number of secondary nodes (1-10)
    engines:         At least one engine to benchmark

Supported Engines:
    lineairdb    LineairDB with FENCE=off (async commits, fast)
    fence        LineairDB with FENCE=on (sync commits, safe)
    innodb       MySQL InnoDB storage engine

Options:
    --tool TOOL        Benchmark tool: benchbase (system MySQL) or binbench (local build)
    --terminals N      Number of concurrent terminals for benchmark (default: 4)
    --time N           Benchmark duration in seconds (default: 30)
    --remote HOST      Remote host as secondary (can specify multiple times)
    --setup-remote     Auto-setup MySQL on remote hosts if not accessible

Examples:
    python3 run_all.py 2 lineairdb                    # 2 Docker secondaries
    python3 run_all.py 3 lineairdb fence innodb       # Compare 3 engines
    python3 run_all.py 2 lineairdb --tool binbench    # Use local build MySQL
    python3 run_all.py 2 lineairdb fence --terminals 8 --time 60
    python3 run_all.py 5 lineairdb --remote 192.168.1.10 --remote 192.168.1.11 --remote 192.168.1.12
"""

import argparse
import json
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from utils import (
    ClusterConfig,
    Colors,
    MYSQL_PASSWORD,
    MYSQL_USER,
    VALID_ENGINES,
    get_engine_description,
    get_repl_dir,
    get_root_dir,
    is_lineairdb_engine,
    load_config_if_exists,
    mysql_execute,
    mysql_is_running,
    mysql_query,
    ping_host,
    print_error,
    print_header,
    print_info,
    print_step,
    print_success,
    print_warning,
    run_command,
)


@dataclass
class BenchmarkResult:
    """Stores benchmark results for an engine."""
    success: bool = False
    throughput: str = "N/A"
    goodput: str = "N/A"
    latency: str = "N/A"


def verify_remote_hosts(remote_hosts: List[str], setup_remote: bool) -> bool:
    """
    Verify remote host connectivity.
    
    Returns:
        True if all hosts are accessible
    """
    print_step("Verifying remote host connectivity...")
    
    network_unreachable = []
    mysql_unreachable = []
    
    for host_spec in remote_hosts:
        # Parse host:port:user format
        parts = host_spec.split(":")
        host = parts[0]
        port = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 3306
        ssh_user = parts[2] if len(parts) > 2 else "root"
        
        # Handle case where port is missing but user is present
        if len(parts) == 2 and not parts[1].isdigit():
            port = 3306
            ssh_user = parts[1]
        
        print(f"  {host}: ", end="", flush=True)
        
        # Check network connectivity
        if not ping_host(host):
            print(f"{Colors.RED}✗ Network unreachable{Colors.NC}")
            network_unreachable.append(host)
            continue
        print("network ✓, ", end="", flush=True)
        
        # Check MySQL connectivity
        if mysql_is_running(host, port):
            print(f"{Colors.GREEN}MySQL ✓{Colors.NC}")
        else:
            print(f"{Colors.RED}MySQL ✗ (port {port}){Colors.NC}")
            mysql_unreachable.append(f"{host}:{port}")
    
    # Handle network unreachable hosts
    if network_unreachable:
        print()
        print_error("The following hosts are not reachable on the network:")
        for h in network_unreachable:
            print(f"  - {h}")
        print()
        print("Please check:")
        print("  1. The host is powered on and connected to the network")
        print("  2. The IP address is correct")
        print("  3. No firewall is blocking ICMP (ping)")
        print()
        print_error("Aborting.")
        return False
    
    # Handle MySQL unreachable hosts
    if mysql_unreachable:
        print()
        print_error("The following hosts have MySQL not accessible:")
        for h in mysql_unreachable:
            print(f"  - {h}")
        
        if setup_remote:
            print()
            print_step("Auto-setup MySQL on remote hosts (--setup-remote flag)...")
            
            # Run setup script
            script_dir = Path(__file__).parent
            setup_hosts = []
            for h in mysql_unreachable:
                host = h.split(":")[0]
                for host_spec in remote_hosts:
                    if host_spec.startswith(host):
                        parts = host_spec.split(":")
                        ssh_user = parts[2] if len(parts) > 2 else "root"
                        if len(parts) == 2 and not parts[1].isdigit():
                            ssh_user = parts[1]
                        setup_hosts.append(f"{host}:{ssh_user}" if ssh_user != "root" else host)
                        break
            
            result = run_command(
                [sys.executable, str(script_dir / "setup_remote_mysql.py")] + setup_hosts,
                capture_output=False,
            )
            
            if result.returncode == 0:
                print_success("MySQL setup completed on remote hosts")
                print()
                print_step("Re-verifying MySQL connectivity...")
                
                # Re-check
                still_unreachable = []
                for host_spec in remote_hosts:
                    parts = host_spec.split(":")
                    host = parts[0]
                    port = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 3306
                    
                    print(f"  {host}:{port}: ", end="", flush=True)
                    if mysql_is_running(host, port):
                        print(f"{Colors.GREEN}MySQL ✓{Colors.NC}")
                    else:
                        print(f"{Colors.RED}MySQL ✗{Colors.NC}")
                        still_unreachable.append(f"{host}:{port}")
                
                if still_unreachable:
                    print()
                    print_error("MySQL still not accessible on some hosts after setup:")
                    for h in still_unreachable:
                        print(f"  - {h}")
                    print_error("Aborting.")
                    return False
            else:
                print_error("MySQL setup failed on some remote hosts")
                print_error("Aborting.")
                return False
        else:
            print()
            print("Please ensure on each remote host:")
            print("  1. MySQL is installed and running")
            print("  2. MySQL is configured with bind-address = 0.0.0.0")
            print("  3. Firewall allows connections on the MySQL port")
            print(f"  4. MySQL user 'root'@'%' exists with password '{MYSQL_PASSWORD}'")
            print()
            print("Or use --setup-remote to automatically set up MySQL via SSH")
            print()
            print_error("Aborting.")
            return False
    
    print_success("All remote hosts are accessible")
    return True


def setup_cluster_and_benchmark(
    engine: str,
    num_secondaries: int,
    remote_hosts: List[str],
    tool: str,
    terminals: int,
    duration: int,
) -> bool:
    """
    Setup cluster and run benchmark for a specific engine.
    
    Returns:
        True if successful
    """
    engine_desc = get_engine_description(engine)
    script_dir = Path(__file__).parent
    
    print_header(f"Setting up cluster for {engine_desc}")
    
    # Step 1: Cleanup
    print_step("Cleaning up any existing cluster...")
    run_command(
        [sys.executable, str(script_dir / "cleanup.py"), "--all"],
        capture_output=False,
    )
    print_success("Cleanup complete")
    
    # Step 2: Build plugin (only for LineairDB engines)
    if is_lineairdb_engine(engine):
        print_step("Building LineairDB plugin...")
        run_command(
            [sys.executable, str(script_dir / "install_plugin.py"), f"--{engine}", "--release", "--build-only"],
            capture_output=False,
        )
        print_success("Plugin built")
    else:
        print_info(f"Using MySQL built-in engine: {engine}")
    
    # Step 3: Initialize cluster
    print_step(f"Initializing cluster with {num_secondaries} secondary nodes...")
    init_cmd = [
        sys.executable, str(script_dir / "init_cluster.py"),
        str(num_secondaries),
    ]
    if remote_hosts:
        init_cmd.extend(["--remote"] + remote_hosts)
    run_command(init_cmd, capture_output=False)
    print_success("Cluster initialized")
    
    # Step 4: Start cluster
    print_step("Starting cluster with Group Replication...")
    start_cmd = [sys.executable, str(script_dir / "start_cluster.py")]
    if tool == "binbench":
        start_cmd.append("--binbench")
    result = run_command(start_cmd, capture_output=False)
    if result.returncode != 0:
        print_error("Failed to start cluster")
        return False
    print_success("Cluster started")
    
    # Wait for GR to stabilize
    time.sleep(5)
    
    # Step 5: Verify cluster
    print_step("Verifying cluster status...")
    run_command(
        [sys.executable, str(script_dir / "status.py")],
        capture_output=False,
    )
    
    # Step 6: Run benchmark
    print_step(f"Running benchmark with engine={engine} using {tool}...")
    print_info(engine_desc)
    print()
    
    benchmark_cmd = [
        sys.executable, str(script_dir / "run_benchmark.py"),
        engine, "ycsb",
        "--tool", tool,
        "--terminals", str(terminals),
        "--time", str(duration),
        "--no-rebuild",
    ]
    result = run_command(benchmark_cmd, capture_output=False)
    
    return result.returncode == 0


def extract_results(engine: str) -> BenchmarkResult:
    """Extract benchmark results from JSON summary."""
    root_dir = get_root_dir()
    result_dir = root_dir / "bench" / "results" / engine / "gr_test"
    
    result = BenchmarkResult()
    
    # Find latest JSON summary
    try:
        json_files = sorted(result_dir.glob("*summary*.json"), reverse=True)
        if json_files:
            with open(json_files[0]) as f:
                data = json.load(f)
            
            result.throughput = f"{data.get('Throughput (requests/second)', 0):.2f}"
            result.goodput = f"{data.get('Goodput (requests/second)', 0):.2f}"
            
            latency = data.get('Latency Distribution', {}).get('Average Latency (microseconds)', 0)
            result.latency = f"{latency:.2f}"
            result.success = True
    except Exception:
        pass
    
    return result


def verify_replication(config: ClusterConfig) -> bool:
    """
    Verify Group Replication is working.
    
    Returns:
        True if replication is working
    """
    print_step("Testing Group Replication with InnoDB table...")
    
    # Create test table
    mysql_execute("""
        CREATE DATABASE IF NOT EXISTS gr_test;
        USE gr_test;
        DROP TABLE IF EXISTS repl_check;
        CREATE TABLE repl_check (id INT PRIMARY KEY, ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP) ENGINE=InnoDB;
        INSERT INTO repl_check (id) VALUES (1), (2), (3);
    """)
    
    time.sleep(2)  # Wait for replication
    
    # Check primary
    primary_count = mysql_query("SELECT COUNT(*) FROM gr_test.repl_check;")
    print(f"  Primary (port 3306): {primary_count} rows")
    
    replication_ok = True
    
    # Check secondaries
    for sec in config.secondaries:
        name = sec.container_name or sec.hostname
        count = mysql_query(
            "SELECT COUNT(*) FROM gr_test.repl_check;",
            host=sec.host, port=sec.port
        )
        
        if count == primary_count:
            print_success(f"  {name} ({sec.host}:{sec.port}): {count} rows (replicated ✓)")
        else:
            print_error(f"  {name} ({sec.host}:{sec.port}): {count} rows (expected {primary_count})")
            replication_ok = False
    
    print()
    print("Note: Only InnoDB tables participate in Group Replication.")
    print("      LineairDB tables are stored locally on each node.")
    
    return replication_ok


def print_results_table(engines: List[str], results: Dict[str, BenchmarkResult]) -> None:
    """Print benchmark results comparison table."""
    print(f"{Colors.CYAN}╔══════════════════════════════════════════════════════════════════════════╗{Colors.NC}")
    print(f"{Colors.CYAN}║                        BENCHMARK RESULTS                                 ║{Colors.NC}")
    print(f"{Colors.CYAN}╠══════════════════════════════════════════════════════════════════════════╣{Colors.NC}")
    
    header = f"{Colors.CYAN}║{Colors.NC} {'Engine':<22} │ {'Throughput':<15} │ {'Goodput':<15} │ {'Avg Latency':<12} {Colors.CYAN}║{Colors.NC}"
    subheader = f"{Colors.CYAN}║{Colors.NC} {'':<22} │ {'(req/s)':<15} │ {'(req/s)':<15} │ {'(μs)':<12} {Colors.CYAN}║{Colors.NC}"
    print(header)
    print(subheader)
    print(f"{Colors.CYAN}╠══════════════════════════════════════════════════════════════════════════╣{Colors.NC}")
    
    for engine in engines:
        result = results[engine]
        label = get_engine_description(engine).split("(")[0].strip()
        
        if result.success:
            color = Colors.GREEN
            row = f"{Colors.CYAN}║{Colors.NC} {color}{label:<22}{Colors.NC} │ {color}{result.throughput:<15}{Colors.NC} │ {color}{result.goodput:<15}{Colors.NC} │ {color}{result.latency:<12}{Colors.NC} {Colors.CYAN}║{Colors.NC}"
        else:
            color = Colors.RED
            row = f"{Colors.CYAN}║{Colors.NC} {color}{label:<22}{Colors.NC} │ {color}{'FAILED':<15}{Colors.NC} │ {color}{'-':<15}{Colors.NC} │ {color}{'-':<12}{Colors.NC} {Colors.CYAN}║{Colors.NC}"
        print(row)
    
    print(f"{Colors.CYAN}╚══════════════════════════════════════════════════════════════════════════╝{Colors.NC}")


def main():
    parser = argparse.ArgumentParser(
        description="LineairDB Group Replication - Complete Flow",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Supported Engines:
    lineairdb   LineairDB with FENCE=off (async commits)
    fence       LineairDB with FENCE=on (sync commits)
    innodb      MySQL InnoDB storage engine

Remote Host Format: host[:port[:ssh_user]]
    192.168.1.10          -> port=3306, ssh_user=root
    192.168.1.10:3307     -> port=3307
    192.168.1.10:3306:ubuntu -> ssh_user=ubuntu

Examples:
    %(prog)s 2 lineairdb                      # 2 Docker secondaries
    %(prog)s 3 lineairdb fence innodb         # Compare 3 engines
    %(prog)s 2 lineairdb --tool binbench      # Use local build MySQL
    %(prog)s 5 lineairdb --remote 192.168.1.10 --remote 192.168.1.11  # 2 remote + 3 Docker
        """
    )
    
    parser.add_argument('num_secondaries', type=int, help='Number of secondary nodes (1-10)')
    parser.add_argument('engines', nargs='+', choices=VALID_ENGINES, help='Engines to benchmark')
    parser.add_argument('--tool', default='benchbase', choices=['benchbase', 'binbench'],
                        help='benchbase=system MySQL, binbench=local build MySQL')
    parser.add_argument('--terminals', type=int, default=4, help='Number of concurrent terminals')
    parser.add_argument('--time', type=int, default=30, dest='duration', help='Benchmark duration in seconds')
    parser.add_argument('--remote', action='append', default=[], metavar='HOST', help='Remote host (can specify multiple times)')
    parser.add_argument('--setup-remote', action='store_true', help='Auto-setup MySQL on remote hosts')
    
    args = parser.parse_args()
    
    # Validate
    if args.num_secondaries < 1 or args.num_secondaries > 10:
        print_error("num_secondaries must be between 1 and 10")
        sys.exit(1)
    
    if len(args.remote) > args.num_secondaries:
        print_error(f"Too many remote hosts ({len(args.remote)}) for {args.num_secondaries} secondaries")
        sys.exit(1)
    
    num_docker = args.num_secondaries - len(args.remote)
    
    print_header("LineairDB Group Replication - Complete Flow")
    
    print("Configuration:")
    print(f"  Secondary nodes:  {args.num_secondaries}")
    print(f"    Remote hosts:   {len(args.remote)}")
    print(f"    Docker:         {num_docker}")
    if args.remote:
        print("  Remote host list:")
        for host in args.remote:
            print(f"    - {host}")
    print(f"  Benchmark tool:   {args.tool}")
    print(f"  Terminals:        {args.terminals}")
    print(f"  Duration:         {args.duration}s")
    print(f"  Engines:          {' '.join(args.engines)}")
    print()
    
    # Pre-flight check: Verify remote host connectivity
    if args.remote:
        if not verify_remote_hosts(args.remote, args.setup_remote):
            sys.exit(1)
        print()
    
    start_time = time.time()
    results: Dict[str, BenchmarkResult] = {}
    
    # Run benchmarks for each engine
    for engine in args.engines:
        results[engine] = BenchmarkResult()
        engine_desc = get_engine_description(engine)
        
        print_header(f"Benchmarking: {engine_desc}")
        
        if setup_cluster_and_benchmark(
            engine=engine,
            num_secondaries=args.num_secondaries,
            remote_hosts=args.remote,
            tool=args.tool,
            terminals=args.terminals,
            duration=args.duration,
        ):
            print_success(f"{engine} benchmark completed successfully")
            results[engine] = extract_results(engine)
            results[engine].success = True
        else:
            print_error(f"{engine} benchmark failed")
    
    # Final: Verify Replication
    print_header("Verify Group Replication (Final Cluster State)")
    
    config = load_config_if_exists()
    replication_ok = False
    if config:
        replication_ok = verify_replication(config)
    
    # Summary
    print_header("Summary")
    
    end_time = time.time()
    elapsed = int(end_time - start_time)
    
    print(f"Time elapsed: {elapsed}s")
    print()
    print("Configuration:")
    print(f"  Secondary nodes: {args.num_secondaries}")
    print(f"  Benchmark tool:  {args.tool}")
    print(f"  Terminals:       {args.terminals}")
    print(f"  Duration:        {args.duration}s")
    print()
    
    # Print results table
    print_results_table(args.engines, results)
    
    # Find fastest engine
    if len(args.engines) >= 2:
        fastest_engine = None
        fastest_throughput = 0
        for engine in args.engines:
            if results[engine].success and results[engine].throughput != "N/A":
                try:
                    tp = float(results[engine].throughput)
                    if tp > fastest_throughput:
                        fastest_throughput = tp
                        fastest_engine = engine
                except ValueError:
                    pass
        
        if fastest_engine:
            print()
            print(f"{Colors.YELLOW}  ⚡ Fastest engine: {fastest_engine} ({fastest_throughput:.2f} req/s){Colors.NC}")
    
    print()
    
    if replication_ok:
        print_success("Data replication: VERIFIED")
    else:
        print_error("Data replication: ISSUES DETECTED")
    
    print()
    print("Result files:")
    root_dir = get_root_dir()
    for engine in args.engines:
        if results[engine].success:
            print(f"  {engine}: {root_dir}/bench/results/{engine}/gr_test/")
    
    print()
    
    # Determine overall success
    all_passed = all(results[e].success for e in args.engines) and replication_ok
    
    if all_passed:
        print(f"{Colors.GREEN}╔════════════════════════════════════════════════════════════╗{Colors.NC}")
        print(f"{Colors.GREEN}║              ALL TESTS PASSED SUCCESSFULLY!                ║{Colors.NC}")
        print(f"{Colors.GREEN}╚════════════════════════════════════════════════════════════╝{Colors.NC}")
        sys.exit(0)
    else:
        print(f"{Colors.RED}╔════════════════════════════════════════════════════════════╗{Colors.NC}")
        print(f"{Colors.RED}║              SOME TESTS FAILED - CHECK ABOVE               ║{Colors.NC}")
        print(f"{Colors.RED}╚════════════════════════════════════════════════════════════╝{Colors.NC}")
        sys.exit(1)


if __name__ == "__main__":
    main()

