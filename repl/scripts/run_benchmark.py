#!/usr/bin/env python3
"""
Run benchmark with specified storage engine on Group Replication cluster.

This script runs YCSB/TPC-C benchmarks with various storage engines.
Supports: lineairdb, fence, innodb

Usage:
    python3 run_benchmark.py <engine> <benchmark_type> [options]

Arguments:
    engine:         lineairdb | fence | innodb
    benchmark_type: ycsb or tpcc

Options:
    --tool TOOL     Benchmark tool: benchbase (system MySQL) or binbench (local build MySQL)
    --terminals N   Number of concurrent terminals (default: 4)
    --time N        Benchmark duration in seconds (default: 30)
    --debug         Use debug build instead of release (LineairDB only)
    --no-rebuild    Don't rebuild plugin (assume it's already installed)
    --standalone    Run without Group Replication cluster

Examples:
    python3 run_benchmark.py lineairdb ycsb              # LineairDB with FENCE=off
    python3 run_benchmark.py fence ycsb                  # LineairDB with FENCE=on
    python3 run_benchmark.py innodb ycsb                 # MySQL InnoDB
    python3 run_benchmark.py lineairdb ycsb --tool binbench  # Use local build MySQL
    python3 run_benchmark.py lineairdb ycsb --terminals 8 --time 60
"""

import argparse
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from utils import (
    ClusterConfig,
    MYSQL_PASSWORD,
    MYSQL_USER,
    VALID_ENGINES,
    get_engine_description,
    get_mysql_engine_name,
    get_repl_dir,
    get_root_dir,
    is_lineairdb_engine,
    load_config_if_exists,
    mysql_execute,
    mysql_is_running,
    mysql_query,
    mysqlsh_execute,
    print_error,
    print_header,
    print_info,
    print_step,
    print_success,
    print_warning,
    run_command,
)


def generate_benchbase_config(
    write_host: str,
    write_port: int,
    db_name: str,
    terminals: int,
    duration: int,
) -> str:
    """Generate BenchBase configuration XML."""
    return f'''<?xml version="1.0"?>
<parameters>
    <!-- Connection details -->
    <type>MYSQL</type>
    <driver>com.mysql.cj.jdbc.Driver</driver>
    <url>jdbc:mysql://{write_host}:{write_port}/{db_name}?rewriteBatchedStatements=true&amp;sslMode=DISABLED</url>
    <username>root</username>
    <password>kamo</password>
    <isolation>TRANSACTION_SERIALIZABLE</isolation>
    <batchsize>128</batchsize>
    <allowPublicKeyRetrieval>true</allowPublicKeyRetrieval>

    <!-- Scalefactor in YCSB is *1000 the number of rows in the USERTABLE-->
    <scalefactor>1</scalefactor>

    <!-- Workload -->
    <terminals>{terminals}</terminals>
    <works>
        <work>
            <time>{duration}</time>
            <rate>unlimited</rate>
            <weights>50,0,0,50,0,0</weights>
        </work>
    </works>

    <!-- YCSB Procedures declaration -->
    <transactiontypes>
        <transactiontype>
            <name>ReadRecord</name>
        </transactiontype>
        <transactiontype>
            <name>InsertRecord</name>
        </transactiontype>
        <transactiontype>
            <name>ScanRecord</name>
        </transactiontype>
        <transactiontype>
            <name>UpdateRecord</name>
        </transactiontype>
        <transactiontype>
            <name>DeleteRecord</name>
        </transactiontype>
        <transactiontype>
            <name>ReadModifyWriteRecord</name>
        </transactiontype>
    </transactiontypes>
</parameters>
'''


def find_write_endpoint(standalone: bool) -> tuple:
    """
    Find the write endpoint for the cluster.
    
    Returns:
        Tuple of (host, port)
    """
    write_host = "127.0.0.1"
    write_port = 3306
    
    if standalone:
        print("  Using local MySQL (standalone mode)")
        return write_host, write_port
    
    # Check if GR cluster is available
    success, output = mysqlsh_execute("""
    try {
        var c = dba.getCluster();
        var s = c.status();
        print(s.defaultReplicaSet.primary);
    } catch(e) {
        print('null');
    }
    """)
    
    if success and output and output != "null":
        print(f"  GR Primary: {output}")
        
        # Parse the primary host
        primary_host_gr = output.split(":")[0]
        
        # Determine port
        if "secondary" in primary_host_gr:
            try:
                sec_num = int(primary_host_gr.split("-")[-1])
                write_port = 33061 + sec_num
            except ValueError:
                pass
    else:
        print("  Using local MySQL (no GR cluster)")
    
    return write_host, write_port


def run_benchbase(
    engine: str,
    benchmark_type: str,
    write_host: str,
    write_port: int,
    db_name: str,
    terminals: int,
    duration: int,
    timestamp: int,
) -> bool:
    """
    Run benchmark using BenchBase with system MySQL.
    
    Returns:
        True if successful
    """
    root_dir = get_root_dir()
    benchbase_dir = root_dir / "third_party" / "benchbase"
    
    # Generate config
    config_file = Path(f"/tmp/{engine}_{benchmark_type}_gr_test.xml")
    config_content = generate_benchbase_config(
        write_host, write_port, db_name, terminals, duration
    )
    config_file.write_text(config_content)
    
    print(f"Config: {config_file}")
    print(f"Endpoint: {write_host}:{write_port}")
    print(f"Engine: {engine} ({get_engine_description(engine)})")
    print()
    
    # Check if JAR exists
    jar_file = benchbase_dir / "benchbase-mysql" / "benchbase.jar"
    if not jar_file.exists():
        print_error(f"benchbase.jar not found at {jar_file}")
        print("Please build benchbase first")
        return False
    
    # Run benchbase
    result = run_command(
        [
            "java", "-jar", str(jar_file),
            "-b", benchmark_type,
            "-c", str(config_file),
            "--create=true",
            "--load=true",
            "--execute=true",
        ],
        cwd=benchbase_dir,
        capture_output=False,
    )
    
    if result.returncode != 0:
        return False
    
    # Save results
    result_dir = root_dir / "bench" / "results" / engine / "benchbase"
    result_dir.mkdir(parents=True, exist_ok=True)
    
    results_dir = benchbase_dir / "results"
    if results_dir.exists():
        for f in results_dir.iterdir():
            if f.is_file():
                ext = f.suffix
                name = f.stem
                dest = result_dir / f"{name}_{timestamp}{ext}"
                shutil.move(str(f), str(dest))
        
        print()
        print(f"Results saved to: {result_dir}")
    
    return True


def run_binbench(
    engine: str,
    benchmark_type: str,
    write_host: str,
    write_port: int,
    db_name: str,
    terminals: int,
    duration: int,
    timestamp: int,
) -> bool:
    """
    Run benchmark using BenchBase against the current cluster.
    
    NOTE: For true binbench mode (using local build MySQL as primary),
    the cluster must be started with --binbench flag in start_cluster.py.
    This function runs benchmarks against whatever cluster is currently running.
    
    Returns:
        True if successful
    """
    root_dir = get_root_dir()
    benchbase_dir = root_dir / "third_party" / "benchbase"
    
    # Generate config
    config_file = Path(f"/tmp/{engine}_{benchmark_type}_binbench.xml")
    config_content = generate_benchbase_config(
        write_host, write_port, db_name, terminals, duration
    )
    config_file.write_text(config_content)
    
    print(f"Config: {config_file}")
    print(f"Endpoint: {write_host}:{write_port}")
    print(f"Engine: {engine} ({get_engine_description(engine)})")
    print()
    
    # Check if JAR exists
    jar_file = benchbase_dir / "benchbase-mysql" / "benchbase.jar"
    if not jar_file.exists():
        print_error(f"benchbase.jar not found at {jar_file}")
        print("Please build benchbase first")
        return False
    
    # Run benchbase
    result = run_command(
        [
            "java", "-jar", str(jar_file),
            "-b", benchmark_type,
            "-c", str(config_file),
            "--create=true",
            "--load=true",
            "--execute=true",
        ],
        cwd=benchbase_dir,
        capture_output=False,
    )
    
    if result.returncode != 0:
        return False
    
    # Save results to binbench directory
    result_dir = root_dir / "bench" / "results" / engine / "binbench"
    result_dir.mkdir(parents=True, exist_ok=True)
    
    results_dir = benchbase_dir / "results"
    if results_dir.exists():
        for f in results_dir.iterdir():
            if f.is_file():
                ext = f.suffix
                name = f.stem
                dest = result_dir / f"{name}_{timestamp}{ext}"
                shutil.move(str(f), str(dest))
        
        print()
        print(f"Results saved to: {result_dir}")
    
    return True


def main():
    parser = argparse.ArgumentParser(
        description="Run benchmark with specified storage engine",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Supported Engines:
    lineairdb   LineairDB with FENCE=off (async commits)
    fence       LineairDB with FENCE=on (sync commits)
    innodb      MySQL InnoDB storage engine

Examples:
    %(prog)s lineairdb ycsb              # LineairDB with FENCE=off (system MySQL)
    %(prog)s fence ycsb                  # LineairDB with FENCE=on
    %(prog)s innodb ycsb                 # MySQL InnoDB
    %(prog)s lineairdb ycsb --tool binbench  # Use local build MySQL
    %(prog)s lineairdb ycsb --terminals 8 --time 60
        """
    )
    
    parser.add_argument('engine', choices=VALID_ENGINES, help='Storage engine to use')
    parser.add_argument('benchmark_type', default='ycsb', nargs='?', help='Benchmark type (default: ycsb)')
    parser.add_argument('--tool', default='benchbase', choices=['benchbase', 'binbench'], 
                        help='benchbase=system MySQL, binbench=local build MySQL')
    parser.add_argument('--terminals', type=int, default=4, help='Number of concurrent terminals')
    parser.add_argument('--time', type=int, default=30, dest='duration', help='Benchmark duration in seconds')
    parser.add_argument('--debug', action='store_true', help='Use debug build')
    parser.add_argument('--no-rebuild', action='store_true', help="Don't rebuild plugin")
    parser.add_argument('--standalone', action='store_true', help='Run without GR cluster')
    
    args = parser.parse_args()
    
    engine = args.engine
    benchmark_type = args.benchmark_type
    mysql_engine = get_mysql_engine_name(engine)
    engine_desc = get_engine_description(engine)
    build_type = "debug" if args.debug else "release"
    
    # Generate unique database name
    timestamp = int(time.time())
    db_name = f"bench_{engine}_{timestamp}"
    
    print_header("Storage Engine Benchmark")
    
    print(f"Engine:     {engine} ({engine_desc})")
    print(f"MySQL name: {mysql_engine}")
    print(f"Tool:       {args.tool}")
    print(f"Benchmark:  {benchmark_type}")
    if is_lineairdb_engine(engine):
        print(f"Build:      {build_type}")
    print(f"Terminals:  {args.terminals}")
    print(f"Duration:   {args.duration}s")
    print(f"Database:   {db_name}")
    print()
    
    # Step 1: Check MySQL is running (try both passwords for binbench mode)
    print_step("Checking MySQL status...")
    if not mysql_is_running() and not mysql_is_running(password=""):
        print_error("MySQL not available at 127.0.0.1:3306")
        print("Please start MySQL first")
        sys.exit(1)
    print_success("MySQL is running")
    
    # Check GR cluster (optional)
    gr_available = False
    if args.standalone:
        print_info("Running in standalone mode (no Group Replication)")
    else:
        success, _ = mysqlsh_execute("dba.getCluster();")
        if success:
            gr_available = True
            print_success("GR Cluster is available")
        else:
            print_warning("GR Cluster not configured")
    
    # Step 2: Handle plugin/engine setup
    print()
    if is_lineairdb_engine(engine):
        if args.no_rebuild:
            print_step("Skipping plugin rebuild (--no-rebuild flag)")
            print("  Verifying existing plugin...")
        else:
            print_step(f"Installing LineairDB plugin with {engine_desc}...")
            
            # Import and call install_plugin
            script_dir = Path(__file__).parent
            install_cmd = [
                sys.executable, str(script_dir / "install_plugin.py"),
                f"--{engine}",
                f"--{build_type}",
            ]
            run_command(install_cmd, capture_output=False)
        
        # Verify plugin is active
        status = mysql_query(
            "SELECT PLUGIN_STATUS FROM INFORMATION_SCHEMA.PLUGINS WHERE PLUGIN_NAME = 'LINEAIRDB';"
        )
        if status != "ACTIVE":
            print_error("LineairDB plugin not active")
            sys.exit(1)
        print_success("LineairDB plugin is active")
    else:
        print_step(f"Using MySQL built-in engine: {engine}")
        
        # Verify built-in engine is available
        status = mysql_query(
            f"SELECT SUPPORT FROM INFORMATION_SCHEMA.ENGINES WHERE ENGINE = UPPER('{mysql_engine}');"
        )
        if status not in ("YES", "DEFAULT"):
            print_error(f"Engine '{mysql_engine}' not supported (status: {status})")
            sys.exit(1)
        print_success(f"Engine '{mysql_engine}' is available")
    
    # Step 3: Set default storage engine
    print()
    print_step(f"Setting default storage engine to {mysql_engine}...")
    mysql_execute(f"SET GLOBAL default_storage_engine = {mysql_engine};")
    
    # Set on secondaries too
    config = load_config_if_exists()
    if config and not args.standalone:
        for sec in config.secondaries:
            mysql_execute(
                f"SET GLOBAL default_storage_engine = {mysql_engine};",
                host=sec.host, port=sec.port
            )
    
    if args.standalone:
        print_success(f"Default storage engine set to {mysql_engine} (standalone)")
    else:
        print_success(f"Default storage engine set to {mysql_engine} on all nodes")
    
    # Step 4: Find the write endpoint
    print()
    print_step("Finding write endpoint...")
    write_host, write_port = find_write_endpoint(args.standalone)
    print(f"  Write endpoint: {write_host}:{write_port}")
    
    # Step 5: Prepare benchmark database
    print()
    print_step("Preparing benchmark database...")
    mysql_execute(f"DROP DATABASE IF EXISTS {db_name};", host=write_host, port=write_port)
    mysql_execute(f"CREATE DATABASE {db_name};", host=write_host, port=write_port)
    print_success(f"Database '{db_name}' created")
    
    # Step 6: Run benchmark
    print()
    print_step(f"Running {benchmark_type} benchmark with engine={engine} using {args.tool}...")
    print("=" * 42)
    print()
    
    if args.tool == "binbench":
        # binbench runs against the current cluster (results saved to binbench/ directory)
        success = run_binbench(
            engine=engine,
            benchmark_type=benchmark_type,
            write_host=write_host,
            write_port=write_port,
            db_name=db_name,
            terminals=args.terminals,
            duration=args.duration,
            timestamp=timestamp,
        )
    else:
        # benchbase uses system MySQL (GR cluster or standalone)
        success = run_benchbase(
            engine=engine,
            benchmark_type=benchmark_type,
            write_host=write_host,
            write_port=write_port,
            db_name=db_name,
            terminals=args.terminals,
            duration=args.duration,
            timestamp=timestamp,
        )
    
    print()
    print_header("Benchmark Complete!")
    
    # Verify table was created with correct engine
    print("Verifying table engine...")
    table_engine = mysql_query(
        f"SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA='{db_name}' AND TABLE_NAME='usertable';",
        host=write_host, port=write_port
    )
    print(f"  USERTABLE engine: {table_engine}")
    
    expected_engine = mysql_engine.upper()
    actual_engine = table_engine.upper() if table_engine else ""
    
    if actual_engine == expected_engine:
        print()
        print_success(f"SUCCESS: Benchmark completed with engine={engine} ({engine_desc})")
    else:
        print()
        print_warning(f"WARNING: Table was created with engine={table_engine} (expected {mysql_engine})")
    
    print()
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()

