#!/usr/bin/env python3
"""
GTID Causal Read Test for ProxySQL with LineairDB Storage Engine

This test verifies GTID-based causal consistency reads work correctly
when using the LineairDB storage engine.

Note: Python MySQL connectors (mysql.connector, PyMySQL) do NOT properly support
the SESSION_TRACK protocol required for GTID causal reads. Therefore, this test
uses the MySQL CLI which properly implements the protocol.

Usage:
    python3 gtid_causal_read_test.py
    python3 gtid_causal_read_test.py --iterations 50
    python3 gtid_causal_read_test.py --verbose
"""

import argparse
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

# Import shared configuration
from config import TestConfig as BaseTestConfig, check_lineairdb_available, verify_table_engine


@dataclass
class TestConfig(BaseTestConfig):
    """Test configuration for GTID causal read tests."""
    db_name: str = "gtid_causal_test"
    table_name: str = ""
    
    def __post_init__(self):
        super().__post_init__()
        # Use a unique table name with timestamp to avoid storage engine conflicts
        if not self.table_name:
            self.table_name = f"gtid_t{int(time.time()) % 100000}"


def run_mysql_cli(config: TestConfig, sql: str, use_proxysql: bool = True) -> Tuple[int, str, str]:
    """
    Run SQL using MySQL CLI (which properly supports GTID session tracking).
    
    Returns: (exit_code, stdout, stderr)
    """
    if use_proxysql:
        host = config.proxysql_host
        port = config.proxysql_port
        user = config.proxysql_user
        password = config.proxysql_pass
    else:
        host = config.admin_host
        port = config.admin_port
        user = config.admin_user
        password = config.admin_pass
    
    cmd = [
        "mysql",
        f"-h{host}",
        f"-P{port}",
        f"-u{user}",
        f"-p{password}",
        "-N",  # No column names
        "-B",  # Batch mode (tab-separated)
        "-e", sql
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    return result.returncode, result.stdout.strip(), result.stderr


def run_admin_sql(config: TestConfig, sql: str) -> str:
    """Run SQL on ProxySQL admin interface."""
    _, stdout, _ = run_mysql_cli(config, sql, use_proxysql=False)
    return stdout


def check_gtid_config(config: TestConfig) -> Dict[str, Any]:
    """Check if GTID causal reads are properly configured."""
    result = {
        "binlog_readers_ok": False,
        "gtid_tracking_enabled": False,
        "gtid_query_rules": False,
        "gtid_executed_data": False,
    }
    
    # Check binlog reader connectivity
    servers_sql = "SELECT hostname, gtid_port FROM runtime_mysql_servers"
    output = run_admin_sql(config, servers_sql)
    
    binlog_ok = True
    for line in output.split('\n'):
        if line.strip():
            parts = line.split('\t')
            if len(parts) >= 2:
                hostname, gtid_port = parts[0], parts[1]
                if gtid_port == '0' or gtid_port == 'NULL':
                    binlog_ok = False
    result["binlog_readers_ok"] = binlog_ok
    
    # Check GTID tracking variables
    vars_sql = """
    SELECT variable_value FROM global_variables 
    WHERE variable_name = 'mysql-client_session_track_gtid'
    """
    output = run_admin_sql(config, vars_sql)
    result["gtid_tracking_enabled"] = output.strip() in ('1', 'true')
    
    # Check GTID query rules
    rules_sql = """
    SELECT COUNT(*) FROM runtime_mysql_query_rules 
    WHERE gtid_from_hostgroup IS NOT NULL
    """
    output = run_admin_sql(config, rules_sql)
    result["gtid_query_rules"] = int(output.strip() or 0) > 0
    
    # Check GTID executed data
    gtid_sql = "SELECT COUNT(*) FROM stats_mysql_gtid_executed"
    output = run_admin_sql(config, gtid_sql)
    result["gtid_executed_data"] = int(output.strip() or 0) > 0
    
    return result


def setup_schema(config: TestConfig) -> bool:
    """Create test database and table using LineairDB storage engine."""
    print(f"  Table name: {config.table_name}")
    print(f"  Storage engine: {config.storage_engine}")
    
    # Create database
    sql = f"CREATE DATABASE IF NOT EXISTS {config.db_name};"
    run_mysql_cli(config, sql)
    
    # Create table and verify in a single transaction (pins to writer)
    # Using START TRANSACTION ensures both CREATE and SELECT go to same backend
    combined_sql = f"""
    START TRANSACTION;
    DROP TABLE IF EXISTS {config.db_name}.{config.table_name};
    CREATE TABLE {config.db_name}.{config.table_name} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        test_id VARCHAR(60) NOT NULL UNIQUE,
        value VARCHAR(100) NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    ) ENGINE={config.storage_engine};
    SELECT ENGINE FROM information_schema.TABLES 
    WHERE TABLE_SCHEMA = '{config.db_name}' AND TABLE_NAME = '{config.table_name}';
    COMMIT;
    """
    exit_code, stdout, stderr = run_mysql_cli(config, combined_sql)
    
    if exit_code != 0:
        print(f"Schema setup failed: {stderr}")
        return False
    
    # Parse engine from output
    engine = stdout.strip().split('\n')[-1] if stdout.strip() else 'UNKNOWN'
    
    if engine.upper() != "LINEAIRDB":
        print(f"  WARNING: Table created with {engine} instead of LineairDB")
        print(f"  This test requires LineairDB storage engine.")
        return False
    else:
        print(f"  ✅ Using LineairDB storage engine")
    
    return True


def test_single_connection_gtid(config: TestConfig, iterations: int, verbose: bool) -> Dict[str, Any]:
    """
    Test GTID causal reads using MySQL CLI with a single connection.
    """
    print(f"\n=== Single Connection GTID Test ({iterations} iterations) ===")
    print("  (Using MySQL CLI with stored procedure for proper GTID tracking)\n")
    
    results = []
    consistent = 0
    stale = 0
    
    for i in range(iterations):
        test_id = f"gtid_test_{uuid.uuid4().hex[:16]}"
        test_val = f"val_{uuid.uuid4().hex[:8]}"
        
        # Single session write-then-read test
        sql = f"""
        SET SESSION session_track_gtids = OWN_GTID;
        INSERT INTO {config.db_name}.{config.table_name} (test_id, value) VALUES ('{test_id}', '{test_val}');
        SELECT value, @@hostname AS host FROM {config.db_name}.{config.table_name} WHERE test_id = '{test_id}';
        """
        
        exit_code, stdout, _ = run_mysql_cli(config, sql)
        
        if exit_code == 0 and stdout:
            lines = [l for l in stdout.split('\n') if l.strip()]
            if lines:
                # Parse result: value\thost
                last_line = lines[-1]
                parts = last_line.split('\t')
                if len(parts) >= 2:
                    read_val, host = parts[0], parts[1]
                    is_consistent = (read_val == test_val)
                    
                    if is_consistent:
                        consistent += 1
                        if verbose:
                            print(f"  ✅ {i+1}: Consistent (read from {host})")
                    else:
                        stale += 1
                        if verbose:
                            print(f"  ❌ {i+1}: STALE - wrote '{test_val}', read '{read_val}' from {host}")
                    
                    results.append({
                        "iteration": i + 1,
                        "consistent": is_consistent,
                        "host": host,
                    })
                    continue
        
        # If we get here, something went wrong
        stale += 1
        if verbose:
            print(f"  ❌ {i+1}: ERROR - could not verify")
    
    consistency_rate = (consistent / iterations * 100) if iterations > 0 else 0
    
    return {
        "test_type": "single_connection_mysql_cli",
        "iterations": iterations,
        "consistent": consistent,
        "stale": stale,
        "consistency_rate": consistency_rate,
        "results": results,
    }


def test_rapid_burst(config: TestConfig, burst_size: int, num_bursts: int, verbose: bool) -> Dict[str, Any]:
    """
    Test rapid bursts of write-then-read operations.
    """
    print(f"\n=== Rapid Burst Test ({num_bursts} bursts x {burst_size} ops) ===\n")
    
    total_consistent = 0
    total_stale = 0
    
    for burst in range(num_bursts):
        burst_consistent = 0
        burst_stale = 0
        
        for i in range(burst_size):
            test_id = f"burst_{burst}_{i}_{uuid.uuid4().hex[:8]}"
            test_val = f"v_{uuid.uuid4().hex[:8]}"
            
            sql = f"""
            SET SESSION session_track_gtids = OWN_GTID;
            INSERT INTO {config.db_name}.{config.table_name} (test_id, value) VALUES ('{test_id}', '{test_val}');
            SELECT value FROM {config.db_name}.{config.table_name} WHERE test_id = '{test_id}';
            """
            
            _, stdout, _ = run_mysql_cli(config, sql)
            lines = [l for l in stdout.split('\n') if l.strip()]
            
            if lines and lines[-1] == test_val:
                burst_consistent += 1
            else:
                burst_stale += 1
        
        total_consistent += burst_consistent
        total_stale += burst_stale
        
        burst_rate = burst_consistent / burst_size * 100
        status = "✅" if burst_stale == 0 else "⚠️"
        print(f"  {status} Burst {burst + 1}: {burst_rate:.0f}% consistent ({burst_stale} stale)")
    
    total = num_bursts * burst_size
    consistency_rate = total_consistent / total * 100 if total > 0 else 0
    
    return {
        "test_type": "burst",
        "total_operations": total,
        "consistent": total_consistent,
        "stale": total_stale,
        "consistency_rate": consistency_rate,
    }


def test_backend_distribution(config: TestConfig, iterations: int, verbose: bool) -> Dict[str, Any]:
    """
    Test that reads are actually being distributed to replicas (not just primary).
    """
    print(f"\n=== Backend Distribution Test ({iterations} iterations) ===\n")
    
    backend_counts = {}
    consistent = 0
    
    for i in range(iterations):
        test_id = f"dist_{uuid.uuid4().hex[:16]}"
        test_val = f"d_{uuid.uuid4().hex[:8]}"
        
        sql = f"""
        SET SESSION session_track_gtids = OWN_GTID;
        INSERT INTO {config.db_name}.{config.table_name} (test_id, value) VALUES ('{test_id}', '{test_val}');
        SELECT value, @@hostname FROM {config.db_name}.{config.table_name} WHERE test_id = '{test_id}';
        """
        
        _, stdout, _ = run_mysql_cli(config, sql)
        lines = [l for l in stdout.split('\n') if l.strip()]
        
        if lines:
            parts = lines[-1].split('\t')
            if len(parts) >= 2:
                read_val, host = parts[0], parts[1]
                backend_counts[host] = backend_counts.get(host, 0) + 1
                if read_val == test_val:
                    consistent += 1
    
    print("  Backend distribution:")
    for host, count in sorted(backend_counts.items()):
        pct = count / iterations * 100
        role = "PRIMARY" if "01" in host else "REPLICA"
        print(f"    {host} ({role}): {count} queries ({pct:.1f}%)")
    
    # Check if replicas are being used
    replica_queries = sum(c for h, c in backend_counts.items() if "01" not in h)
    replica_pct = replica_queries / iterations * 100 if iterations > 0 else 0
    
    return {
        "test_type": "distribution",
        "iterations": iterations,
        "consistent": consistent,
        "consistency_rate": consistent / iterations * 100 if iterations > 0 else 0,
        "backend_counts": backend_counts,
        "replica_percentage": replica_pct,
    }


def print_gtid_status(config: TestConfig):
    """Print current GTID configuration status."""
    print("\n=== GTID Causal Read Configuration ===")
    
    gtid_config = check_gtid_config(config)
    
    all_ok = all(gtid_config.values())
    status = "✅ CONFIGURED" if all_ok else "⚠️ INCOMPLETE"
    print(f"  Status: {status}")
    print(f"  Binlog Readers: {'✅' if gtid_config['binlog_readers_ok'] else '❌'}")
    print(f"  GTID Tracking: {'✅' if gtid_config['gtid_tracking_enabled'] else '❌'}")
    print(f"  GTID Query Rules: {'✅' if gtid_config['gtid_query_rules'] else '❌'}")
    print(f"  GTID Executed Data: {'✅' if gtid_config['gtid_executed_data'] else '❌'}")
    
    if not all_ok:
        print("\n  To configure GTID causal reads, run:")
        print("    sudo ../scripts/enable_gtid_causal_read.sh")
    
    return gtid_config


def main():
    parser = argparse.ArgumentParser(
        description="Test GTID-based causal consistency reads in ProxySQL with LineairDB",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Note: This test uses MySQL CLI instead of Python MySQL connectors because
Python connectors don't properly support the SESSION_TRACK protocol required
for GTID causal reads.

This test requires LineairDB storage engine to be available.
        """
    )
    parser.add_argument(
        "--iterations", "-n",
        type=int,
        default=30,
        help="Number of iterations for each test (default: 30)"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Print detailed output for each iteration"
    )
    parser.add_argument(
        "--skip-burst",
        action="store_true",
        help="Skip the burst test"
    )
    parser.add_argument(
        "--proxysql-host",
        default="127.0.0.1",
        help="ProxySQL host (default: 127.0.0.1)"
    )
    parser.add_argument(
        "--proxysql-port",
        type=int,
        default=6033,
        help="ProxySQL client port (default: 6033)"
    )
    args = parser.parse_args()
    
    config = TestConfig(
        proxysql_host=args.proxysql_host,
        proxysql_port=args.proxysql_port,
    )
    
    print("=" * 60)
    print("GTID Causal Read Test (LineairDB Storage Engine)")
    print("=" * 60)
    print("\nUsing MySQL CLI for proper GTID session tracking support.")
    print(f"Storage Engine: {config.storage_engine}")
    
    # Check LineairDB availability
    print("\n[Prereq] Checking LineairDB availability...")
    if not check_lineairdb_available(config.proxysql_cfg()):
        print("  ❌ LineairDB storage engine is NOT available!")
        print("  Please ensure you're running the LineairDB-enabled MySQL build.")
        return 1
    print("  ✅ LineairDB storage engine is available")
    
    # Check configuration
    gtid_config = print_gtid_status(config)
    
    if not all(gtid_config.values()):
        print("\n⚠️  GTID causal reads may not work properly.")
        print("   Proceeding with tests anyway...\n")
    
    # Setup schema
    print("\n[Setup] Creating test schema...")
    if not setup_schema(config):
        print("❌ Failed to setup schema with LineairDB")
        return 1
    print("  ✅ Schema ready")
    
    # Wait for schema replication
    print("  Waiting for schema to replicate...")
    time.sleep(1)
    
    # Run tests
    results = {}
    
    # Test 1: Single connection GTID test
    results["single_connection"] = test_single_connection_gtid(
        config, args.iterations, args.verbose
    )
    
    # Test 2: Burst test
    if not args.skip_burst:
        results["burst"] = test_rapid_burst(
            config, burst_size=10, num_bursts=5, verbose=args.verbose
        )
    
    # Test 3: Backend distribution
    results["distribution"] = test_backend_distribution(
        config, args.iterations, args.verbose
    )
    
    # Summary
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    
    sc = results["single_connection"]
    print(f"\n  Single Connection Test:")
    print(f"    Consistency: {sc['consistency_rate']:.1f}% ({sc['consistent']}/{sc['iterations']})")
    
    if "burst" in results:
        bt = results["burst"]
        print(f"\n  Burst Test:")
        print(f"    Consistency: {bt['consistency_rate']:.1f}% ({bt['consistent']}/{bt['total_operations']})")
    
    dist = results["distribution"]
    print(f"\n  Backend Distribution:")
    print(f"    Consistency: {dist['consistency_rate']:.1f}%")
    print(f"    Replica Usage: {dist['replica_percentage']:.1f}%")
    
    # Overall assessment
    print("\n" + "-" * 60)
    
    overall_consistency = sc['consistency_rate']
    replica_usage = dist['replica_percentage']
    
    if overall_consistency >= 99 and replica_usage > 20:
        print("✅ GTID Causal Reads: WORKING")
        print(f"   {overall_consistency:.1f}% consistency with {replica_usage:.1f}% replica usage")
        print("   Reads are being distributed to replicas while maintaining consistency.")
    elif overall_consistency >= 99 and replica_usage <= 20:
        print("⚠️ GTID Causal Reads: CONSISTENT but PRIMARY-HEAVY")
        print(f"   {overall_consistency:.1f}% consistency but only {replica_usage:.1f}% to replicas")
        print("   Reads are falling back to primary. Check if replicas are lagging.")
    elif overall_consistency >= 90:
        print("⚠️ GTID Causal Reads: PARTIAL")
        print(f"   {overall_consistency:.1f}% consistency")
        print("   Some stale reads detected. May need tuning.")
    else:
        print("❌ GTID Causal Reads: NOT WORKING")
        print(f"   Only {overall_consistency:.1f}% consistency")
        print("   Verify binlog readers are running and gtid_port is configured.")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
