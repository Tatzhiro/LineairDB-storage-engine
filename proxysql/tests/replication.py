#!/usr/bin/env python3
"""
Replication Test for LineairDB Storage Engine via ProxySQL

This test verifies that data written to the primary is properly replicated
to replica nodes when using the LineairDB storage engine.

Usage:
    python3 replication.py
    python3 replication.py --verbose
    python3 replication.py --engine InnoDB  # for comparison

Requirements:
    - ProxySQL must be running and configured (run scripts/setup_proxysql.sh first)
    - MySQL replication must be configured between primary and replicas
    - LineairDB storage engine must be available
"""

import argparse
import sys
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import mysql.connector
from mysql.connector import Error as MySQLError

# Import shared configuration
from config import TestConfig as BaseTestConfig, check_lineairdb_available, verify_table_engine


# -----------------------------
# MySQL connection helpers
# -----------------------------
def mysql_connect(cfg: Dict[str, Any], database: Optional[str] = None):
    """Create a MySQL connection with sensible defaults."""
    allowed_keys = {
        "host", "port", "user", "password", "database",
        "connection_timeout", "use_pure", "autocommit", "ssl_disabled",
        "read_timeout", "write_timeout",
    }
    c = {k: v for k, v in cfg.items() if k in allowed_keys}
    c.setdefault("use_pure", True)
    c.setdefault("autocommit", True)
    c.setdefault("ssl_disabled", True)
    c.setdefault("connection_timeout", 10)
    c.setdefault("read_timeout", 30)
    c.setdefault("write_timeout", 30)
    if database:
        c["database"] = database
    return mysql.connector.connect(**c)


def mysql_query_one(cfg: Dict[str, Any], sql: str, params=None, database: Optional[str] = None):
    """Execute a query and return the first row."""
    conn = mysql_connect(cfg, database=database)
    try:
        cur = conn.cursor()
        cur.execute(sql, params or ())
        return cur.fetchone()
    finally:
        cur.close()
        conn.close()


def mysql_query_all(cfg: Dict[str, Any], sql: str, params=None, database: Optional[str] = None):
    """Execute a query and return all rows."""
    conn = mysql_connect(cfg, database=database)
    try:
        cur = conn.cursor()
        cur.execute(sql, params or ())
        return cur.fetchall()
    finally:
        cur.close()
        conn.close()


def mysql_exec(cfg: Dict[str, Any], sql: str, params=None, database: Optional[str] = None):
    """Execute a statement (INSERT, UPDATE, etc.)."""
    conn = mysql_connect(cfg, database=database)
    try:
        cur = conn.cursor()
        cur.execute(sql, params or ())
        return cur.lastrowid
    finally:
        cur.close()
        conn.close()


# -----------------------------
# Configuration
# -----------------------------
@dataclass
class TestConfig(BaseTestConfig):
    """Test configuration for replication tests."""
    db_name: str = "replication_test"
    table_name: str = field(default_factory=lambda: f"items_{uuid.uuid4().hex[:8]}")


# -----------------------------
# Replication Status Checker
# -----------------------------
class ReplicationChecker:
    """Check replication status on backend nodes."""
    
    def __init__(self, config: TestConfig, verbose: bool = False):
        self.config = config
        self.verbose = verbose
    
    def check_storage_engine_available(self, cfg: Dict[str, Any], engine: str) -> Tuple[bool, str]:
        """Check if a storage engine is available on a MySQL instance."""
        try:
            rows = mysql_query_all(cfg, "SHOW ENGINES")
            for row in rows:
                if row[0].upper() == engine.upper():
                    support = row[1]
                    return support in ("YES", "DEFAULT"), support
            return False, "NOT FOUND"
        except MySQLError as e:
            return False, f"ERROR: {e}"
    
    def get_replication_status(self, replica_host: str) -> Dict[str, Any]:
        """Get replication status from a replica node."""
        cfg = self.config.replica_cfg(replica_host)
        try:
            row = mysql_query_one(cfg, "SHOW REPLICA STATUS")
            if not row:
                row = mysql_query_one(cfg, "SHOW SLAVE STATUS")
            
            if not row:
                return {"error": "No replication configured", "io_running": False, "sql_running": False}
            
            conn = mysql_connect(cfg)
            cur = conn.cursor()
            cur.execute("SHOW REPLICA STATUS")
            columns = [desc[0] for desc in cur.description] if cur.description else []
            row = cur.fetchone()
            cur.close()
            conn.close()
            
            if not row or not columns:
                return {"error": "Could not parse replication status"}
            
            status = dict(zip(columns, row))
            return {
                "io_running": status.get("Replica_IO_Running", "No") == "Yes",
                "sql_running": status.get("Replica_SQL_Running", "No") == "Yes",
                "seconds_behind": status.get("Seconds_Behind_Source"),
                "last_error": status.get("Last_SQL_Error", ""),
                "last_errno": status.get("Last_SQL_Errno", 0),
                "source_log_file": status.get("Source_Log_File", ""),
                "exec_source_log_pos": status.get("Exec_Source_Log_Pos", 0),
                "retrieved_gtid_set": status.get("Retrieved_Gtid_Set", ""),
                "executed_gtid_set": status.get("Executed_Gtid_Set", ""),
            }
        except MySQLError as e:
            return {"error": str(e), "io_running": False, "sql_running": False}
    
    def print_replication_status(self):
        """Print replication status for all replicas."""
        print("\n" + "=" * 60)
        print("Replication Status")
        print("=" * 60)
        
        for replica_host in self.config.replica_hosts:
            print(f"\n  Replica: {replica_host}")
            status = self.get_replication_status(replica_host)
            
            if "error" in status and status.get("io_running") is False:
                print(f"    ❌ Error: {status['error']}")
                continue
            
            io_status = "✅ Yes" if status["io_running"] else "❌ No"
            sql_status = "✅ Yes" if status["sql_running"] else "❌ No"
            
            print(f"    IO Running:  {io_status}")
            print(f"    SQL Running: {sql_status}")
            
            if status["seconds_behind"] is not None:
                print(f"    Seconds Behind: {status['seconds_behind']}")
            
            if status["last_errno"] and status["last_errno"] != 0:
                print(f"    ⚠️  Last Error ({status['last_errno']}): {status['last_error'][:80]}")
    
    def check_engine_on_all_nodes(self, engine: str):
        """Check if storage engine is available on primary and all replicas."""
        print("\n" + "=" * 60)
        print(f"Storage Engine Availability: {engine}")
        print("=" * 60)
        
        # Check primary
        print(f"\n  Primary ({self.config.primary_host}):")
        available, support = self.check_storage_engine_available(
            self.config.primary_cfg(), engine
        )
        status = "✅ Available" if available else f"❌ {support}"
        print(f"    {engine}: {status}")
        
        all_available = available
        
        # Check replicas
        for replica_host in self.config.replica_hosts:
            print(f"\n  Replica ({replica_host}):")
            try:
                available, support = self.check_storage_engine_available(
                    self.config.replica_cfg(replica_host), engine
                )
                status = "✅ Available" if available else f"❌ {support}"
                print(f"    {engine}: {status}")
                all_available = all_available and available
            except MySQLError as e:
                print(f"    ❌ Connection error: {e}")
                all_available = False
        
        return all_available


# -----------------------------
# Replication Tester
# -----------------------------
class ReplicationTester:
    """Test data replication with LineairDB storage engine."""
    
    def __init__(self, config: TestConfig, verbose: bool = False):
        self.config = config
        self.verbose = verbose
        self.checker = ReplicationChecker(config, verbose)
    
    def setup_schema(self) -> bool:
        """Create test database and table with specified storage engine."""
        cfg = self.config.proxysql_cfg()
        engine = self.config.storage_engine
        
        print(f"\n[1] Setting up schema with {engine} storage engine...")
        
        try:
            # Create database
            mysql_exec(cfg, f"CREATE DATABASE IF NOT EXISTS {self.config.db_name}")
            
            # Drop existing table to ensure clean state
            mysql_exec(cfg, f"DROP TABLE IF EXISTS {self.config.db_name}.{self.config.table_name}")
            
            # Create table and verify engine in same connection (transaction pins to writer)
            conn = mysql_connect(cfg)
            try:
                conn.autocommit = False  # Use transaction to pin to writer
                cur = conn.cursor()
                
                # Create table with specified engine
                create_sql = f"""
                    CREATE TABLE {self.config.db_name}.{self.config.table_name} (
                        id VARCHAR(36) PRIMARY KEY,
                        value VARCHAR(255),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    ) ENGINE={engine}
                """
                cur.execute(create_sql)
                
                # Verify engine in same connection (before commit, still on writer)
                cur.execute(
                    "SELECT ENGINE FROM information_schema.TABLES "
                    "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
                    (self.config.db_name, self.config.table_name)
                )
                row = cur.fetchone()
                actual_engine = row[0] if row else 'UNKNOWN'
                
                conn.commit()
                
            finally:
                cur.close()
                conn.close()
            
            if actual_engine.upper() == engine.upper():
                print(f"    ✅ Table created with {actual_engine} engine")
                return True
            else:
                print(f"    ⚠️  Table created with {actual_engine} instead of {engine}")
                print(f"       This may indicate {engine} is not available.")
                return False
                
        except MySQLError as e:
            print(f"    ❌ Schema setup failed: {e}")
            return False
    
    def write_test_data(self, num_rows: int = 5) -> List[Tuple[str, str]]:
        """Write test data through ProxySQL (should go to primary)."""
        cfg = self.config.proxysql_cfg()
        written_data = []
        
        print(f"\n[2] Writing {num_rows} rows through ProxySQL...")
        
        for i in range(num_rows):
            row_id = f"test_{uuid.uuid4().hex[:12]}"
            value = f"value_{uuid.uuid4().hex[:8]}"
            
            try:
                mysql_exec(
                    cfg,
                    f"INSERT INTO {self.config.db_name}.{self.config.table_name} (id, value) VALUES (%s, %s)",
                    (row_id, value),
                )
                written_data.append((row_id, value))
                if self.verbose:
                    print(f"    ✅ Wrote: {row_id} = {value}")
            except MySQLError as e:
                print(f"    ❌ Write failed: {e}")
        
        print(f"    ✅ Wrote {len(written_data)} rows to primary")
        return written_data
    
    def verify_on_primary(self, written_data: List[Tuple[str, str]]) -> int:
        """Verify data exists on primary (direct connection)."""
        cfg = self.config.primary_cfg()
        found = 0
        
        print(f"\n[3] Verifying data on primary ({self.config.primary_host})...")
        
        for row_id, expected_value in written_data:
            try:
                row = mysql_query_one(
                    cfg,
                    f"SELECT value FROM {self.config.db_name}.{self.config.table_name} WHERE id = %s",
                    (row_id,),
                )
                if row and row[0] == expected_value:
                    found += 1
                    if self.verbose:
                        print(f"    ✅ Found: {row_id}")
                else:
                    print(f"    ❌ Missing or wrong value for: {row_id}")
            except MySQLError as e:
                print(f"    ❌ Query failed: {e}")
        
        status = "✅" if found == len(written_data) else "❌"
        print(f"    {status} Found {found}/{len(written_data)} rows on primary")
        return found
    
    def verify_on_replicas(self, written_data: List[Tuple[str, str]], 
                           wait_seconds: float = 5.0,
                           poll_interval: float = 0.5) -> Dict[str, int]:
        """Verify data is replicated to all replicas."""
        results = {}
        
        print(f"\n[4] Verifying replication to replicas (waiting up to {wait_seconds}s)...")
        
        for replica_host in self.config.replica_hosts:
            cfg = self.config.replica_cfg(replica_host)
            print(f"\n    Replica: {replica_host}")
            
            found = 0
            elapsed = 0
            last_found = -1
            
            while elapsed < wait_seconds:
                found = 0
                table_exists = True
                
                for row_id, expected_value in written_data:
                    try:
                        row = mysql_query_one(
                            cfg,
                            f"SELECT value FROM {self.config.db_name}.{self.config.table_name} WHERE id = %s",
                            (row_id,),
                        )
                        if row and row[0] == expected_value:
                            found += 1
                    except MySQLError as e:
                        if "1049" in str(e) or "doesn't exist" in str(e):
                            table_exists = False
                            break
                
                if found == len(written_data):
                    break
                
                if found != last_found:
                    if self.verbose:
                        if not table_exists:
                            print(f"      [{elapsed:.1f}s] Table not yet replicated...")
                        else:
                            print(f"      [{elapsed:.1f}s] Found {found}/{len(written_data)} rows...")
                    last_found = found
                
                time.sleep(poll_interval)
                elapsed += poll_interval
            
            results[replica_host] = found
            
            if found == len(written_data):
                print(f"      ✅ All {found} rows replicated (took ~{elapsed:.1f}s)")
            elif found > 0:
                print(f"      ⚠️  Partial replication: {found}/{len(written_data)} rows")
            else:
                print(f"      ❌ No data replicated after {wait_seconds}s")
                
                status = self.checker.get_replication_status(replica_host)
                if not status.get("sql_running", True):
                    print(f"      ⚠️  Replication SQL thread is STOPPED!")
                    if status.get("last_errno"):
                        print(f"         Error {status['last_errno']}: {status['last_error'][:60]}...")
        
        return results
    
    def verify_via_proxysql_reads(self, written_data: List[Tuple[str, str]],
                                   iterations: int = 10) -> Dict[str, Any]:
        """
        Verify data can be read through ProxySQL (which routes to replicas).
        """
        cfg = self.config.proxysql_cfg()
        
        print(f"\n[5] Testing reads through ProxySQL ({iterations} iterations)...")
        
        consistent_reads = 0
        stale_reads = 0
        backends_hit = {}
        
        for i in range(iterations):
            all_found = True
            
            for row_id, expected_value in written_data:
                try:
                    row = mysql_query_one(
                        cfg,
                        f"SELECT value FROM {self.config.db_name}.{self.config.table_name} WHERE id = %s",
                        (row_id,),
                    )
                    if not row or row[0] != expected_value:
                        all_found = False
                        break
                except MySQLError:
                    all_found = False
                    break
            
            try:
                backend_row = mysql_query_one(cfg, "SELECT @@hostname")
                backend = backend_row[0] if backend_row else "unknown"
                backends_hit[backend] = backends_hit.get(backend, 0) + 1
            except Exception:
                pass
            
            if all_found:
                consistent_reads += 1
            else:
                stale_reads += 1
        
        consistency_rate = (consistent_reads / iterations) * 100
        
        print(f"    Consistent reads: {consistent_reads}/{iterations} ({consistency_rate:.1f}%)")
        print(f"    Backends hit: {backends_hit}")
        
        if consistency_rate == 100:
            print(f"    ✅ All reads returned correct data")
        elif consistency_rate > 0:
            print(f"    ⚠️  Some reads returned stale data")
        else:
            print(f"    ❌ All reads returned stale/missing data")
        
        return {
            "consistent_reads": consistent_reads,
            "stale_reads": stale_reads,
            "consistency_rate": consistency_rate,
            "backends_hit": backends_hit,
        }
    
    def run(self, wait_seconds: float = 5.0) -> int:
        """Run the full replication test."""
        print("=" * 60)
        print(f"LineairDB Replication Test via ProxySQL")
        print(f"Storage Engine: {self.config.storage_engine}")
        print("=" * 60)
        
        # Check LineairDB availability
        if self.config.storage_engine.upper() == "LINEAIRDB":
            print("\n[Prereq] Checking LineairDB availability...")
            if not check_lineairdb_available(self.config.proxysql_cfg()):
                print("  ❌ LineairDB storage engine is NOT available!")
                print("  Please ensure you're running the LineairDB-enabled MySQL build.")
                return 1
            print("  ✅ LineairDB storage engine is available")
        
        # Check storage engine availability
        self.checker.check_engine_on_all_nodes(self.config.storage_engine)
        
        # Check replication status
        self.checker.print_replication_status()
        
        # Setup schema
        if not self.setup_schema():
            print("\n❌ Schema setup failed. Cannot proceed with test.")
            return 1
        
        # Write test data
        written_data = self.write_test_data(num_rows=5)
        if not written_data:
            print("\n❌ No data written. Cannot proceed with test.")
            return 1
        
        # Verify on primary
        primary_count = self.verify_on_primary(written_data)
        if primary_count != len(written_data):
            print("\n❌ Data not fully written to primary!")
            return 1
        
        # Verify replication to replicas
        replica_results = self.verify_on_replicas(written_data, wait_seconds=wait_seconds)
        
        # Test reads through ProxySQL
        proxysql_results = self.verify_via_proxysql_reads(written_data)
        
        # Summary
        print("\n" + "=" * 60)
        print("Summary")
        print("=" * 60)
        
        all_replicated = all(
            count == len(written_data) 
            for count in replica_results.values()
        )
        
        if all_replicated and proxysql_results["consistency_rate"] == 100:
            print(f"""
✅ Replication test PASSED!

- Storage Engine: {self.config.storage_engine}
- Data written to primary: {len(written_data)} rows
- All replicas received data: Yes
- ProxySQL read consistency: {proxysql_results['consistency_rate']:.1f}%

Replication with {self.config.storage_engine} is working correctly.
""")
            return 0
        else:
            failed_replicas = [
                host for host, count in replica_results.items()
                if count < len(written_data)
            ]
            print(f"""
❌ Replication test FAILED!

- Storage Engine: {self.config.storage_engine}
- Data written to primary: {len(written_data)} rows
- Failed replicas: {failed_replicas or 'None'}
- ProxySQL read consistency: {proxysql_results['consistency_rate']:.1f}%

Possible causes:
1. Replication is not configured or broken
2. {self.config.storage_engine} engine doesn't support binary log replication
3. Replication SQL thread is stopped (check SHOW REPLICA STATUS)

Run with --verbose for more details.
""")
            return 1


# -----------------------------
# Main
# -----------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Test replication with LineairDB storage engine via ProxySQL",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Test with LineairDB engine (default)
    python3 replication.py
    
    # Test with InnoDB for comparison
    python3 replication.py --engine InnoDB
    
    # Verbose output
    python3 replication.py --verbose
    
    # Longer wait for slow replication
    python3 replication.py --wait 30

This test requires LineairDB storage engine to be available when testing
with the default engine.
"""
    )
    parser.add_argument(
        "--engine", "-e",
        default="LINEAIRDB",
        help="Storage engine to test (default: LINEAIRDB)"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Print detailed output"
    )
    parser.add_argument(
        "--wait", "-w",
        type=float,
        default=5.0,
        help="Max seconds to wait for replication (default: 5)"
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
    parser.add_argument(
        "--db-name",
        default="replication_test",
        help="Test database name (default: replication_test)"
    )
    args = parser.parse_args()
    
    config = TestConfig(
        proxysql_host=args.proxysql_host,
        proxysql_port=args.proxysql_port,
        db_name=args.db_name,
        storage_engine=args.engine.upper(),
    )
    
    tester = ReplicationTester(config, verbose=args.verbose)
    return tester.run(wait_seconds=args.wait)


if __name__ == "__main__":
    sys.exit(main())
