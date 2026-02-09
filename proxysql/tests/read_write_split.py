#!/usr/bin/env python3
"""
Read/Write Split Test for ProxySQL with LineairDB Storage Engine

This test verifies that ProxySQL correctly routes:
- Write queries (INSERT, UPDATE, DELETE) to the primary (hostgroup 0)
- Read queries (SELECT) to replicas (hostgroup 1)

All tests use the LineairDB storage engine.

Usage:
    python3 read_write_split.py
    python3 read_write_split.py --iterations 100
    python3 read_write_split.py --verbose
"""

import argparse
import sys
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
from collections import defaultdict

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
    c.setdefault("connection_timeout", 5)
    c.setdefault("read_timeout", 15)
    c.setdefault("write_timeout", 15)
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
    """Test configuration for read/write split tests."""
    db_name: str = "rw_split_test"
    table_name: str = "test_items"


# -----------------------------
# Query Routing Checker
# -----------------------------
class QueryRoutingChecker:
    """Check which backend handles queries via ProxySQL stats."""
    
    def __init__(self, config: TestConfig):
        self.config = config
    
    def get_backend_servers(self) -> List[Dict[str, Any]]:
        """Get list of backend servers from ProxySQL with hostgroup info."""
        rows = mysql_query_all(
            self.config.admin_cfg(),
            """
            SELECT hostgroup_id, hostname, port, status, weight, max_connections
            FROM runtime_mysql_servers
            ORDER BY hostgroup_id, hostname
            """
        )
        servers = []
        for row in rows:
            servers.append({
                "hostgroup_id": row[0],
                "hostname": row[1],
                "port": row[2],
                "status": row[3],
                "weight": row[4],
                "max_connections": row[5],
            })
        return servers
    
    def get_query_rules(self) -> List[Dict[str, Any]]:
        """Get active query rules from ProxySQL."""
        rows = mysql_query_all(
            self.config.admin_cfg(),
            """
            SELECT rule_id, active, match_pattern, destination_hostgroup, apply
            FROM runtime_mysql_query_rules
            WHERE active = 1
            ORDER BY rule_id
            """
        )
        rules = []
        for row in rows:
            rules.append({
                "rule_id": row[0],
                "active": row[1],
                "match_pattern": row[2],
                "destination_hostgroup": row[3],
                "apply": row[4],
            })
        return rules
    
    def get_connection_pool_stats(self) -> Dict[str, Dict[str, int]]:
        """Get connection pool statistics per server."""
        rows = mysql_query_all(
            self.config.admin_cfg(),
            """
            SELECT hostgroup, srv_host, srv_port, Queries, 
                   Latency_us, ConnUsed, ConnFree
            FROM stats_mysql_connection_pool
            ORDER BY hostgroup, srv_host
            """
        )
        stats = {}
        for row in rows:
            key = f"{row[1]}:{row[2]}"
            stats[key] = {
                "hostgroup": row[0],
                "queries": row[3],
                "latency_us": row[4],
                "conn_used": row[5],
                "conn_free": row[6],
            }
        return stats
    
    def get_query_digest_stats(self) -> List[Dict[str, Any]]:
        """Get recent query digest statistics."""
        rows = mysql_query_all(
            self.config.admin_cfg(),
            """
            SELECT hostgroup, schemaname, username, digest_text, count_star, 
                   sum_time, first_seen, last_seen
            FROM stats_mysql_query_digest
            ORDER BY last_seen DESC
            LIMIT 50
            """
        )
        stats = []
        for row in rows:
            stats.append({
                "hostgroup": row[0],
                "schema": row[1],
                "username": row[2],
                "digest": row[3][:80] if row[3] else "",
                "count": row[4],
                "sum_time": row[5],
            })
        return stats
    
    def reset_query_digest(self):
        """Reset query digest statistics for clean measurement."""
        mysql_query_all(
            self.config.admin_cfg(),
            "SELECT * FROM stats_mysql_query_digest_reset"
        )
    
    def print_backend_status(self):
        """Print current backend server status."""
        print("\n" + "=" * 60)
        print("Backend Server Configuration")
        print("=" * 60)
        
        servers = self.get_backend_servers()
        
        # Group by hostgroup
        by_hostgroup = defaultdict(list)
        for server in servers:
            by_hostgroup[server["hostgroup_id"]].append(server)
        
        for hg_id in sorted(by_hostgroup.keys()):
            role = "WRITER" if hg_id == self.config.writer_hostgroup else "READER"
            print(f"\n  Hostgroup {hg_id} ({role}):")
            for server in by_hostgroup[hg_id]:
                print(f"    - {server['hostname']}:{server['port']} "
                      f"[{server['status']}] weight={server['weight']}")
    
    def print_query_rules(self):
        """Print active query rules."""
        print("\n" + "=" * 60)
        print("Active Query Rules")
        print("=" * 60)
        
        rules = self.get_query_rules()
        if not rules:
            print("\n  No active query rules configured!")
            print("  All queries will go to default hostgroup.")
            return
        
        for rule in rules:
            hg = rule["destination_hostgroup"]
            hg_type = "WRITER" if int(hg) == self.config.writer_hostgroup else "READER"
            pattern = rule["match_pattern"] or "(default)"
            print(f"\n  Rule {rule['rule_id']}: → HG{hg} ({hg_type})")
            print(f"    Pattern: {pattern}")


# -----------------------------
# Read/Write Split Tester
# -----------------------------
class ReadWriteSplitTester:
    """Test read/write splitting through ProxySQL."""
    
    def __init__(self, config: TestConfig, verbose: bool = False):
        self.config = config
        self.verbose = verbose
        self.checker = QueryRoutingChecker(config)
    
    def setup_schema(self):
        """Create test database and table with LineairDB engine."""
        cfg = self.config.proxysql_cfg()
        
        print("\n[1] Setting up test schema...")
        print(f"    Storage engine: {self.config.storage_engine}")
        
        # Create database (goes to writer)
        mysql_exec(cfg, f"CREATE DATABASE IF NOT EXISTS {self.config.db_name}")
        
        # Create table and verify engine in same connection (transaction pins to writer)
        conn = mysql_connect(cfg)
        try:
            conn.autocommit = False  # Use transaction to pin to writer
            cur = conn.cursor()
            
            # Drop and recreate table
            cur.execute(f"DROP TABLE IF EXISTS {self.config.db_name}.{self.config.table_name}")
            
            cur.execute(
                f"""
                CREATE TABLE {self.config.db_name}.{self.config.table_name} (
                    id VARCHAR(36) PRIMARY KEY,
                    value VARCHAR(255),
                    counter INT DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                ) ENGINE={self.config.storage_engine}
                """
            )
            
            # Verify engine in same connection (still on writer)
            cur.execute(
                "SELECT ENGINE FROM information_schema.TABLES "
                "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
                (self.config.db_name, self.config.table_name)
            )
            row = cur.fetchone()
            engine = row[0] if row else 'UNKNOWN'
            
            conn.commit()
            
        finally:
            cur.close()
            conn.close()
        
        if engine.upper() != self.config.storage_engine.upper():
            print(f"    WARNING: Table created with {engine} instead of {self.config.storage_engine}")
        else:
            print(f"    ✅ Table using {engine} engine")
        
        # Wait for schema replication
        print("    Waiting for schema to replicate...")
        time.sleep(1)
        
        print(f"    ✅ Schema ready: {self.config.db_name}.{self.config.table_name}")
    
    def get_backend_for_query(self, sql: str, params=None) -> Tuple[str, int, int]:
        """
        Execute a query and return which backend handled it.
        
        Returns:
            Tuple of (hostname, server_id, hostgroup)
        """
        cfg = self.config.proxysql_cfg()
        conn = mysql_connect(cfg)
        try:
            cur = conn.cursor()
            
            # Execute the actual query
            cur.execute(sql, params or ())
            cur.fetchall()  # Consume results
            
            # Get backend identity
            cur.execute("SELECT @@hostname, @@server_id")
            result = cur.fetchone()
            hostname = result[0] if result else "unknown"
            server_id = int(result[1]) if result else 0
            
            # Determine hostgroup based on hostname/server_id
            if hostname == "database2-01" or self.config.primary_host in str(server_id):
                hostgroup = self.config.writer_hostgroup
            else:
                hostgroup = self.config.reader_hostgroup
            
            return hostname, server_id, hostgroup
            
        finally:
            cur.close()
            conn.close()
    
    def test_write_routing(self, iterations: int = 20) -> Dict[str, Any]:
        """Test that write queries are routed to the primary."""
        print(f"\n[2] Testing WRITE routing ({iterations} iterations)...")
        
        cfg = self.config.proxysql_cfg()
        results = {
            "total": iterations,
            "to_writer": 0,
            "to_reader": 0,
            "hosts_hit": defaultdict(int),
            "operations": [],
        }
        
        for i in range(iterations):
            row_id = f"write_test_{uuid.uuid4().hex[:12]}"
            value = f"value_{i}"
            
            conn = mysql_connect(cfg)
            try:
                conn.autocommit = False  # Start transaction - pins to writer
                cur = conn.cursor()
                
                cur.execute(
                    f"INSERT INTO {self.config.db_name}.{self.config.table_name} (id, value) "
                    f"VALUES (%s, %s)",
                    (row_id, value)
                )
                
                cur.execute("SELECT @@hostname, @@server_id")
                result = cur.fetchone()
                hostname = result[0] if result else "unknown"
                server_id = int(result[1]) if result else 0
                
                conn.commit()
                
            finally:
                cur.close()
                conn.close()
            
            results["hosts_hit"][hostname] += 1
            is_writer = (server_id == 1 or hostname == "database2-01")
            
            if is_writer:
                results["to_writer"] += 1
                if self.verbose:
                    print(f"    ✅ INSERT #{i+1}: → {hostname} (writer)")
            else:
                results["to_reader"] += 1
                if self.verbose:
                    print(f"    ❌ INSERT #{i+1}: → {hostname} (UNEXPECTED: reader!)")
            
            results["operations"].append({
                "operation": "INSERT",
                "hostname": hostname,
                "server_id": server_id,
                "correct_routing": is_writer,
            })
        
        write_rate = (results["to_writer"] / iterations) * 100
        print(f"    Routed to WRITER: {results['to_writer']}/{iterations} ({write_rate:.1f}%)")
        print(f"    Hosts hit: {dict(results['hosts_hit'])}")
        
        if write_rate == 100:
            print(f"    ✅ All writes correctly routed to primary")
        else:
            print(f"    ❌ Some writes went to replicas (misconfigured!)")
        
        results["write_rate"] = write_rate
        return results
    
    def test_read_routing(self, iterations: int = 20) -> Dict[str, Any]:
        """Test that read queries are routed to replicas."""
        print(f"\n[3] Testing READ routing ({iterations} iterations)...")
        
        cfg = self.config.proxysql_cfg()
        
        # First, insert some test data to read
        test_id = f"read_test_{uuid.uuid4().hex[:8]}"
        mysql_exec(
            cfg,
            f"INSERT INTO {self.config.db_name}.{self.config.table_name} (id, value) "
            f"VALUES (%s, %s)",
            (test_id, "test_value")
        )
        
        time.sleep(0.1)
        
        results = {
            "total": iterations,
            "to_writer": 0,
            "to_reader": 0,
            "hosts_hit": defaultdict(int),
            "operations": [],
        }
        
        for i in range(iterations):
            conn = mysql_connect(cfg)
            try:
                cur = conn.cursor()
                
                cur.execute(
                    f"SELECT value FROM {self.config.db_name}.{self.config.table_name} "
                    f"WHERE id = %s",
                    (test_id,)
                )
                cur.fetchall()
                
                cur.execute("SELECT @@hostname, @@server_id")
                result = cur.fetchone()
                hostname = result[0] if result else "unknown"
                server_id = int(result[1]) if result else 0
                
            finally:
                cur.close()
                conn.close()
            
            results["hosts_hit"][hostname] += 1
            is_reader = (server_id > 1 or hostname in ["database2-02", "database2-03"])
            
            if is_reader:
                results["to_reader"] += 1
                if self.verbose:
                    print(f"    ✅ SELECT #{i+1}: → {hostname} (reader)")
            else:
                results["to_writer"] += 1
                if self.verbose:
                    print(f"    ⚠️  SELECT #{i+1}: → {hostname} (writer - not ideal)")
            
            results["operations"].append({
                "operation": "SELECT",
                "hostname": hostname,
                "server_id": server_id,
                "correct_routing": is_reader,
            })
        
        read_rate = (results["to_reader"] / iterations) * 100
        print(f"    Routed to READER: {results['to_reader']}/{iterations} ({read_rate:.1f}%)")
        print(f"    Routed to WRITER: {results['to_writer']}/{iterations}")
        print(f"    Hosts hit: {dict(results['hosts_hit'])}")
        
        if read_rate > 80:
            print(f"    ✅ Reads mostly routed to replicas")
        elif read_rate > 0:
            print(f"    ⚠️  Mixed read routing (some to primary)")
        else:
            print(f"    ❌ All reads went to primary (no read scaling!)")
        
        results["read_rate"] = read_rate
        return results
    
    def test_update_routing(self, iterations: int = 10) -> Dict[str, Any]:
        """Test that UPDATE queries are routed to the primary."""
        print(f"\n[4] Testing UPDATE routing ({iterations} iterations)...")
        
        cfg = self.config.proxysql_cfg()
        
        test_ids = []
        for i in range(iterations):
            test_id = f"update_test_{uuid.uuid4().hex[:8]}"
            mysql_exec(
                cfg,
                f"INSERT INTO {self.config.db_name}.{self.config.table_name} (id, value, counter) "
                f"VALUES (%s, %s, %s)",
                (test_id, f"value_{i}", 0)
            )
            test_ids.append(test_id)
        
        results = {
            "total": iterations,
            "to_writer": 0,
            "to_reader": 0,
            "hosts_hit": defaultdict(int),
        }
        
        for i, test_id in enumerate(test_ids):
            conn = mysql_connect(cfg)
            try:
                conn.autocommit = False
                cur = conn.cursor()
                
                cur.execute(
                    f"UPDATE {self.config.db_name}.{self.config.table_name} "
                    f"SET counter = counter + 1 WHERE id = %s",
                    (test_id,)
                )
                
                cur.execute("SELECT @@hostname, @@server_id")
                result = cur.fetchone()
                hostname = result[0] if result else "unknown"
                server_id = int(result[1]) if result else 0
                
                conn.commit()
                
            finally:
                cur.close()
                conn.close()
            
            results["hosts_hit"][hostname] += 1
            is_writer = (server_id == 1 or hostname == "database2-01")
            
            if is_writer:
                results["to_writer"] += 1
                if self.verbose:
                    print(f"    ✅ UPDATE #{i+1}: → {hostname} (writer)")
            else:
                results["to_reader"] += 1
                if self.verbose:
                    print(f"    ❌ UPDATE #{i+1}: → {hostname} (UNEXPECTED: reader!)")
        
        update_rate = (results["to_writer"] / iterations) * 100
        print(f"    Routed to WRITER: {results['to_writer']}/{iterations} ({update_rate:.1f}%)")
        
        if update_rate == 100:
            print(f"    ✅ All updates correctly routed to primary")
        else:
            print(f"    ❌ Some updates went to replicas (DANGEROUS!)")
        
        results["update_rate"] = update_rate
        return results
    
    def test_delete_routing(self, iterations: int = 10) -> Dict[str, Any]:
        """Test that DELETE queries are routed to the primary."""
        print(f"\n[5] Testing DELETE routing ({iterations} iterations)...")
        
        cfg = self.config.proxysql_cfg()
        
        test_ids = []
        for i in range(iterations):
            test_id = f"delete_test_{uuid.uuid4().hex[:8]}"
            mysql_exec(
                cfg,
                f"INSERT INTO {self.config.db_name}.{self.config.table_name} (id, value) "
                f"VALUES (%s, %s)",
                (test_id, f"to_delete_{i}")
            )
            test_ids.append(test_id)
        
        results = {
            "total": iterations,
            "to_writer": 0,
            "to_reader": 0,
            "hosts_hit": defaultdict(int),
        }
        
        for i, test_id in enumerate(test_ids):
            conn = mysql_connect(cfg)
            try:
                conn.autocommit = False
                cur = conn.cursor()
                
                cur.execute(
                    f"DELETE FROM {self.config.db_name}.{self.config.table_name} WHERE id = %s",
                    (test_id,)
                )
                
                cur.execute("SELECT @@hostname, @@server_id")
                result = cur.fetchone()
                hostname = result[0] if result else "unknown"
                server_id = int(result[1]) if result else 0
                
                conn.commit()
                
            finally:
                cur.close()
                conn.close()
            
            results["hosts_hit"][hostname] += 1
            is_writer = (server_id == 1 or hostname == "database2-01")
            
            if is_writer:
                results["to_writer"] += 1
                if self.verbose:
                    print(f"    ✅ DELETE #{i+1}: → {hostname} (writer)")
            else:
                results["to_reader"] += 1
                if self.verbose:
                    print(f"    ❌ DELETE #{i+1}: → {hostname} (UNEXPECTED: reader!)")
        
        delete_rate = (results["to_writer"] / iterations) * 100
        print(f"    Routed to WRITER: {results['to_writer']}/{iterations} ({delete_rate:.1f}%)")
        
        if delete_rate == 100:
            print(f"    ✅ All deletes correctly routed to primary")
        else:
            print(f"    ❌ Some deletes went to replicas (DANGEROUS!)")
        
        results["delete_rate"] = delete_rate
        return results
    
    def test_transaction_routing(self, iterations: int = 5) -> Dict[str, Any]:
        """Test that transactions are routed to the primary."""
        print(f"\n[6] Testing TRANSACTION routing ({iterations} iterations)...")
        
        cfg = self.config.proxysql_cfg()
        
        results = {
            "total": iterations,
            "to_writer": 0,
            "to_reader": 0,
            "hosts_hit": defaultdict(int),
        }
        
        for i in range(iterations):
            conn = mysql_connect(cfg)
            try:
                conn.autocommit = False
                cur = conn.cursor()
                
                test_id = f"tx_test_{uuid.uuid4().hex[:8]}"
                cur.execute(
                    f"INSERT INTO {self.config.db_name}.{self.config.table_name} (id, value) "
                    f"VALUES (%s, %s)",
                    (test_id, f"tx_value_{i}")
                )
                
                cur.execute(
                    f"SELECT value FROM {self.config.db_name}.{self.config.table_name} "
                    f"WHERE id = %s",
                    (test_id,)
                )
                cur.fetchall()
                
                cur.execute("SELECT @@hostname, @@server_id")
                result = cur.fetchone()
                hostname = result[0] if result else "unknown"
                server_id = int(result[1]) if result else 0
                
                conn.commit()
                
            finally:
                cur.close()
                conn.close()
            
            results["hosts_hit"][hostname] += 1
            is_writer = (server_id == 1 or hostname == "database2-01")
            
            if is_writer:
                results["to_writer"] += 1
                if self.verbose:
                    print(f"    ✅ Transaction #{i+1}: → {hostname} (writer)")
            else:
                results["to_reader"] += 1
                if self.verbose:
                    print(f"    ❌ Transaction #{i+1}: → {hostname} (UNEXPECTED: reader!)")
        
        tx_rate = (results["to_writer"] / iterations) * 100
        print(f"    Routed to WRITER: {results['to_writer']}/{iterations} ({tx_rate:.1f}%)")
        
        if tx_rate == 100:
            print(f"    ✅ All transactions correctly routed to primary")
        else:
            print(f"    ❌ Some transactions went to replicas (DANGEROUS!)")
        
        results["tx_rate"] = tx_rate
        return results
    
    def test_read_load_balancing(self, iterations: int = 50) -> Dict[str, Any]:
        """Test that reads are load-balanced across replicas."""
        print(f"\n[7] Testing READ load balancing ({iterations} iterations)...")
        
        cfg = self.config.proxysql_cfg()
        
        test_id = f"lb_test_{uuid.uuid4().hex[:8]}"
        mysql_exec(
            cfg,
            f"INSERT INTO {self.config.db_name}.{self.config.table_name} (id, value) "
            f"VALUES (%s, %s)",
            (test_id, "lb_test_value")
        )
        time.sleep(0.2)
        
        hosts_hit = defaultdict(int)
        
        for i in range(iterations):
            conn = mysql_connect(cfg)
            try:
                cur = conn.cursor()
                cur.execute(
                    f"SELECT value FROM {self.config.db_name}.{self.config.table_name} "
                    f"WHERE id = %s",
                    (test_id,)
                )
                cur.fetchall()
                
                cur.execute("SELECT @@hostname")
                result = cur.fetchone()
                hostname = result[0] if result else "unknown"
                hosts_hit[hostname] += 1
                
            finally:
                cur.close()
                conn.close()
        
        print(f"    Distribution: {dict(hosts_hit)}")
        
        reader_hosts = [h for h in hosts_hit.keys() if h != "database2-01"]
        if len(reader_hosts) >= 2:
            counts = [hosts_hit[h] for h in reader_hosts]
            min_count, max_count = min(counts), max(counts)
            balance_ratio = min_count / max_count if max_count > 0 else 0
            
            if balance_ratio >= 0.5:
                print(f"    ✅ Load reasonably balanced across readers (ratio: {balance_ratio:.2f})")
            else:
                print(f"    ⚠️  Uneven load distribution (ratio: {balance_ratio:.2f})")
        elif len(reader_hosts) == 1:
            print(f"    ⚠️  Only one reader received traffic")
        else:
            print(f"    ❌ No readers received traffic (all went to writer)")
        
        return {"hosts_hit": dict(hosts_hit), "iterations": iterations}
    
    def analyze_query_digest(self) -> Dict[str, Any]:
        """Analyze ProxySQL query digest to see routing patterns."""
        print("\n[8] Analyzing Query Digest (ProxySQL stats)...")
        
        digests = self.checker.get_query_digest_stats()
        
        write_patterns = ["INSERT", "UPDATE", "DELETE"]
        read_patterns = ["SELECT"]
        
        write_to_hg = defaultdict(int)
        read_to_hg = defaultdict(int)
        
        for d in digests:
            digest_upper = d["digest"].upper()
            hg = int(d["hostgroup"])
            count = int(d["count"])
            
            for pattern in write_patterns:
                if pattern in digest_upper:
                    write_to_hg[hg] += count
                    break
            else:
                for pattern in read_patterns:
                    if pattern in digest_upper:
                        read_to_hg[hg] += count
                        break
        
        print(f"    Write queries by hostgroup: {dict(write_to_hg)}")
        print(f"    Read queries by hostgroup: {dict(read_to_hg)}")
        
        writer_hg = self.config.writer_hostgroup
        reader_hg = self.config.reader_hostgroup
        
        writes_correct = write_to_hg.get(writer_hg, 0)
        writes_wrong = sum(v for k, v in write_to_hg.items() if k != writer_hg)
        reads_to_reader = read_to_hg.get(reader_hg, 0)
        reads_to_writer = read_to_hg.get(writer_hg, 0)
        
        if writes_wrong == 0 and writes_correct > 0:
            print(f"    ✅ All logged writes went to HG{writer_hg} (writer)")
        elif writes_wrong > 0:
            print(f"    ❌ {writes_wrong} writes went to wrong hostgroup!")
        
        if reads_to_reader > reads_to_writer:
            print(f"    ✅ Most reads went to HG{reader_hg} (readers)")
        elif reads_to_reader > 0:
            print(f"    ⚠️  Mixed read routing")
        
        return {
            "write_to_hg": dict(write_to_hg),
            "read_to_hg": dict(read_to_hg),
        }
    
    def run(self, iterations: int = 20) -> int:
        """Run the full read/write split test."""
        print("=" * 60)
        print("ProxySQL Read/Write Split Test (LineairDB)")
        print("=" * 60)
        print(f"\nStorage Engine: {self.config.storage_engine}")
        
        # Check LineairDB availability
        print("\n[Prereq] Checking LineairDB availability...")
        if not check_lineairdb_available(self.config.proxysql_cfg()):
            print("  ❌ LineairDB storage engine is NOT available!")
            print("  Please ensure you're running the LineairDB-enabled MySQL build.")
            return 1
        print("  ✅ LineairDB storage engine is available")
        
        # Show configuration
        self.checker.print_backend_status()
        self.checker.print_query_rules()
        
        # Setup schema
        try:
            self.setup_schema()
        except MySQLError as e:
            print(f"\n❌ Schema setup failed: {e}")
            return 1
        
        # Reset query digest
        self.checker.reset_query_digest()
        
        # Run tests
        write_results = self.test_write_routing(iterations=iterations)
        read_results = self.test_read_routing(iterations=iterations)
        update_results = self.test_update_routing(iterations=iterations // 2)
        delete_results = self.test_delete_routing(iterations=iterations // 2)
        tx_results = self.test_transaction_routing(iterations=5)
        lb_results = self.test_read_load_balancing(iterations=iterations * 2)
        
        # Analyze digest
        digest_results = self.analyze_query_digest()
        
        # Summary
        print("\n" + "=" * 60)
        print("Summary")
        print("=" * 60)
        
        all_writes_correct = (
            write_results["write_rate"] == 100 and
            update_results["update_rate"] == 100 and
            delete_results["delete_rate"] == 100 and
            tx_results["tx_rate"] == 100
        )
        reads_to_replicas = read_results["read_rate"] > 50
        
        if all_writes_correct and reads_to_replicas:
            print(f"""
✅ Read/Write Split is working correctly with LineairDB!

Write Routing:
  - INSERT:      {write_results['write_rate']:.0f}% to primary
  - UPDATE:      {update_results['update_rate']:.0f}% to primary
  - DELETE:      {delete_results['delete_rate']:.0f}% to primary
  - Transaction: {tx_results['tx_rate']:.0f}% to primary

Read Routing:
  - SELECT:      {read_results['read_rate']:.0f}% to replicas
  - Load balance: {lb_results['hosts_hit']}

ProxySQL is correctly splitting reads and writes.
""")
            return 0
        else:
            issues = []
            if write_results["write_rate"] < 100:
                issues.append(f"- INSERTs not all to primary ({write_results['write_rate']:.0f}%)")
            if update_results["update_rate"] < 100:
                issues.append(f"- UPDATEs not all to primary ({update_results['update_rate']:.0f}%)")
            if delete_results["delete_rate"] < 100:
                issues.append(f"- DELETEs not all to primary ({delete_results['delete_rate']:.0f}%)")
            if tx_results["tx_rate"] < 100:
                issues.append(f"- Transactions not all to primary ({tx_results['tx_rate']:.0f}%)")
            if read_results["read_rate"] < 50:
                issues.append(f"- SELECTs not going to replicas ({read_results['read_rate']:.0f}%)")
            
            print(f"""
⚠️  Read/Write Split has issues!

Issues detected:
{chr(10).join(issues)}

Check ProxySQL query rules configuration:
  - Ensure write patterns (INSERT/UPDATE/DELETE) → HG0
  - Ensure read patterns (SELECT) → HG1
  - Check mysql_query_rules in ProxySQL admin
""")
            return 1


# -----------------------------
# Main
# -----------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Test read/write split configuration in ProxySQL with LineairDB",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Run with default settings
    python3 read_write_split.py
    
    # More iterations for thorough testing
    python3 read_write_split.py --iterations 100
    
    # Verbose output
    python3 read_write_split.py --verbose

This test requires LineairDB storage engine to be available.
"""
    )
    parser.add_argument(
        "--iterations", "-n",
        type=int,
        default=20,
        help="Number of iterations per test (default: 20)"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Print detailed output for each operation"
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
    
    tester = ReadWriteSplitTester(config, verbose=args.verbose)
    return tester.run(iterations=args.iterations)


if __name__ == "__main__":
    sys.exit(main())
