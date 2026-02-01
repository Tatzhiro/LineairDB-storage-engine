#!/usr/bin/env python3
import sys
import argparse
import uuid
import traceback
import subprocess
import os
import socket
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import mysql.connector

from mysql.connector import Error as MySQLError

# Helper to detect "Unknown database" error
def is_unknown_database(err: Exception, db_name: str) -> bool:
    msg = str(err)
    # mysql-connector raises ProgrammingError with code 1049 for unknown database
    return ("1049" in msg and "Unknown database" in msg and db_name in msg)


# -----------------------------
# Robust mysql-connector helpers
# -----------------------------
def mysql_connect(cfg: Dict[str, Any], database: Optional[str] = None):
    """Robust connector for ProxySQL/MySQL.

    IMPORTANT: `backend_nodes` may contain extra keys (e.g., ssh_user/mysql_bin) that are
    not valid mysql-connector arguments. We filter them here.
    """
    allowed_keys = {
        "host",
        "port",
        "user",
        "password",
        "database",
        "connection_timeout",
        "use_pure",
        "autocommit",
        "ssl_disabled",
        "ssl_ca",
        "ssl_cert",
        "ssl_key",
        "unix_socket",
        "auth_plugin",
        "raise_on_warnings",
        "charset",
        "collation",
        "read_timeout",
        "write_timeout",
    }

    c = {k: v for k, v in dict(cfg).items() if k in allowed_keys}

    # Defaults tuned for ProxySQL admin/client
    c.setdefault("use_pure", True)
    c.setdefault("autocommit", True)
    c.setdefault("ssl_disabled", True)
    # Prevent tests from hanging forever on a stuck backend / network path.
    c.setdefault("connection_timeout", 5)
    c.setdefault("read_timeout", 15)
    c.setdefault("write_timeout", 15)

    if database:
        c["database"] = database

    return mysql.connector.connect(**c)


def mysql_exec(cfg: Dict[str, Any], sql: str, params=None, database: Optional[str] = None):
    conn = mysql_connect(cfg, database=database)
    try:
        cur = conn.cursor()
        cur.execute(sql, params or ())
        return cur.fetchall() if cur.with_rows else None
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()


def mysql_exec_many(cfg: Dict[str, Any], statements: List[str], database: Optional[str] = None):
    """Execute multiple SQL statements sequentially.

    This avoids mysql-connector parameter processing which queries @@session.sql_mode,
    something ProxySQL Admin Module may not support.
    """
    conn = mysql_connect(cfg, database=database)
    try:
        cur = conn.cursor()
        for stmt in statements:
            cur.execute(stmt)
        return True
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()


def mysql_query_one(cfg: Dict[str, Any], sql: str, params=None, database: Optional[str] = None):
    conn = mysql_connect(cfg, database=database)
    try:
        cur = conn.cursor()
        cur.execute(sql, params or ())
        return cur.fetchone()
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()


# -----------------------------
# ProxySQL admin client
# -----------------------------
@dataclass
class ProxySQLAdminClient:
    host: str
    port: int
    user: str
    password: str

    def cfg(self) -> Dict[str, Any]:
        return {"host": self.host, "port": self.port, "user": self.user, "password": self.password}

    def ping_or_die(self):
        try:
            row = mysql_query_one(self.cfg(), "SELECT 1")
            # ProxySQL admin may return string '1' depending on connector/driver.
            if not (row and str(row[0]) == "1"):
                raise RuntimeError(f"Unexpected ping result: {row}")
        except Exception as e:
            print("❌ ProxySQL admin ping failed.")
            print(f"   host={self.host} port={self.port} user={self.user}")
            print(f"   error={e}")
            raise

    def runtime_servers(self) -> List[Tuple[int, str, int, str, int]]:
        rows = mysql_exec(
            self.cfg(),
            """
            SELECT hostgroup_id, hostname, port, status, weight
            FROM runtime_mysql_servers
            ORDER BY hostgroup_id, hostname
            """,
        )
        return rows or []

    def conn_pool_stats(self) -> List[Tuple[Any, ...]]:
        rows = mysql_exec(
            self.cfg(),
            """
            SELECT hostgroup, srv_host, srv_port, status, ConnUsed, ConnFree, ConnOK, ConnERR, Queries
            FROM stats_mysql_connection_pool
            ORDER BY hostgroup, srv_host
            """,
        )
        return rows or []

    def set_writer(self, new_writer_host: str, new_writer_port: int, writer_hg: int = 0):
        """Point writer hostgroup to a single backend.

        We intentionally avoid parameterized queries here because mysql-connector-python
        tries to read @@session.sql_mode when binding parameters, and ProxySQL Admin Module
        can reject that query.
        """
        host = str(new_writer_host).replace("'", "")
        port = int(new_writer_port)
        hg = int(writer_hg)

        stmts = [
            "USE main",
            f"DELETE FROM mysql_servers WHERE hostgroup_id={hg}",
            f"INSERT INTO mysql_servers(hostgroup_id, hostname, port) VALUES ({hg}, '{host}', {port})",
            "LOAD MYSQL SERVERS TO RUNTIME",
            "SAVE MYSQL SERVERS TO DISK",
        ]
        mysql_exec_many(self.cfg(), stmts)

    def remove_from_hostgroup(self, hostgroup_id: int, hostname: str, port: int = 3306):
        """Remove a backend from a specific hostgroup (and persist)."""
        host = str(hostname).replace("'", "")
        hg = int(hostgroup_id)
        p = int(port)

        stmts = [
            "USE main",
            f"DELETE FROM mysql_servers WHERE hostgroup_id={hg} AND hostname='{host}' AND port={p}",
            "LOAD MYSQL SERVERS TO RUNTIME",
            "SAVE MYSQL SERVERS TO DISK",
        ]
        mysql_exec_many(self.cfg(), stmts)

    def reset_mysql_servers(self, writer: Tuple[str, int], readers: List[Tuple[str, int]], writer_hg: int = 0, reader_hg: int = 1):
        """Reset ProxySQL mysql_servers table to a clean baseline (persisted)."""
        w_host = str(writer[0]).replace("'", "")
        w_port = int(writer[1])
        rhg = int(reader_hg)
        whg = int(writer_hg)

        stmts = [
            "USE main",
            "DELETE FROM mysql_servers",
            f"INSERT INTO mysql_servers(hostgroup_id, hostname, port, weight, max_connections) VALUES ({whg}, '{w_host}', {w_port}, 1, 1000)",
        ]

        for (h, p) in readers:
            h2 = str(h).replace("'", "")
            p2 = int(p)
            stmts.append(
                f"INSERT INTO mysql_servers(hostgroup_id, hostname, port, weight, max_connections) VALUES ({rhg}, '{h2}', {p2}, 1, 1000)"
            )

        stmts += [
            "LOAD MYSQL SERVERS TO RUNTIME",
            "SAVE MYSQL SERVERS TO DISK",
        ]
        mysql_exec_many(self.cfg(), stmts)


# -----------------------------
# ProxySQL failover tester
# -----------------------------
class ProxySQLFailoverTester:
    def proxysql_exec_with_timeouts(self, sql: str, params=None, database: Optional[str] = None, read_timeout: int = 60, write_timeout: int = 60):
        cfg = dict(self.proxysql_client_cfg)
        cfg["read_timeout"] = int(read_timeout)
        cfg["write_timeout"] = int(write_timeout)
        return mysql_exec(cfg, sql, params=params, database=database)
    def __init__(
        self,
        admin: ProxySQLAdminClient,
        proxysql_client_cfg: Dict[str, Any],
        backend_nodes: Dict[str, Dict[str, Any]],   # name -> direct mysql cfg
        primary_name: str,
        replica_names: List[str],
        writer_hg: int = 0,
        reader_hg: int = 1,
        db_name: str = "proxysql_failover_test",
        table_name: str = "items",
    ):
        self.admin = admin
        self.proxysql_client_cfg = proxysql_client_cfg
        self.backend_nodes = backend_nodes
        self.primary_name = primary_name
        self.replica_names = replica_names
        self.writer_hg = writer_hg
        self.reader_hg = reader_hg
        self.db_name = db_name
        self.table_name = table_name
        self.payload1 = f"before_{uuid.uuid4().hex[:8]}"
        self.payload2 = f"after_{uuid.uuid4().hex[:8]}"
        # Use a per-run unique key space so re-running the test doesn't hit duplicate primary keys.
        self.key_base = int(uuid.uuid4().hex[:6], 16) % 1000000


    def print_runtime(self):
        print("\n[ProxySQL] runtime_mysql_servers:")
        for (hg, h, p, st, w) in self.admin.runtime_servers():
            print(f"  HG{hg} {h}:{p}  {st}  weight={w}")

        print("\n[ProxySQL] stats_mysql_connection_pool:")
        for row in self.admin.conn_pool_stats():
            print("  ", row)

    def check_writer_responsive(self, timeout: int = 10) -> bool:
        """Pre-flight check: verify the writer backend is responsive via ProxySQL."""
        cfg = dict(self.proxysql_client_cfg)
        cfg["connection_timeout"] = timeout
        cfg["read_timeout"] = timeout
        cfg["write_timeout"] = timeout
        
        try:
            row = mysql_query_one(cfg, "SELECT 1")
            return row and str(row[0]) == "1"
        except Exception:
            return False

    def proxysql_query_one(self, sql: str, params=None, database: Optional[str] = None):
        return mysql_query_one(self.proxysql_client_cfg, sql, params=params, database=database)

    def proxysql_exec(self, sql: str, params=None, database: Optional[str] = None):
        return mysql_exec(self.proxysql_client_cfg, sql, params=params, database=database)

    def get_backend_identity_via_proxysql(self) -> Tuple[str, int, int]:
        row = self.proxysql_query_one("SELECT @@hostname, @@server_id, @@read_only;")
        if not row:
            raise RuntimeError("Could not query backend identity via ProxySQL (6033).")
        return row[0], int(row[1]), int(row[2])

    def setup_schema(self, ddl_timeout: int = 30):
        """Setup test database and table."""
        if not self.check_writer_responsive(timeout=10):
            raise RuntimeError("Writer backend is not responsive. Cannot proceed with schema setup.")
        
        self.proxysql_exec_with_timeouts(
            f"CREATE DATABASE IF NOT EXISTS {self.db_name}",
            read_timeout=ddl_timeout,
            write_timeout=ddl_timeout,
        )
        
        self.proxysql_exec_with_timeouts(
            f"""
            CREATE TABLE IF NOT EXISTS {self.db_name}.{self.table_name} (
              id INT PRIMARY KEY,
              content VARCHAR(255)
            )
            """,
            read_timeout=ddl_timeout,
            write_timeout=ddl_timeout,
        )
        
        # Make the test repeatable: clear previous rows (best-effort).
        try:
            self.proxysql_exec_with_timeouts(
                f"TRUNCATE TABLE {self.db_name}.{self.table_name}",
                read_timeout=ddl_timeout,
                write_timeout=ddl_timeout,
            )
        except Exception:
            pass

    def insert(self, row_id: int, payload: str):
        try:
            self.proxysql_exec(
                f"INSERT INTO {self.db_name}.{self.table_name} (id, content) VALUES (%s, %s)",
                (row_id, payload),
            )
        except MySQLError as e:
            # If failover promoted a replica that doesn't yet have the schema (replication lag/filter),
            # recreate schema on the new writer and retry once.
            if is_unknown_database(e, self.db_name):
                print(f"[warn] Database '{self.db_name}' not found on current writer. Recreating schema and retrying...")
                self.setup_schema()
                self.proxysql_exec(
                    f"INSERT INTO {self.db_name}.{self.table_name} (id, content) VALUES (%s, %s)",
                    (row_id, payload),
                )
                return
            raise

    def set_readonly_via_ssh(self, node_name: str, readonly: bool):
        """Set (super_)read_only on a node using SSH + local socket mysql.

        This avoids TCP auth plugin / SSL requirements.
        """
        host = self.backend_nodes[node_name]["host"]
        ssh_user = self.backend_nodes[node_name].get("ssh_user", "ubuntu")
        mysql_bin = self.backend_nodes[node_name].get("mysql_bin", "~/LineairDB-storage-engine/build-release/bin/mysql")

        # If the target host is local, run the command locally instead of SSH.
        local_ips = {"127.0.0.1", "localhost", "133.125.85.242"}
        if host in local_ips:
            # Try both defaults files; use the known socket.
            sock = "/tmp/mysql.sock"
            cnf1 = os.path.expanduser("~/LineairDB-storage-engine/my_release.cnf")
            cnf2 = os.path.expanduser("~/LineairDB-storage-engine/my_replica_release.cnf")
            if readonly:
                sql = "SET GLOBAL super_read_only=ON; SET GLOBAL read_only=ON;"
            else:
                sql = "SET GLOBAL super_read_only=OFF; SET GLOBAL read_only=OFF;"
            cmd_variants = [
                f"sudo {mysql_bin} --defaults-file={cnf1} --protocol=SOCKET --socket={sock} -u root -e \"{sql}\"",
                f"sudo {mysql_bin} --defaults-file={cnf2} --protocol=SOCKET --socket={sock} -u root -e \"{sql}\"",
                f"{mysql_bin} --defaults-file={cnf1} --protocol=SOCKET --socket={sock} -u root -e \"{sql}\"",
                f"{mysql_bin} --defaults-file={cnf2} --protocol=SOCKET --socket={sock} -u root -e \"{sql}\"",
            ]
            last = None
            for cmd in cmd_variants:
                try:
                    subprocess.run(cmd, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=30)
                    return
                except subprocess.CalledProcessError as e:
                    last = e
                except subprocess.TimeoutExpired as e:
                    last = e
            raise last

        if readonly:
            sql = "SET GLOBAL super_read_only=ON; SET GLOBAL read_only=ON;"
        else:
            sql = "SET GLOBAL super_read_only=OFF; SET GLOBAL read_only=OFF;"

        # Try both possible defaults files (master/replica) and the known socket.
        # Using --protocol=SOCKET avoids TCP auth issues.
        remote_cmd = (
            "set -e; "
            "SOCK=/tmp/mysql.sock; "
            f"SQL=\"{sql}\"; "
            "CNF1=~/LineairDB-storage-engine/my_release.cnf; "
            "CNF2=~/LineairDB-storage-engine/my_replica_release.cnf; "
            f"(sudo {mysql_bin} --defaults-file=$CNF1 --protocol=SOCKET --socket=$SOCK -u root -e \"$SQL\" "
            f" || sudo {mysql_bin} --defaults-file=$CNF2 --protocol=SOCKET --socket=$SOCK -u root -e \"$SQL\" "
            f" || {mysql_bin} --defaults-file=$CNF1 --protocol=SOCKET --socket=$SOCK -u root -e \"$SQL\" "
            f" || {mysql_bin} --defaults-file=$CNF2 --protocol=SOCKET --socket=$SOCK -u root -e \"$SQL\" )"
        )

        subprocess.run(
            ["ssh", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=accept-new", "-o", "ConnectTimeout=10", f"{ssh_user}@{host}", remote_cmd],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=30,  # Prevent indefinite hang on SSH operations
        )

    def reset_baseline(self):
        """Reset ProxySQL routing and MySQL read_only flags to the initial baseline."""
        print("\n[-] Reset baseline: ProxySQL HGs + MySQL read_only flags")

        # Reset ProxySQL hostgroups baseline
        writer = (self.backend_nodes[self.primary_name]["host"], self.backend_nodes[self.primary_name]["port"])
        readers = [(self.backend_nodes[n]["host"], self.backend_nodes[n]["port"]) for n in self.replica_names]
        self.admin.reset_mysql_servers(writer=writer, readers=readers, writer_hg=self.writer_hg, reader_hg=self.reader_hg)

        # Reset MySQL read_only flags
        try:
            self.set_readonly_via_ssh(self.primary_name, readonly=False)
            for r in self.replica_names:
                self.set_readonly_via_ssh(r, readonly=True)
            print("    ✅ Baseline reset applied")
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
            out = getattr(e, 'stdout', None) or ""
            print(f"    ⚠️ Baseline reset via SSH failed ({type(e).__name__}).")
            print(out)
            print("    Fix SSH (key-based) from database2-01 to replicas, or reset manually:")
            print("      - On primary:  SET GLOBAL super_read_only=OFF; SET GLOBAL read_only=OFF;")
            print("      - On replicas: SET GLOBAL super_read_only=ON;  SET GLOBAL read_only=ON;")
            input("    Press ENTER after you have reset the flags manually... ")

    def promote_replica(self, new_primary: str):
        """Promote a replica to writer.

        We prefer executing the promotion commands *locally on the replica* via SSH to avoid
        mysql auth plugin issues over TCP (e.g., sha256_password requiring SSL).

        Requirements:
          - SSH from database2-01 to the replica host works (passwordless recommended)
          - The remote user can run `sudo` for mysql commands
        """
        print(f"[*] Promoting {new_primary}: setting read_only=OFF ...")

        host = self.backend_nodes[new_primary]["host"]
        ssh_user = self.backend_nodes[new_primary].get("ssh_user", "ubuntu")
        mysql_bin = self.backend_nodes[new_primary].get("mysql_bin", "~/LineairDB-storage-engine/build/bin/mysql")

        # Run promotion SQL locally on the replica (no remote MySQL auth).
        # Try sudo first; if sudo isn't needed it will still work.
        promotion_sql = "SET GLOBAL super_read_only=OFF; SET GLOBAL read_only=OFF;"
        remote_cmd = f"sudo {mysql_bin} -u root -e \"{promotion_sql}\" || {mysql_bin} -u root -e \"{promotion_sql}\""

        try:
            subprocess.run(
                ["ssh", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=accept-new", "-o", "ConnectTimeout=10", f"{ssh_user}@{host}", remote_cmd],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                timeout=30,  # Prevent indefinite hang on SSH operations
            )
            print(f"    ✅ Promotion SQL applied on {new_primary} via SSH")
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
            out = getattr(e, 'stdout', None) or ""
            print(f"    ⚠️ SSH-based promotion failed ({type(e).__name__}). Output:")
            print(out)
            print("    SSH is not configured, so this script cannot flip read_only automatically.")
            print("    Do ONE of the following, then press ENTER:")
            print("      A) Set up SSH keys from database2-01 -> replica and rerun the test")
            print("      B) Manually promote the replica by running on the replica node:")
            print("         sudo ~/LineairDB-storage-engine/build/bin/mysql -u root -e \"SET GLOBAL super_read_only=OFF; SET GLOBAL read_only=OFF;\"")
            input("    Press ENTER after you have manually set read_only=OFF on the replica... ")

        h = self.backend_nodes[new_primary]["host"]
        p = self.backend_nodes[new_primary]["port"]
        print(f"[*] Repointing ProxySQL writer HG{self.writer_hg} => {h}:{p}")
        self.admin.set_writer(h, p, writer_hg=self.writer_hg)

        # Optional hygiene: ensure the promoted writer is not also in the reader hostgroup.
        try:
            self.admin.remove_from_hostgroup(self.reader_hg, h, p)
            print(f"[*] Removed promoted writer {h}:{p} from reader HG{self.reader_hg}")
        except Exception as e:
            print(f"[warn] Could not remove promoted writer from reader hostgroup: {e}")

    def run(self, kill_primary_instructions: str) -> int:
        print("[0] Admin connectivity")
        self.admin.ping_or_die()
        print("    ✅ ProxySQL admin ping OK")
        # Optional: reset baseline so replicas are guaranteed read_only=ON and routing is clean.
        if str(os.environ.get("RESET_BASELINE", "1")) not in ("0", "false", "False"):
            self.reset_baseline()
        self.print_runtime()

        print("\n[1] Pre-check: SELECT identity via ProxySQL client 6033")
        h, sid, ro = self.get_backend_identity_via_proxysql()
        print(f"    hit backend: {h} server_id={sid} read_only={ro} (expect replica read_only=1)")

        print("\n[2] Pre-failover: setup schema + write (should go to writer)")
        self.setup_schema()
        self.insert(self.key_base + 1, self.payload1)
        print(f"    ✅ wrote payload: {self.payload1}")

        print("\n=== ACTION REQUIRED: trigger primary failure ===")
        print(kill_primary_instructions)
        print("=============================================\n")
        input("Press ENTER after you have killed the primary mysqld... ")

        print("[3] During failure: attempt a write")
        try:
            self.insert(self.key_base + 99, "during_failure")
            print("    ✅ write succeeded (ProxySQL still had a writable backend available).")
        except MySQLError as e:
            print(f"    ✅ write failed (no writer available yet): {e}")

        new_primary = self.replica_names[0]
        print(f"\n[4] Promote {new_primary} and repoint writer HG")
        self.promote_replica(new_primary)
        self.print_runtime()

        # After repointing the writer to a newly promoted replica, ensure schema exists on that writer.
        # This is idempotent and avoids failures if the replica hadn't replicated the DDL yet.
        self.setup_schema()

        print("\n[5] Post-failover: write should succeed")
        self.insert(self.key_base + 2, self.payload2)
        print(f"    ✅ wrote payload: {self.payload2}")

        h2, sid2, ro2 = self.get_backend_identity_via_proxysql()
        print(f"[6] Identity now: {h2} server_id={sid2} read_only={ro2} (expect writer read_only=0 for writes)")
        print("\n🎉 ProxySQL failover test finished.")
        return 0


def detect_mysqld_binary() -> Optional[str]:
    """Detect the running mysqld binary path from current process list."""
    try:
        result = subprocess.run(
            ["pgrep", "-a", "-u", os.environ.get("USER", "ubuntu"), "mysqld"],
            capture_output=True, text=True, timeout=5
        )
        if result.returncode == 0 and result.stdout.strip():
            # Parse first line: "PID /path/to/mysqld --options..."
            line = result.stdout.strip().split('\n')[0]
            parts = line.split()
            if len(parts) >= 2:
                mysqld_path = parts[1]
                if os.path.exists(mysqld_path):
                    return mysqld_path
    except Exception:
        pass
    return None


def get_local_ip() -> str:
    """Get the local machine's IP address."""
    try:
        # Connect to a public DNS to determine local IP
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


# Default node configurations (can be overridden by command-line)
DEFAULT_NODES = {
    "database2-01": {"host": "133.125.85.242", "port": 3306},
    "database2-02": {"host": "133.242.17.72",  "port": 3306},
    "database2-03": {"host": "153.120.20.111", "port": 3306},
}


def parse_args():
    parser = argparse.ArgumentParser(
        description="ProxySQL failover test",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Use defaults (database2-01 as primary, database2-02/03 as replicas)
  python3 fail_over.py

  # Specify primary and replicas explicitly
  python3 fail_over.py --primary database2-01 --replicas database2-02,database2-03

  # Custom node configuration
  python3 fail_over.py --primary mymaster --replicas replica1,replica2 \\
      --node mymaster:192.168.1.10:3306 \\
      --node replica1:192.168.1.11:3306 \\
      --node replica2:192.168.1.12:3306
        """
    )
    parser.add_argument(
        "--primary", "-p",
        default="database2-01",
        help="Name of the primary/master node (default: database2-01)"
    )
    parser.add_argument(
        "--replicas", "-r",
        default="database2-02,database2-03",
        help="Comma-separated list of replica node names (default: database2-02,database2-03)"
    )
    parser.add_argument(
        "--node", "-n",
        action="append",
        metavar="NAME:HOST:PORT",
        help="Define a node as NAME:HOST:PORT (can be specified multiple times)"
    )
    parser.add_argument(
        "--proxysql-host",
        default=None,
        help="ProxySQL client host (default: auto-detect local IP)"
    )
    parser.add_argument(
        "--proxysql-port",
        type=int,
        default=6033,
        help="ProxySQL client port (default: 6033)"
    )
    parser.add_argument(
        "--proxysql-user",
        default="proxysql_user",
        help="ProxySQL frontend user (default: proxysql_user)"
    )
    parser.add_argument(
        "--proxysql-pass",
        default="proxysql_pass",
        help="ProxySQL frontend password (default: proxysql_pass)"
    )
    parser.add_argument(
        "--no-reset",
        action="store_true",
        help="Skip baseline reset (don't reset read_only flags)"
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    # Auto-detect mysqld binary
    mysqld_bin = detect_mysqld_binary()
    if mysqld_bin:
        mysql_bin = os.path.join(os.path.dirname(mysqld_bin), "mysql")
        print(f"[auto] Detected mysqld: {mysqld_bin}")
        print(f"[auto] Using mysql client: {mysql_bin}")
    else:
        mysql_bin = "~/LineairDB-storage-engine/build-release/bin/mysql"
        mysqld_bin = "~/LineairDB-storage-engine/build-release/bin/mysqld"
        print(f"[warn] Could not detect running mysqld, using default: {mysqld_bin}")

    # Build backend nodes configuration
    backend_nodes = {}
    
    # Start with defaults
    for name, cfg in DEFAULT_NODES.items():
        backend_nodes[name] = {
            "host": cfg["host"],
            "port": cfg["port"],
            "user": "root",
            "password": "",
            "ssh_user": "ubuntu",
            "mysql_bin": mysql_bin,
        }
    
    # Override with command-line --node options
    if args.node:
        for node_spec in args.node:
            parts = node_spec.split(":")
            if len(parts) != 3:
                print(f"ERROR: Invalid node spec '{node_spec}'. Expected NAME:HOST:PORT")
                return 1
            name, host, port = parts
            backend_nodes[name] = {
                "host": host,
                "port": int(port),
                "user": "root",
                "password": "",
                "ssh_user": "ubuntu",
                "mysql_bin": mysql_bin,
            }

    # Parse replica names
    replica_names = [r.strip() for r in args.replicas.split(",") if r.strip()]
    
    # Validate node names
    all_node_names = [args.primary] + replica_names
    for name in all_node_names:
        if name not in backend_nodes:
            print(f"ERROR: Node '{name}' not defined. Use --node {name}:HOST:PORT to define it.")
            return 1

    # ProxySQL host (auto-detect or use provided)
    proxysql_host = args.proxysql_host or get_local_ip()
    
    # ProxySQL admin (6032)
    admin = ProxySQLAdminClient(host="127.0.0.1", port=6032, user="admin", password="admin")

    # ProxySQL client config
    proxysql_client_cfg = {
        "host": proxysql_host,
        "port": args.proxysql_port,
        "user": args.proxysql_user,
        "password": args.proxysql_pass,
        "connection_timeout": 5,
        "ssl_disabled": True,
        "use_pure": True,
        "autocommit": True,
        "read_timeout": 15,
        "write_timeout": 15,
    }

    print(f"\n[config] Primary: {args.primary} ({backend_nodes[args.primary]['host']}:{backend_nodes[args.primary]['port']})")
    print(f"[config] Replicas: {replica_names}")
    print(f"[config] ProxySQL: {proxysql_host}:{args.proxysql_port}")

    # Set RESET_BASELINE env var if --no-reset
    if args.no_reset:
        os.environ["RESET_BASELINE"] = "0"

    tester = ProxySQLFailoverTester(
        admin=admin,
        proxysql_client_cfg=proxysql_client_cfg,
        backend_nodes=backend_nodes,
        primary_name=args.primary,
        replica_names=replica_names,
    )

    # Generate kill instruction with correct binary path
    primary_host = backend_nodes[args.primary]["host"]
    kill_primary_instructions = (
        f"In another terminal on {args.primary}, stop the primary mysqld:\n"
        f"  sudo pkill -9 -f {mysqld_bin}\n"
        f"(or: ssh ubuntu@{primary_host} 'sudo pkill -9 -f {mysqld_bin}')\n"
    )

    try:
        return tester.run(kill_primary_instructions)
    except Exception as e:
        print(f"\n🚨 TEST FAILED: {e}")
        print(traceback.format_exc())
        return 1


if __name__ == "__main__":
    sys.exit(main())