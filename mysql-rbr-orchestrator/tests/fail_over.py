#!/usr/bin/env python3
import time
import traceback
import uuid
import sys
from typing import Tuple, Optional, Dict, Any, List
import re

import mysql.connector
import requests
from mysql.connector import Error as MySQLError


def mysql_exec(cfg: Dict[str, Any], sql: str, params=None, database: Optional[str] = None):
    conn = mysql.connector.connect(**cfg, database=database) if database else mysql.connector.connect(**cfg)
    try:
        cur = conn.cursor()
        cur.execute(sql, params or ())
        conn.commit()
        return cur.fetchall() if cur.with_rows else None
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()


def mysql_query_one(cfg: Dict[str, Any], sql: str, params=None, database: Optional[str] = None):
    conn = mysql.connector.connect(**cfg, database=database) if database else mysql.connector.connect(**cfg)
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
        

def get_gtid_executed(cfg: Dict[str, Any]) -> str:
    row = mysql_query_one(cfg, "SELECT @@GLOBAL.gtid_executed")
    return row[0] if row and row[0] else ""


def wait_for_executed_gtid_set(cfg: Dict[str, Any], gtid_set: str, timeout_sec: int = 180) -> bool:
    """
    WAIT_FOR_EXECUTED_GTID_SET returns:
      0 on success (set executed)
      1 on timeout
      NULL on error
    """
    row = mysql_query_one(cfg, "SELECT WAIT_FOR_EXECUTED_GTID_SET(%s, %s)", (gtid_set, timeout_sec))
    return row is not None and row[0] == 0


class OrchestratorClient:
    """
    Version-tolerant Orchestrator API wrapper.
    We avoid /api/cluster/<host>/<port> because some builds don't expose it.
    """
    def __init__(self, base_url: str, user: str, password: str):
        self.base_url = base_url.rstrip("/")
        self.auth = (user, password)

    def _get_raw(self, path: str) -> requests.Response:
        url = f"{self.base_url}{path}"
        r = requests.get(url, auth=self.auth, timeout=5)
        return r

    def _get_json(self, path: str) -> Any:
        r = self._get_raw(path)
        if r.status_code == 404:
            raise FileNotFoundError(path)
        r.raise_for_status()
        return r.json()

    def discover(self, host: str, port: int):
        return self._get_json(f"/api/discover/{host}/{port}")

    def master_of(self, host: str, port: int) -> Tuple[str, int]:
        """
        Try a few endpoints in order; different orchestrator builds expose different paths.
        Returns (master_host, master_port).
        """
        # 1) /api/master/<host>/<port>
        try:
            j = self._get_json(f"/api/master/{host}/{port}")
            # usually returns instance object (dict)
            mh = (j.get("Key", {}) or {}).get("Hostname") or j.get("Hostname")
            mp = (j.get("Key", {}) or {}).get("Port") or j.get("Port")
            if mh and mp:
                return mh, int(mp)
        except FileNotFoundError:
            pass

        # 2) /api/instance/<host>/<port> and read MasterKey
        try:
            j = self._get_json(f"/api/instance/{host}/{port}")
            mk = j.get("MasterKey") or {}
            mh = mk.get("Hostname")
            mp = mk.get("Port")
            if mh and mp:
                return mh, int(mp)
        except FileNotFoundError:
            pass

        # 3) /api/topology/<host>/<port> and find the root master
        try:
            j = self._get_json(f"/api/topology/{host}/{port}")
            # topology often returns a list including the master + replicas; master has MasterKey null/empty
            if isinstance(j, list) and len(j) > 0:
                # pick the instance whose MasterKey is empty (or that is not ReadOnly)
                for inst in j:
                    mk = inst.get("MasterKey")
                    if not mk or mk == {}:
                        mh = (inst.get("Key", {}) or {}).get("Hostname") or inst.get("Hostname")
                        mp = (inst.get("Key", {}) or {}).get("Port") or inst.get("Port")
                        if mh and mp:
                            return mh, int(mp)
                # fallback: choose ReadOnly==False
                for inst in j:
                    if inst.get("ReadOnly") is False:
                        mh = (inst.get("Key", {}) or {}).get("Hostname") or inst.get("Hostname")
                        mp = (inst.get("Key", {}) or {}).get("Port") or inst.get("Port")
                        if mh and mp:
                            return mh, int(mp)
        except FileNotFoundError:
            pass

        raise RuntimeError(
            "Could not determine master. Your orchestrator build may use different API paths. "
            "Try: curl -u admin:admin http://127.0.0.1:3000/api/help"
        )
        
    def clusters_info(self) -> List[Dict[str, Any]]:
        j = self._get_json("/api/clusters-info")
        return j if isinstance(j, list) else []

    def topology_text(self, host: str, port: int) -> str:
        j = self._get_json(f"/api/topology/{host}/{port}")
        # This endpoint is wrapped: {"Code":"OK","Message":..., "Details":"...text..."}
        if isinstance(j, dict) and "Details" in j and isinstance(j["Details"], str):
            return j["Details"]
        raise RuntimeError(f"Unexpected topology response: {j}")

    def instance(self, host: str, port: int) -> Dict[str, Any]:
        j = self._get_json(f"/api/instance/{host}/{port}")
        if not isinstance(j, dict):
            raise RuntimeError(f"Unexpected instance response: {j}")
        return j

    def list_instances_from_topology(self, replica_host: str, replica_port: int) -> List[Tuple[str, int]]:
        """
        Parse host:port tokens out of topology text.
        Works with your build where Details is a printable topology table.
        """
        txt = self.topology_text(replica_host, replica_port)
        # matches tokens like "replica1:3306" or "sk030:3306"
        pairs = re.findall(r"([A-Za-z0-9._-]+):(\d+)", txt)
        uniq = []
        seen = set()
        for h, p in pairs:
            key = (h, int(p))
            if key not in seen:
                seen.add(key)
                uniq.append(key)
        return uniq

    def leader_of_seed_topology(self, replica_host: str, replica_port: int) -> Tuple[str, int]:
        """
        Determine leader by querying /api/instance for each host:port seen in topology,
        and picking the one that is writable and has no MasterKey.
        """
        nodes = self.list_instances_from_topology(replica_host, replica_port)

        best = None
        for h, p in nodes:
            try:
                inst = self.instance(h, p)
            except Exception:
                continue

            mk = inst.get("MasterKey")
            ro = inst.get("ReadOnly")

            # Primary: no master key, and read_only false
            if (mk is None or mk == {} ) and (ro is False):
                return h, p

            # keep a fallback: writable even if mk is weird
            if ro is False and best is None:
                best = (h, p)

        if best:
            return best

        raise RuntimeError(f"Could not determine leader from topology nodes: {nodes}")


class FailoverTester:
    def __init__(
        self,
        orch: OrchestratorClient,
        replica_hosts: List[Tuple[str, int]],
        writer_cfg_template: Dict[str, Any],
        replica_cfgs: List[Dict[str, Any]],
        db_name="failover_test_db",
        table_name="items",
    ):
        self.orch = orch
        self.replica_hosts = replica_hosts
        self.writer_cfg_template = writer_cfg_template
        self.replica_cfgs = replica_cfgs
        self.db_name = db_name
        self.table_name = table_name
        self.payload1 = f"before_failover_{uuid.uuid4().hex[:6]}"
        self.payload2 = f"after_failover_{uuid.uuid4().hex[:6]}"

    def cfg_for(self, host: str, port: int) -> Dict[str, Any]:
        """
        Map orchestrator-reported (host,port) to a host-reachable endpoint AND the right credentials.
        - Host mysqld: connect via 127.0.0.1:3306, root password "" (as in your script)
        - Replica containers: connect via 127.0.0.1:3307/3308, root password "rootpass"
        """
        # 1) endpoint mapping (orchestrator sees docker names / ids, host test needs forwarded ports)
        hostport_map = {
            ("replica1", 3306): ("127.0.0.1", 3307),
            ("replica2", 3306): ("127.0.0.1", 3308),
            ("host.docker.internal", 3306): ("127.0.0.1", 3306),
            ("sk030", 3306): ("127.0.0.1", 3306),
            ("127.0.0.1", 3306): ("127.0.0.1", 3306),
        }
        h2, p2 = hostport_map.get((host, port), (host, port))

        # 2) credential mapping based on the resolved endpoint
        if (h2, p2) == ("127.0.0.1", 3306):
            # host mysqld
            user = "root"
            password = ""          # <-- keep consistent with your current setup script
        elif p2 in (3307, 3308):
            # docker replicas
            user = "root"
            password = "rootpass"
        else:
            # fallback (you can extend this)
            user = self.writer_cfg_template.get("user", "root")
            password = self.writer_cfg_template.get("password", "")

        return {"host": h2, "port": p2, "user": user, "password": password}


    def setup_schema(self, writer_cfg: Dict[str, Any]):
        print(f"[*] Creating schema on writer {writer_cfg['host']}:{writer_cfg['port']}")
        mysql_exec(writer_cfg, f"CREATE DATABASE IF NOT EXISTS {self.db_name}")
        mysql_exec(
            writer_cfg,
            f"""
            CREATE TABLE IF NOT EXISTS {self.db_name}.{self.table_name} (
              id INT PRIMARY KEY,
              content VARCHAR(255)
            )
            """
        )

    def insert(self, writer_cfg: Dict[str, Any], row_id: int, payload: str):
        print(f"[*] Inserting id={row_id} payload='{payload}' to {writer_cfg['host']}:{writer_cfg['port']}")
        mysql_exec(
            writer_cfg,
            f"INSERT INTO {self.db_name}.{self.table_name} (id, content) VALUES (%s, %s)",
            (row_id, payload),
        )
        
    def insert_and_get_gtid_set(self, writer_cfg: Dict[str, Any], row_id: int, payload: str) -> str:
        """
        Insert a row and return the writer's @@GLOBAL.gtid_executed AFTER the commit.
        We'll use that GTID set as the "applied up to here" watermark for replicas.
        """
        _before = get_gtid_executed(writer_cfg)

        print(f"[*] Inserting id={row_id} payload='{payload}' to {writer_cfg['host']}:{writer_cfg['port']}")
        mysql_exec(
            writer_cfg,
            f"INSERT INTO {self.db_name}.{self.table_name} (id, content) VALUES (%s, %s)",
            (row_id, payload),
        )

        after = get_gtid_executed(writer_cfg)
        return after

    def verify_payload(self, instances: List[Dict[str, Any]], payload: str) -> bool:
        ok = True
        time.sleep(2)
        for cfg in instances:
            h, p = cfg["host"], cfg["port"]
            print(f"[*] Checking {h}:{p} for payload='{payload}' ...")
            try:
                row = mysql_query_one(
                    cfg, f"SELECT content FROM {self.table_name} WHERE content=%s", (payload,), database=self.db_name
                )
                if row:
                    print("    ✅ found")
                else:
                    print("    ❌ not found")
                    ok = False
            except MySQLError as e:
                print(f"    ❌ connection/query error: {e}")
                ok = False
        return ok
    
    def wait_for_replicas_apply(self, target_instances: List[Dict[str, Any]], gtid_set: str, timeout_sec: int = 180) -> bool:
        """
        Wait for each instance to apply (execute) up to gtid_set.
        """
        all_ok = True
        for cfg in target_instances:
            h, p = cfg["host"], cfg["port"]
            print(f"[*] Waiting for {h}:{p} to execute GTID set (timeout={timeout_sec}s)...")
            try:
                if wait_for_executed_gtid_set(cfg, gtid_set, timeout_sec=timeout_sec):
                    print("    ✅ applied")
                else:
                    print("    ❌ timeout waiting for apply")
                    all_ok = False
            except Exception as e:
                print(f"    ❌ error waiting for apply: {e}")
                all_ok = False
        return all_ok

    def wait_for_master_change(self, replica_hosts: List[Tuple[str, int]], old_master: Tuple[str, int], timeout_sec=180):
        print(f"[*] Waiting for master change (timeout={timeout_sec}s)...")
        deadline = time.time() + timeout_sec

        while time.time() < deadline:
            for rh, rp in replica_hosts:
                try:
                    m = self.orch.leader_of_seed_topology(rh, rp)
                    if m != old_master:
                        print(f"    ✅ New master: {m[0]}:{m[1]}")
                        return m
                    print(f"    ...still master {m[0]}:{m[1]}")
                    break
                except Exception as e:
                    # try next seed
                    last = e
                    continue
            else:
                print(f"    ...orchestrator/leader lookup not ready yet: {last}")
            time.sleep(2)

        raise TimeoutError("Timed out waiting for master to change")

    def cleanup_best_effort(self, any_cfg: Dict[str, Any]):
        try:
            print(f"[*] Cleanup: dropping database '{self.db_name}' on {any_cfg['host']}:{any_cfg['port']}")
            mysql_exec(any_cfg, f"DROP DATABASE IF EXISTS {self.db_name}")
            print(f"    ✅ Cleanup succeeded for {any_cfg['host']}:{any_cfg['port']}")
        except Exception as e:
            print(f"[!] Cleanup (ignored): {e}")

    def run(self, kill_primary_instructions: str) -> int:
        for replica_host, replica_port in self.replica_hosts:
            # Ensure seed is discovered
            print(f"[*] Orchestrator discover seed instance {replica_host}:{replica_port}")
            self.orch.discover(replica_host, replica_port)

        # Determine current master via orchestrator
        old_master = self.orch.master_of(self.replica_hosts[0][0], self.replica_hosts[0][1])
        print(f"[*] Current master: {old_master[0]}:{old_master[1]}")
        old_master_cfg = self.cfg_for(*old_master)
        self.cleanup_best_effort(old_master_cfg)

        try:
            self.setup_schema(old_master_cfg)
            gtid_set_1 = self.insert_and_get_gtid_set(old_master_cfg, 1, self.payload1)

            # Pre-failover: wait for BOTH replicas to apply up to this point, then verify payload (sanity)
            if not self.wait_for_replicas_apply(self.replica_cfgs, gtid_set_1, timeout_sec=180):
                print("\n🚨 Pre-failover GTID apply wait failed.")
                return 2

            if not self.verify_payload(self.replica_cfgs, self.payload1):
                print("\n🚨 Pre-failover replication verification failed.")
                return 2

            print("\n=== ACTION REQUIRED: trigger primary failure ===")
            print(kill_primary_instructions)
            print("=============================================\n")

            new_master = self.wait_for_master_change(self.replica_hosts, old_master)
            print(f"[*] New master (orch view): {new_master[0]}:{new_master[1]}")
            new_master_cfg = self.cfg_for(*new_master)

            # Post-failover write: capture GTID watermark on NEW master
            gtid_set_2 = self.insert_and_get_gtid_set(new_master_cfg, 2, self.payload2)

            # Post-failover: wait for replicas (excluding the new master endpoint) to apply the new watermark
            targets = []
            for cfg in self.replica_cfgs:
                if cfg["host"] == new_master_cfg["host"] and cfg["port"] == new_master_cfg["port"]:
                    continue
                targets.append(cfg)

            if not self.wait_for_replicas_apply(targets, gtid_set_2, timeout_sec=300):
                print("\n🚨 Post-failover GTID apply wait failed.")
                return 3

            # Sanity: now the payload should be visible on remaining replicas
            if not self.verify_payload(targets, self.payload2):
                print("\n🚨 Post-failover replication verification failed.")
                return 3

            print("\n🎉 FAILOVER VERIFIED (master changed + replicas applied GTIDs after promotion)")
            return 0
        except Exception as e:
            print(f"\n🚨 TEST FAILED: {e}")
            print(traceback.format_exc())
        finally:
            # try cleanup on all nodes
            for cfg in [old_master_cfg] + self.replica_cfgs:
                self.cleanup_best_effort(cfg)
            


def main():
    orch = OrchestratorClient(
        base_url="http://127.0.0.1:3000",
        user="admin",
        password="admin",
    )

    replica_hosts = [("replica1", 3306), ("replica2", 3306)]

    # How to connect to whichever master orchestrator promotes
    # IMPORTANT: When the master becomes a Docker container (replica1/replica2),
    # you CANNOT reach it as "replica1:3306" from the host.
    # You must use your host port-forward: 3307/3308.
    #
    # So for writing, we will map promoted hostnames to host ports in a tiny mapping below.
    writer_cfg_template = {"user": "root", "password": ""}

    # replicas reachable from host
    replica_cfgs = [
        {"host": "127.0.0.1", "port": 3307, "user": "root", "password": "rootpass"},
        {"host": "127.0.0.1", "port": 3308, "user": "root", "password": "rootpass"},
    ]

    tester = FailoverTester(
        orch=orch,
        replica_hosts=replica_hosts,
        writer_cfg_template=writer_cfg_template,
        replica_cfgs=replica_cfgs,
    )

    kill_primary_instructions = (
        "In another terminal, stop the CURRENT PRIMARY (your host mysqld at 3306):\n"
        "  sudo systemctl stop mysql   # or mysqld (use `systemctl list-units | grep -i mysql`)\n"
        "Then come back here and wait.\n\n"
        "After the test, restore:\n"
        "  sudo systemctl start mysql\n"
    )

    rc = tester.run(kill_primary_instructions)
    sys.exit(rc)


if __name__ == "__main__":
    main()
