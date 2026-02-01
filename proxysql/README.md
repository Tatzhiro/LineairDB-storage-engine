# ProxySQL Setup Guide

Set up ProxySQL with MySQL/LineairDB backends for read/write splitting and failover.

## Quick Start

```bash
cd ~/LineairDB-storage-engine/proxysql

# 1. Setup - registers backends, creates user, configures routing
./setup_proxysql.sh

# 2. Check status
./status.sh

# 3. Test connection
mysql -u proxysql_user -pproxysql_pass -h <PROXYSQL_HOST> -P 6033 \
  -e "SELECT @@hostname, @@server_id, @@read_only;"

# 4. Teardown (optional)
./teardown_proxysql.sh
```

## Failover Test

```bash
# Run with defaults
python3 tests/fail_over.py

# Specify nodes
python3 tests/fail_over.py --primary database2-01 --replicas database2-02,database2-03

# Custom nodes
python3 tests/fail_over.py \
    --node mymaster:192.168.1.10:3306 \
    --node replica1:192.168.1.11:3306 \
    --primary mymaster --replicas replica1

# See all options
python3 tests/fail_over.py --help
```

> **Note:** After the failover test, remember to restart mysqld on the primary node and run `./setup_proxysql.sh` to restore the original configuration.

## Configuration

**Default Nodes:**
| Node | Host | Port |
|------|------|------|
| database2-01 (primary) | 133.125.85.242 | 3306 |
| database2-02 (replica) | 133.242.17.72 | 3306 |
| database2-03 (replica) | 153.120.20.111 | 3306 |

**ProxySQL Ports:**
- `6032` - Admin interface
- `6033` - Client interface

**Query Routing:**
- `SELECT ... FOR UPDATE` → Writer (HG 0)
- `SELECT` → Reader (HG 1)
- All other queries → Writer (HG 0)

## Troubleshooting

**ProxySQL not running:**
```bash
sudo systemctl status proxysql
sudo journalctl -u proxysql --no-pager | tail -50
```

**DDL hangs (semi-sync issue):**
```bash
# Check and fix semi-sync timeout
mysql -u root -e "SET GLOBAL rpl_semi_sync_source_timeout = 10000;"
```

**Stuck queries:**
```bash
mysql -u root -e "SHOW PROCESSLIST;"
# Temporarily disable semi-sync to release
mysql -u root -e "SET GLOBAL rpl_semi_sync_source_enabled = OFF;"
```
