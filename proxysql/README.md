# ProxySQL Setup for LineairDB

ProxySQL configuration for MySQL/LineairDB backends with read/write splitting, failover, and GTID causal reads.

## Quick Start

### 1. Configure Environment

Edit `config` to set your server IP addresses and hostnames:

```bash
vim config
# Update PRIMARY_HOST, REPLICA1_HOST, REPLICA2_HOST, etc.
```

### 2. Install ProxySQL (if not installed)

```bash
sudo ./install/install_proxysql.sh
```

### 3. Setup ProxySQL

```bash
sudo ./scripts/setup_proxysql.sh
```

### 4. Check Status

```bash
./scripts/status.sh
```

### 5. Test Connection

```bash
mysql -u proxysql_user -pproxysql_pass -h 127.0.0.1 -P 6033 \
  -e "SELECT @@hostname, @@server_id, @@read_only;"
```

## Configuration

All configuration is centralized in the `config` file:

| Variable | Description | Default |
|----------|-------------|---------|
| `PRIMARY_HOST` | Primary (writer) IP | 133.125.85.242 |
| `REPLICA1_HOST` | First replica IP | 133.242.17.72 |
| `REPLICA2_HOST` | Second replica IP | 153.120.20.111 |
| `FRONTEND_USER` | ProxySQL client user | proxysql_user |
| `FRONTEND_PASS` | ProxySQL client password | proxysql_pass |
| `WRITER_HG` | Writer hostgroup ID | 0 |
| `READER_HG` | Reader hostgroup ID | 1 |

## Query Routing

| Query Type | Destination |
|------------|-------------|
| `SELECT ... FOR UPDATE` | Writer (HG 0) |
| `SELECT` | Reader (HG 1) |
| All other queries | Writer (HG 0) |

## GTID Causal Reads

For read-after-write consistency:

### Prerequisites

1. Install binlog reader on each MySQL server:
   ```bash
   sudo ./install/install_binlog_reader.sh
   ```

2. Start binlog reader on each server:
   ```bash
   proxysql_binlog_reader -h 127.0.0.1 -u repl_user -p repl_pass -P 3306 -l 6020 &
   ```

### Enable

```bash
sudo ./scripts/enable_gtid_causal_read.sh
```

### Test

```bash
cd tests && python3 gtid_causal_read_test.py --verbose
```

### Disable

To restore simple read/write splitting (no consistency guarantees):

```bash
sudo ./scripts/disable_gtid_causal_read.sh
```

## Tests

All tests verify LineairDB storage engine is used.

| Test | Command | Description |
|------|---------|-------------|
| Read/Write Split | `python3 tests/read_write_split.py` | Verify query routing |
| Replication | `python3 tests/replication.py` | Verify data replication |
| GTID Causal | `python3 tests/gtid_causal_read_test.py` | Verify read consistency |
| Failover | `python3 tests/fail_over.py` | Test primary failure recovery |

## Teardown

```bash
sudo ./scripts/teardown_proxysql.sh
```

## Ports

| Port | Service |
|------|---------|
| 6032 | ProxySQL Admin |
| 6033 | ProxySQL Client |
| 6020 | Binlog Reader |
| 3306 | MySQL |
