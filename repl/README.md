# MySQL Group Replication Auto-Configuration for LineairDB

This module provides **automatic MySQL Group Replication configuration** for the LineairDB storage engine, enabling distributed database operations with a simple script execution.

---

## Goals

### Functional Requirements

1. **Automatic Group Replication Configuration**
   - MySQL Group Replication is configured **simply by executing the script**
   - Leader/Follower role assignment is performed automatically

2. **Node Specification via Configuration File**
   - Leader address configurable
   - Follower addresses configurable (multiple allowed)
   - **Default**: Local MySQL server is configured as the Leader

3. **Script Format**
   - Implemented in both Shell Script and Python
   - Easy to read with clear processing flow

### Benchmark Requirements

- Benchmark runs after GR configuration completes
- Supports `engine=lineairdb` specification
- Both **benchbase** and **binbench** must complete successfully

---

## Quick Start

### Prerequisites

- MySQL 8.0.43+ (installed locally)
- MySQL Shell (`mysqlsh`)
- Docker and Docker Compose
- Python 3.8+
- Java 17+ (for benchmarks)

### Complete Flow (Master Script)

```bash
cd repl/

# Run the complete flow: init → start → benchmark → verify
./scripts/run_all.sh

# Or with custom number of secondary nodes
./scripts/run_all.sh 5
```

### Step-by-Step

```bash
cd repl/

# 1. Initialize cluster (default: 2 secondaries)
./scripts/init_cluster.sh 3

# 2. Start cluster with Group Replication
./scripts/start_cluster.sh

# 3. Check status (nodes + GR + LineairDB plugin)
./scripts/status.sh

# 4. Run benchmark with LineairDB
./scripts/run_benchmark.sh benchbase ycsb
./scripts/run_benchmark.sh binbench ycsb

# 5. Clean up when done
./scripts/cleanup.sh --all
```

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                    LineairDB Storage Engine                         │
│                   (ha_lineairdb_storage_engine.so)                  │
└─────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│                 Group Replication Cluster                           │
│                    (InnoDB Cluster)                                 │
└─────────────────────────────────────────────────────────────────────┘
                                   │
         ┌─────────────────────────┼─────────────────────────┐
         ▼                         ▼                         ▼
┌─────────────────┐       ┌─────────────────┐       ┌─────────────────┐
│    Primary      │       │   Secondary 1   │       │   Secondary N   │
│    (Local)      │◄─────►│   (Docker)      │◄─────►│   (Docker)      │
│  127.0.0.1:3306 │       │ 127.0.0.1:33062 │       │ 127.0.0.1:3306X │
│    R/W Mode     │       │    R/O Mode     │       │    R/O Mode     │
└─────────────────┘       └─────────────────┘       └─────────────────┘
```

---

## Scripts Reference

| Script | Description |
|--------|-------------|
| `run_all.sh [n]` | **Master script**: Complete flow (init → start → benchmark → verify) |
| `init_cluster.sh [n]` | Initialize cluster with n secondary nodes |
| `start_cluster.sh` | Start cluster and configure Group Replication |
| `stop_cluster.sh` | Stop secondary containers |
| `status.sh` | Check node status, GR status, and LineairDB plugin |
| `install_plugin.sh` | Install LineairDB plugin on all nodes |
| `run_benchmark.sh <tool> <type>` | Run benchmark (benchbase/binbench) |
| `cleanup.sh [--all]` | Clean up cluster resources |

---

## Configuration

### File: `config/cluster_config.json`

```json
{
  "cluster_name": "lineairdb_cluster",
  "mysql_root_password": "kamo",
  "primary": {
    "host": "127.0.0.1",
    "port": 3306,
    "role": "primary"
  },
  "secondaries": [
    {"host": "127.0.0.1", "port": 33062, "container_name": "mysql-secondary-1"},
    {"host": "127.0.0.1", "port": 33063, "container_name": "mysql-secondary-2"}
  ]
}
```

### Default Credentials

| Setting | Value |
|---------|-------|
| Root Password | `kamo` |
| Cluster Name | `lineairdb_cluster` |
| Primary Port | `3306` |
| Secondary Ports | `33062`, `33063`, ... |

⚠️ **WARNING**: Change passwords in production!

---

## Directory Structure

```
repl/
├── __init__.py              # Python module init
├── replication.py           # Python API (optional)
├── README.md                # This file
├── config/                  # Configuration (created at runtime)
│   └── cluster_config.json
└── scripts/
    ├── run_all.sh           # Master script
    ├── init_cluster.sh      # Initialize cluster
    ├── start_cluster.sh     # Start with GR setup
    ├── stop_cluster.sh      # Stop containers
    ├── status.sh            # Status check
    ├── install_plugin.sh    # Install LineairDB plugin
    ├── run_benchmark.sh     # Run benchmarks
    └── cleanup.sh           # Clean up resources
```

---

## Python API (Optional)

```python
from repl import LineairDBReplicationManager

manager = LineairDBReplicationManager()

# Initialize and start
manager.init(num_secondaries=3)
manager.start()

# Check status
print(manager.get_status())

# Run benchmark
manager.run_benchmark("benchbase", "ycsb")

# Clean up
manager.cleanup(remove_all=True)
```

---

## Benchmark Results

After running `./scripts/run_benchmark.sh`, results are saved to:
- Benchbase: `bench/results/lineairdb/gr_test/`
- Binbench: `bench/results/lineairdb/binbench_gr_test/`

### Expected Output

```
Tool: benchbase
Benchmark: ycsb
Engine: lineairdb

✓ GR Cluster is running
✓ LineairDB plugin is active
✓ Database 'benchbase' created

Throughput: ~10,000+ requests/sec
Transactions: 300,000+
Errors: 0

✓ SUCCESS: Benchmark completed with engine=lineairdb
```

---

## Troubleshooting

### Cluster Won't Start

1. Check Docker: `docker ps`
2. Check MySQL: `sudo systemctl status mysql`
3. Check ports: `netstat -tuln | grep 3306`
4. Check logs: `docker logs mysql-secondary-1`

### LineairDB Plugin Issues

1. Build plugin: `cd .. && cmake --build release --target ha_lineairdb_storage_engine`
2. Check plugin: `ls ../release/library_output_directory/plugin/`
3. Install manually: `./scripts/install_plugin.sh --release`

### Replication Issues

1. Check GR status: `mysqlsh --uri root:kamo@127.0.0.1:3306 -e "dba.getCluster().status()"`
2. Check connectivity between nodes
3. Try cleanup and restart: `./scripts/cleanup.sh --all && ./scripts/run_all.sh`

### "Table already exists" Error

LineairDB stores data in `/var/lib/mysql/lineairdb_logs/`. This persists after `DROP TABLE`.
The benchmark script handles this, but you can manually clean:
```bash
sudo rm -rf /var/lib/mysql/lineairdb_logs/*
sudo systemctl restart mysql
```

---

## Completion Checklist

- [x] MySQL Group Replication automatically configured
- [x] Leader/Follower roles assigned automatically
- [x] Node specification via configuration file
- [x] Local MySQL configured as Leader by default
- [x] Shell script implementation
- [x] Python API implementation
- [x] Benchbase with engine=lineairdb succeeds
- [x] Binbench with engine=lineairdb succeeds
