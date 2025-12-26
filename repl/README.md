# LineairDB Group Replication

Tools for setting up MySQL Group Replication clusters and benchmarking storage engines.

## What Can You Do?

### 1. Benchmark Storage Engines on GR Cluster

Compare performance of storage engines on a Group Replication cluster.

```bash
cd repl/scripts

# Compare LineairDB vs InnoDB (using system MySQL)
python3 run_all.py 2 lineairdb innodb

# Benchmark only LineairDB
python3 run_all.py 2 lineairdb

# Compare all three engines
python3 run_all.py 2 lineairdb fence innodb

# Use local build MySQL as primary (binbench mode)
python3 run_all.py 2 lineairdb --tool binbench
```

### 2. Just Set Up Group Replication (No Benchmark)

If you only want to set up a cluster without running benchmarks:

```bash
cd repl/scripts

# Initialize and start a 2-node cluster (system MySQL)
python3 init_cluster.py 2
python3 start_cluster.py

# Or use local build MySQL as primary
python3 start_cluster.py --binbench

# Check cluster status
python3 status.py

# Stop cluster when done
python3 stop_cluster.py
```

## Command Format

```
python3 run_all.py <num_secondaries> <engine1> [engine2] ... [options]
```

| Argument | Description |
|----------|-------------|
| `num_secondaries` | Number of secondary nodes (1-10) |
| `engine1, engine2, ...` | Storage engines to benchmark |

| Option | Description |
|--------|-------------|
| `--tool TOOL` | `benchbase` (system MySQL) or `binbench` (local build MySQL) |
| `--remote HOST` | Use remote host as secondary (repeatable) |
| `--terminals N` | Concurrent connections (default: 4) |
| `--time N` | Benchmark duration in seconds (default: 30) |

## Benchmark Tools

| Tool | Primary MySQL | Results Directory |
|------|---------------|-------------------|
| `benchbase` (default) | System MySQL (`systemctl`) | `bench/results/<engine>/benchbase/` |
| `binbench` | Local build (`build/bin/mysqld`) | `bench/results/<engine>/binbench/` |

```bash
# Use system MySQL (default)
python3 run_all.py 2 lineairdb

# Use local build MySQL (requires MySQL built in build/ directory)
python3 run_all.py 2 lineairdb --tool binbench
```

**Note:** `binbench` requires MySQL built from source in `build/` directory:
```bash
mkdir -p build && cd build
cmake ../third_party/mysql-server -DCMAKE_BUILD_TYPE=Debug -G Ninja ...
ninja mysqld
```

## Using Remote Hosts

By default, secondary nodes run as Docker containers. To use remote MySQL servers:

```bash
# 2 remote hosts as secondaries (format: host[:port[:ssh_user]])
python3 run_all.py 2 lineairdb --remote 192.168.1.10::ubuntu --remote 192.168.1.11::ubuntu
```

## Example Output

```
% python3 run_all.py 2 innodb fence lineairdb
╔══════════════════════════════════════════════════════════════════════════╗
║ Engine                 │ Throughput      │ Goodput         │ Avg Latency  ║
╠══════════════════════════════════════════════════════════════════════════╣
║ InnoDB                 │ 2992.98         │ 3105.69         │ 1324.00      ║
║ LineairDB-Fence        │ 49.73           │ 51.60           │ 80102.00     ║
║ LineairDB              │ 10163.59        │ 10377.69        │ 389.00       ║
╚══════════════════════════════════════════════════════════════════════════╝
```

## Notes

- **LineairDB tables are local only** - they do not replicate across nodes
- InnoDB tables replicate via Group Replication
- Run `python3 <script>.py --help` for more options
