#!/bin/bash
#
# LineairDB Group Replication - Master Script
#
# This script runs the complete flow:
#   1. Clean up any existing cluster
#   2. Initialize cluster with specified number of secondaries
#   3. Start cluster with Group Replication
#   4. Run benchmarks with LineairDB (FENCE=off) and Fence (FENCE=on)
#   5. Verify replication is working
#
# Usage:
#   ./run_all.sh [num_secondaries] [options]
#
# Options:
#   --terminals N   Number of concurrent terminals for benchmark (default: 4)
#   --time N        Benchmark duration in seconds (default: 30)
#   --skip-fence    Skip the fence benchmark (only run lineairdb)
#   --skip-lineairdb Skip the lineairdb benchmark (only run fence)
#
# Examples:
#   ./run_all.sh           # Default: 2 secondaries, both benchmarks
#   ./run_all.sh 3         # 3 secondaries
#   ./run_all.sh 2 --terminals 8 --time 60
#   ./run_all.sh 2 --skip-fence  # Only run lineairdb benchmark

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPL_DIR="$(dirname "$SCRIPT_DIR")"
ROOT_DIR="$(dirname "$REPL_DIR")"

# Parse arguments
NUM_SECONDARIES=${1:-2}
shift 2>/dev/null || true

# Defaults
TERMINALS=4
DURATION=30
RUN_LINEAIRDB=true
RUN_FENCE=true

# Configuration
MYSQL_USER="root"
MYSQL_PASSWORD="kamo"

# Parse optional arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --terminals)
            TERMINALS="$2"
            shift 2
            ;;
        --time)
            DURATION="$2"
            shift 2
            ;;
        --skip-fence)
            RUN_FENCE=false
            shift
            ;;
        --skip-lineairdb)
            RUN_LINEAIRDB=false
            shift
            ;;
        *)
            shift
            ;;
    esac
done

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

print_header() {
    echo ""
    echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${BLUE}║${NC} $1"
    echo -e "${BLUE}╚════════════════════════════════════════════════════════════╝${NC}"
    echo ""
}

print_step() {
    echo -e "${YELLOW}▶ $1${NC}"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

print_info() {
    echo -e "${CYAN}ℹ $1${NC}"
}

print_header "LineairDB Group Replication - Complete Flow"

echo "Configuration:"
echo "  Secondary nodes:  $NUM_SECONDARIES"
echo "  Terminals:        $TERMINALS"
echo "  Duration:         ${DURATION}s"
echo "  Run LineairDB:    $RUN_LINEAIRDB (FENCE=off, async)"
echo "  Run Fence:        $RUN_FENCE (FENCE=on, sync)"
echo ""

START_TIME=$(date +%s)

# ===========================================
# Helper function to setup cluster with specific FENCE mode
# ===========================================
setup_cluster_and_benchmark() {
    local ENGINE=$1
    local FENCE_VALUE=$2
    local FENCE_DESC=$3
    
    print_header "Setting up cluster for $ENGINE (FENCE=$FENCE_VALUE)"
    
    # Step 1: Cleanup
    print_step "Cleaning up any existing cluster..."
    "$SCRIPT_DIR/cleanup.sh" --all 2>/dev/null || true
    print_success "Cleanup complete"
    
    # Step 2: Build plugin with correct FENCE mode (build only, no install yet)
    print_step "Building plugin with FENCE=$FENCE_VALUE..."
    "$SCRIPT_DIR/install_plugin.sh" --$ENGINE --release --build-only
    print_success "Plugin built with FENCE=$FENCE_VALUE"
    
    # Step 3: Initialize cluster
    print_step "Initializing cluster with $NUM_SECONDARIES secondary nodes..."
    "$SCRIPT_DIR/init_cluster.sh" "$NUM_SECONDARIES"
    print_success "Cluster initialized"
    
    # Step 4: Start cluster (this installs the pre-built plugin)
    print_step "Starting cluster with Group Replication..."
    if ! "$SCRIPT_DIR/start_cluster.sh"; then
        print_error "Failed to start cluster"
        return 1
    fi
    print_success "Cluster started"
    
    # Wait for GR to stabilize
    sleep 5
    
    # Step 5: Verify cluster
    print_step "Verifying cluster status..."
    "$SCRIPT_DIR/status.sh"
    
    # Step 6: Run benchmark (no rebuild - plugin already installed)
    print_step "Running YCSB benchmark with engine=$ENGINE..."
    print_info "$FENCE_DESC"
    echo ""
    
    if "$SCRIPT_DIR/run_benchmark.sh" $ENGINE ycsb --terminals $TERMINALS --time $DURATION --no-rebuild; then
        return 0
    else
        return 1
    fi
}

# ===========================================
# Run LineairDB Benchmark (FENCE=off)
# ===========================================
LINEAIRDB_SUCCESS=false
LINEAIRDB_THROUGHPUT=""
LINEAIRDB_GOODPUT=""
LINEAIRDB_LATENCY=""

if [ "$RUN_LINEAIRDB" = true ]; then
    print_header "LineairDB Benchmark (FENCE=off, async)"
    
    if setup_cluster_and_benchmark "lineairdb" "false" "LineairDB uses asynchronous commits for maximum performance"; then
        print_success "LineairDB benchmark completed successfully"
        LINEAIRDB_SUCCESS=true
        
        # Extract throughput from latest JSON summary
        LATEST_RESULT=$(ls -t "$ROOT_DIR/bench/results/lineairdb/gr_test/"*summary*.json 2>/dev/null | head -1)
        if [ -n "$LATEST_RESULT" ] && [ -f "$LATEST_RESULT" ]; then
            LINEAIRDB_THROUGHPUT=$(python3 -c "
import json
with open('$LATEST_RESULT') as f:
    data = json.load(f)
    print(f\"{data.get('Throughput (requests/second)', 0):.2f}\")
" 2>/dev/null || echo "N/A")
            LINEAIRDB_GOODPUT=$(python3 -c "
import json
with open('$LATEST_RESULT') as f:
    data = json.load(f)
    print(f\"{data.get('Goodput (requests/second)', 0):.2f}\")
" 2>/dev/null || echo "N/A")
            LINEAIRDB_LATENCY=$(python3 -c "
import json
with open('$LATEST_RESULT') as f:
    data = json.load(f)
    lat = data.get('Latency Distribution', {}).get('Average Latency (microseconds)', 0)
    print(f\"{lat:.2f}\")
" 2>/dev/null || echo "N/A")
        fi
    else
        print_error "LineairDB benchmark failed"
    fi
else
    print_header "LineairDB Benchmark (SKIPPED)"
    print_info "Skipped by --skip-lineairdb flag"
fi

# ===========================================
# Run Fence Benchmark (FENCE=on)
# ===========================================
FENCE_SUCCESS=false
FENCE_THROUGHPUT=""
FENCE_GOODPUT=""
FENCE_LATENCY=""

if [ "$RUN_FENCE" = true ]; then
    print_header "Fence Benchmark (FENCE=on, sync)"
    
    if setup_cluster_and_benchmark "fence" "true" "Fence uses synchronous commits for durability guarantees"; then
        print_success "Fence benchmark completed successfully"
        FENCE_SUCCESS=true
        
        # Extract throughput from latest JSON summary
        LATEST_RESULT=$(ls -t "$ROOT_DIR/bench/results/fence/gr_test/"*summary*.json 2>/dev/null | head -1)
        if [ -n "$LATEST_RESULT" ] && [ -f "$LATEST_RESULT" ]; then
            FENCE_THROUGHPUT=$(python3 -c "
import json
with open('$LATEST_RESULT') as f:
    data = json.load(f)
    print(f\"{data.get('Throughput (requests/second)', 0):.2f}\")
" 2>/dev/null || echo "N/A")
            FENCE_GOODPUT=$(python3 -c "
import json
with open('$LATEST_RESULT') as f:
    data = json.load(f)
    print(f\"{data.get('Goodput (requests/second)', 0):.2f}\")
" 2>/dev/null || echo "N/A")
            FENCE_LATENCY=$(python3 -c "
import json
with open('$LATEST_RESULT') as f:
    data = json.load(f)
    lat = data.get('Latency Distribution', {}).get('Average Latency (microseconds)', 0)
    print(f\"{lat:.2f}\")
" 2>/dev/null || echo "N/A")
        fi
    else
        print_error "Fence benchmark failed"
    fi
else
    print_header "Fence Benchmark (SKIPPED)"
    print_info "Skipped by --skip-fence flag"
fi

# ===========================================
# Final: Verify Replication (InnoDB)
# ===========================================
print_header "Verify Group Replication (Final Cluster State)"

print_step "Testing Group Replication with InnoDB table..."

# Create a test database and table with InnoDB
mysql -h 127.0.0.1 -P 3306 -u$MYSQL_USER -p$MYSQL_PASSWORD -e "
CREATE DATABASE IF NOT EXISTS gr_test;
USE gr_test;
DROP TABLE IF EXISTS repl_check;
CREATE TABLE repl_check (id INT PRIMARY KEY, ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP) ENGINE=InnoDB;
INSERT INTO repl_check (id) VALUES (1), (2), (3);
" 2>/dev/null

sleep 2  # Wait for replication

# Verify replication
REPLICATION_OK=true
PRIMARY_COUNT=$(mysql -h 127.0.0.1 -P 3306 -u$MYSQL_USER -p$MYSQL_PASSWORD -N -e \
    "SELECT COUNT(*) FROM gr_test.repl_check;" 2>/dev/null || echo "0")
echo "  Primary (port 3306): $PRIMARY_COUNT rows"

for i in $(seq 1 $NUM_SECONDARIES); do
    port=$((33061 + i))
    count=$(mysql -h 127.0.0.1 -P $port -u$MYSQL_USER -p$MYSQL_PASSWORD -N -e \
        "SELECT COUNT(*) FROM gr_test.repl_check;" 2>/dev/null || echo "0")
    
    if [ "$count" = "$PRIMARY_COUNT" ]; then
        print_success "  mysql-secondary-$i (port $port): $count rows (replicated ✓)"
    else
        print_error "  mysql-secondary-$i (port $port): $count rows (expected $PRIMARY_COUNT)"
        REPLICATION_OK=false
    fi
done

echo ""
echo "Note: LineairDB tables do NOT participate in Group Replication."
echo "      Only InnoDB tables are replicated via GR."

# ===========================================
# Summary
# ===========================================
print_header "Summary"

END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))

echo "Time elapsed: ${ELAPSED}s"
echo ""
echo "Configuration:"
echo "  Secondary nodes: $NUM_SECONDARIES"
echo "  Terminals:       $TERMINALS"
echo "  Duration:        ${DURATION}s"
echo ""

# ===========================================
# Benchmark Results Comparison
# ===========================================
echo -e "${CYAN}╔══════════════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${CYAN}║                        BENCHMARK RESULTS                                 ║${NC}"
echo -e "${CYAN}╠══════════════════════════════════════════════════════════════════════════╣${NC}"
printf "${CYAN}║${NC} %-22s │ %-15s │ %-15s │ %-12s ${CYAN}║${NC}\n" "Engine" "Throughput" "Goodput" "Avg Latency"
printf "${CYAN}║${NC} %-22s │ %-15s │ %-15s │ %-12s ${CYAN}║${NC}\n" "" "(req/s)" "(req/s)" "(μs)"
echo -e "${CYAN}╠══════════════════════════════════════════════════════════════════════════╣${NC}"

if [ "$RUN_LINEAIRDB" = true ]; then
    if [ "$LINEAIRDB_SUCCESS" = true ]; then
        printf "${CYAN}║${NC} ${GREEN}%-22s${NC} │ ${GREEN}%-15s${NC} │ ${GREEN}%-15s${NC} │ ${GREEN}%-12s${NC} ${CYAN}║${NC}\n" \
            "LineairDB (FENCE=off)" "${LINEAIRDB_THROUGHPUT:-N/A}" "${LINEAIRDB_GOODPUT:-N/A}" "${LINEAIRDB_LATENCY:-N/A}"
    else
        printf "${CYAN}║${NC} ${RED}%-22s${NC} │ ${RED}%-15s${NC} │ ${RED}%-15s${NC} │ ${RED}%-12s${NC} ${CYAN}║${NC}\n" \
            "LineairDB (FENCE=off)" "FAILED" "-" "-"
    fi
fi

if [ "$RUN_FENCE" = true ]; then
    if [ "$FENCE_SUCCESS" = true ]; then
        printf "${CYAN}║${NC} ${GREEN}%-22s${NC} │ ${GREEN}%-15s${NC} │ ${GREEN}%-15s${NC} │ ${GREEN}%-12s${NC} ${CYAN}║${NC}\n" \
            "Fence (FENCE=on)" "${FENCE_THROUGHPUT:-N/A}" "${FENCE_GOODPUT:-N/A}" "${FENCE_LATENCY:-N/A}"
    else
        printf "${CYAN}║${NC} ${RED}%-22s${NC} │ ${RED}%-15s${NC} │ ${RED}%-15s${NC} │ ${RED}%-12s${NC} ${CYAN}║${NC}\n" \
            "Fence (FENCE=on)" "FAILED" "-" "-"
    fi
fi

echo -e "${CYAN}╚══════════════════════════════════════════════════════════════════════════╝${NC}"

# Show speedup if both benchmarks ran successfully
if [ "$RUN_LINEAIRDB" = true ] && [ "$RUN_FENCE" = true ] && [ "$LINEAIRDB_SUCCESS" = true ] && [ "$FENCE_SUCCESS" = true ]; then
    if [ "$LINEAIRDB_THROUGHPUT" != "N/A" ] && [ "$FENCE_THROUGHPUT" != "N/A" ]; then
        SPEEDUP=$(python3 -c "print(f'{float(\"$LINEAIRDB_THROUGHPUT\") / float(\"$FENCE_THROUGHPUT\"):.1f}')" 2>/dev/null || echo "N/A")
        echo ""
        echo -e "${YELLOW}  ⚡ LineairDB (FENCE=off) is ${SPEEDUP}x faster than Fence (FENCE=on)${NC}"
    fi
fi

echo ""

if [ "$REPLICATION_OK" = true ]; then
    print_success "Data replication: VERIFIED"
else
    print_error "Data replication: ISSUES DETECTED"
fi

echo ""
echo "Result files:"
if [ "$RUN_LINEAIRDB" = true ]; then
    echo "  LineairDB: $ROOT_DIR/bench/results/lineairdb/gr_test/"
fi
if [ "$RUN_FENCE" = true ]; then
    echo "  Fence:     $ROOT_DIR/bench/results/fence/gr_test/"
fi

echo ""

# Determine overall success
ALL_PASSED=true
if [ "$RUN_LINEAIRDB" = true ] && [ "$LINEAIRDB_SUCCESS" != true ]; then
    ALL_PASSED=false
fi
if [ "$RUN_FENCE" = true ] && [ "$FENCE_SUCCESS" != true ]; then
    ALL_PASSED=false
fi
if [ "$REPLICATION_OK" != true ]; then
    ALL_PASSED=false
fi

if [ "$ALL_PASSED" = true ]; then
    echo -e "${GREEN}╔════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${GREEN}║              ALL TESTS PASSED SUCCESSFULLY!                ║${NC}"
    echo -e "${GREEN}╚════════════════════════════════════════════════════════════╝${NC}"
    exit 0
else
    echo -e "${RED}╔════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${RED}║              SOME TESTS FAILED - CHECK ABOVE               ║${NC}"
    echo -e "${RED}╚════════════════════════════════════════════════════════════╝${NC}"
    exit 1
fi
