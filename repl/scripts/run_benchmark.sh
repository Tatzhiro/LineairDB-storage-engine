#!/bin/bash
# Run benchmark with LineairDB on Group Replication cluster
# 
# This script runs YCSB/TPC-C benchmarks with LineairDB storage engine
# Supports both "lineairdb" (FENCE=false) and "fence" (FENCE=true) modes
#
# Usage:
#   ./run_benchmark.sh <engine> <benchmark_type> [options]
#
# Arguments:
#   engine:         lineairdb (FENCE=off, fast) or fence (FENCE=on, safe)
#   benchmark_type: ycsb or tpcc
#
# Options:
#   --terminals N   Number of concurrent terminals (default: 4)
#   --time N        Benchmark duration in seconds (default: 30)
#   --debug         Use debug build instead of release
#   --no-rebuild    Don't rebuild plugin (assume it's already installed with correct FENCE)
#
# Examples:
#   ./run_benchmark.sh lineairdb ycsb              # Fast LineairDB YCSB
#   ./run_benchmark.sh fence ycsb                  # Safe LineairDB YCSB
#   ./run_benchmark.sh lineairdb ycsb --terminals 8 --time 60
#   ./run_benchmark.sh lineairdb ycsb --no-rebuild # Skip plugin rebuild

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPL_DIR="$(dirname "$SCRIPT_DIR")"
ROOT_DIR="$(dirname "$REPL_DIR")"
BENCHBASE_DIR="$ROOT_DIR/third_party/benchbase"

# Configuration
MYSQL_USER="root"
MYSQL_PASSWORD="kamo"
MYSQL_HOST="127.0.0.1"
MYSQL_PORT="3306"

# Parse arguments
ENGINE=${1:-lineairdb}
BENCHMARK_TYPE=${2:-ycsb}
shift 2 2>/dev/null || true

# Defaults
TERMINALS=4
DURATION=30
BUILD_TYPE="release"
NO_REBUILD=false

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
        --debug)
            BUILD_TYPE="debug"
            shift
            ;;
        --no-rebuild)
            NO_REBUILD=true
            shift
            ;;
        *)
            shift
            ;;
    esac
done

# Validate engine
if [ "$ENGINE" != "lineairdb" ] && [ "$ENGINE" != "fence" ]; then
    echo "Error: Invalid engine '$ENGINE'"
    echo "Valid options: lineairdb (FENCE=off) or fence (FENCE=on)"
    exit 1
fi

# Determine FENCE setting based on engine
if [ "$ENGINE" = "fence" ]; then
    FENCE_VALUE="true"
    FENCE_DESC="FENCE=ON (synchronous)"
else
    FENCE_VALUE="false"
    FENCE_DESC="FENCE=OFF (async)"
fi

# Use unique database name to avoid LineairDB "table already exists" issue
TIMESTAMP=$(date +%s)
DB_NAME="bench_${ENGINE}_${TIMESTAMP}"

echo "=========================================="
echo "LineairDB GR Cluster Benchmark"
echo "=========================================="
echo ""
echo "Engine:     $ENGINE ($FENCE_DESC)"
echo "Benchmark:  $BENCHMARK_TYPE"
echo "Build:      $BUILD_TYPE"
echo "Terminals:  $TERMINALS"
echo "Duration:   ${DURATION}s"
echo "Database:   $DB_NAME"
echo ""

# Step 1: Check MySQL is running
echo "Step 1: Checking MySQL status..."
if ! mysql -h $MYSQL_HOST -P $MYSQL_PORT -u$MYSQL_USER -p$MYSQL_PASSWORD -e "SELECT 1;" &>/dev/null; then
    echo "Error: MySQL not available at $MYSQL_HOST:$MYSQL_PORT"
    echo "Please start MySQL first"
    exit 1
fi
echo "  ✓ MySQL is running"

# Check if GR cluster is available (optional - LineairDB doesn't use GR)
GR_AVAILABLE=false
if mysqlsh --uri $MYSQL_USER:$MYSQL_PASSWORD@$MYSQL_HOST:$MYSQL_PORT --js -e "dba.getCluster();" &>/dev/null; then
    GR_AVAILABLE=true
    echo "  ✓ GR Cluster is available"
else
    echo "  ⚠ GR Cluster not configured (LineairDB doesn't require it)"
fi

# Step 2: Install plugin with correct FENCE setting (unless --no-rebuild)
echo ""
if [ "$NO_REBUILD" = true ]; then
    echo "Step 2: Skipping plugin rebuild (--no-rebuild flag)"
    echo "  Verifying existing plugin..."
else
    echo "Step 2: Installing LineairDB plugin with $FENCE_DESC..."
    
    if [ "$BUILD_TYPE" = "debug" ]; then
        "$SCRIPT_DIR/install_plugin.sh" --$ENGINE --debug
    else
        "$SCRIPT_DIR/install_plugin.sh" --$ENGINE --release
    fi
fi

# Verify plugin is active
LINEAIRDB_STATUS=$(mysql -h $MYSQL_HOST -P $MYSQL_PORT -u$MYSQL_USER -p$MYSQL_PASSWORD -N -e \
    "SELECT PLUGIN_STATUS FROM INFORMATION_SCHEMA.PLUGINS WHERE PLUGIN_NAME = 'LINEAIRDB';" 2>/dev/null)
if [ "$LINEAIRDB_STATUS" != "ACTIVE" ]; then
    echo "Error: LineairDB plugin not active"
    exit 1
fi
echo "  ✓ LineairDB plugin is active"

# Step 3: Set default storage engine
echo ""
echo "Step 3: Setting default storage engine to lineairdb..."
mysql -h $MYSQL_HOST -P $MYSQL_PORT -u$MYSQL_USER -p$MYSQL_PASSWORD -e "SET GLOBAL default_storage_engine = lineairdb;" 2>/dev/null

# Get number of secondaries from config
NUM_SECONDARIES=0
if [ -f "$REPL_DIR/config/cluster_config.json" ]; then
    NUM_SECONDARIES=$(python3 -c "
import json
with open('$REPL_DIR/config/cluster_config.json') as f:
    config = json.load(f)
print(len(config.get('secondaries', [])))
" 2>/dev/null || echo "0")
fi

# Set on secondaries too
for i in $(seq 1 $NUM_SECONDARIES); do
    port=$((33061 + i))
    mysql -h 127.0.0.1 -P $port -u$MYSQL_USER -p$MYSQL_PASSWORD -e "SET GLOBAL default_storage_engine = lineairdb;" 2>/dev/null || true
done
echo "  ✓ Default storage engine set to lineairdb on all nodes"

# Step 4: Find the write endpoint
echo ""
echo "Step 4: Finding write endpoint..."

WRITE_HOST="127.0.0.1"
WRITE_PORT="3306"

if [ "$GR_AVAILABLE" = true ]; then
    PRIMARY_INFO=$(mysqlsh --uri $MYSQL_USER:$MYSQL_PASSWORD@$MYSQL_HOST:$MYSQL_PORT --js -e "
var c = dba.getCluster();
var s = c.status();
print(s.defaultReplicaSet.primary);
" 2>/dev/null)

    echo "  GR Primary: $PRIMARY_INFO"

    # Parse the primary host and use it for writes
    PRIMARY_HOST_GR=$(echo "$PRIMARY_INFO" | cut -d':' -f1)

    # Determine which port to use for the primary
    if [[ "$PRIMARY_HOST_GR" == *"secondary"* ]]; then
        SEC_NUM=$(echo "$PRIMARY_HOST_GR" | grep -o '[0-9]*$')
        WRITE_PORT=$((33061 + SEC_NUM))
    fi
else
    echo "  Using local MySQL (no GR cluster)"
fi

echo "  Write endpoint: $WRITE_HOST:$WRITE_PORT"

# Step 5: Prepare benchmark database
echo ""
echo "Step 5: Preparing benchmark database..."

mysql -h $WRITE_HOST -P $WRITE_PORT -u$MYSQL_USER -p$MYSQL_PASSWORD -e "
DROP DATABASE IF EXISTS $DB_NAME;
CREATE DATABASE $DB_NAME;
" 2>/dev/null
echo "  ✓ Database '$DB_NAME' created"

# Step 6: Run benchmark
echo ""
echo "Step 6: Running $BENCHMARK_TYPE benchmark with engine=$ENGINE..."
echo "=========================================="
echo ""

cd "$BENCHBASE_DIR"

# Generate config with the correct primary endpoint
CONFIG_FILE="/tmp/${ENGINE}_${BENCHMARK_TYPE}_gr_test.xml"

cat > "$CONFIG_FILE" <<EOF
<?xml version="1.0"?>
<parameters>
    <!-- Connection details -->
    <type>MYSQL</type>
    <driver>com.mysql.cj.jdbc.Driver</driver>
    <url>jdbc:mysql://${WRITE_HOST}:${WRITE_PORT}/${DB_NAME}?rewriteBatchedStatements=true&amp;sslMode=DISABLED</url>
    <username>root</username>
    <password>kamo</password>
    <isolation>TRANSACTION_SERIALIZABLE</isolation>
    <batchsize>128</batchsize>
    <allowPublicKeyRetrieval>true</allowPublicKeyRetrieval>

    <!-- Scalefactor in YCSB is *1000 the number of rows in the USERTABLE-->
    <scalefactor>1</scalefactor>

    <!-- Workload -->
    <terminals>$TERMINALS</terminals>
    <works>
        <work>
            <time>$DURATION</time>
            <rate>unlimited</rate>
            <weights>50,0,0,50,0,0</weights>
        </work>
    </works>

    <!-- YCSB Procedures declaration -->
    <transactiontypes>
        <transactiontype>
            <name>ReadRecord</name>
        </transactiontype>
        <transactiontype>
            <name>InsertRecord</name>
        </transactiontype>
        <transactiontype>
            <name>ScanRecord</name>
        </transactiontype>
        <transactiontype>
            <name>UpdateRecord</name>
        </transactiontype>
        <transactiontype>
            <name>DeleteRecord</name>
        </transactiontype>
        <transactiontype>
            <name>ReadModifyWriteRecord</name>
        </transactiontype>
    </transactiontypes>
</parameters>
EOF

echo "Config: $CONFIG_FILE"
echo "Endpoint: $WRITE_HOST:$WRITE_PORT"
echo "Engine: $ENGINE (FENCE=$FENCE_VALUE)"
echo ""

# Check if JAR exists
JAR_FILE="benchbase-mysql/benchbase.jar"
if [ ! -f "$JAR_FILE" ]; then
    echo "Error: benchbase.jar not found at $JAR_FILE"
    echo "Please build benchbase first"
    exit 1
fi

# Run benchbase
java -jar "$JAR_FILE" -b $BENCHMARK_TYPE -c "$CONFIG_FILE" --create=true --load=true --execute=true

# Save results
RESULT_DIR="$ROOT_DIR/bench/results/${ENGINE}/gr_test"
mkdir -p "$RESULT_DIR"

# Copy all result files (CSV, JSON, etc.)
if ls results/*.csv 1>/dev/null 2>&1 || ls results/*.json 1>/dev/null 2>&1; then
    # Add timestamp to result files
    for f in results/*; do
        if [ -f "$f" ]; then
            base=$(basename "$f")
            ext="${base##*.}"
            name="${base%.*}"
            mv "$f" "$RESULT_DIR/${name}_${TIMESTAMP}.${ext}"
        fi
    done
    echo ""
    echo "Results saved to: $RESULT_DIR"
fi

echo ""
echo "=========================================="
echo "Benchmark Complete!"
echo "=========================================="
echo ""

# Verify table was created with LineairDB engine
echo "Verifying table engine..."
TABLE_ENGINE=$(mysql -h $WRITE_HOST -P $WRITE_PORT -u$MYSQL_USER -p$MYSQL_PASSWORD -N -e \
    "SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA='$DB_NAME' AND TABLE_NAME='usertable';" 2>/dev/null)
echo "  USERTABLE engine: $TABLE_ENGINE"

if [ "$TABLE_ENGINE" = "LINEAIRDB" ]; then
    echo ""
    echo "✓ SUCCESS: Benchmark completed with engine=$ENGINE (FENCE=$FENCE_VALUE)"
else
    echo ""
    echo "⚠ WARNING: Table was created with engine=$TABLE_ENGINE (expected LINEAIRDB)"
fi

echo ""
