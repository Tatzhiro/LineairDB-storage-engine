#!/bin/bash
# Initialize MySQL Cluster for LineairDB Replication
# This script sets up the cluster bridge environment

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPL_DIR="$(dirname "$SCRIPT_DIR")"
ROOT_DIR="$(dirname "$REPL_DIR")"
CLUSTER_DIR="$ROOT_DIR/third_party/mysql-cluster"

# Default values
NUM_SECONDARIES=${1:-2}
MYSQL_ROOT_PASSWORD=${MYSQL_ROOT_PASSWORD:-"kamo"}

echo "=========================================="
echo "LineairDB Cluster Initialization"
echo "=========================================="
echo ""
echo "Configuration:"
echo "  Root directory: $ROOT_DIR"
echo "  Cluster module: $CLUSTER_DIR"
echo "  Secondary nodes: $NUM_SECONDARIES"
echo ""

# Check if mysql-cluster submodule exists
if [ ! -d "$CLUSTER_DIR" ]; then
    echo "Error: mysql-cluster submodule not found at $CLUSTER_DIR"
    echo "Please run: git submodule update --init third_party/mysql-cluster"
    exit 1
fi

# Check if bridge module exists
if [ ! -f "$CLUSTER_DIR/bridge/__init__.py" ]; then
    echo "Error: bridge module not found in mysql-cluster"
    echo "Please update the submodule to the latest version"
    exit 1
fi

# Install Python dependencies
echo "Step 1: Installing Python dependencies..."
if [ -f "$CLUSTER_DIR/requirements.txt" ]; then
    python3 -m pip install -r "$CLUSTER_DIR/requirements.txt" --quiet
    echo "  ✓ Dependencies installed"
else
    echo "  ⚠ requirements.txt not found, skipping"
fi

# Create config directory
echo ""
echo "Step 2: Creating configuration directory..."
mkdir -p "$REPL_DIR/config"
echo "  ✓ Created $REPL_DIR/config"

# Initialize the cluster using Python bridge
echo ""
echo "Step 3: Initializing cluster configuration..."
cd "$CLUSTER_DIR"
python3 -c "
import sys
from pathlib import Path
sys.path.insert(0, '.')
from bridge.cluster import create_cluster

cluster = create_cluster(
    num_secondaries=$NUM_SECONDARIES,
)
# Save config to the repl config directory
config_path = Path('$REPL_DIR/config/cluster_config.json')
cluster.save(config_path)
print('  ✓ Cluster configuration created')
print(f'  Config saved to: {config_path}')
print(f'  Primary: {cluster.config.primary.host}:{cluster.config.primary.port}')
for i, sec in enumerate(cluster.config.secondaries, 1):
    print(f'  Secondary {i}: {sec.host}:{sec.port} ({sec.node_type.value})')
"

echo ""
echo "=========================================="
echo "Initialization Complete!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "  1. Start the cluster:     ./scripts/start_cluster.sh"
echo "  2. Check status:          ./scripts/status.sh"
echo "  3. Install LineairDB:     ./scripts/install_plugin.sh"
echo ""

