#!/bin/bash
# Stop the MySQL Cluster for LineairDB Replication

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPL_DIR="$(dirname "$SCRIPT_DIR")"
ROOT_DIR="$(dirname "$REPL_DIR")"
CLUSTER_DIR="$ROOT_DIR/third_party/mysql-cluster"

echo "=========================================="
echo "Stopping LineairDB Cluster"
echo "=========================================="
echo ""

# Stop Docker containers using sudo
COMPOSE_FILE="$CLUSTER_DIR/docker-compose-secondaries.yml"
if [ -f "$COMPOSE_FILE" ]; then
    echo "Stopping Docker containers..."
    sudo docker-compose -f "$COMPOSE_FILE" down
else
    # Try to stop containers directly
    echo "Stopping containers directly..."
    sudo docker stop mysql-secondary-1 mysql-secondary-2 mysql-secondary-3 2>/dev/null || true
fi

echo ""
echo "=========================================="
echo "Cluster Stopped!"
echo "=========================================="
echo ""
echo "Note: Local MySQL (primary) is managed by systemctl and was not stopped."
echo "To stop primary: sudo systemctl stop mysql"
echo ""
