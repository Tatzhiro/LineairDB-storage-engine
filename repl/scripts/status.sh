#!/bin/bash
# Check the status of LineairDB MySQL Cluster
#
# Shows:
# - Node status (primary + secondaries)
# - Group Replication status
# - LineairDB plugin status

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPL_DIR="$(dirname "$SCRIPT_DIR")"
ROOT_DIR="$(dirname "$REPL_DIR")"

# Configuration
MYSQL_USER="root"
MYSQL_PASSWORD="kamo"
PRIMARY_HOST="127.0.0.1"
PRIMARY_PORT="3306"

echo "=========================================="
echo "LineairDB Cluster Status"
echo "=========================================="
echo ""

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

# ===========================================
# Node Status
# ===========================================
echo "=== Node Status ==="
echo ""

# Check primary (local MySQL)
echo "Primary Node (local):"
echo "  Host: $PRIMARY_HOST:$PRIMARY_PORT"
if systemctl is-active --quiet mysql 2>/dev/null; then
    echo "  Running: ✓"
    if mysql -h $PRIMARY_HOST -P $PRIMARY_PORT -u$MYSQL_USER -p$MYSQL_PASSWORD -e "SELECT 1" &>/dev/null; then
        echo "  Reachable: ✓"
        
        # Check LineairDB
        lineairdb_status=$(mysql -h $PRIMARY_HOST -P $PRIMARY_PORT -u$MYSQL_USER -p$MYSQL_PASSWORD -N -e \
            "SELECT PLUGIN_STATUS FROM INFORMATION_SCHEMA.PLUGINS WHERE PLUGIN_NAME = 'LINEAIRDB';" 2>/dev/null || echo "NOT_INSTALLED")
        if [ "$lineairdb_status" = "ACTIVE" ]; then
            echo "  LineairDB: ✓ ACTIVE"
        else
            echo "  LineairDB: ✗ $lineairdb_status"
        fi
    else
        echo "  Reachable: ✗"
    fi
else
    echo "  Running: ✗"
fi

echo ""
echo "Docker Secondary Nodes ($NUM_SECONDARIES):"

# Check each secondary container
for i in $(seq 1 $NUM_SECONDARIES); do
    container="mysql-secondary-$i"
    port=$((33061 + i))
    echo "  $container (port $port):"
    
    if sudo docker ps --format '{{.Names}}' 2>/dev/null | grep -q "^${container}$"; then
        echo "    Running: ✓"
        
        # Check health status
        health=$(sudo docker inspect --format='{{.State.Health.Status}}' "$container" 2>/dev/null || echo "unknown")
        echo "    Health: $health"
        
        # Check if reachable
        if mysql -h $PRIMARY_HOST -P $port -u$MYSQL_USER -p$MYSQL_PASSWORD -e "SELECT 1" &>/dev/null; then
            echo "    Reachable: ✓"
            
            # Check LineairDB
            lineairdb_status=$(mysql -h $PRIMARY_HOST -P $port -u$MYSQL_USER -p$MYSQL_PASSWORD -N -e \
                "SELECT PLUGIN_STATUS FROM INFORMATION_SCHEMA.PLUGINS WHERE PLUGIN_NAME = 'LINEAIRDB';" 2>/dev/null || echo "NOT_INSTALLED")
            if [ "$lineairdb_status" = "ACTIVE" ]; then
                echo "    LineairDB: ✓ ACTIVE"
            else
                echo "    LineairDB: ✗ $lineairdb_status"
            fi
        else
            echo "    Reachable: ✗"
        fi
    else
        echo "    Running: ✗"
    fi
done

# ===========================================
# Group Replication Status
# ===========================================
echo ""
echo "=== Group Replication Status ==="
echo ""

# Check if GR cluster exists
if mysqlsh --uri $MYSQL_USER:$MYSQL_PASSWORD@$PRIMARY_HOST:$PRIMARY_PORT --js -e "dba.getCluster();" &>/dev/null; then
    mysqlsh --uri $MYSQL_USER:$MYSQL_PASSWORD@$PRIMARY_HOST:$PRIMARY_PORT --js -e "
    var cluster = dba.getCluster();
    var status = cluster.status();
    print('Cluster: ' + status.clusterName);
    print('Status: ' + status.defaultReplicaSet.status);
    print('Primary: ' + status.defaultReplicaSet.primary);
    print('');
    print('Topology:');
    for (var member in status.defaultReplicaSet.topology) {
        var m = status.defaultReplicaSet.topology[member];
        print('  ' + member + ': ' + m.memberRole + ' (' + m.mode + ') - ' + m.status);
    }
    " 2>/dev/null | grep -v WARNING
else
    echo "  InnoDB Cluster not configured or not reachable"
fi

echo ""
