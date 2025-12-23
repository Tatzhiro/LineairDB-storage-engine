#!/bin/bash
# Start the MySQL InnoDB Cluster for LineairDB Replication
#
# This script:
# 1. Starts primary node (local MySQL)
# 2. Starts secondary nodes (Docker containers)
# 3. Installs LineairDB plugin on ALL nodes
# 4. Sets up InnoDB Cluster with Group Replication
#
# Order matters! Plugin must be installed before cluster setup.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPL_DIR="$(dirname "$SCRIPT_DIR")"
ROOT_DIR="$(dirname "$REPL_DIR")"
CLUSTER_DIR="$ROOT_DIR/third_party/mysql-cluster"

# Configuration
MYSQL_USER="root"
MYSQL_PASSWORD="kamo"
CLUSTER_NAME="lineairdb_cluster"
PRIMARY_HOST="127.0.0.1"
PRIMARY_PORT="3306"

# Get primary's actual IP (for Docker containers to reach it)
PRIMARY_IP=$(hostname -I | awk '{print $1}')
PRIMARY_HOSTNAME=$(hostname)

# Options
SETUP_REPLICATION="true"
INSTALL_PLUGIN="true"

while [[ $# -gt 0 ]]; do
    case $1 in
        --no-replication)
            SETUP_REPLICATION="false"
            shift
            ;;
        --no-plugin)
            INSTALL_PLUGIN="false"
            shift
            ;;
        -h|--help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --no-replication    Start without setting up InnoDB Cluster"
            echo "  --no-plugin         Don't install LineairDB plugin"
            echo "  -h, --help          Show this help message"
            exit 0
            ;;
        *)
            shift
            ;;
    esac
done

echo "=========================================="
echo "Starting LineairDB InnoDB Cluster"
echo "=========================================="
echo ""
echo "Primary: $PRIMARY_HOSTNAME ($PRIMARY_IP:$PRIMARY_PORT)"
echo "Setup Replication: $SETUP_REPLICATION"
echo "Install Plugin: $INSTALL_PLUGIN"
echo ""

# Check configuration exists
if [ ! -f "$REPL_DIR/config/cluster_config.json" ]; then
    echo "Error: Cluster configuration not found"
    echo "Please run: ./scripts/init_cluster.sh"
    exit 1
fi

# Get number of secondaries from config
NUM_SECONDARIES=$(python3 -c "
import json
with open('$REPL_DIR/config/cluster_config.json') as f:
    config = json.load(f)
print(len(config.get('secondaries', [])))
")

echo "Secondary nodes: $NUM_SECONDARIES"
echo ""

# ===========================================
# Step 1: Check/Build Docker Image
# ===========================================
echo "Step 1: Checking Docker image..."
if ! sudo docker images | grep -q "mysql-lineairdb-ubuntu"; then
    echo "  Docker image not found. Building it first..."
    sudo "$CLUSTER_DIR/docker/build-image.sh"
else
    echo "  ✓ mysql-lineairdb-ubuntu image found"
fi

# ===========================================
# Step 2: Configure Primary MySQL
# ===========================================
echo ""
echo "Step 2: Configuring primary MySQL..."

# Check if MySQL is running
if ! systemctl is-active --quiet mysql; then
    echo "  Starting MySQL..."
    sudo systemctl start mysql
    sleep 3
fi

# Ensure read_only mode is disabled
mysql -h $PRIMARY_HOST -P $PRIMARY_PORT -u$MYSQL_USER -p$MYSQL_PASSWORD -e "SET GLOBAL super_read_only = OFF; SET GLOBAL read_only = OFF;" 2>/dev/null || true

# Ensure bind-address is 0.0.0.0
BIND_ADDR=$(mysql -h $PRIMARY_HOST -P $PRIMARY_PORT -u$MYSQL_USER -p$MYSQL_PASSWORD -N -e "SELECT @@bind_address;" 2>/dev/null)
if [ "$BIND_ADDR" = "127.0.0.1" ]; then
    echo "  Updating bind-address to 0.0.0.0..."
    sudo sed -i 's/bind-address.*=.*127.0.0.1/bind-address = 0.0.0.0/' /etc/mysql/mysql.conf.d/mysqld.cnf
    sudo sed -i 's/mysqlx-bind-address.*=.*127.0.0.1/mysqlx-bind-address = 0.0.0.0/' /etc/mysql/mysql.conf.d/mysqld.cnf
    sudo systemctl restart mysql
    sleep 3
fi

# Create root@'%' user if not exists
mysql -h $PRIMARY_HOST -P $PRIMARY_PORT -u$MYSQL_USER -p$MYSQL_PASSWORD -e "
CREATE USER IF NOT EXISTS 'root'@'%' IDENTIFIED BY '$MYSQL_PASSWORD';
GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION;
FLUSH PRIVILEGES;
" 2>/dev/null
echo "  ✓ Primary MySQL configured"

# ===========================================
# Step 3: Start Secondary Nodes (Docker)
# ===========================================
echo ""
echo "Step 3: Starting secondary nodes..."

cd "$CLUSTER_DIR"

# Generate docker-compose file
python3 -c "
import sys
sys.path.insert(0, '.')
from bridge.cluster import ClusterBridge

bridge = ClusterBridge.load('$REPL_DIR/config/cluster_config.json')
bridge.secondary_manager.generate_docker_compose()
"

# Start with docker-compose
COMPOSE_FILE="$CLUSTER_DIR/docker-compose-secondaries.yml"
if [ -f "$COMPOSE_FILE" ]; then
    sudo docker-compose -f "$COMPOSE_FILE" up -d
    
    # Wait for containers to be healthy
    echo "  Waiting for containers to be healthy..."
    for i in $(seq 1 $NUM_SECONDARIES); do
        container="mysql-secondary-$i"
        if sudo docker ps -a --format '{{.Names}}' | grep -q "^${container}$"; then
            echo -n "    $container: "
            for j in {1..60}; do
                status=$(sudo docker inspect --format='{{.State.Health.Status}}' "$container" 2>/dev/null || echo "starting")
                if [ "$status" = "healthy" ]; then
                    echo "✓ healthy"
                    break
                fi
                sleep 2
                echo -n "."
            done
            if [ "$status" != "healthy" ]; then
                echo " (current: $status)"
            fi
        fi
    done
fi

# ===========================================
# Step 4: Setup Network (Host Resolution)
# ===========================================
echo ""
echo "Step 4: Setting up network resolution..."

# Remove old entries
sudo sed -i '/mysql-secondary/d' /etc/hosts
sudo sed -i '/# MySQL Cluster/d' /etc/hosts

# Add container hostnames to primary's /etc/hosts
echo "# MySQL Cluster Secondaries" | sudo tee -a /etc/hosts > /dev/null
for i in $(seq 1 $NUM_SECONDARIES); do
    container="mysql-secondary-$i"
    container_ip=$(sudo docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$container")
    echo "$container_ip $container" | sudo tee -a /etc/hosts > /dev/null
    echo "  Added: $container_ip $container"
done

# Add primary hostname to container /etc/hosts
for i in $(seq 1 $NUM_SECONDARIES); do
    container="mysql-secondary-$i"
    sudo docker exec "$container" bash -c "grep -q '$PRIMARY_HOSTNAME' /etc/hosts || echo '$PRIMARY_IP $PRIMARY_HOSTNAME' >> /etc/hosts" 2>/dev/null
done
echo "  ✓ Network resolution configured"

# ===========================================
# Step 5: Create root@'%' on Secondaries
# ===========================================
echo ""
echo "Step 5: Configuring secondary MySQL users..."
for i in $(seq 1 $NUM_SECONDARIES); do
    port=$((33061 + i))
    container="mysql-secondary-$i"
    
    # Wait for container to be reachable
    echo -n "  Waiting for $container (port $port)..."
    for j in {1..30}; do
        if mysql -h $PRIMARY_HOST -P $port -u$MYSQL_USER -p$MYSQL_PASSWORD -e "SELECT 1" &>/dev/null; then
            echo " ready"
            break
        fi
        sleep 1
        echo -n "."
    done
    
    # Configure user (ignore errors if container not reachable)
    if mysql -h $PRIMARY_HOST -P $port -u$MYSQL_USER -p$MYSQL_PASSWORD -e "
    CREATE USER IF NOT EXISTS 'root'@'%' IDENTIFIED BY '$MYSQL_PASSWORD';
    GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION;
    FLUSH PRIVILEGES;
    " 2>/dev/null; then
        echo "  ✓ $container configured"
    else
        echo "  ⚠ $container not reachable, skipping"
    fi
done

# ===========================================
# Step 6: Install LineairDB Plugin (ALL NODES)
# ===========================================
if [ "$INSTALL_PLUGIN" = "true" ]; then
    echo ""
    echo "Step 6: Installing LineairDB plugin on all nodes..."
    
    # Find plugin
    PLUGIN_PATH=""
    if [ -f "$ROOT_DIR/release/library_output_directory/plugin/ha_lineairdb_storage_engine.so" ]; then
        PLUGIN_PATH="$ROOT_DIR/release/library_output_directory/plugin/ha_lineairdb_storage_engine.so"
    elif [ -f "$ROOT_DIR/build/library_output_directory/plugin/ha_lineairdb_storage_engine.so" ]; then
        PLUGIN_PATH="$ROOT_DIR/build/library_output_directory/plugin/ha_lineairdb_storage_engine.so"
    fi
    
    if [ -z "$PLUGIN_PATH" ]; then
        echo "  Warning: LineairDB plugin not found, skipping installation"
        INSTALL_PLUGIN="false"
    else
        echo "  Plugin: $PLUGIN_PATH"
        
        # Install on primary - ALWAYS copy new binary (may have different FENCE setting)
        echo "  Installing on primary..."
        
        # Always copy the new plugin binary (it may have different FENCE compile-time setting)
        sudo cp "$PLUGIN_PATH" /usr/lib/mysql/plugin/ha_lineairdb_storage_engine.so
        sudo chmod 644 /usr/lib/mysql/plugin/ha_lineairdb_storage_engine.so
        
        # Check if plugin needs to be uninstalled and reinstalled
        primary_status=$(mysql -h $PRIMARY_HOST -P $PRIMARY_PORT -u$MYSQL_USER -p$MYSQL_PASSWORD -N -e "SELECT PLUGIN_STATUS FROM INFORMATION_SCHEMA.PLUGINS WHERE PLUGIN_NAME = 'LINEAIRDB';" 2>/dev/null || echo "")
        if [ "$primary_status" = "ACTIVE" ]; then
            # Uninstall first to force MySQL to reload the new binary
            echo "    Uninstalling existing plugin..."
            mysql -h $PRIMARY_HOST -P $PRIMARY_PORT -u$MYSQL_USER -p$MYSQL_PASSWORD -e "UNINSTALL PLUGIN lineairdb;" 2>/dev/null || true
            sleep 1
        fi
        
        # Install the plugin (loads the new binary)
        echo "    Installing plugin..."
        mysql -h $PRIMARY_HOST -P $PRIMARY_PORT -u$MYSQL_USER -p$MYSQL_PASSWORD -e "INSTALL PLUGIN lineairdb SONAME 'ha_lineairdb_storage_engine.so';" 2>/dev/null || true
        
        # Verify installation
        primary_status=$(mysql -h $PRIMARY_HOST -P $PRIMARY_PORT -u$MYSQL_USER -p$MYSQL_PASSWORD -N -e "SELECT PLUGIN_STATUS FROM INFORMATION_SCHEMA.PLUGINS WHERE PLUGIN_NAME = 'LINEAIRDB';" 2>/dev/null || echo "")
        if [ "$primary_status" = "ACTIVE" ]; then
            echo "    ✓ Primary: LineairDB installed and ACTIVE"
        else
            echo "    ⚠ Primary: Plugin install attempted (status: $primary_status)"
        fi
        
        # Install on secondaries - ALWAYS copy new binary
        for i in $(seq 1 $NUM_SECONDARIES); do
            container="mysql-secondary-$i"
            port=$((33061 + i))
            
            # Always copy the new plugin binary
            sudo docker cp "$PLUGIN_PATH" "$container:/usr/lib64/mysql/plugin/ha_lineairdb_storage_engine.so" 2>/dev/null || true
            
            # Check if reachable
            if mysql -h $PRIMARY_HOST -P $port -u$MYSQL_USER -p$MYSQL_PASSWORD -e "SELECT 1" &>/dev/null; then
                # Uninstall first to force reload
                sec_status=$(mysql -h $PRIMARY_HOST -P $port -u$MYSQL_USER -p$MYSQL_PASSWORD -N -e "SELECT PLUGIN_STATUS FROM INFORMATION_SCHEMA.PLUGINS WHERE PLUGIN_NAME = 'LINEAIRDB';" 2>/dev/null || echo "")
                if [ "$sec_status" = "ACTIVE" ]; then
                    mysql -h $PRIMARY_HOST -P $port -u$MYSQL_USER -p$MYSQL_PASSWORD -e "UNINSTALL PLUGIN lineairdb;" 2>/dev/null || true
                    sleep 1
                fi
                mysql -h $PRIMARY_HOST -P $port -u$MYSQL_USER -p$MYSQL_PASSWORD -e "INSTALL PLUGIN lineairdb SONAME 'ha_lineairdb_storage_engine.so';" 2>/dev/null || true
                echo "    ✓ $container: LineairDB installed"
            else
                echo "    ⚠ $container: Not reachable, skipping plugin install"
            fi
        done
    fi
fi

# ===========================================
# Step 7: Setup InnoDB Cluster
# ===========================================
if [ "$SETUP_REPLICATION" = "true" ]; then
    echo ""
    echo "Step 7: Setting up InnoDB Cluster..."
    
    # Check for non-InnoDB tables and warn
    non_innodb=$(mysql -h $PRIMARY_HOST -P $PRIMARY_PORT -u$MYSQL_USER -p$MYSQL_PASSWORD -N -e "
    SELECT COUNT(*) FROM information_schema.TABLES 
    WHERE ENGINE NOT IN ('InnoDB', 'PERFORMANCE_SCHEMA', 'MEMORY', 'CSV', 'FEDERATED', 'ARCHIVE', 'BLACKHOLE', 'MRG_MYISAM', 'MyISAM')
    AND TABLE_SCHEMA NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys');
    " 2>/dev/null)
    
    if [ "$non_innodb" -gt 0 ]; then
        echo "  Warning: Found $non_innodb non-InnoDB tables (LineairDB tables)"
        echo "  These tables won't participate in Group Replication"
    fi
    
    # Configure primary for GR
    echo "  Configuring primary for Group Replication..."
    mysqlsh --uri $MYSQL_USER:$MYSQL_PASSWORD@$PRIMARY_HOST:$PRIMARY_PORT --js -e "
    shell.options.useWizards = false;
    try {
        dba.configureInstance('$MYSQL_USER:$MYSQL_PASSWORD@$PRIMARY_HOST:$PRIMARY_PORT', {
            clusterAdmin: 'root@%',
            restart: true
        });
    } catch(e) {
        if (!e.message.includes('already prepared') && !e.message.includes('already configured')) {
            throw e;
        }
    }
    " 2>&1 | grep -v WARNING || true
    
    sleep 3
    
    # Configure secondaries for GR
    echo "  Configuring secondaries for Group Replication..."
    for i in $(seq 1 $NUM_SECONDARIES); do
        port=$((33061 + i))
        mysqlsh --uri $MYSQL_USER:$MYSQL_PASSWORD@$PRIMARY_HOST:$port --js -e "
        shell.options.useWizards = false;
        try {
            dba.configureInstance('$MYSQL_USER:$MYSQL_PASSWORD@$PRIMARY_HOST:$port', {
                clusterAdmin: 'root@%'
            });
        } catch(e) {
            if (!e.message.includes('already prepared') && !e.message.includes('already configured')) {
                print('Warning: ' + e.message);
            }
        }
        " 2>&1 | grep -v WARNING || true
        echo "    ✓ mysql-secondary-$i configured"
    done
    
    # Clean up LineairDB persistent data (blocks cluster creation)
    echo "  Cleaning up LineairDB persistent data..."
    sudo rm -rf /var/lib/mysql/lineairdb_logs 2>/dev/null || true
    
    # Drop any leftover benchmark databases (LineairDB tables block cluster creation)
    echo "  Cleaning up benchmark databases..."
    BENCH_DBS=$(mysql -h $PRIMARY_HOST -P $PRIMARY_PORT -u$MYSQL_USER -p$MYSQL_PASSWORD -N -e "SHOW DATABASES LIKE 'benchbase%';" 2>/dev/null || true)
    for db in $BENCH_DBS; do
        TABLES=$(mysql -h $PRIMARY_HOST -P $PRIMARY_PORT -u$MYSQL_USER -p$MYSQL_PASSWORD -N -e "SELECT table_name FROM information_schema.tables WHERE table_schema='$db';" 2>/dev/null || true)
        for table in $TABLES; do
            mysql -h $PRIMARY_HOST -P $PRIMARY_PORT -u$MYSQL_USER -p$MYSQL_PASSWORD -e "DROP TABLE IF EXISTS \`$db\`.\`$table\`;" 2>/dev/null || true
        done
        mysql -h $PRIMARY_HOST -P $PRIMARY_PORT -u$MYSQL_USER -p$MYSQL_PASSWORD -e "DROP DATABASE IF EXISTS \`$db\`;" 2>/dev/null || true
    done
    
    # Create or get cluster
    echo "  Creating InnoDB Cluster..."
    mysqlsh --uri $MYSQL_USER:$MYSQL_PASSWORD@$PRIMARY_HOST:$PRIMARY_PORT --js -e "
    shell.options.useWizards = false;
    var cluster;
    try {
        cluster = dba.getCluster('$CLUSTER_NAME');
        print('Cluster already exists: ' + cluster.getName());
    } catch(e) {
        if (e.message.includes('standalone') || e.message.includes('GR is not active') || e.message.includes('51314')) {
            // Metadata exists but GR not active - try reboot or drop metadata
            print('GR not active, trying to reboot cluster...');
            try {
                cluster = dba.rebootClusterFromCompleteOutage('$CLUSTER_NAME', {force: true});
                print('Cluster rebooted: ' + cluster.getName());
            } catch(e2) {
                print('Reboot failed, dropping metadata and creating new cluster...');
                try {
                    dba.dropMetadataSchema({force: true});
                } catch(e3) {}
                cluster = dba.createCluster('$CLUSTER_NAME', {
                    communicationStack: 'XCOM',
                    gtidSetIsComplete: true
                });
                print('Cluster created: ' + cluster.getName());
            }
        } else if (e.message.includes('not found') || e.message.includes('does not exist')) {
            print('Creating new cluster...');
            cluster = dba.createCluster('$CLUSTER_NAME', {
                communicationStack: 'XCOM',
                gtidSetIsComplete: true
            });
            print('Cluster created: ' + cluster.getName());
        } else {
            throw e;
        }
    }
    " 2>&1 | grep -v WARNING
    
    # Add secondaries to cluster
    echo "  Adding secondary nodes to cluster..."
    for i in $(seq 1 $NUM_SECONDARIES); do
        container="mysql-secondary-$i"
        
        echo -n "    Adding $container... "
        mysqlsh --uri $MYSQL_USER:$MYSQL_PASSWORD@$PRIMARY_HOST:$PRIMARY_PORT --js -e "
        shell.options.useWizards = false;
        var cluster = dba.getCluster('$CLUSTER_NAME');
        try {
            cluster.addInstance('$MYSQL_USER:$MYSQL_PASSWORD@$container:3306', {
                recoveryMethod: 'clone'
            });
            print('Added successfully');
        } catch(e) {
            if (e.message.includes('already a member') || e.message.includes('already in the cluster')) {
                print('Already in cluster');
            } else {
                print('Error: ' + e.message);
            }
        }
        " 2>&1 | grep -v WARNING | tail -1
    done
    
    echo "  ✓ InnoDB Cluster setup complete"
fi

# ===========================================
# Final Status
# ===========================================
echo ""
echo "=========================================="
echo "Cluster Started!"
echo "=========================================="
echo ""
echo "Primary (local):      $PRIMARY_HOST:$PRIMARY_PORT"
for i in $(seq 1 $NUM_SECONDARIES); do
    port=$((33061 + i))
    echo "Secondary $i (Docker): $PRIMARY_HOST:$port"
done
echo ""

if [ "$SETUP_REPLICATION" = "true" ]; then
    echo "InnoDB Cluster Status:"
    mysqlsh --uri $MYSQL_USER:$MYSQL_PASSWORD@$PRIMARY_HOST:$PRIMARY_PORT --js -e "
    var cluster = dba.getCluster();
    var status = cluster.status();
    print('  Cluster: ' + status.clusterName);
    print('  Status: ' + status.defaultReplicaSet.status);
    for (var member in status.defaultReplicaSet.topology) {
        var m = status.defaultReplicaSet.topology[member];
        print('    ' + member + ': ' + m.memberRole + ' (' + m.mode + ')');
    }
    " 2>&1 | grep -v WARNING
    echo ""
fi

echo "Commands:"
echo "  Check cluster: mysqlsh --uri root:kamo@127.0.0.1:3306 -e \"dba.getCluster().status()\""
echo "  Check status:  ./scripts/status.sh"
echo ""
