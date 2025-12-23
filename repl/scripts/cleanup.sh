#!/bin/bash
# Clean up LineairDB cluster resources

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPL_DIR="$(dirname "$SCRIPT_DIR")"
ROOT_DIR="$(dirname "$REPL_DIR")"
CLUSTER_DIR="$ROOT_DIR/third_party/mysql-cluster"

# Configuration
MYSQL_USER="root"
MYSQL_PASSWORD="kamo"
CLUSTER_NAME="lineairdb_cluster"

# Options
REMOVE_ALL=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --all|-a)
            REMOVE_ALL=true
            shift
            ;;
        -h|--help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --all, -a    Remove containers, volumes, data, and cluster"
            echo "  -h, --help   Show this help message"
            exit 0
            ;;
        *)
            shift
            ;;
    esac
done

echo "=========================================="
echo "LineairDB Cluster Cleanup"
echo "=========================================="
echo ""

# ===========================================
# Step 0: Drop benchmark databases (LineairDB tables block cluster ops)
# ===========================================
echo "Cleaning up benchmark databases..."
# Get list of ALL benchmark databases (benchbase_*, bench_*, gr_test)
BENCH_DBS=$(mysql -h 127.0.0.1 -P 3306 -u$MYSQL_USER -p$MYSQL_PASSWORD -N -e "
SELECT SCHEMA_NAME FROM information_schema.SCHEMATA 
WHERE SCHEMA_NAME LIKE 'benchbase%' 
   OR SCHEMA_NAME LIKE 'bench\\_%'
   OR SCHEMA_NAME = 'gr_test'
   OR SCHEMA_NAME = 'repl_test';" 2>/dev/null || true)

for db in $BENCH_DBS; do
    echo "  Dropping database: $db"
    # First drop tables (needed for LineairDB)
    TABLES=$(mysql -h 127.0.0.1 -P 3306 -u$MYSQL_USER -p$MYSQL_PASSWORD -N -e "SELECT table_name FROM information_schema.tables WHERE table_schema='$db';" 2>/dev/null || true)
    for table in $TABLES; do
        mysql -h 127.0.0.1 -P 3306 -u$MYSQL_USER -p$MYSQL_PASSWORD -e "DROP TABLE IF EXISTS \`$db\`.\`$table\`;" 2>/dev/null || true
    done
    mysql -h 127.0.0.1 -P 3306 -u$MYSQL_USER -p$MYSQL_PASSWORD -e "DROP DATABASE IF EXISTS \`$db\`;" 2>/dev/null || true
done

# ===========================================
# Step 1: Dissolve InnoDB Cluster (if exists)
# ===========================================
if [ "$REMOVE_ALL" = true ]; then
    echo "Dissolving InnoDB Cluster (if exists)..."
    mysqlsh --uri $MYSQL_USER:$MYSQL_PASSWORD@127.0.0.1:3306 --js -e "
    shell.options.useWizards = false;
    try {
        var cluster = dba.getCluster('$CLUSTER_NAME');
        cluster.dissolve({force: true});
        print('Cluster dissolved');
    } catch(e) {
        if (e.message.includes('standalone') || e.message.includes('GR is not active') || e.message.includes('51314')) {
            // GR not active but metadata exists - drop the metadata schema
            print('GR not active, dropping metadata schema...');
            try {
                dba.dropMetadataSchema({force: true});
                print('Metadata schema dropped');
            } catch(e2) {
                print('No metadata to drop: ' + e2.message.substring(0, 50));
            }
        } else {
            print('No cluster to dissolve: ' + e.message.substring(0, 50));
        }
    }
    " 2>&1 | grep -v WARNING || true
    
    # Reset read_only mode on primary after dissolving cluster
    echo "Resetting read_only mode on primary..."
    mysql -h 127.0.0.1 -P 3306 -u$MYSQL_USER -p$MYSQL_PASSWORD -e "SET GLOBAL super_read_only = 0; SET GLOBAL read_only = 0;" 2>/dev/null || true
fi

# ===========================================
# Step 2: Stop Docker Containers
# ===========================================
COMPOSE_FILE="$CLUSTER_DIR/docker-compose-secondaries.yml"
if [ -f "$COMPOSE_FILE" ]; then
    echo "Stopping and removing containers..."
    sudo docker-compose -f "$COMPOSE_FILE" down 2>/dev/null || true
fi

# Remove any remaining mysql-secondary containers
echo "Removing any remaining containers..."
for container in $(sudo docker ps -a --format '{{.Names}}' | grep "mysql-secondary" || true); do
    sudo docker rm -f "$container" 2>/dev/null || true
done

# Remove network
echo "Removing network..."
sudo docker network rm mysql-cluster-net 2>/dev/null || true
sudo docker network rm mysql-cluster_mysql-cluster-net 2>/dev/null || true

# ===========================================
# Step 3: Clean up /etc/hosts
# ===========================================
echo "Cleaning up /etc/hosts..."
sudo sed -i '/mysql-secondary/d' /etc/hosts
sudo sed -i '/# MySQL Cluster/d' /etc/hosts

# ===========================================
# Step 4: Remove volumes, config, data (if --all)
# ===========================================
if [ "$REMOVE_ALL" = true ]; then
    echo ""
    echo "Removing volumes..."
    for vol in $(sudo docker volume ls -q 2>/dev/null | grep -E "secondary|mysql-cluster" || true); do
        sudo docker volume rm "$vol" 2>/dev/null || true
    done
    
    echo "Removing configuration..."
    rm -rf "$REPL_DIR/config/"* 2>/dev/null || true
    
    echo "Removing data directories..."
    sudo rm -rf "$CLUSTER_DIR/data/" 2>/dev/null || true
    sudo rm -rf "$CLUSTER_DIR/logs/" 2>/dev/null || true
    
    # Clean up LineairDB internal logs (persists even after DROP DATABASE)
    echo "Removing LineairDB logs..."
    sudo rm -rf /tmp/lineairdb_logs 2>/dev/null || true
    sudo rm -rf /var/lib/mysql/lineairdb_logs 2>/dev/null || true
    
    # Restart MySQL to ensure clean state
    echo "Restarting MySQL..."
    sudo systemctl restart mysql 2>/dev/null || true
    
    # Wait for MySQL to be fully ready
    echo "Waiting for MySQL to be ready..."
    for i in {1..30}; do
        if mysql -h 127.0.0.1 -P 3306 -u$MYSQL_USER -p$MYSQL_PASSWORD -e "SELECT 1" 2>/dev/null; then
            echo "MySQL is ready"
            break
        fi
        sleep 1
    done
    
    # Reset read_only mode again after restart
    echo "Ensuring read_only is disabled..."
    mysql -h 127.0.0.1 -P 3306 -u$MYSQL_USER -p$MYSQL_PASSWORD -e "SET GLOBAL super_read_only = OFF; SET GLOBAL read_only = OFF;" 2>/dev/null || true
fi

echo ""
echo "=========================================="
echo "Cleanup Complete!"
echo "=========================================="
echo ""
