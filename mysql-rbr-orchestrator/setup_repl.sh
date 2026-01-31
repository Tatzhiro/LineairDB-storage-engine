#!/bin/bash
set -eux

# Set up config
# Define the command to connect to the host MySQL. 
# If your host MySQL root user has a password, add -p'yourpassword' here.
BUILD_DIR=$(realpath ../build)
CUSTOM_MYSQLD="$BUILD_DIR/bin/mysqld"
CUSTOM_MYSQL_CLIENT="$BUILD_DIR/bin/mysql"

if [ -x "$CUSTOM_MYSQLD" ]; then
  echo "✅ Found custom built mysqld. Using it to ensure API compatibility."

  # Stop system mysql to free port 3306
  sudo systemctl stop mysql || true
  if pgrep mysqld > /dev/null; then
    sudo kill -9 $(pgrep mysqld) || true
  fi
  
  if [ ! -d "../build/mysql" ]; then
    echo "Initializing custom datadir..."
    "$CUSTOM_MYSQLD" --defaults-file=../my.cnf --initialize-insecure
  fi

  # Start custom mysqld
  "$CUSTOM_MYSQLD" --defaults-file=../my.cnf --daemonize
  
  # Wait for start
  sleep 5
  
  # Set password and define client
  HOST_MYSQL="$CUSTOM_MYSQL_CLIENT -u root"

else
  echo "⚠️ Custom mysqld not found. Falling back to system MySQL."
  HOST_MYSQL="sudo mysql -u root"
  sudo tee /etc/mysql/mysql.conf.d/z-toggle.cnf >/dev/null <<'EOF'
[mysqld]
server-id = 1
log_bin = /var/log/mysql/mysql-bin.log
gtid_mode = ON
enforce_gtid_consistency = ON
EOF
  sudo systemctl restart mysql
fi

# Build the MySQL Docker image and start MySQL containers as replicas
sudo docker build -t mysql-lineairdb:8.0.43 .
sudo docker compose down
sudo docker compose up -d


# Wait for replicas to be ready
until sudo docker exec replica1 mysql -uroot -prootpass -e "SELECT 1" &> /dev/null; do 
  sleep 10; 
done
until sudo docker exec replica2 mysql -uroot -prootpass -e "SELECT 1" &> /dev/null; do 
  sleep 2;
done

# Create the replication user on the Source
$HOST_MYSQL -e "
  CREATE USER IF NOT EXISTS 'repl'@'%' IDENTIFIED WITH mysql_native_password BY 'replpass';
  GRANT REPLICATION SLAVE ON *.* TO 'repl'@'%';
  CREATE USER 'root'@'127.0.0.1' IDENTIFIED WITH mysql_native_password BY '';
  GRANT ALL PRIVILEGES ON *.* TO 'root'@'127.0.0.1';
  FLUSH PRIVILEGES;
"

# Clear any existing binary logs and GTIDs on the Source
$HOST_MYSQL -e "RESET MASTER;"

# Initialize Replicas
for i in 1 2; do
  echo "Setting up replica$i..."
  sudo docker exec -i replica$i mysql -uroot -prootpass -e "
    CREATE USER IF NOT EXISTS 'repl'@'%' IDENTIFIED WITH mysql_native_password BY 'replpass';
    ALTER USER 'repl'@'%' IDENTIFIED WITH mysql_native_password BY 'replpass';
    GRANT REPLICATION SLAVE ON *.* TO 'repl'@'%';
    FLUSH PRIVILEGES;
    
    STOP REPLICA;
    RESET REPLICA ALL;
    CHANGE REPLICATION SOURCE TO
      SOURCE_HOST='host.docker.internal',
      SOURCE_PORT=3306,
      SOURCE_USER='repl',
      SOURCE_PASSWORD='replpass',
      SOURCE_AUTO_POSITION=1;
    CHANGE REPLICATION SOURCE TO SOURCE_CONNECT_RETRY = 2;
    START REPLICA;
    SET GLOBAL rpl_semi_sync_replica_enabled = 1;
  "
done

# Start Semi-Synchronous Replication on the Source
$HOST_MYSQL -e "INSTALL PLUGIN rpl_semi_sync_source SONAME 'semisync_source.so';" 2>/dev/null || true
$HOST_MYSQL -e "
  SET GLOBAL rpl_semi_sync_source_enabled = 1;
  SET GLOBAL rpl_semi_sync_source_timeout = 4294967295;
  SET GLOBAL rpl_semi_sync_source_wait_for_replica_count = 1;
"

echo "Verifying Semi-Sync Status..."
$HOST_MYSQL -e "SHOW STATUS LIKE 'Rpl_semi_sync_source_clients';"
$HOST_MYSQL -e "SHOW STATUS LIKE 'Rpl_semi_sync_source_status';"
