#!/bin/bash
set -eux

# Set up config
sudo tee /etc/mysql/mysql.conf.d/z-toggle.cnf >/dev/null <<'EOF'
[mysqld]
server-id = 1
log_bin = /var/log/mysql/mysql-bin.log
gtid_mode = ON
enforce_gtid_consistency = ON
EOF

# Initialize MySQL servers
sudo systemctl restart mysql

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
sudo mysql -u root -e "
  CREATE USER IF NOT EXISTS 'repl'@'%' IDENTIFIED WITH mysql_native_password BY 'replpass';
  GRANT REPLICATION SLAVE ON *.* TO 'repl'@'%';
  FLUSH PRIVILEGES;
"

# Clear any existing binary logs and GTIDs on the Source
sudo mysql -u root -e "RESET MASTER;"

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
    START REPLICA;
    SET GLOBAL rpl_semi_sync_replica_enabled = 1;
  "
done

# Start Semi-Synchronous Replication on the Source
sudo mysql -u root -e "INSTALL PLUGIN rpl_semi_sync_source SONAME 'semisync_source.so';" 2>/dev/null || true
sudo mysql -u root -e "
  SET GLOBAL rpl_semi_sync_source_enabled = 1;
  SET GLOBAL rpl_semi_sync_source_timeout = 4294967295;
  SET GLOBAL rpl_semi_sync_source_wait_for_replica_count = 1;
"

echo "Verifying Semi-Sync Status..."
sudo mysql -u root -e "SHOW STATUS LIKE 'Rpl_semi_sync_source_clients';"
sudo mysql -u root -e "SHOW STATUS LIKE 'Rpl_semi_sync_source_status';"
