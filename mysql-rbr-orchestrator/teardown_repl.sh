#!/bin/bash
set -eux

# stop semi-sync on SOURCE
sudo mysql -u root -e "SET GLOBAL rpl_semi_sync_source_enabled = 0;"

echo "[*] Clear binlog/GTID state on SOURCE"
sudo mysql -u root -e "RESET MASTER;"
sudo mysql -u root -e "RESET REPLICA ALL;"
sudo mysql -u root -e "RESET SLAVE ALL;"

echo "[*] Stop docker replicas and remove orphans"
sudo docker compose down -v --remove-orphans

echo "[*] Enable SINGLE mode on host mysql (toggle file)"
sudo tee /etc/mysql/mysql.conf.d/z-toggle.cnf >/dev/null <<'EOF'
[mysqld]
gtid_mode = OFF
enforce_gtid_consistency = OFF
EOF
sudo systemctl restart mysql

echo "[*] Verify host is single-node (gtid_mode/log_bin)"
sudo mysql -u root -e "SELECT @@gtid_mode AS gtid_mode;"
