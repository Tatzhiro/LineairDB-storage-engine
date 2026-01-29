#!/bin/bash
set -eux

sudo mysql -u root -e "
  CREATE USER IF NOT EXISTS 'orc'@'%' IDENTIFIED WITH mysql_native_password BY 'orcpass';
  GRANT SUPER, PROCESS, REPLICATION SLAVE, RELOAD, REPLICATION CLIENT ON *.* TO 'orc'@'%';
  GRANT SELECT ON performance_schema.* TO 'orc'@'%';
  FLUSH PRIVILEGES;
"

curl -u admin:admin "http://127.0.0.1:3000/api/discover/host.docker.internal/3306"
curl -u admin:admin "http://127.0.0.1:3000/api/discover/replica1/3306"
curl -u admin:admin "http://127.0.0.1:3000/api/discover/replica2/3306"
