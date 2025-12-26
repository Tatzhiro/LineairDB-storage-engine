# Ubuntu-based MySQL 8.0 image for LineairDB Group Replication
# Based on Ubuntu 22.04 with MySQL 8.0.43+
#
# Features:
# - Pre-configured for Group Replication (GTID, binary logging)
# - MySQL Shell pre-installed
# - Plugin directory ready for LineairDB installation

FROM ubuntu:22.04

ARG MYSQL_VERSION=8.0
ARG DEBIAN_FRONTEND=noninteractive

# Install MySQL and dependencies
RUN apt-get update && apt-get install -y \
    gnupg \
    wget \
    lsb-release \
    && wget https://dev.mysql.com/get/mysql-apt-config_0.8.29-1_all.deb \
    && dpkg -i mysql-apt-config_0.8.29-1_all.deb \
    && apt-get update \
    && apt-get install -y \
        mysql-server \
        mysql-shell \
    && rm -rf /var/lib/apt/lists/* \
    && rm mysql-apt-config_0.8.29-1_all.deb

# Create necessary directories
RUN mkdir -p /var/run/mysqld /var/lib/mysql /var/log/mysql \
    && chown -R mysql:mysql /var/run/mysqld /var/lib/mysql /var/log/mysql \
    && chmod 755 /var/run/mysqld

# Create plugin directory (for LineairDB)
RUN mkdir -p /usr/lib64/mysql/plugin \
    && chmod 755 /usr/lib64/mysql/plugin

# Copy entrypoint script
COPY docker-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# MySQL configuration for Group Replication
RUN echo "[mysqld]\n\
server-id=1\n\
log-bin=mysql-bin\n\
gtid-mode=ON\n\
enforce-gtid-consistency=ON\n\
binlog-format=ROW\n\
relay-log=mysql-relay-bin\n\
log-slave-updates=ON\n\
master-info-repository=TABLE\n\
relay-log-info-repository=TABLE\n\
binlog-checksum=NONE\n\
\n\
# Group Replication settings\n\
plugin_load_add='group_replication.so'\n\
group_replication_start_on_boot=OFF\n\
transaction_write_set_extraction=XXHASH64\n\
\n\
# Network settings\n\
bind-address=0.0.0.0\n\
mysqlx-bind-address=0.0.0.0\n\
\n\
# Performance settings\n\
innodb_buffer_pool_size=256M\n\
max_connections=500\n\
" > /etc/mysql/conf.d/gr.cnf

EXPOSE 3306 33060 33061

ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["mysqld"]

