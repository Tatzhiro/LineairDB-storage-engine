#!/bin/bash
# Docker entrypoint for MySQL LineairDB secondary containers
set -e

# MySQL data directory
DATADIR="/var/lib/mysql"

# Initialize MySQL if not already initialized
if [ ! -d "$DATADIR/mysql" ]; then
    echo "Initializing MySQL data directory..."
    
    # Initialize with --initialize-insecure (we'll set password later)
    mysqld --initialize-insecure --user=mysql --datadir="$DATADIR"
    
    echo "MySQL data directory initialized."
fi

# Start MySQL temporarily to set up users
echo "Starting MySQL temporarily for setup..."
mysqld --user=mysql --datadir="$DATADIR" --skip-networking &
pid="$!"

# Wait for MySQL to be ready
echo "Waiting for MySQL to be ready..."
for i in {1..60}; do
    if mysqladmin ping --silent 2>/dev/null; then
        break
    fi
    sleep 1
done

# Set root password and create users if MYSQL_ROOT_PASSWORD is set
if [ -n "$MYSQL_ROOT_PASSWORD" ]; then
    echo "Setting up MySQL users..."
    
    mysql --protocol=socket -uroot <<-EOSQL
        ALTER USER 'root'@'localhost' IDENTIFIED BY '${MYSQL_ROOT_PASSWORD}';
        CREATE USER IF NOT EXISTS 'root'@'%' IDENTIFIED BY '${MYSQL_ROOT_PASSWORD}';
        GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION;
        FLUSH PRIVILEGES;
EOSQL
    
    # Create additional user if specified
    if [ -n "$MYSQL_USER" ] && [ -n "$MYSQL_PASSWORD" ]; then
        mysql --protocol=socket -uroot -p"${MYSQL_ROOT_PASSWORD}" <<-EOSQL
            CREATE USER IF NOT EXISTS '${MYSQL_USER}'@'%' IDENTIFIED BY '${MYSQL_PASSWORD}';
            GRANT ALL PRIVILEGES ON *.* TO '${MYSQL_USER}'@'%';
            FLUSH PRIVILEGES;
EOSQL
    fi
    
    # Create database if specified
    if [ -n "$MYSQL_DATABASE" ]; then
        mysql --protocol=socket -uroot -p"${MYSQL_ROOT_PASSWORD}" <<-EOSQL
            CREATE DATABASE IF NOT EXISTS \`${MYSQL_DATABASE}\`;
EOSQL
    fi
    
    echo "MySQL users configured."
fi

# Stop the temporary MySQL instance
echo "Stopping temporary MySQL instance..."
if ! mysqladmin -uroot -p"${MYSQL_ROOT_PASSWORD}" shutdown 2>/dev/null; then
    kill "$pid" 2>/dev/null || true
fi
wait "$pid" 2>/dev/null || true

# Update server-id from environment if provided
if [ -n "$MYSQL_SERVER_ID" ]; then
    sed -i "s/^server-id=.*/server-id=$MYSQL_SERVER_ID/" /etc/mysql/conf.d/gr.cnf
fi

echo "Starting MySQL server..."
exec "$@" --user=mysql --datadir="$DATADIR"

