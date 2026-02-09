#!/usr/bin/env bash
############################################
# ProxySQL Teardown Script
#
# This script reverses setup_proxysql.sh:
#   - Removes primary + replicas from mysql_servers
#   - Removes frontend user from mysql_users
#   - Removes query rules
#   - Stops ProxySQL service
#
# Safe to run multiple times (idempotent).
############################################

set -euo pipefail

############################################
# Source configuration
############################################
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROXYSQL_DIR="$(dirname "${SCRIPT_DIR}")"
CONFIG_FILE="${PROXYSQL_DIR}/config"

if [[ ! -f "${CONFIG_FILE}" ]]; then
    echo "ERROR: Configuration file not found: ${CONFIG_FILE}" >&2
    exit 1
fi

source "${CONFIG_FILE}"

############################################
# Check LineairDB availability (informational)
############################################
echo "[info] Checking LineairDB status..."

if check_lineairdb_running; then
    echo "    LineairDB mysqld: RUNNING"
else
    echo "    LineairDB mysqld: NOT RUNNING (proceeding with teardown anyway)"
fi

############################################
# Helpers
############################################
MYSQL_CLI="${MYSQL_CLI:-mysql}"

run_admin_sql() {
    local sql="$1"
    MYSQL_PWD="${PROXYSQL_ADMIN_PASS}" "${MYSQL_CLI}" \
        -h "${PROXYSQL_ADMIN_HOST}" -P "${PROXYSQL_ADMIN_PORT}" \
        -u "${PROXYSQL_ADMIN_USER}" \
        --protocol=tcp \
        -e "${sql}"
}

############################################
# Check ProxySQL admin connectivity
############################################
echo ""
echo "[1/5] Checking ProxySQL admin connectivity..."

if ! run_admin_sql "SELECT 1;" >/dev/null 2>&1; then
    echo "    Cannot connect to ProxySQL admin. Service may already be stopped."
    echo "    Skipping to service stop..."
    
    # Try to stop the service anyway
    if command -v systemctl >/dev/null 2>&1; then
        if systemctl is-active --quiet proxysql; then
            sudo systemctl stop proxysql
            echo "    ProxySQL service stopped."
        fi
    fi
    
    echo "✅ Teardown complete."
    exit 0
fi

echo "    ✅ Connected to ProxySQL admin"

############################################
# Remove query rules
############################################
echo ""
echo "[2/5] Removing query rules (rule_id=${RULE_SELECT_FOR_UPDATE_ID},${RULE_SELECT_ID})..."

run_admin_sql "
USE main;

DELETE FROM mysql_query_rules
WHERE rule_id IN (${RULE_SELECT_ID}, ${RULE_SELECT_FOR_UPDATE_ID});

LOAD MYSQL QUERY RULES TO RUNTIME;
SAVE MYSQL QUERY RULES TO DISK;
"
echo "    ✅ Query rules removed"

############################################
# Remove backend servers
############################################
echo ""
echo "[3/5] Removing backend servers (writer HG ${WRITER_HG} + reader HG ${READER_HG})..."

SQL="USE main;\n"

# Remove primary from writer hostgroup
SQL+="DELETE FROM mysql_servers WHERE hostgroup_id=${WRITER_HG} AND hostname='${PRIMARY_HOST}' AND port=${PRIMARY_PORT};\n"

# Remove replicas from reader hostgroup
for h in $(csv_to_lines "${REPLICA_HOSTS}"); do
    SQL+="DELETE FROM mysql_servers WHERE hostgroup_id=${READER_HG} AND hostname='${h}' AND port=${PRIMARY_PORT};\n"
done

SQL+="LOAD MYSQL SERVERS TO RUNTIME;\nSAVE MYSQL SERVERS TO DISK;\n"
run_admin_sql "$SQL"
echo "    ✅ Backend servers removed"

############################################
# Remove frontend user
############################################
echo ""
echo "[4/5] Removing frontend user (${FRONTEND_USER})..."

run_admin_sql "
USE main;

DELETE FROM mysql_users
WHERE username='${FRONTEND_USER}';

LOAD MYSQL USERS TO RUNTIME;
SAVE MYSQL USERS TO DISK;
"
echo "    ✅ Frontend user removed"

############################################
# Verify teardown
############################################
echo ""
echo "[5/5] Verifying teardown result..."

run_admin_sql "
USE main;

SELECT 'runtime_mysql_servers' AS tbl;
SELECT hostgroup_id, hostname, port, status
FROM runtime_mysql_servers
WHERE hostgroup_id IN (${WRITER_HG}, ${READER_HG})
ORDER BY hostgroup_id, hostname;

SELECT 'runtime_mysql_users' AS tbl;
SELECT username, default_hostgroup, active
FROM runtime_mysql_users
WHERE username='${FRONTEND_USER}';

SELECT 'mysql_query_rules' AS tbl;
SELECT rule_id, active, match_pattern, destination_hostgroup
FROM runtime_mysql_query_rules
WHERE rule_id IN (${RULE_SELECT_ID}, ${RULE_SELECT_FOR_UPDATE_ID});
"

############################################
# Stop ProxySQL service
############################################
echo ""
echo "[FINAL] Stopping ProxySQL service..."

if command -v systemctl >/dev/null 2>&1; then
    if systemctl is-active --quiet proxysql; then
        sudo systemctl stop proxysql
        echo "    ProxySQL service stopped (systemctl)."
    else
        echo "    ProxySQL service is not running."
    fi
else
    echo "    systemctl not found; skipping ProxySQL service stop."
fi

echo ""
echo "✅ ProxySQL teardown complete."
