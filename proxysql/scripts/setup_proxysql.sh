#!/usr/bin/env bash
############################################
# ProxySQL Setup Script
#
# This script configures ProxySQL with MySQL/LineairDB backends
# for read/write splitting with failover support.
#
# Prerequisites:
#   - ProxySQL must be installed (see install/install_proxysql.sh)
#   - LineairDB mysqld must be running
############################################

# Re-run as root (needed for MySQL root socket auth + non-interactive sudo issues)
if [[ "${EUID}" -ne 0 ]]; then
    exec sudo -E bash "$0" "$@"
fi

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
# Verify LineairDB is available
############################################
echo "[0/6] Checking LineairDB availability..."

if ! verify_lineairdb_available; then
    exit 1
fi

echo "    ✅ LineairDB is available and running"

############################################
# Get LineairDB paths
############################################
LINEAIRDB_MYSQL_CLI=$(get_lineairdb_mysql_client)
LINEAIRDB_MYSQL_CNF=$(get_lineairdb_defaults_file)
LINEAIRDB_MYSQL_SOCKET=$(get_mysql_socket "${LINEAIRDB_MYSQL_CNF}")
ROLE=$(detect_node_role "${LINEAIRDB_MYSQL_CNF}")

echo "[auto] mysql client: ${LINEAIRDB_MYSQL_CLI}"
echo "[auto] defaults-file: ${LINEAIRDB_MYSQL_CNF}"
echo "[auto] mysql socket: ${LINEAIRDB_MYSQL_SOCKET}"
echo "[auto] detected role: ${ROLE}"

if [[ "${ROLE}" == "REPLICA" ]]; then
    echo "[warn] This node appears to be a replica. Normally, setup should run on the master."
fi

############################################
# Ensure ProxySQL service is running
############################################
echo ""
echo "[1/6] Starting ProxySQL service if not running..."

if command -v systemctl >/dev/null 2>&1; then
    if systemctl is-active --quiet proxysql; then
        echo "    ProxySQL service already running."
    else
        systemctl start proxysql
        echo "    ProxySQL service start requested."
    fi

    # Wait until the service is active
    for i in $(seq 1 20); do
        if systemctl is-active --quiet proxysql; then
            break
        fi
        sleep 0.25
    done

    if ! systemctl is-active --quiet proxysql; then
        echo "ERROR: proxysql service is not active after start." >&2
        systemctl status proxysql --no-pager -n 50 >&2 || true
        exit 1
    fi

    # Wait until admin port is listening (6032)
    echo "    Waiting for ProxySQL admin port (:${PROXYSQL_ADMIN_PORT}) to be ready..."
    for i in $(seq 1 40); do
        if ss -lnt 2>/dev/null | grep -qE "LISTEN\s+.*:${PROXYSQL_ADMIN_PORT}\b"; then
            echo "    ProxySQL admin port is listening."
            break
        fi
        sleep 0.25
    done

    if ! ss -lnt 2>/dev/null | grep -qE "LISTEN\s+.*:${PROXYSQL_ADMIN_PORT}\b"; then
        echo "ERROR: ProxySQL admin port (:${PROXYSQL_ADMIN_PORT}) is not listening." >&2
        exit 1
    fi
else
    echo "    systemctl not found; assuming ProxySQL is managed manually."
fi

############################################
# Helper functions
############################################
run_backend_sql_local() {
    local sql="$1"
    if [[ -n "${LINEAIRDB_MYSQL_SOCKET}" ]]; then
        "${LINEAIRDB_MYSQL_CLI}" --defaults-file="${LINEAIRDB_MYSQL_CNF}" -u root --protocol=SOCKET --socket="${LINEAIRDB_MYSQL_SOCKET}" -e "${sql}" && return 0
    fi
    "${LINEAIRDB_MYSQL_CLI}" --defaults-file="${LINEAIRDB_MYSQL_CNF}" -u root --protocol=TCP -h 127.0.0.1 -P ${PRIMARY_PORT} -e "${sql}" && return 0
    "${LINEAIRDB_MYSQL_CLI}" --defaults-file="${LINEAIRDB_MYSQL_CNF}" -u root -e "${sql}"
}

run_admin_sql() {
    MYSQL_PWD="${PROXYSQL_ADMIN_PASS}" \
    mysql -u "${PROXYSQL_ADMIN_USER}" \
          -h "${PROXYSQL_ADMIN_HOST}" \
          -P "${PROXYSQL_ADMIN_PORT}" \
          --protocol=tcp \
          -e "$1"
}

############################################
# Check ProxySQL admin connectivity
############################################
echo ""
echo "[2/6] Checking ProxySQL admin connectivity..."
run_admin_sql "SELECT 1;" >/dev/null
echo "    ✅ Connected to ProxySQL admin"

############################################
# Register backend servers
############################################
echo ""
echo "[3/6] Registering primary + replicas..."

SQL="USE main;"

# Only update PRIMARY_HOST if this node is master
if [[ "${ROLE}" == "MASTER" ]]; then
    SQL+="
DELETE FROM mysql_servers WHERE hostgroup_id IN (${WRITER_HG}, ${READER_HG});
"
    # Primary (writer)
    SQL+="
INSERT INTO mysql_servers(hostgroup_id, hostname, port, weight, max_connections)
VALUES (${WRITER_HG}, '${PRIMARY_HOST}', ${PRIMARY_PORT}, 1, 1000);
"
else
    echo "    [info] Skipping PRIMARY registration (this is a replica)"
fi

# Replicas (readers)
for r in $(csv_to_lines "${REPLICA_HOSTS}"); do
    SQL+="
INSERT INTO mysql_servers(hostgroup_id, hostname, port, weight, max_connections)
VALUES (${READER_HG}, '${r}', ${PRIMARY_PORT}, 1, 1000);
"
done

SQL+="
LOAD MYSQL SERVERS TO RUNTIME;
SAVE MYSQL SERVERS TO DISK;
"

run_admin_sql "${SQL}"
echo "    ✅ Backend servers registered"

############################################
# Register frontend user
############################################
echo ""
echo "[4/6] Registering frontend user..."

run_admin_sql "
USE main;

DELETE FROM mysql_users WHERE username='${FRONTEND_USER}';

INSERT INTO mysql_users(username, password, default_hostgroup, active, frontend, backend)
VALUES ('${FRONTEND_USER}', '${FRONTEND_PASS}', ${WRITER_HG}, 1, 1, 1);

LOAD MYSQL USERS TO RUNTIME;
SAVE MYSQL USERS TO DISK;
"
echo "    ✅ Frontend user registered"

############################################
# Add query rules (SELECT → replicas)
############################################
echo ""
echo "[5/6] Adding query rules..."

run_admin_sql "
USE main;

DELETE FROM mysql_query_rules WHERE rule_id IN (${RULE_SELECT_FOR_UPDATE_ID}, ${RULE_SELECT_ID});

-- Send SELECT ... FOR UPDATE to writer FIRST (more specific rule, higher priority)
INSERT INTO mysql_query_rules
(rule_id, active, apply, match_pattern, destination_hostgroup)
VALUES
(${RULE_SELECT_FOR_UPDATE_ID}, 1, 1, '^SELECT.*FOR UPDATE', ${WRITER_HG});

-- Generic SELECT goes to reader
INSERT INTO mysql_query_rules
(rule_id, active, apply, match_pattern, destination_hostgroup)
VALUES
(${RULE_SELECT_ID}, 1, 1, '^SELECT', ${READER_HG});

LOAD MYSQL QUERY RULES TO RUNTIME;
SAVE MYSQL QUERY RULES TO DISK;
"
echo "    ✅ Query rules configured"

############################################
# Verify runtime state
############################################
echo ""
echo "[6/6] Verifying runtime state..."

run_admin_sql "
USE main;

SELECT hostgroup_id, hostname, port, status
FROM runtime_mysql_servers
ORDER BY hostgroup_id, hostname;

SELECT username, password, default_hostgroup, active, frontend, backend
FROM runtime_mysql_users
WHERE username='${FRONTEND_USER}'
ORDER BY frontend DESC, backend DESC, default_hostgroup;
"

############################################
# Done
############################################
echo ""
echo "=============================================="
echo "✅ ProxySQL fully configured."
echo "=============================================="
echo ""
echo "Writer  (HG ${WRITER_HG}): ${PRIMARY_HOST}:${PRIMARY_PORT}"
echo "Readers (HG ${READER_HG}): ${REPLICA_HOSTS}"
echo ""
echo "Test from ANY node:"
echo "  MYSQL_PWD=${FRONTEND_PASS} ${LINEAIRDB_MYSQL_CLI} \\"
echo "    -u ${FRONTEND_USER} \\"
echo "    -h ${PRIMARY_HOST} -P ${PROXYSQL_CLIENT_PORT} \\"
echo "    -e \"SELECT @@hostname, @@server_id, @@read_only;\""
