#!/usr/bin/env bash
############################################
# ProxySQL Status Check Script
#
# This script checks the status of ProxySQL and its backends.
# Can be run without root privileges.
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
# Check LineairDB availability (informational only)
############################################
echo "=============================================="
echo "LineairDB Status"
echo "=============================================="

if check_lineairdb_running; then
    echo "✅ LineairDB mysqld: RUNNING"
    
    LINEAIRDB_MYSQL_CLI=$(get_lineairdb_mysql_client)
    LINEAIRDB_MYSQL_CNF=$(get_lineairdb_defaults_file)
    ROLE=$(detect_node_role "${LINEAIRDB_MYSQL_CNF}")
    
    echo "   Client: ${LINEAIRDB_MYSQL_CLI}"
    echo "   Config: ${LINEAIRDB_MYSQL_CNF}"
    echo "   Role: ${ROLE}"
else
    echo "⚠️  LineairDB mysqld: NOT RUNNING"
fi

############################################
# Helpers
############################################
run_admin_sql() {
    MYSQL_PWD="${PROXYSQL_ADMIN_PASS}" \
    mysql -u "${PROXYSQL_ADMIN_USER}" \
          -h "${PROXYSQL_ADMIN_HOST}" \
          -P "${PROXYSQL_ADMIN_PORT}" \
          --protocol=tcp \
          -N -e "$1"
}

section() {
    echo ""
    echo "=============================================="
    echo "$1"
    echo "=============================================="
}

############################################
# 1) systemd status
############################################
section "ProxySQL Service (systemd)"

if command -v systemctl >/dev/null 2>&1; then
    if systemctl is-active --quiet proxysql; then
        echo "✅ proxysql service: ACTIVE"
    else
        echo "❌ proxysql service: NOT ACTIVE"
        systemctl status proxysql --no-pager -n 10 || true
    fi
else
    echo "⚠️ systemctl not available"
fi

############################################
# 2) Port check
############################################
section "Listening Ports"

if ss -lnt 2>/dev/null | grep -qE ":${PROXYSQL_ADMIN_PORT}\b"; then
    echo "✅ Admin port ${PROXYSQL_ADMIN_PORT}: LISTENING"
else
    echo "❌ Admin port ${PROXYSQL_ADMIN_PORT}: NOT LISTENING"
fi

if ss -lnt 2>/dev/null | grep -qE ":${PROXYSQL_CLIENT_PORT}\b"; then
    echo "✅ Client port ${PROXYSQL_CLIENT_PORT}: LISTENING"
else
    echo "⚠️ Client port ${PROXYSQL_CLIENT_PORT}: NOT LISTENING"
fi

############################################
# 3) Admin connectivity
############################################
section "ProxySQL Admin Connectivity"

if run_admin_sql "SELECT 1;" >/dev/null 2>&1; then
    echo "✅ Can connect to ProxySQL admin (${PROXYSQL_ADMIN_PORT})"
else
    echo "❌ Cannot connect to ProxySQL admin (${PROXYSQL_ADMIN_PORT})"
    exit 1
fi

############################################
# 4) Backend MySQL servers status
############################################
section "Backend MySQL Servers (runtime_mysql_servers)"

run_admin_sql "
SELECT
    hostgroup_id,
    hostname,
    port,
    status,
    weight,
    max_connections
FROM runtime_mysql_servers
ORDER BY hostgroup_id, hostname;
" | column -t

############################################
# 5) Connection pool stats
############################################
section "Connection Pool Stats (stats_mysql_connection_pool)"

run_admin_sql "
SELECT
    hostgroup,
    srv_host,
    srv_port,
    status,
    ConnUsed,
    ConnFree,
    ConnOK,
    ConnERR,
    Queries
FROM stats_mysql_connection_pool
ORDER BY hostgroup, srv_host;
" | column -t

############################################
# 6) GTID Causal Read Status
############################################
section "GTID Causal Read Status"

# Check if GTID tracking is enabled
gtid_tracking=$(run_admin_sql "
SELECT variable_value FROM global_variables 
WHERE variable_name = 'mysql-client_session_track_gtid';
" 2>/dev/null || echo "0")

gtid_session=$(run_admin_sql "
SELECT variable_value FROM global_variables 
WHERE variable_name = 'mysql-default_session_track_gtids';
" 2>/dev/null || echo "OFF")

# Check if gtid_from_hostgroup is configured in query rules
gtid_rules=$(run_admin_sql "
SELECT COUNT(*) FROM runtime_mysql_query_rules 
WHERE gtid_from_hostgroup IS NOT NULL;
" 2>/dev/null || echo "0")

# Check if gtid_port is configured for servers
gtid_ports=$(run_admin_sql "
SELECT COUNT(*) FROM runtime_mysql_servers 
WHERE gtid_port > 0;
" 2>/dev/null || echo "0")

# Check if we have GTID data from binlog readers
gtid_data=$(run_admin_sql "
SELECT COUNT(*) FROM stats_mysql_gtid_executed;
" 2>/dev/null || echo "0")

if [[ "${gtid_tracking}" == "1" || "${gtid_tracking}" == "true" ]] && \
   [[ "${gtid_session}" == "OWN_GTID" ]] && \
   [[ "${gtid_rules}" != "0" ]] && \
   [[ "${gtid_ports}" != "0" ]] && \
   [[ "${gtid_data}" != "0" ]]; then
    echo "✅ GTID Causal Reads: ENABLED"
    echo "   - GTID tracking: ${gtid_tracking}"
    echo "   - session_track_gtids: ${gtid_session}"
    echo "   - GTID query rules: ${gtid_rules}"
    echo "   - Servers with gtid_port: ${gtid_ports}"
    echo "   - GTID data from binlog readers: ${gtid_data} entries"
else
    echo "⚠️  GTID Causal Reads: NOT FULLY CONFIGURED"
    echo "   - GTID tracking: ${gtid_tracking} (need: 1/true)"
    echo "   - session_track_gtids: ${gtid_session} (need: OWN_GTID)"
    echo "   - GTID query rules: ${gtid_rules} (need: >0)"
    echo "   - Servers with gtid_port: ${gtid_ports} (need: >0)"
    echo "   - GTID data entries: ${gtid_data} (need: >0)"
    echo ""
    echo "   To enable: ${SCRIPT_DIR}/enable_gtid_causal_read.sh"
fi

############################################
# 7) Summary
############################################
section "Summary"

offline_cnt=$(run_admin_sql "
SELECT COUNT(*)
FROM runtime_mysql_servers
WHERE status NOT IN ('ONLINE');
")

if [[ "${offline_cnt}" == "0" ]]; then
    echo "✅ All backend MySQL servers are ONLINE"
else
    echo "❌ ${offline_cnt} backend MySQL server(s) NOT ONLINE"
fi

echo ""
echo "✔ ProxySQL status check complete."
