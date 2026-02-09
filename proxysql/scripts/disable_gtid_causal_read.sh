#!/usr/bin/env bash
############################################
# Disable GTID Causal Reads for ProxySQL
#
# This script disables GTID-based causal consistency reads and
# restores simple read/write splitting.
#
# After running this script:
# - SELECT queries go to reader hostgroup (round-robin)
# - Write queries go to writer hostgroup
# - No GTID tracking or consistency guarantees
############################################

set -euo pipefail

# Re-run as root if needed
if [[ "${EUID}" -ne 0 ]]; then
    exec sudo -E bash "$0" "$@"
fi

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

echo "=============================================="
echo "Disable GTID Causal Reads"
echo "=============================================="

############################################
# Check LineairDB availability
############################################
echo ""
echo "[0/4] Checking LineairDB availability..."

if ! verify_lineairdb_available; then
    echo "    ⚠️  LineairDB not running, but proceeding with ProxySQL changes..."
else
    echo "    ✅ LineairDB is available"
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

run_admin_sql_verbose() {
    MYSQL_PWD="${PROXYSQL_ADMIN_PASS}" \
    mysql -u "${PROXYSQL_ADMIN_USER}" \
          -h "${PROXYSQL_ADMIN_HOST}" \
          -P "${PROXYSQL_ADMIN_PORT}" \
          --protocol=tcp \
          -e "$1"
}

############################################
# Step 1: Check ProxySQL connectivity
############################################
echo ""
echo "[1/4] Checking ProxySQL admin connectivity..."

if ! run_admin_sql "SELECT 1;" >/dev/null 2>&1; then
    echo "    ❌ Cannot connect to ProxySQL admin on ${PROXYSQL_ADMIN_HOST}:${PROXYSQL_ADMIN_PORT}"
    exit 1
fi
echo "    ✅ ProxySQL admin connection OK"

############################################
# Step 2: Disable GTID tracking variables
############################################
echo ""
echo "[2/4] Disabling GTID tracking variables..."

run_admin_sql "
SET mysql-client_session_track_gtid = false;
SET mysql-default_session_track_gtids = 'OFF';
LOAD MYSQL VARIABLES TO RUNTIME;
SAVE MYSQL VARIABLES TO DISK;
"
echo "    ✅ GTID tracking disabled"

############################################
# Step 3: Remove gtid_port from servers
############################################
echo ""
echo "[3/4] Removing gtid_port from MySQL servers..."

run_admin_sql "
USE main;

-- Reset gtid_port to 0 for all servers
UPDATE mysql_servers SET gtid_port = 0 WHERE hostname = '${PRIMARY_HOST}';
UPDATE mysql_servers SET gtid_port = 0 WHERE hostname = '${REPLICA1_HOST}';
UPDATE mysql_servers SET gtid_port = 0 WHERE hostname = '${REPLICA2_HOST}';

LOAD MYSQL SERVERS TO RUNTIME;
SAVE MYSQL SERVERS TO DISK;
"
echo "    ✅ gtid_port removed from all servers"

############################################
# Step 4: Restore simple query rules
############################################
echo ""
echo "[4/4] Restoring simple read/write split query rules..."

run_admin_sql "
USE main;

-- Remove existing SELECT rules
DELETE FROM mysql_query_rules WHERE rule_id IN (${RULE_SELECT_FOR_UPDATE_ID}, ${RULE_SELECT_ID});

-- SELECT FOR UPDATE goes to writer
INSERT INTO mysql_query_rules
(rule_id, active, apply, match_pattern, destination_hostgroup)
VALUES
(${RULE_SELECT_FOR_UPDATE_ID}, 1, 1, '^SELECT.*FOR UPDATE', ${WRITER_HG});

-- Generic SELECT goes to reader (NO gtid_from_hostgroup)
INSERT INTO mysql_query_rules
(rule_id, active, apply, match_pattern, destination_hostgroup)
VALUES
(${RULE_SELECT_ID}, 1, 1, '^SELECT', ${READER_HG});

LOAD MYSQL QUERY RULES TO RUNTIME;
SAVE MYSQL QUERY RULES TO DISK;
"
echo "    ✅ Simple read/write split rules restored"

############################################
# Verification
############################################
echo ""
echo "=============================================="
echo "Configuration Complete - Verification"
echo "=============================================="

echo ""
echo "ProxySQL Variables:"
run_admin_sql_verbose "
SELECT variable_name, variable_value 
FROM global_variables 
WHERE variable_name IN ('mysql-client_session_track_gtid', 'mysql-default_session_track_gtids');
"

echo ""
echo "MySQL Servers (gtid_port should be 0):"
run_admin_sql_verbose "
SELECT hostgroup_id, hostname, port, gtid_port, status
FROM runtime_mysql_servers
ORDER BY hostgroup_id, hostname;
"

echo ""
echo "Query Rules (no gtid_from_hostgroup):"
run_admin_sql_verbose "
SELECT rule_id, match_pattern, destination_hostgroup as dest_hg, gtid_from_hostgroup as gtid_hg
FROM runtime_mysql_query_rules
WHERE rule_id IN (${RULE_SELECT_FOR_UPDATE_ID}, ${RULE_SELECT_ID})
ORDER BY rule_id;
"

echo ""
echo "=============================================="
echo "✅ GTID Causal Reads Disabled"
echo "=============================================="
cat <<EOF

ProxySQL is now using simple read/write splitting:

  - SELECT ... FOR UPDATE → Writer (HG ${WRITER_HG})
  - SELECT              → Reader (HG ${READER_HG}) - round-robin
  - All other queries   → Writer (HG ${WRITER_HG})

Note: Without GTID causal reads, there is NO read-after-write consistency
guarantee. Reads may return stale data if replicas are lagging.

To re-enable GTID causal reads:
  ${SCRIPT_DIR}/enable_gtid_causal_read.sh

To check status:
  ${SCRIPT_DIR}/status.sh

EOF
