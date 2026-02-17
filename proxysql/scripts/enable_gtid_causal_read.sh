#!/usr/bin/env bash
############################################
# GTID Causal Reads Setup for ProxySQL
#
# This script sets up true GTID-based causal consistency reads.
# It requires ProxySQL Binlog Reader to be installed and running
# on each MySQL server.
#
# How it works:
# 1. ProxySQL Binlog Reader runs on each MySQL server, reads binlog,
#    and streams GTID events to ProxySQL
# 2. ProxySQL tracks which GTIDs have been executed on each server
# 3. When a client writes, ProxySQL captures the GTID from the OK packet
# 4. When routing a read, ProxySQL checks which servers have that GTID
# 5. Read is sent only to servers that have the required GTID
#
# Requirements:
# - ProxySQL 2.0+
# - MySQL 5.7.5+ with GTID enabled
# - ProxySQL Binlog Reader installed on each MySQL server
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
echo "ProxySQL GTID Causal Reads Setup"
echo "=============================================="

############################################
# Check LineairDB availability
############################################
echo ""
echo "[0/5] Checking LineairDB availability..."

if ! verify_lineairdb_available; then
    exit 1
fi

echo "    ✅ LineairDB is available and running"

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
# Step 1: Check prerequisites
############################################
echo ""
echo "[1/5] Checking prerequisites..."

# Check ProxySQL connectivity
if ! run_admin_sql "SELECT 1;" >/dev/null 2>&1; then
    echo "    ❌ Cannot connect to ProxySQL admin on ${PROXYSQL_ADMIN_HOST}:${PROXYSQL_ADMIN_PORT}"
    exit 1
fi
echo "    ✅ ProxySQL admin connection OK"

# Check if binlog readers are running
echo ""
echo "    Checking ProxySQL Binlog Reader on each server..."

BINLOG_READERS_OK=true
for HOST in ${PRIMARY_HOST} ${REPLICA1_HOST} ${REPLICA2_HOST}; do
    if nc -z -w2 "${HOST}" "${BINLOG_READER_PORT}" 2>/dev/null; then
        echo "      ✅ ${HOST}:${BINLOG_READER_PORT} - Binlog Reader responding"
    else
        echo "      ❌ ${HOST}:${BINLOG_READER_PORT} - Binlog Reader NOT running"
        BINLOG_READERS_OK=false
    fi
done

if [[ "${BINLOG_READERS_OK}" != "true" ]]; then
    echo ""
    echo "=============================================="
    echo "ProxySQL Binlog Reader NOT installed/running"
    echo "=============================================="
    cat <<EOF

GTID causal reads require ProxySQL Binlog Reader on each MySQL server.

To install:
  ${PROXYSQL_DIR}/install/install_binlog_reader.sh

To run on each MySQL server:

  # Create replication user (on primary)
  mysql -e "
    CREATE USER IF NOT EXISTS '${MYSQL_REPL_USER}'@'localhost' IDENTIFIED BY '${MYSQL_REPL_PASS}';
    GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO '${MYSQL_REPL_USER}'@'localhost';
  "

  # Start binlog reader (on each server)
  proxysql_binlog_reader \\
    -h 127.0.0.1 \\
    -u ${MYSQL_REPL_USER} \\
    -p ${MYSQL_REPL_PASS} \\
    -P 3306 \\
    -l ${BINLOG_READER_PORT} \\
    -L /var/log/proxysql_binlog_reader.log &

After starting binlog readers on all servers, run this script again.

EOF
    exit 1
fi

############################################
# Step 2: Configure MySQL session_track_gtids
############################################
echo ""
echo "[2/5] Note: Ensure MySQL servers have session_track_gtids=OWN_GTID"
echo "      Add to my.cnf: session_track_gtids=OWN_GTID"

############################################
# Step 3: Configure ProxySQL GTID tracking
############################################
echo ""
echo "[3/5] Configuring ProxySQL for GTID tracking..."

run_admin_sql "
SET mysql-client_session_track_gtid = true;
SET mysql-default_session_track_gtids = 'OWN_GTID';
LOAD MYSQL VARIABLES TO RUNTIME;
SAVE MYSQL VARIABLES TO DISK;
"
echo "    ✅ GTID tracking variables set"

############################################
# Step 4: Configure gtid_port for each server
############################################
echo ""
echo "[4/5] Configuring gtid_port for MySQL servers..."

run_admin_sql "
USE main;

-- Update gtid_port for all servers to point to their binlog reader
UPDATE mysql_servers SET gtid_port = ${BINLOG_READER_PORT} WHERE hostname = '${PRIMARY_HOST}';
UPDATE mysql_servers SET gtid_port = ${BINLOG_READER_PORT} WHERE hostname = '${REPLICA1_HOST}';
UPDATE mysql_servers SET gtid_port = ${BINLOG_READER_PORT} WHERE hostname = '${REPLICA2_HOST}';

LOAD MYSQL SERVERS TO RUNTIME;
SAVE MYSQL SERVERS TO DISK;
"
echo "    ✅ gtid_port configured for all servers"

############################################
# Step 5: Configure GTID-aware query rules
############################################
echo ""
echo "[5/5] Configuring GTID-aware query rules..."

run_admin_sql "
USE main;

-- Remove existing SELECT rules
DELETE FROM mysql_query_rules WHERE rule_id IN (${RULE_SELECT_FOR_UPDATE_ID}, ${RULE_SELECT_ID});

-- SELECT FOR UPDATE always goes to writer
INSERT INTO mysql_query_rules
(rule_id, active, apply, match_pattern, destination_hostgroup)
VALUES
(${RULE_SELECT_FOR_UPDATE_ID}, 1, 1, '^SELECT.*FOR UPDATE', ${WRITER_HG});

-- SELECT with GTID causal read: route to reader ONLY if it has the required GTID
-- gtid_from_hostgroup=0 means: check if target has GTID from writer (HG0)
INSERT INTO mysql_query_rules
(rule_id, active, apply, match_pattern, destination_hostgroup, gtid_from_hostgroup)
VALUES
(${RULE_SELECT_ID}, 1, 1, '^SELECT', ${READER_HG}, ${WRITER_HG});

LOAD MYSQL QUERY RULES TO RUNTIME;
SAVE MYSQL QUERY RULES TO DISK;
"
echo "    ✅ GTID-aware query rules configured"

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
echo "MySQL Servers with gtid_port:"
run_admin_sql_verbose "
SELECT hostgroup_id, hostname, port, gtid_port, status
FROM runtime_mysql_servers
ORDER BY hostgroup_id, hostname;
"

echo ""
echo "Query Rules:"
run_admin_sql_verbose "
SELECT rule_id, match_pattern, destination_hostgroup as dest_hg, gtid_from_hostgroup as gtid_hg
FROM runtime_mysql_query_rules
WHERE rule_id IN (${RULE_SELECT_FOR_UPDATE_ID}, ${RULE_SELECT_ID})
ORDER BY rule_id;
"

echo ""
echo "GTID Executed Status (from binlog readers):"
run_admin_sql_verbose "
SELECT * FROM stats_mysql_gtid_executed LIMIT 10;
"

echo ""
echo "=============================================="
echo "✅ GTID Causal Reads Enabled"
echo "=============================================="
cat <<EOF

How it works:
  1. Client writes to primary → ProxySQL captures GTID from OK packet
  2. Client issues SELECT → ProxySQL checks which replicas have that GTID
  3. SELECT routed to replica that has the GTID (or primary if none do)

This provides:
  ✅ Read-after-write consistency
  ✅ Read scaling to replicas
  ✅ Automatic fallback to primary if replicas lag

To test:
  cd ${PROXYSQL_DIR}/tests && python3 gtid_causal_read_test.py --verbose

To check status:
  ${SCRIPT_DIR}/status.sh

To disable (restore simple read/write split):
  ${SCRIPT_DIR}/setup_proxysql.sh

Note: Python MySQL connectors (mysql.connector, PyMySQL) do NOT support
      the SESSION_TRACK protocol. The test uses MySQL CLI which works correctly.

EOF
