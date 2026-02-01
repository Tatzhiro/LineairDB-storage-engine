#!/usr/bin/env bash
set -euo pipefail

############################################
# This teardown script reverses setup_proxysql.sh:
#  - Remove primary + replicas from mysql_servers (writer HG + reader HG)
#  - Remove frontend user from mysql_users
#  - Remove query rules used for read/write split (rule_id 100/101)
#  - LOAD/SAVE changes to runtime + disk
#
# Safe to run multiple times (idempotent).
############################################

# ---------- ProxySQL admin ----------
PROXYSQL_ADMIN_HOST="${PROXYSQL_ADMIN_HOST:-127.0.0.1}"
PROXYSQL_ADMIN_PORT="${PROXYSQL_ADMIN_PORT:-6032}"
PROXYSQL_ADMIN_USER="${PROXYSQL_ADMIN_USER:-admin}"
PROXYSQL_ADMIN_PASS="${PROXYSQL_ADMIN_PASS:-admin}"

# ---------- Cluster topology (defaults match your setup) ----------
PRIMARY_HOST="${PRIMARY_HOST:-133.125.85.242}"                # database2-01
REPLICA_HOSTS="${REPLICA_HOSTS:-133.242.17.72,153.120.20.111}" # database2-02,database2-03
MYSQL_PORT="${MYSQL_PORT:-3306}"

WRITER_HG="${WRITER_HG:-0}"
READER_HG="${READER_HG:-1}"

# ---------- Frontend user ----------
FRONTEND_USER="${FRONTEND_USER:-proxysql_user}"

# ---------- Query rule IDs used by setup ----------
RULE_SELECT_FOR_UPDATE_TO_PRIMARY_ID="${RULE_SELECT_FOR_UPDATE_TO_PRIMARY_ID:-90}"
RULE_SELECT_TO_REPLICA_ID="${RULE_SELECT_TO_REPLICA_ID:-100}"

MYSQL_CLI="${MYSQL_CLI:-mysql}"

run_admin_sql() {
  local sql="$1"
  MYSQL_PWD="${PROXYSQL_ADMIN_PASS}" "${MYSQL_CLI}" \
    -h "${PROXYSQL_ADMIN_HOST}" -P "${PROXYSQL_ADMIN_PORT}" \
    -u "${PROXYSQL_ADMIN_USER}" \
    --protocol=tcp \
    -e "${sql}"
}

csv_to_lines() {
  echo "$1" | tr ',' '\n' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//' | sed '/^$/d'
}

echo "[1/5] Checking ProxySQL admin connectivity..."
run_admin_sql "SELECT 1;" >/dev/null

echo "[2/5] Removing query rules (rule_id=${RULE_SELECT_FOR_UPDATE_TO_PRIMARY_ID},${RULE_SELECT_TO_REPLICA_ID})..."
run_admin_sql "
  USE main;

  DELETE FROM mysql_query_rules
   WHERE rule_id IN (${RULE_SELECT_TO_REPLICA_ID}, ${RULE_SELECT_FOR_UPDATE_TO_PRIMARY_ID});

  LOAD MYSQL QUERY RULES TO RUNTIME;
  SAVE MYSQL QUERY RULES TO DISK;
"

echo "[3/5] Removing backend servers (writer HG ${WRITER_HG} + reader HG ${READER_HG})..."
# Build a single SQL batch for server removals.
SQL="USE main;\n"

# Remove primary from writer hostgroup
SQL+="DELETE FROM mysql_servers WHERE hostgroup_id=${WRITER_HG} AND hostname='${PRIMARY_HOST}' AND port=${MYSQL_PORT};\n"

# Remove replicas from reader hostgroup
if [[ -n "${REPLICA_HOSTS}" ]]; then
  while IFS= read -r h; do
    SQL+="DELETE FROM mysql_servers WHERE hostgroup_id=${READER_HG} AND hostname='${h}' AND port=${MYSQL_PORT};\n"
  done < <(csv_to_lines "${REPLICA_HOSTS}")
fi

SQL+="LOAD MYSQL SERVERS TO RUNTIME;\nSAVE MYSQL SERVERS TO DISK;\n"
run_admin_sql "$SQL"

echo "[4/5] Removing frontend user (${FRONTEND_USER})..."
run_admin_sql "
  USE main;

  DELETE FROM mysql_users
   WHERE username='${FRONTEND_USER}';

  LOAD MYSQL USERS TO RUNTIME;
  SAVE MYSQL USERS TO DISK;
"

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
   WHERE rule_id IN (${RULE_SELECT_TO_REPLICA_ID}, ${RULE_SELECT_FOR_UPDATE_TO_PRIMARY_ID});
"

echo "✅ ProxySQL teardown complete (reversed setup_proxysql.sh)."

# ---------- Final step: stop ProxySQL service ----------
echo "[FINAL] Stopping ProxySQL service to disable admin/client access..."

if command -v systemctl >/dev/null 2>&1; then
  if systemctl is-active --quiet proxysql; then
    sudo systemctl stop proxysql
    echo "ProxySQL service stopped (systemctl)."
  else
    echo "ProxySQL service is not running."
  fi
else
  echo "systemctl not found; skipping ProxySQL service stop."
fi

