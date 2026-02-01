#!/usr/bin/env bash

# Re-run as root (needed for MySQL root socket auth + non-interactive sudo issues)
if [[ "${EUID}" -ne 0 ]]; then
  exec sudo -E bash "$0" "$@"
fi

set -euo pipefail

############################################
# Ensure ProxySQL service is running (and admin port is ready)
############################################
echo "[0/6] Starting ProxySQL service if not running..."

if command -v systemctl >/dev/null 2>&1; then
  if systemctl is-active --quiet proxysql; then
    echo "ProxySQL service already running."
  else
    systemctl start proxysql
    echo "ProxySQL service start requested."
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
    echo "---- systemctl status proxysql (last 50 lines) ----" >&2
    systemctl status proxysql --no-pager -n 50 >&2 || true
    exit 1
  fi

  # Wait until admin port is listening (6032)
  echo "Waiting for ProxySQL admin port (:6032) to be ready..."
  for i in $(seq 1 40); do
    if ss -lnt 2>/dev/null | grep -qE 'LISTEN\s+.*:6032\b'; then
      echo "ProxySQL admin port is listening."
      break
    fi
    sleep 0.25
  done

  if ! ss -lnt 2>/dev/null | grep -qE 'LISTEN\s+.*:6032\b'; then
    echo "ERROR: ProxySQL admin port (:6032) is not listening." >&2
    echo "---- ss -lntp | grep 6032/6033 ----" >&2
    ss -lntp 2>/dev/null | egrep ':(6032|6033)\b' >&2 || true
    echo "---- systemctl status proxysql (last 50 lines) ----" >&2
    systemctl status proxysql --no-pager -n 50 >&2 || true
    exit 1
  fi
else
  echo "systemctl not found; assuming ProxySQL is managed manually." >&2
  echo "NOTE: If you see connection refused to 6032, ensure ProxySQL is running and listening." >&2
fi

############################################
# ProxySQL admin (running on database2-01)
############################################
PROXYSQL_ADMIN_HOST="127.0.0.1"
PROXYSQL_ADMIN_PORT=6032
PROXYSQL_ADMIN_USER="admin"
PROXYSQL_ADMIN_PASS="admin"

############################################
# Cluster topology
############################################
PRIMARY_HOST="133.125.85.242"        # database2-01
REPLICA_HOSTS="133.242.17.72,153.120.20.111"   # database2-02,database2-03
MYSQL_PORT=3306

WRITER_HG=0
READER_HG=1

############################################
# Frontend (client → ProxySQL) user
############################################
FRONTEND_USER="proxysql_user"
FRONTEND_PASS="proxysql_pass"

MYSQL="mysql"

############################################
# Backend MySQL (ProxySQL → MySQL) detection
# Auto-detect the actual running mysqld (LineairDB build) and its defaults-file
############################################

echo "[auto] Detecting running mysqld..."

# Find candidate mysqld processes owned by ubuntu (ignore ProxySQL-internal replicas)
MYSQLD_LINE=$(ps -u ubuntu -o pid,cmd | grep '[m]ysqld' | head -n 1 | awk '{$1=""; print substr($0,2)}')

if [[ -z "${MYSQLD_LINE}" ]]; then
  echo "ERROR: No running mysqld process found for user 'ubuntu'." >&2
  exit 1
fi

echo "[auto] mysqld cmd: ${MYSQLD_LINE}"

# Extract mysqld binary
LINEAIRDB_MYSQLD_BIN=$(echo "${MYSQLD_LINE}" | awk '{print $1}')

# Derive mysql client next to mysqld
LINEAIRDB_MYSQL_CLI=$(dirname "${LINEAIRDB_MYSQLD_BIN}")/mysql

# Extract defaults-file if present
LINEAIRDB_MYSQL_CNF=$(echo "${MYSQLD_LINE}" | sed -n "s/.*--defaults-file=\([^ ]*\).*/\1/p")

if [[ -z "${LINEAIRDB_MYSQL_CNF}" ]]; then
  echo "ERROR: Could not detect --defaults-file from mysqld command line." >&2
  exit 1
fi

echo "[auto] mysql client: ${LINEAIRDB_MYSQL_CLI}"
echo "[auto] defaults-file: ${LINEAIRDB_MYSQL_CNF}"

# Detect master/replica role by defaults-file name
ROLE=""
if [[ "${LINEAIRDB_MYSQL_CNF}" == *my_release.cnf ]]; then
  ROLE="MASTER"
  echo "[auto] detected role: MASTER"
elif [[ "${LINEAIRDB_MYSQL_CNF}" == *my_replica_release.cnf ]]; then
  ROLE="REPLICA"
  echo "[auto] detected role: REPLICA"
  echo "[warn] This node appears to be a replica. Normally, setup_proxysql.sh should be run only on the master."
else
  echo "ERROR: Could not determine master/replica role from defaults-file name: ${LINEAIRDB_MYSQL_CNF}" >&2
  exit 1
fi

# Try to extract socket path from the defaults-file (supports both 'socket=...' and 'socket = ...')
LINEAIRDB_MYSQL_SOCKET=$(grep -E '^[[:space:]]*socket[[:space:]]*=' "${LINEAIRDB_MYSQL_CNF}" 2>/dev/null | tail -n 1 | sed -E 's/^[[:space:]]*socket[[:space:]]*=[[:space:]]*//')
if [[ -n "${LINEAIRDB_MYSQL_SOCKET}" ]]; then
  echo "[auto] mysql socket: ${LINEAIRDB_MYSQL_SOCKET}"
else
  echo "[auto] mysql socket: (not found in defaults-file)"
fi

run_backend_sql_local() {
  local sql="$1"
  # Prefer socket (root auth_socket or similar) if socket is known.
  if [[ -n "${LINEAIRDB_MYSQL_SOCKET}" ]]; then
    "${LINEAIRDB_MYSQL_CLI}" --defaults-file="${LINEAIRDB_MYSQL_CNF}" -u root --protocol=SOCKET --socket="${LINEAIRDB_MYSQL_SOCKET}" -e "${sql}" \
      && return 0
  fi

  # Fall back to TCP on localhost:${MYSQL_PORT}
  "${LINEAIRDB_MYSQL_CLI}" --defaults-file="${LINEAIRDB_MYSQL_CNF}" -u root --protocol=TCP -h 127.0.0.1 -P ${MYSQL_PORT} -e "${sql}" \
    && return 0

  # Last resort: whatever defaults-file provides
  "${LINEAIRDB_MYSQL_CLI}" --defaults-file="${LINEAIRDB_MYSQL_CNF}" -u root -e "${sql}"
}

run_admin_sql() {
  MYSQL_PWD="${PROXYSQL_ADMIN_PASS}" \
  ${MYSQL} -u ${PROXYSQL_ADMIN_USER} \
           -h ${PROXYSQL_ADMIN_HOST} \
           -P ${PROXYSQL_ADMIN_PORT} \
           --protocol=tcp \
           -e "$1"
}


csv_to_lines() {
  echo "$1" | tr ',' '\n' | sed '/^$/d'
}

echo "[1/6] Checking ProxySQL admin connectivity..."
run_admin_sql "SELECT 1;" >/dev/null

############################################
# Register backend servers
############################################
echo "[2/6] Registering primary + replicas..."

SQL="USE main;"

# Only update PRIMARY_HOST if this node is master; for replica, skip primary registration
if [[ "${ROLE}" == "MASTER" ]]; then
  # Make setup idempotent: reset the writer/reader hostgroups completely.
  SQL+="
DELETE FROM mysql_servers WHERE hostgroup_id IN (${WRITER_HG}, ${READER_HG});
"

  # Primary (writer)
  SQL+="
INSERT INTO mysql_servers(hostgroup_id, hostname, port, weight, max_connections)
VALUES (${WRITER_HG}, '${PRIMARY_HOST}', ${MYSQL_PORT}, 1, 1000);
"
else
  # On replica: warn and skip changing PRIMARY_HOST
  echo "[info] Skipping registration of PRIMARY_HOST because this node is a replica."
fi

# Replicas (readers)
for r in $(csv_to_lines "${REPLICA_HOSTS}"); do
  SQL+="
INSERT INTO mysql_servers(hostgroup_id, hostname, port, weight, max_connections)
VALUES (${READER_HG}, '${r}', ${MYSQL_PORT}, 1, 1000);
"
done

SQL+="
LOAD MYSQL SERVERS TO RUNTIME;
SAVE MYSQL SERVERS TO DISK;
"

run_admin_sql "${SQL}"

############################################
# Register frontend user
############################################
echo "[3/6] Registering frontend user..."

run_admin_sql "
USE main;

DELETE FROM mysql_users WHERE username='${FRONTEND_USER}';

INSERT INTO mysql_users(username, password, default_hostgroup, active, frontend, backend)
VALUES ('${FRONTEND_USER}', '${FRONTEND_PASS}', ${WRITER_HG}, 1, 1, 1);

LOAD MYSQL USERS TO RUNTIME;
SAVE MYSQL USERS TO DISK;
"

############################################
# Add query rules (SELECT → replicas)
############################################
echo '[4/6] Adding query rules...'

run_admin_sql "
USE main;

DELETE FROM mysql_query_rules WHERE rule_id IN (90,100);

-- Send SELECT ... FOR UPDATE to writer FIRST (more specific rule, higher priority)
INSERT INTO mysql_query_rules
(rule_id, active, apply, match_pattern, destination_hostgroup)
VALUES
(90, 1, 1, '^SELECT.*FOR UPDATE', ${WRITER_HG});

-- Generic SELECT goes to reader
INSERT INTO mysql_query_rules
(rule_id, active, apply, match_pattern, destination_hostgroup)
VALUES
(100, 1, 1, '^SELECT', ${READER_HG});

LOAD MYSQL QUERY RULES TO RUNTIME;
SAVE MYSQL QUERY RULES TO DISK;
"

############################################
# Verify runtime state
############################################
echo "[5/6] Verifying runtime state..."

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
echo "[6/6] DONE."

cat <<EOF

✅ ProxySQL fully configured.

Writer  (HG ${WRITER_HG}): ${PRIMARY_HOST}:${MYSQL_PORT}
Readers (HG ${READER_HG}): ${REPLICA_HOSTS}

Test from ANY node:
  MYSQL_PWD=${FRONTEND_PASS} ${LINEAIRDB_MYSQL_CLI} \\
    -u ${FRONTEND_USER} \\
    -h ${PRIMARY_HOST} -P 6033 \\
    -e "SELECT @@hostname, @@server_id, @@read_only;"

EOF