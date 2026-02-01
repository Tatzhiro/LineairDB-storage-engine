#!/usr/bin/env bash
set -euo pipefail

############################################
# Config
############################################
PROXYSQL_ADMIN_HOST="127.0.0.1"
PROXYSQL_ADMIN_PORT=6032
PROXYSQL_ADMIN_USER="admin"
PROXYSQL_ADMIN_PASS="admin"

MYSQL="mysql"

############################################
# Helpers
############################################
run_admin_sql() {
  MYSQL_PWD="${PROXYSQL_ADMIN_PASS}" \
  ${MYSQL} -u ${PROXYSQL_ADMIN_USER} \
           -h ${PROXYSQL_ADMIN_HOST} \
           -P ${PROXYSQL_ADMIN_PORT} \
           --protocol=tcp \
           -N -e "$1"
}

section() {
  echo
  echo "=============================="
  echo "$1"
  echo "=============================="
}

############################################
# 1) systemd status
############################################
section "ProxySQL service (systemd)"

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
section "Listening ports"

if ss -lnt 2>/dev/null | grep -qE ':(6032)\b'; then
  echo "✅ Admin port 6032: LISTENING"
else
  echo "❌ Admin port 6032: NOT LISTENING"
fi

if ss -lnt 2>/dev/null | grep -qE ':(6033)\b'; then
  echo "✅ Client port 6033: LISTENING"
else
  echo "⚠️ Client port 6033: NOT LISTENING"
fi

############################################
# 3) Admin connectivity
############################################
section "ProxySQL admin connectivity"

if run_admin_sql "SELECT 1;" >/dev/null 2>&1; then
  echo "✅ Can connect to ProxySQL admin (6032)"
else
  echo "❌ Cannot connect to ProxySQL admin (6032)"
  exit 1
fi

############################################
# 4) Backend MySQL servers status
############################################
section "Backend MySQL servers (runtime_mysql_servers)"

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
section "Connection pool stats (stats_mysql_connection_pool)"

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
# 6) Summary
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

echo
echo "✔ ProxySQL status check complete."