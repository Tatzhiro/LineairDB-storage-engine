#!/usr/bin/env bash
############################################
# ProxySQL Binlog Reader Installation Script
#
# This script downloads and installs the ProxySQL Binlog Reader.
# The binlog reader is required for GTID causal reads functionality.
# Run this on each MySQL server (primary and replicas).
############################################

set -euo pipefail

# Re-run as root if needed
if [[ "${EUID}" -ne 0 ]]; then
    exec sudo -E bash "$0" "$@"
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROXYSQL_DIR="$(dirname "${SCRIPT_DIR}")"

# Source configuration
if [[ -f "${PROXYSQL_DIR}/config" ]]; then
    source "${PROXYSQL_DIR}/config"
fi

# Binlog reader version (using latest stable as of 2024)
BINLOG_READER_VERSION="${BINLOG_READER_VERSION:-2.1-5-g7f50bd0}"

echo "=============================================="
echo "ProxySQL Binlog Reader Installation"
echo "=============================================="
echo ""

############################################
# Check if already installed
############################################
if command -v proxysql_binlog_reader >/dev/null 2>&1; then
    echo "ProxySQL Binlog Reader is already installed."
    INSTALLED_PATH=$(which proxysql_binlog_reader)
    echo "    Location: ${INSTALLED_PATH}"
    echo ""
    read -p "Do you want to reinstall? [y/N]: " -r REPLY
    if [[ ! "${REPLY}" =~ ^[Yy]$ ]]; then
        echo "Installation cancelled."
        exit 0
    fi
fi

############################################
# Detect OS and architecture
############################################
echo "[1/4] Detecting system..."

if [[ -f /etc/os-release ]]; then
    . /etc/os-release
    OS_NAME="${ID}"
    OS_VERSION="${VERSION_ID}"
else
    echo "ERROR: Cannot detect OS. /etc/os-release not found."
    exit 1
fi

ARCH=$(uname -m)
case "${ARCH}" in
    x86_64)
        ARCH_NAME="amd64"
        ;;
    aarch64)
        ARCH_NAME="arm64"
        ;;
    *)
        echo "ERROR: Unsupported architecture: ${ARCH}"
        exit 1
        ;;
esac

echo "    OS: ${OS_NAME} ${OS_VERSION}"
echo "    Architecture: ${ARCH} (${ARCH_NAME})"

############################################
# Download Binlog Reader
############################################
echo ""
echo "[2/4] Downloading ProxySQL Binlog Reader..."

TMP_DIR=$(mktemp -d)
cd "${TMP_DIR}"

# Binlog reader package URL
# Note: The binlog reader is built for Ubuntu 20.04 but works on newer versions
PKG_NAME="proxysql-mysqlbinlog_${BINLOG_READER_VERSION}-ubuntu20_${ARCH_NAME}.deb"
PKG_URL="https://github.com/sysown/proxysql_mysqlbinlog/releases/download/v2.1/${PKG_NAME}"

echo "    Downloading from: ${PKG_URL}"

if ! wget -q --show-progress "${PKG_URL}" -O "${PKG_NAME}" 2>/dev/null; then
    echo ""
    echo "ERROR: Failed to download Binlog Reader package."
    echo ""
    echo "Manual download:"
    echo "  1. Visit: https://github.com/sysown/proxysql_mysqlbinlog/releases"
    echo "  2. Download the appropriate .deb package for your system"
    echo "  3. Install: sudo dpkg -i <package>.deb"
    rm -rf "${TMP_DIR}"
    exit 1
fi

############################################
# Install Binlog Reader
############################################
echo ""
echo "[3/4] Installing Binlog Reader..."

dpkg -i "${PKG_NAME}" || apt-get install -f -y

############################################
# Create systemd service (optional)
############################################
echo ""
echo "[4/4] Creating systemd service..."

cat > /etc/systemd/system/proxysql-binlog-reader.service <<EOF
[Unit]
Description=ProxySQL Binlog Reader
After=mysql.service mysqld.service

[Service]
Type=simple
User=root
ExecStart=/usr/bin/proxysql_binlog_reader -h 127.0.0.1 -u ${MYSQL_REPL_USER:-repl_user} -p ${MYSQL_REPL_PASS:-repl_pass} -P 3306 -l ${BINLOG_READER_PORT:-6020}
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload

echo ""
echo "✅ ProxySQL Binlog Reader installed successfully!"
echo ""
echo "Location: $(which proxysql_binlog_reader)"
echo "Listen port: ${BINLOG_READER_PORT:-6020}"
echo ""
echo "To start manually:"
echo "  proxysql_binlog_reader -h 127.0.0.1 -u ${MYSQL_REPL_USER:-repl_user} -p ${MYSQL_REPL_PASS:-repl_pass} -P 3306 -l ${BINLOG_READER_PORT:-6020} &"
echo ""
echo "To start as a service:"
echo "  sudo systemctl enable proxysql-binlog-reader"
echo "  sudo systemctl start proxysql-binlog-reader"
echo ""
echo "Prerequisites for binlog reader to work:"
echo "  1. Create MySQL replication user on primary:"
echo "     CREATE USER IF NOT EXISTS '${MYSQL_REPL_USER:-repl_user}'@'localhost' IDENTIFIED BY '${MYSQL_REPL_PASS:-repl_pass}';"
echo "     GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO '${MYSQL_REPL_USER:-repl_user}'@'localhost';"
echo ""
echo "  2. Ensure GTID is enabled in my.cnf:"
echo "     gtid_mode=ON"
echo "     enforce_gtid_consistency=ON"

# Cleanup
rm -rf "${TMP_DIR}"
