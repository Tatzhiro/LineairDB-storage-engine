#!/usr/bin/env bash
############################################
# ProxySQL Installation Script
#
# This script downloads and installs ProxySQL on Ubuntu/Debian systems.
# Run this on each node where you want ProxySQL installed.
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

# ProxySQL version to install
PROXYSQL_VERSION="${PROXYSQL_VERSION:-2.5.5}"

echo "=============================================="
echo "ProxySQL Installation"
echo "=============================================="
echo ""

############################################
# Check if already installed
############################################
if command -v proxysql >/dev/null 2>&1; then
    INSTALLED_VERSION=$(proxysql --version 2>&1 | grep -oP 'ProxySQL version \K[0-9.]+' || echo "unknown")
    echo "ProxySQL is already installed (version: ${INSTALLED_VERSION})"
    echo ""
    read -p "Do you want to reinstall/upgrade? [y/N]: " -r REPLY
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
# Download ProxySQL
############################################
echo ""
echo "[2/4] Downloading ProxySQL ${PROXYSQL_VERSION}..."

TMP_DIR=$(mktemp -d)
cd "${TMP_DIR}"

# Determine package URL based on OS
case "${OS_NAME}" in
    ubuntu)
        # Map Ubuntu version to codename
        case "${OS_VERSION}" in
            24.04) UBUNTU_CODENAME="noble" ;;
            22.04) UBUNTU_CODENAME="jammy" ;;
            20.04) UBUNTU_CODENAME="focal" ;;
            18.04) UBUNTU_CODENAME="bionic" ;;
            *)     UBUNTU_CODENAME="focal" ;;  # fallback
        esac
        PKG_NAME="proxysql_${PROXYSQL_VERSION}-ubuntu${OS_VERSION}_${ARCH_NAME}.deb"
        PKG_URL="https://github.com/sysown/proxysql/releases/download/v${PROXYSQL_VERSION}/${PKG_NAME}"
        ;;
    debian)
        PKG_NAME="proxysql_${PROXYSQL_VERSION}-debian${OS_VERSION}_${ARCH_NAME}.deb"
        PKG_URL="https://github.com/sysown/proxysql/releases/download/v${PROXYSQL_VERSION}/${PKG_NAME}"
        ;;
    *)
        echo "ERROR: Unsupported OS: ${OS_NAME}"
        echo "Manual installation required. Visit: https://proxysql.com/documentation/installing-proxysql/"
        exit 1
        ;;
esac

echo "    Downloading from: ${PKG_URL}"

if ! wget -q --show-progress "${PKG_URL}" -O "${PKG_NAME}" 2>/dev/null; then
    echo ""
    echo "ERROR: Failed to download ProxySQL package."
    echo "The package URL may have changed. Please check:"
    echo "  https://github.com/sysown/proxysql/releases"
    echo ""
    echo "Alternative installation using ProxySQL repository:"
    echo "  wget -nv -O /etc/apt/trusted.gpg.d/proxysql.gpg 'https://repo.proxysql.com/ProxySQL/proxysql-2.5.x/repo_pub_key.gpg'"
    echo "  echo 'deb https://repo.proxysql.com/ProxySQL/proxysql-2.5.x/${OS_NAME}/ ./' > /etc/apt/sources.list.d/proxysql.list"
    echo "  apt update && apt install -y proxysql"
    rm -rf "${TMP_DIR}"
    exit 1
fi

############################################
# Install ProxySQL
############################################
echo ""
echo "[3/4] Installing ProxySQL..."

dpkg -i "${PKG_NAME}" || apt-get install -f -y

############################################
# Configure and start
############################################
echo ""
echo "[4/4] Configuring ProxySQL..."

# Enable service
systemctl enable proxysql

# Start service
systemctl start proxysql

# Wait for admin port
echo "    Waiting for ProxySQL to start..."
for i in $(seq 1 30); do
    if ss -lnt 2>/dev/null | grep -qE ':6032\b'; then
        break
    fi
    sleep 0.5
done

# Verify installation
if systemctl is-active --quiet proxysql; then
    echo ""
    echo "✅ ProxySQL ${PROXYSQL_VERSION} installed successfully!"
    echo ""
    echo "Admin interface: 127.0.0.1:6032"
    echo "Client interface: 127.0.0.1:6033"
    echo ""
    echo "Next steps:"
    echo "  1. Configure ProxySQL: ${PROXYSQL_DIR}/scripts/setup_proxysql.sh"
    echo "  2. Check status: ${PROXYSQL_DIR}/scripts/status.sh"
else
    echo ""
    echo "⚠️ ProxySQL installed but service not running."
    echo "Check: systemctl status proxysql"
fi

# Cleanup
rm -rf "${TMP_DIR}"
