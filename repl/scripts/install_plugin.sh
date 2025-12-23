#!/bin/bash
# Install LineairDB Storage Engine Plugin on all cluster nodes
#
# Usage:
#   ./install_plugin.sh [--fence|--lineairdb] [--debug|--release] [--rebuild] [--build-only]
#
# Options:
#   --fence       Build and install with FENCE=true (synchronous commits)
#   --lineairdb   Build and install with FENCE=false (async commits, faster) [default]
#   --debug       Use debug build from build/ directory
#   --release     Use release build from release/ directory (default)
#   --rebuild     Force rebuild even if plugin exists
#   --build-only  Only build the plugin, don't install or restart MySQL
#   --path        Install from custom path (skips build)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPL_DIR="$(dirname "$SCRIPT_DIR")"
ROOT_DIR="$(dirname "$REPL_DIR")"
CLUSTER_DIR="$ROOT_DIR/third_party/mysql-cluster"

# Configuration
MYSQL_USER="root"
MYSQL_PASSWORD="kamo"
PRIMARY_HOST="127.0.0.1"
PRIMARY_PORT="3306"

# Defaults
BUILD_TYPE="release"
FENCE_MODE="lineairdb"  # lineairdb = FENCE off, fence = FENCE on
PLUGIN_PATH=""
FORCE_REBUILD=false
BUILD_ONLY=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --fence)
            FENCE_MODE="fence"
            shift
            ;;
        --lineairdb)
            FENCE_MODE="lineairdb"
            shift
            ;;
        --debug)
            BUILD_TYPE="debug"
            shift
            ;;
        --release)
            BUILD_TYPE="release"
            shift
            ;;
        --rebuild)
            FORCE_REBUILD=true
            shift
            ;;
        --build-only)
            BUILD_ONLY=true
            shift
            ;;
        --path)
            PLUGIN_PATH="$2"
            shift 2
            ;;
        -h|--help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "FENCE Mode Options:"
            echo "  --lineairdb   Build with FENCE=false (async commits, faster) [default]"
            echo "  --fence       Build with FENCE=true (synchronous commits)"
            echo ""
            echo "Build Options:"
            echo "  --debug       Use debug build directory (build/)"
            echo "  --release     Use release build directory (release/) [default]"
            echo "  --rebuild     Force rebuild even if plugin exists"
            echo "  --build-only  Only build the plugin, don't install or restart MySQL"
            echo "  --path PATH   Install from custom path (skips build)"
            echo ""
            echo "Examples:"
            echo "  $0 --lineairdb --release   # Fast LineairDB (FENCE=false, Release)"
            echo "  $0 --fence --release       # Safe LineairDB (FENCE=true, Release)"
            echo "  $0 --fence --debug         # Debug with FENCE=true"
            echo "  $0 --fence --build-only    # Build only, don't install"
            exit 0
            ;;
        *)
            shift
            ;;
    esac
done

echo "=========================================="
echo "Installing LineairDB Storage Engine Plugin"
echo "=========================================="
echo ""
echo "FENCE mode:  $FENCE_MODE ($([ "$FENCE_MODE" = "fence" ] && echo "FENCE=true, sync" || echo "FENCE=false, async"))"
echo "Build type:  $BUILD_TYPE"
echo ""

# Determine FENCE value
if [ "$FENCE_MODE" = "fence" ]; then
    FENCE_VALUE="true"
else
    FENCE_VALUE="false"
fi

# Determine build directory
if [ "$BUILD_TYPE" = "release" ]; then
    BUILD_DIR="$ROOT_DIR/release"
else
    BUILD_DIR="$ROOT_DIR/build"
fi

# If no custom path, we need to build/find the plugin
if [ -z "$PLUGIN_PATH" ]; then
    PLUGIN_PATH="$BUILD_DIR/library_output_directory/plugin/ha_lineairdb_storage_engine.so"
    
    # Check current FENCE setting in source
    CURRENT_FENCE=$(grep -oP '#define FENCE \K(true|false)' "$ROOT_DIR/ha_lineairdb.cc" 2>/dev/null || echo "unknown")
    
    echo "Current FENCE in source: $CURRENT_FENCE"
    echo "Required FENCE:          $FENCE_VALUE"
    echo ""
    
    # Determine if we need to rebuild
    NEED_REBUILD=false
    
    if [ "$FORCE_REBUILD" = true ]; then
        echo "Force rebuild requested."
        NEED_REBUILD=true
    elif [ ! -f "$PLUGIN_PATH" ]; then
        echo "Plugin not found at $PLUGIN_PATH"
        NEED_REBUILD=true
    elif [ "$CURRENT_FENCE" != "$FENCE_VALUE" ]; then
        echo "FENCE value mismatch - rebuild required."
        NEED_REBUILD=true
    fi
    
    if [ "$NEED_REBUILD" = true ]; then
        echo ""
        echo "=== Building Plugin with FENCE=$FENCE_VALUE ==="
        echo ""
        
        # Update FENCE in source
        echo "Setting FENCE=$FENCE_VALUE in ha_lineairdb.cc..."
        sed -i "s/#define FENCE.*/#define FENCE $FENCE_VALUE/" "$ROOT_DIR/ha_lineairdb.cc"
        
        # Verify the change
        NEW_FENCE=$(grep -oP '#define FENCE \K(true|false)' "$ROOT_DIR/ha_lineairdb.cc")
        echo "FENCE is now: $NEW_FENCE"
        
        # Check if build directory exists and has CMakeCache
        if [ ! -f "$BUILD_DIR/CMakeCache.txt" ]; then
            echo ""
            echo "Error: Build directory not configured at $BUILD_DIR"
            echo "Please run cmake first:"
            if [ "$BUILD_TYPE" = "release" ]; then
                echo "  mkdir -p release && cd release"
                echo "  cmake ../third_party/mysql-server -DCMAKE_BUILD_TYPE=Release -G Ninja ..."
            else
                echo "  mkdir -p build && cd build"
                echo "  cmake ../third_party/mysql-server -DCMAKE_BUILD_TYPE=Debug -G Ninja ..."
            fi
            exit 1
        fi
        
        # Build the plugin
        echo ""
        echo "Building plugin..."
        cd "$BUILD_DIR"
        ninja lineairdb_storage_engine -j$(nproc)
        cd "$ROOT_DIR"
        
        echo ""
        echo "✓ Plugin built successfully"
        
        # If build-only mode, exit here without installing/restarting
        if [ "$BUILD_ONLY" = true ]; then
            echo ""
            echo "=========================================="
            echo "Build Only Mode - Plugin ready at: $PLUGIN_PATH"
            echo "=========================================="
            exit 0
        fi
        
        # MySQL needs a restart to load new plugin binary
        echo ""
        echo "Restarting MySQL to load new plugin binary..."
        sudo systemctl restart mysql
        
        # Wait for MySQL to be fully ready
        for i in {1..30}; do
            if mysql -h $PRIMARY_HOST -P $PRIMARY_PORT -u$MYSQL_USER -p$MYSQL_PASSWORD -e "SELECT 1" 2>/dev/null; then
                break
            fi
            sleep 1
        done
        sleep 2
        echo "✓ MySQL restarted"
    else
        echo "Plugin already exists with correct FENCE setting."
        
        # If build-only mode, exit here
        if [ "$BUILD_ONLY" = true ]; then
            echo ""
            echo "=========================================="
            echo "Build Only Mode - Plugin ready at: $PLUGIN_PATH"
            echo "=========================================="
            exit 0
        fi
    fi
fi

# If build-only mode and using custom path, just exit
if [ "$BUILD_ONLY" = true ]; then
    echo ""
    echo "=========================================="
    echo "Build Only Mode - Plugin ready"
    echo "=========================================="
    exit 0
fi

echo ""
echo "Plugin path: $PLUGIN_PATH"

if [ ! -f "$PLUGIN_PATH" ]; then
    echo ""
    echo "Error: Plugin not found at $PLUGIN_PATH"
    exit 1
fi

# Get number of secondaries from config
NUM_SECONDARIES=0
if [ -f "$REPL_DIR/config/cluster_config.json" ]; then
    NUM_SECONDARIES=$(python3 -c "
import json
with open('$REPL_DIR/config/cluster_config.json') as f:
    config = json.load(f)
print(len(config.get('secondaries', [])))
" 2>/dev/null || echo "0")
fi

# ===========================================
# Uninstall existing plugin first (to reload new .so)
# ===========================================
echo ""
echo "=== Uninstalling existing plugin (if any) ==="

# Uninstall from primary (handle super_read_only)
super_ro=$(mysql -h $PRIMARY_HOST -P $PRIMARY_PORT -u$MYSQL_USER -p$MYSQL_PASSWORD -N -e "SELECT @@super_read_only;" 2>/dev/null || echo "0")
if [ "$super_ro" = "1" ]; then
    mysql -h $PRIMARY_HOST -P $PRIMARY_PORT -u$MYSQL_USER -p$MYSQL_PASSWORD -e "
    SET GLOBAL super_read_only = 0;
    UNINSTALL PLUGIN lineairdb;
    SET GLOBAL super_read_only = 1;
    " 2>/dev/null || true
else
    mysql -h $PRIMARY_HOST -P $PRIMARY_PORT -u$MYSQL_USER -p$MYSQL_PASSWORD -e "UNINSTALL PLUGIN lineairdb;" 2>/dev/null || true
fi
echo "  Primary: Uninstalled"

# Uninstall from secondaries
for i in $(seq 1 $NUM_SECONDARIES); do
    port=$((33061 + i))
    super_ro=$(mysql -h $PRIMARY_HOST -P $port -u$MYSQL_USER -p$MYSQL_PASSWORD -N -e "SELECT @@super_read_only;" 2>/dev/null || echo "0")
    if [ "$super_ro" = "1" ]; then
        mysql -h $PRIMARY_HOST -P $port -u$MYSQL_USER -p$MYSQL_PASSWORD -e "
        SET GLOBAL super_read_only = 0;
        UNINSTALL PLUGIN lineairdb;
        SET GLOBAL super_read_only = 1;
        " 2>/dev/null || true
    else
        mysql -h $PRIMARY_HOST -P $port -u$MYSQL_USER -p$MYSQL_PASSWORD -e "UNINSTALL PLUGIN lineairdb;" 2>/dev/null || true
    fi
    echo "  mysql-secondary-$i: Uninstalled"
done

# ===========================================
# Install on Primary
# ===========================================
echo ""
echo "=== Installing on Primary (local MySQL) ==="

# Copy plugin
sudo cp "$PLUGIN_PATH" /usr/lib/mysql/plugin/ha_lineairdb_storage_engine.so
sudo chmod 644 /usr/lib/mysql/plugin/ha_lineairdb_storage_engine.so

# Check if super_read_only is enabled (can happen in GR cluster)
super_ro=$(mysql -h $PRIMARY_HOST -P $PRIMARY_PORT -u$MYSQL_USER -p$MYSQL_PASSWORD -N -e "SELECT @@super_read_only;" 2>/dev/null || echo "0")

# First, ensure read_only is off on primary (can happen if cluster metadata exists)
mysql -h $PRIMARY_HOST -P $PRIMARY_PORT -u$MYSQL_USER -p$MYSQL_PASSWORD -e "SET GLOBAL super_read_only = 0; SET GLOBAL read_only = 0;" 2>/dev/null || true

# Install plugin
install_output=$(mysql -h $PRIMARY_HOST -P $PRIMARY_PORT -u$MYSQL_USER -p$MYSQL_PASSWORD -e "INSTALL PLUGIN lineairdb SONAME 'ha_lineairdb_storage_engine.so';" 2>&1) || true

# Verify
new_status=$(mysql -h $PRIMARY_HOST -P $PRIMARY_PORT -u$MYSQL_USER -p$MYSQL_PASSWORD -N -e "SELECT PLUGIN_STATUS FROM INFORMATION_SCHEMA.PLUGINS WHERE PLUGIN_NAME = 'LINEAIRDB';" 2>/dev/null || echo "FAILED")

if [ "$new_status" = "ACTIVE" ]; then
    echo "  ✓ Primary: Plugin installed and ACTIVE (FENCE=$FENCE_VALUE)"
else
    # If plugin exists but reported not ACTIVE, try to check again
    if echo "$install_output" | grep -q "already exists"; then
        new_status=$(mysql -h $PRIMARY_HOST -P $PRIMARY_PORT -u$MYSQL_USER -p$MYSQL_PASSWORD -N -e "SELECT PLUGIN_STATUS FROM INFORMATION_SCHEMA.PLUGINS WHERE PLUGIN_NAME = 'LINEAIRDB';" 2>/dev/null || echo "FAILED")
        if [ "$new_status" = "ACTIVE" ]; then
            echo "  ✓ Primary: Plugin already ACTIVE (FENCE=$FENCE_VALUE)"
        else
            echo "  ✗ Primary: Plugin installation failed (status: $new_status)"
            echo "     Error: $install_output"
        fi
    else
        echo "  ✗ Primary: Plugin installation failed (status: $new_status)"
        if [ -n "$install_output" ]; then
            echo "     Error: $install_output"
        fi
    fi
fi

# ===========================================
# Install on Docker Secondary Nodes
# ===========================================
if [ "$NUM_SECONDARIES" -gt 0 ]; then
    echo ""
    echo "=== Installing on Docker Secondary Nodes ==="
    
    for i in $(seq 1 $NUM_SECONDARIES); do
        container="mysql-secondary-$i"
        port=$((33061 + i))
        
        echo "Installing on $container..."
        
        # Check if container is running
        if ! sudo docker ps --format '{{.Names}}' | grep -q "^${container}$"; then
            echo "  ✗ $container: Container not running"
            continue
        fi
        
        # Copy plugin to container
        sudo docker cp "$PLUGIN_PATH" "$container:/usr/lib64/mysql/plugin/ha_lineairdb_storage_engine.so"
        sudo docker exec "$container" chmod 644 /usr/lib64/mysql/plugin/ha_lineairdb_storage_engine.so
        
        # Check if super_read_only is enabled (happens after joining cluster)
        super_ro=$(mysql -h $PRIMARY_HOST -P $port -u$MYSQL_USER -p$MYSQL_PASSWORD -N -e "SELECT @@super_read_only;" 2>/dev/null || echo "0")
        
        if [ "$super_ro" = "1" ]; then
            # Temporarily disable super_read_only to install plugin
            mysql -h $PRIMARY_HOST -P $port -u$MYSQL_USER -p$MYSQL_PASSWORD -e "
            SET GLOBAL super_read_only = 0;
            INSTALL PLUGIN lineairdb SONAME 'ha_lineairdb_storage_engine.so';
            SET GLOBAL super_read_only = 1;
            " 2>/dev/null || true
        else
            # Direct install
            mysql -h $PRIMARY_HOST -P $port -u$MYSQL_USER -p$MYSQL_PASSWORD -e "INSTALL PLUGIN lineairdb SONAME 'ha_lineairdb_storage_engine.so';" 2>/dev/null || true
        fi
        
        # Verify
        new_status=$(mysql -h $PRIMARY_HOST -P $port -u$MYSQL_USER -p$MYSQL_PASSWORD -N -e "SELECT PLUGIN_STATUS FROM INFORMATION_SCHEMA.PLUGINS WHERE PLUGIN_NAME = 'LINEAIRDB';" 2>/dev/null || echo "FAILED")
        
        if [ "$new_status" = "ACTIVE" ]; then
            echo "  ✓ $container: Plugin installed and ACTIVE (FENCE=$FENCE_VALUE)"
        else
            echo "  ✗ $container: Plugin installation failed (status: $new_status)"
        fi
    done
fi

echo ""
echo "=========================================="
echo "Plugin Installation Complete!"
echo "=========================================="
echo ""
echo "Mode: $FENCE_MODE (FENCE=$FENCE_VALUE)"
echo ""
