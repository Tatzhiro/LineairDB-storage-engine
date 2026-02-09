#!/usr/bin/env python3
"""
Shared configuration for ProxySQL tests.

This module reads configuration from the proxysql/config file
and provides a consistent interface for all test scripts.
"""

import os
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


def _parse_bash_config(config_path: str) -> Dict[str, str]:
    """
    Parse bash-style configuration file (KEY=VALUE format).
    Handles simple variable assignments and ignores functions.
    """
    config = {}
    
    if not os.path.exists(config_path):
        return config
    
    with open(config_path, 'r') as f:
        content = f.read()
    
    # Simple regex to match KEY=VALUE or KEY="VALUE" patterns
    # Skip function definitions and complex bash constructs
    pattern = r'^([A-Z_][A-Z0-9_]*)=["\']?([^"\'#\n]*)["\']?'
    
    for line in content.split('\n'):
        line = line.strip()
        if not line or line.startswith('#') or line.startswith('function '):
            continue
        
        match = re.match(pattern, line)
        if match:
            key, value = match.groups()
            # Handle variable references like ${VAR} or $VAR
            value = value.strip()
            config[key] = value
    
    return config


def get_config_path() -> str:
    """Get the path to the proxysql config file."""
    # Try relative to this file first
    this_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(os.path.dirname(this_dir), 'config')
    
    if os.path.exists(config_path):
        return config_path
    
    # Try from workspace root
    workspace_root = os.environ.get('LINEAIRDB_BASE', '/home/ubuntu/LineairDB-storage-engine')
    config_path = os.path.join(workspace_root, 'proxysql', 'config')
    
    return config_path


# Load configuration once at module import
_CONFIG = _parse_bash_config(get_config_path())


@dataclass
class TestConfig:
    """Test configuration with defaults from proxysql/config file."""
    
    # ProxySQL client (port 6033)
    proxysql_host: str = "127.0.0.1"
    proxysql_port: int = 6033
    proxysql_user: str = "proxysql_user"
    proxysql_pass: str = "proxysql_pass"
    
    # ProxySQL admin (port 6032)
    admin_host: str = "127.0.0.1"
    admin_port: int = 6032
    admin_user: str = "admin"
    admin_pass: str = "admin"
    
    # Backend nodes
    primary_host: str = "133.125.85.242"
    replica_hosts: List[str] = field(default_factory=lambda: ["133.242.17.72", "153.120.20.111"])
    mysql_port: int = 3306
    
    # Hostgroups
    writer_hostgroup: int = 0
    reader_hostgroup: int = 1
    
    # Storage engine (always use LineairDB)
    storage_engine: str = "LINEAIRDB"
    
    def __post_init__(self):
        """Override defaults with values from config file."""
        if _CONFIG.get('PROXYSQL_ADMIN_HOST'):
            self.admin_host = _CONFIG['PROXYSQL_ADMIN_HOST']
        if _CONFIG.get('PROXYSQL_ADMIN_PORT'):
            self.admin_port = int(_CONFIG['PROXYSQL_ADMIN_PORT'])
        if _CONFIG.get('PROXYSQL_ADMIN_USER'):
            self.admin_user = _CONFIG['PROXYSQL_ADMIN_USER']
        if _CONFIG.get('PROXYSQL_ADMIN_PASS'):
            self.admin_pass = _CONFIG['PROXYSQL_ADMIN_PASS']
        if _CONFIG.get('PROXYSQL_CLIENT_PORT'):
            self.proxysql_port = int(_CONFIG['PROXYSQL_CLIENT_PORT'])
        if _CONFIG.get('FRONTEND_USER'):
            self.proxysql_user = _CONFIG['FRONTEND_USER']
        if _CONFIG.get('FRONTEND_PASS'):
            self.proxysql_pass = _CONFIG['FRONTEND_PASS']
        if _CONFIG.get('PRIMARY_HOST'):
            self.primary_host = _CONFIG['PRIMARY_HOST']
        if _CONFIG.get('PRIMARY_PORT'):
            self.mysql_port = int(_CONFIG['PRIMARY_PORT'])
        if _CONFIG.get('WRITER_HG'):
            self.writer_hostgroup = int(_CONFIG['WRITER_HG'])
        if _CONFIG.get('READER_HG'):
            self.reader_hostgroup = int(_CONFIG['READER_HG'])
        
        # Parse replica hosts from config
        if _CONFIG.get('REPLICA1_HOST') and _CONFIG.get('REPLICA2_HOST'):
            self.replica_hosts = [
                _CONFIG['REPLICA1_HOST'],
                _CONFIG['REPLICA2_HOST'],
            ]
    
    def proxysql_cfg(self) -> Dict[str, Any]:
        """Return connection config for ProxySQL client port."""
        return {
            "host": self.proxysql_host,
            "port": self.proxysql_port,
            "user": self.proxysql_user,
            "password": self.proxysql_pass,
        }
    
    def admin_cfg(self) -> Dict[str, Any]:
        """Return connection config for ProxySQL admin port."""
        return {
            "host": self.admin_host,
            "port": self.admin_port,
            "user": self.admin_user,
            "password": self.admin_pass,
        }
    
    def primary_cfg(self) -> Dict[str, Any]:
        """Return connection config for direct primary connection."""
        return {
            "host": self.primary_host,
            "port": self.mysql_port,
            "user": self.proxysql_user,
            "password": self.proxysql_pass,
        }
    
    def replica_cfg(self, host: str) -> Dict[str, Any]:
        """Return connection config for direct replica connection."""
        return {
            "host": host,
            "port": self.mysql_port,
            "user": self.proxysql_user,
            "password": self.proxysql_pass,
        }


def check_lineairdb_available(cfg: Dict[str, Any]) -> bool:
    """
    Check if LineairDB storage engine is available.
    Returns True if LineairDB is available, False otherwise.
    """
    try:
        import mysql.connector
        
        conn = mysql.connector.connect(
            host=cfg.get('host', '127.0.0.1'),
            port=cfg.get('port', 6033),
            user=cfg.get('user', 'proxysql_user'),
            password=cfg.get('password', 'proxysql_pass'),
            connection_timeout=5,
            use_pure=True,
            autocommit=True,
            ssl_disabled=True,
        )
        
        cursor = conn.cursor()
        cursor.execute("SHOW ENGINES")
        engines = cursor.fetchall()
        
        for engine in engines:
            if engine[0].upper() == 'LINEAIRDB':
                cursor.close()
                conn.close()
                return True
        
        cursor.close()
        conn.close()
        return False
        
    except Exception as e:
        print(f"Warning: Could not check LineairDB availability: {e}")
        return False


def verify_table_engine(cfg: Dict[str, Any], db_name: str, table_name: str, use_transaction: bool = True) -> str:
    """
    Verify the storage engine of a table.
    Returns the engine name or 'UNKNOWN' if not found.
    
    Args:
        cfg: MySQL connection config
        db_name: Database name
        table_name: Table name
        use_transaction: If True, uses a transaction to ensure query goes to writer
    """
    try:
        import mysql.connector
        
        conn = mysql.connector.connect(
            host=cfg.get('host', '127.0.0.1'),
            port=cfg.get('port', 6033),
            user=cfg.get('user', 'proxysql_user'),
            password=cfg.get('password', 'proxysql_pass'),
            connection_timeout=5,
            use_pure=True,
            autocommit=not use_transaction,  # Disable autocommit to use transaction
            ssl_disabled=True,
        )
        
        cursor = conn.cursor()
        
        # Use transaction to force query to go to writer (where table was just created)
        # This avoids replication lag issues when SELECT is routed to replicas
        if use_transaction:
            cursor.execute("START TRANSACTION")
        
        cursor.execute(
            "SELECT ENGINE FROM information_schema.TABLES "
            "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
            (db_name, table_name)
        )
        row = cursor.fetchone()
        
        if use_transaction:
            cursor.execute("COMMIT")
        
        cursor.close()
        conn.close()
        
        return row[0] if row else 'UNKNOWN'
        
    except Exception as e:
        print(f"Warning: Could not verify table engine: {e}")
        return 'UNKNOWN'
