#!/usr/bin/env python3
"""
LineairDB Storage Engine Replication Manager

This module provides Python API for LineairDB Group Replication cluster management.
For most use cases, the shell scripts in scripts/ are recommended.
"""

import subprocess
import json
from pathlib import Path
from typing import Dict, Any, Optional


class LineairDBReplicationManager:
    """
    Python API wrapper for LineairDB Group Replication cluster.
    
    This class provides programmatic access to the cluster management
    functionality. For command-line usage, use the scripts in scripts/.
    
    Example:
        >>> manager = LineairDBReplicationManager()
        >>> status = manager.get_status()
        >>> print(status)
    """
    
    def __init__(self, config_dir: Optional[Path] = None):
        """
        Initialize the replication manager.
        
        Args:
            config_dir: Directory for configuration files (default: repl/config)
        """
        self.config_dir = config_dir or (Path(__file__).parent / "config")
        self.scripts_dir = Path(__file__).parent / "scripts"
    
    def _run_script(self, script_name: str, *args) -> subprocess.CompletedProcess:
        """Run a shell script and return the result."""
        script_path = self.scripts_dir / script_name
        cmd = [str(script_path)] + list(args)
        return subprocess.run(cmd, capture_output=True, text=True)
    
    def init(self, num_secondaries: int = 2) -> bool:
        """Initialize cluster with specified number of secondary nodes."""
        result = self._run_script("init_cluster.sh", str(num_secondaries))
        return result.returncode == 0
    
    def start(self) -> bool:
        """Start the cluster."""
        result = self._run_script("start_cluster.sh")
        return result.returncode == 0
    
    def stop(self) -> bool:
        """Stop the cluster."""
        result = self._run_script("stop_cluster.sh")
        return result.returncode == 0
    
    def get_status(self) -> str:
        """Get cluster status."""
        result = self._run_script("status.sh")
        return result.stdout
    
    def install_plugin(self) -> bool:
        """Install LineairDB plugin on all nodes."""
        result = self._run_script("install_plugin.sh")
        return result.returncode == 0
    
    def run_benchmark(self, tool: str = "benchbase", benchmark: str = "ycsb") -> bool:
        """Run benchmark with LineairDB."""
        result = self._run_script("run_benchmark.sh", tool, benchmark)
        return result.returncode == 0
    
    def cleanup(self, remove_all: bool = False) -> bool:
        """Clean up cluster resources."""
        args = ["--all"] if remove_all else []
        result = self._run_script("cleanup.sh", *args)
        return result.returncode == 0
    
    def get_config(self) -> Dict[str, Any]:
        """Load and return cluster configuration."""
        config_file = self.config_dir / "cluster_config.json"
        if config_file.exists():
            with open(config_file) as f:
                return json.load(f)
        return {}


def main():
    """Command-line interface for the replication manager."""
    import argparse
    
    parser = argparse.ArgumentParser(description="LineairDB Replication Manager")
    parser.add_argument("action", choices=["init", "start", "stop", "status", "install", "benchmark", "cleanup"],
                        help="Action to perform")
    parser.add_argument("--secondaries", "-n", type=int, default=2,
                        help="Number of secondary nodes (for init)")
    parser.add_argument("--all", action="store_true",
                        help="Remove all data (for cleanup)")
    
    args = parser.parse_args()
    manager = LineairDBReplicationManager()
    
    if args.action == "init":
        manager.init(args.secondaries)
    elif args.action == "start":
        manager.start()
    elif args.action == "stop":
        manager.stop()
    elif args.action == "status":
        print(manager.get_status())
    elif args.action == "install":
        manager.install_plugin()
    elif args.action == "benchmark":
        manager.run_benchmark()
    elif args.action == "cleanup":
        manager.cleanup(args.all)


if __name__ == "__main__":
    main()
