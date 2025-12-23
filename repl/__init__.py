"""
LineairDB Storage Engine Replication Module

This module provides Group Replication support for LineairDB storage engine.

Quick Start (Shell):
    cd repl/
    ./scripts/init_cluster.sh 3      # Initialize with 3 secondaries
    ./scripts/start_cluster.sh       # Start cluster
    ./scripts/status.sh              # Check status
    ./scripts/run_benchmark.sh       # Run benchmark

Python API:
    from repl import LineairDBReplicationManager
    manager = LineairDBReplicationManager()
    manager.start()
"""

__version__ = "1.0.0"

from .replication import LineairDBReplicationManager

__all__ = ["LineairDBReplicationManager"]
