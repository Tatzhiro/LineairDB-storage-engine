# Consistent Data Replication and Failover
This directory contains automation scripts to set up semi-sync replication and orchestrator in local environment for testing purpose.
Important files:
- docker-compose.yaml: two replicas and one orchestrator containers are defined here.
- setup_repl.sh: sets up relication with locally-installed mysql (e.g. check if it is running by sudo systemctl status mysql) as leader and docker container mysqls as replicas.
- setup_orch.sh: sets up orchestrator to watch over the mysql nodes.

## Semi-sync Replication Set up
1. Make sure the server-installed MySQL (checked by `sudo systemctl status mysql`) has LineairDB-storage-engine installed and running.
2. Build ha_lineairdb_storage_engine.so in MySQL 8.0.43, and copy it to this directory.
3. Run `./setup_repl.sh`.
4. (Optional) To see if replication is working correctly, run `python tests/replication.py`.

## Orchestrator Set up
1. After setting up semi-sync replication, run `./setup_orch.sh`.
2. (Optional) To see if orchestrator can correctly handle failover, run `python tests/failover.py`. Note that after the test, one of the replicas becomes the leader.
