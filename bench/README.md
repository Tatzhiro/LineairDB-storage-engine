# Benchmarking

We evaluate this project via [Benchbase](https://github.com/cmu-db/benchbase).

## Requirements

OS: Ubuntu (we are assuming that the path to mysqld.cnf is /etc/mysql/mysql.conf.d/mysqld.cnf)

## Executables

```
bench/bin/benchbase tpcc # [tpcc|ycsb]
```

## Plotting

```
python3 bench/bin/plot.py
```