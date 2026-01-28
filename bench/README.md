# Benchmarking

We evaluate this project via [Benchbase](https://github.com/cmu-db/benchbase).

## Requirements

OS: Ubuntu

## Executables
First, install benchbase:
```bash
bench/bin/install_benchbase.sh
```

Run the following to test the performance of locally built MySQL:
```bash
python3 bench/bin/binbench.py ycsb # [tpcc|ycsb]
```

If you want to use server-installed MySQL, run the following (assuming that the path to mysqld.cnf is /etc/mysql/mysql.conf.d/mysqld.cnf):
```bash
bench/bin/benchbase tpcc # [tpcc|ycsb]
```


## Plotting

```bash
pip3 install -r bench/requirements.txt
python3 bench/bin/plot.py
```