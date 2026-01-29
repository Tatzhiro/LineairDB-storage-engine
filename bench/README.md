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

## Notes (TPCC / LineairDB)

- **Important**: When running TPC-C on LineairDB, you may need a small BenchBase tweak to retry commit-time deadlocks (example):
  - MySQL may surface storage-engine deadlocks as `ERROR 1180 (HY000)` with a message like `Got error 149 ... during COMMIT`.
  - Example fix: treat **(sqlState=`HY000`, errorCode=`1180`, message contains `Got error 149`)** as retryable in BenchBase's retry logic, then rebuild `benchbase.jar`.
- **Loader threads**: Fix the TPC-C loader to a single thread to avoid loader-time concurrency issues:
  - Ensure `bench/config/tpcc.xml` contains `<loaderThreads>1</loaderThreads>`.
- **Rebuild BenchBase (mysql profile)** (after applying your patch):

```
cd third_party/benchbase && ./mvnw -DskipTests -P mysql clean package && unzip -o target/benchbase-mysql.zip
```

## Notes (YCSB / LineairDB)

- **Important**: When running YCSB on LineairDB, you may see transient deadlocks during worker initialization:
  - BenchBase's YCSB module queries the initial key range with `SELECT MAX(ycsb_key) FROM usertable`.
  - On LineairDB this may transiently surface as `Deadlock found when trying to get lock; try restarting transaction` (errorCode=1213 / sqlState=40001).
  - Without a retry, BenchBase can end up creating **0 workers** and still run the benchmark, producing **0 requests/sec**.
- **Required fix**: Retry the above initialization query (and fail fast if 0 workers were created), then rebuild `benchbase.jar`:
  - Patch locations:
    - `third_party/benchbase/src/main/java/com/oltpbenchmark/benchmarks/ycsb/YCSBBenchmark.java`
    - `third_party/benchbase/src/main/java/com/oltpbenchmark/DBWorkload.java`
  - Rebuild:

```
cd third_party/benchbase && ./mvnw -DskipTests -P mysql clean package && unzip -o target/benchbase-mysql.zip
```

## Plotting

```bash
pip3 install -r bench/requirements.txt
python3 bench/bin/plot.py
```