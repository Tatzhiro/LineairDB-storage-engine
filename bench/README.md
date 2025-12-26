# Benchmarking

We evaluate this project via [Benchbase](https://github.com/cmu-db/benchbase).

## Requirements

OS: Ubuntu (we are assuming that the path to mysqld.cnf is /etc/mysql/mysql.conf.d/mysqld.cnf)

## Executables

```
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

## Plotting

```
pip3 install -r bench/requirements.txt
python3 bench/bin/plot.py
```