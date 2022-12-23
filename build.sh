#! /bin/bash

cd $(dirname $0)

ln -s $(pwd) third_party/mysql-server/storage/lineairdb

mkdir -p build/data
cd build
cmake ../third_party/mysql-server -DWITH_ASAN=0 -DCMAKE_BUILD_TYPE=Debug -DDOWNLOAD_BOOST=1 -DWITH_BOOST=./boost -DWITHOUT_EXAMPLE_STORAGE_ENGINE=1 -DWITHOUT_FEDERATED_STORAGE_ENGINE=1 -DWITHOUT_ARCHIVE_STORAGE_ENGINE=1 -DWITHOUT_BLACKHOLE_STORAGE_ENGINE=0 -DWITHOUT_NDB_STORAGE_ENGINE=1 -DWITHOUT_NDBCLUSTER_STORAGE_ENGINE=1 -DWITHOUT_PARTITION_STORAGE_ENGINE=1 -DWITH_BOOST=../boost -G Ninja
# ./build.sh lineairdb_storage_engine
ninja $1 -j `nproc`