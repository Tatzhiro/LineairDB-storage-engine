#! /bin/bash

cd $(dirname $0)

ln -s $(pwd) third_party/mysql-server/storage/lineairdb

mkdir -p build/data
cd build
cmake ../third_party/mysql-server -DCMAKE_BUILD_TYPE=Debug -DDOWNLOAD_BOOST=1 -DWITH_BOOST=../boost -G Ninja
ninja
