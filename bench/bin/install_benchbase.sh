#! /bin/bash

set -xue

cd "$(dirname "$0")"
base_path=$(pwd)/../..
cd $base_path/third_party/benchbase

zip_file=target/benchbase-mysql.zip
if [ ! -f "$zip_file" ]; then
  message " Clean & Build the mvn package..."

  ### Install JDK-17
  JAVA_HOME=$(/usr/libexec/java_home -v 17)
  if  [[ ${JAVA_HOME} != *"17"* ]]; then
    message '    JDK-17 is not found. Start installing...'
    if [ "$(uname)" == "Darwin" ]; then
      brew install openjdk@17
      export PATH=$(brew --prefix openjdk@17)/bin:$PATH
      sudo ln -sfn $(brew --prefix openjdk@17)/libexec/openjdk.jdk /Library/Java/JavaVirtualMachines/openjdk-17.jdk
    else
      sudo apt install openjdk-17-jdk
    fi
    export JAVA_HOME=$(/usr/libexec/java_home -v 17)
  fi

  ### Build JAR
  ./mvnw clean package -P mysql -e -DskipTests
  unzip $zip_file
  cd -
fi
message " benchbase is installed."