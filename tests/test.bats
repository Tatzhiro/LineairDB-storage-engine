#!/usr/bin/env bats

exec_sql() {
    cd $BATS_TEST_DIRNAME
    ../build/bin/mysql -uroot <$1
}

setup() {
    # TODO: initialize MySQL data files and restart mysqld daemon
    # TODO: mysql may use caching and thus all testcases need to execute twice or more

    # drop and create database
    cd $BATS_TEST_DIRNAME
    rm -rf ../build/data
    ../build/bin/mysqld --defaults-file=my.cnf --initialize-insecure
    ../build/bin/mysqld --defaults-file=my.cnf --daemonize
    exec_sql reset.sql
    # insert initial data with PK "alice" and "bob"
    exec_sql insert.sql
}

teardown() {
    if [[ ! -z "$DEBUG" ]]; then
        echo "    DEBUG OUTPUT:" >&3
        echo -e "$output" >&3
        echo "    END DEBUG OUTPUT" >&3
    fi
    pkill mysqld
}

@test "SELECT rows" {
    exec_sql select.sql
}

@test "SELECT nullable column" {
    exec_sql select_null_column.sql
}

@test "SELECT with WHERE clause" {
    exec_sql where.sql
}

@test "UPDATE rows" {
    exec_sql update.sql
}

@test "Type INT" {
    exec_sql intpk.sql
    exec_sql select.sql
    exec_sql where.sql
    exec_sql update.sql
}
