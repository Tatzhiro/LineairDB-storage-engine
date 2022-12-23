#!/usr/bin/env bats

setup() {
    cd $BATS_TEST_DIRNAME
    ../build/bin/mysqld --defaults-file=bats.cnf --daemonize
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
    python3 pytest/select.py
}

@test "SELECT nullable column" {
    python3 pytest/select_null.py
}

@test "SELECT with WHERE clause" {
    python3 pytest/where.py
}

@test "UPDATE rows" {
    python3 pytest/update.py
}