#!/usr/bin/env bats

exec_sql() {
    cd $BATS_TEST_DIRNAME
    ../build/bin/mysql -uroot <$1
}

setup() {
    # drop and create database
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
}

@test "SELECT rows" {
    exec_sql select.sql
}

@test "SELECT nullable column" {
    exec_sql select_null_column.sql
}

@test "SELECT with WHERE clause" {
    skip "WHERE clause is not implemented yet. WANTFIX!"
    exec_sql where.sql
}
