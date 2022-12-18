#!/usr/bin/env bats

setup() {
    cd $BATS_TEST_DIRNAME
}

teardown() {
    if [[ ! -z "$DEBUG" ]]; then
        echo "    DEBUG OUTPUT:" >&3
        echo -e "$output" >&3
        echo "    END DEBUG OUTPUT" >&3
    fi
    sudo service mysql restart
}

@test "SELECT rows" {
    python3 pytest/select.py --password root
}

@test "SELECT nullable column" {
    python3 pytest/select_null.py --password root
}

@test "SELECT with WHERE clause" {
    python3 pytest/where.py --password root
}

@test "UPDATE rows" {
    python3 pytest/update.py --password root
}
