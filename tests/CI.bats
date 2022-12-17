#!/usr/bin/env bats

teardown() {
    if [[ ! -z "$DEBUG" ]]; then
        echo "    DEBUG OUTPUT:" >&3
        echo -e "$output" >&3
        echo "    END DEBUG OUTPUT" >&3
    fi
    sudo service mysql restart
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
