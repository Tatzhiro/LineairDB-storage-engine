@test "RESET" {
    bin/mysql -u root < ../storage/lineairdb/tests/reset.sql # drop and create database
}
@test "INSERT rows" {
    bin/mysql -u root < ../storage/lineairdb/tests/insert.sql
}
@test "SELECT rows" {
    bin/mysql -u root < ../storage/lineairdb/tests/select.sql
}
@test "INSERT and SELECT NULLABLE columns" {
    bin/mysql -u root < ../storage/lineairdb/tests/nulls.sql
}