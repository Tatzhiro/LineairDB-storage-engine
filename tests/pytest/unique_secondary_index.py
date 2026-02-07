import argparse
import sys
import time

import mysql.connector

from utils.connection import get_connection


def reset(db, cursor):
    cursor.execute('DROP DATABASE IF EXISTS ha_lineairdb_test')
    cursor.execute('CREATE DATABASE ha_lineairdb_test')
    db.commit()


def expect_duplicate_insert_error(cursor, sql):
    try:
        cursor.execute(sql)
        return False, "duplicate insert unexpectedly succeeded"
    except mysql.connector.Error as err:
        return True, str(err)


def test_unique_index_defined_in_create_table(db, cursor):
    print("UNIQUE SECONDARY INDEX (CREATE TABLE) TEST")
    table_name = f"unique_create_{int(time.time() * 1000000)}"

    cursor.execute(
        f'''CREATE TABLE ha_lineairdb_test.{table_name} (
            id INT NOT NULL,
            email VARCHAR(63) NOT NULL,
            name VARCHAR(64),
            PRIMARY KEY (id),
            UNIQUE INDEX email_uidx (email)
        ) ENGINE = LineairDB'''
    )
    db.commit()

    cursor.execute(
        f"INSERT INTO ha_lineairdb_test.{table_name} (id, email, name) "
        "VALUES (1, 'alice@example.com', 'alice')"
    )
    db.commit()

    ok, detail = expect_duplicate_insert_error(
        cursor,
        f"INSERT INTO ha_lineairdb_test.{table_name} (id, email, name) "
        "VALUES (2, 'alice@example.com', 'bob')",
    )
    if not ok:
        db.commit()
        print(f"\tFailed: {detail}")
        return 1

    db.rollback()
    cursor.execute(
        f"SELECT COUNT(*) FROM ha_lineairdb_test.{table_name} "
        "WHERE email = 'alice@example.com'"
    )
    count = cursor.fetchone()[0]
    if count != 1:
        print(f"\tFailed: expected 1 row for duplicate key, got {count}")
        return 1

    print(f"\tPassed! (Expected duplicate error: {detail})")
    return 0


def main():
    db = get_connection(user=args.user, password=args.password)
    cursor = db.cursor()

    reset(db, cursor)

    result = 0
    result |= test_unique_index_defined_in_create_table(db, cursor)

    if result == 0:
        print("\nALL TESTS PASSED!")
    else:
        print("\nSOME TESTS FAILED!")

    sys.exit(result)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Connect to MySQL')
    parser.add_argument('--user', metavar='user', type=str,
                        help='name of user',
                        default="root")
    parser.add_argument('--password', metavar='pw', type=str,
                        help='password for the user',
                        default="")
    args = parser.parse_args()
    main()
