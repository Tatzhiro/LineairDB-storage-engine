import sys
import mysql.connector
from utils.connection import get_connection
from utils.reset import reset
import argparse


def commit_update_single_row(db, cursor):
    """Basic behavior test for COMMIT and UPDATE"""
    reset(db, cursor)
    print("COMMIT UPDATE SINGLE ROW TEST")

    print("\tTX1 BEGIN")
    cursor.execute('BEGIN')
    cursor.execute(
        'INSERT INTO ha_lineairdb_test.items (\
            title, content\
        ) VALUES ("alice", "before update")'
    )
    print("\tTX1 COMMIT")
    cursor.execute('COMMIT')
    db.commit()

    print("\tTX2 BEGIN")
    cursor.execute('BEGIN')
    cursor.execute(
        'UPDATE ha_lineairdb_test.items SET content="after update" WHERE title="alice"'
    )
    print("\tTX2 COMMIT")
    cursor.execute('COMMIT')
    db.commit()

    cursor.execute('SELECT title, content FROM ha_lineairdb_test.items')
    rows = cursor.fetchall()
    print("\t[DEBUG] Rows after commit:", rows)

    if len(rows) != 1:
        print("\tFailed: Expected exactly one row")
        return 1

    if rows[0][0] != "alice" or rows[0][1] != "after update":
        print("\tFailed: Unexpected row content")
        return 1

    print("\tPassed!")
    return 0


def commit_update_multiple_rows(db, cursor):
    """Behavior test for UPDATE and COMMIT on multiple rows"""
    reset(db, cursor)
    print("COMMIT UPDATE MULTIPLE ROWS TEST")

    print("\tTX1 BEGIN")
    cursor.execute('BEGIN')
    cursor.execute(
        'INSERT INTO ha_lineairdb_test.items (title, content) VALUES '
        '("alice", "v1"), ("bob", "v1"), ("carol", "v1")'
    )
    print("\tTX1 COMMIT")
    cursor.execute('COMMIT')
    db.commit()

    print("\tTX2 BEGIN")
    cursor.execute('BEGIN')
    cursor.execute(
        'UPDATE ha_lineairdb_test.items SET content="v2" WHERE title IN ("alice", "bob")'
    )
    print("\tTX2 COMMIT")
    cursor.execute('COMMIT')

    cursor.execute(
        'SELECT title, content FROM ha_lineairdb_test.items ORDER BY title'
    )
    rows = cursor.fetchall()
    print("\t[DEBUG] Rows after commit:", rows)

    expected = [
        ("alice", "v2"),
        ("bob", "v2"),
        ("carol", "v1"),
    ]

    if rows != expected:
        print("\tFailed: Expected rows", expected)
        return 1

    print("\tPassed!")
    return 0


def main():
    db = get_connection(user=args.user, password=args.password)
    cursor = db.cursor()

    failed = 0

    #1if commit_update_single_row(db, cursor) != 0:
    #    failed += 1

    if commit_update_multiple_rows(db, cursor) != 0:
        failed += 1

    if failed > 0:
        print(f"\n{failed} test(s) failed")
        sys.exit(1)

    print("\nAll tests passed!")
    sys.exit(0)


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

