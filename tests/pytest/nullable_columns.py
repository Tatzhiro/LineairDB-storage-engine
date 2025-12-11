import sys
import mysql.connector
from utils.connection import get_connection
import argparse
import time

from utils.reset import reset


def nullable_columns(db, cursor):
    print("NULLABLE COLUMN BASIC TEST")

    cursor.execute(
        'INSERT INTO ha_lineairdb_test.items (title, content, content9) '
        'VALUES ("alice", NULL, "marker")'
    )
    db.commit()

    cursor.execute(
        'SELECT content FROM ha_lineairdb_test.items WHERE title = "alice"'
    )
    row = cursor.fetchone()
    if row is None:
        print('\tCheck 1 Failed: expected one row, got None')
        return 1
    if row[0] is not None:
        print('\tCheck 2 Failed: expected NULL content, got', row)
        return 1

    cursor.execute(
        'SELECT COUNT(*) FROM ha_lineairdb_test.items WHERE content IS NULL'
    )
    count_row = cursor.fetchone()
    if count_row is None or count_row[0] != 1:
        print('\tCheck 3 Failed: expected one NULL row, got', count_row)
        return 1

    cursor.execute(
        'UPDATE ha_lineairdb_test.items SET content = %s WHERE title = %s',
        ("updated", "alice"),
    )
    db.commit()

    cursor.execute(
        'SELECT content FROM ha_lineairdb_test.items WHERE title = %s',
        ("alice",),
    )
    updated_row = cursor.fetchone()
    if updated_row is None or updated_row[0] != "updated":
        print('\tCheck 4 Failed: expected updated content, got', updated_row)
        return 1

    cursor.execute(
        'UPDATE ha_lineairdb_test.items SET content = %s WHERE title = %s',
        (None, "alice"),
    )
    db.commit()

    cursor.execute(
        'SELECT content FROM ha_lineairdb_test.items WHERE title = %s',
        ("alice",),
    )
    restored_row = cursor.fetchone()
    if restored_row is None or restored_row[0] is not None:
        print('\tCheck 5 Failed: expected restored NULL content, got', restored_row)
        return 1

    print('\tPassed!')
    return 0

def main():
    db = get_connection(user=args.user, password=args.password)
    cursor = db.cursor()
    reset(db, cursor)
    result = 0
    result |= nullable_columns(db, cursor)

    cursor.close()
    db.close()

    sys.exit(result)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="LineairDB nullable column test")
    parser.add_argument('--user', type=str, default="root")
    parser.add_argument('--password', type=str, default="")
    args = parser.parse_args()
    main()

