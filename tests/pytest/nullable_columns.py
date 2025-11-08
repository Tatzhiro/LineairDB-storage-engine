import sys
import mysql.connector
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


def nullable_secondary_index(db, cursor):
    print("NULLABLE SECONDARY INDEX TEST")

    table_name = f"nullable_sec_{int(time.time() * 1000000)}"
    table_created = False

    try:
        cursor.execute(
            f'''CREATE TABLE ha_lineairdb_test.{table_name} (
                title VARCHAR(50) NOT NULL,
                tag VARCHAR(50),
                marker VARCHAR(50),
                INDEX tag_idx (tag)
            ) ENGINE = LineairDB'''
        )
        db.commit()
        table_created = True

        cursor.execute(
            f"INSERT INTO ha_lineairdb_test.{table_name} (title, tag, marker) "
            f"VALUES (%s, %s, %s)",
            ("alpha", None, "null-marker-a"),
        )
        db.commit()
        cursor.execute(
            f"INSERT INTO ha_lineairdb_test.{table_name} (title, tag, marker) "
            f"VALUES (%s, %s, %s)",
            ("beta", "feature", "non-null"),
        )
        db.commit()
        cursor.execute(
            f"INSERT INTO ha_lineairdb_test.{table_name} (title, tag, marker) "
            f"VALUES (%s, %s, %s)",
            ("gamma", None, "null-marker-c"),
        )
        db.commit()

        cursor.execute(
            f"SELECT title FROM ha_lineairdb_test.{table_name} "
            f"WHERE tag IS NULL ORDER BY title"
        )
        null_titles = [row[0] for row in cursor.fetchall()]
        expected_null_titles = ["alpha", "gamma"]
        if null_titles != expected_null_titles:
            print(
                "\tCheck 1 Failed: expected NULL-tag titles",
                expected_null_titles,
                "got",
                null_titles,
            )
            return 1

        cursor.execute(
            f"SELECT COUNT(*) FROM ha_lineairdb_test.{table_name} WHERE tag IS NULL"
        )
        null_count = cursor.fetchone()
        if null_count is None or null_count[0] != 2:
            print("\tCheck 2 Failed: expected two NULL-tag rows, got", null_count)
            return 1

        cursor.execute(
            f"SELECT title FROM ha_lineairdb_test.{table_name} WHERE tag = %s",
            ("feature",),
        )
        feature_rows = cursor.fetchall()
        if len(feature_rows) != 1 or feature_rows[0][0] != "beta":
            print("\tCheck 3 Failed: expected one 'feature' row for beta, got", feature_rows)
            return 1

        cursor.execute(
            f"SELECT title, tag FROM ha_lineairdb_test.{table_name} ORDER BY title"
        )
        print("\tCurrent rows:")
        for row in cursor.fetchall():
            print(f"\t  {row}")

        print('\tPassed!')
        return 0
    finally:
        if table_created:
            cursor.execute(f"DROP TABLE IF EXISTS ha_lineairdb_test.{table_name}")
            db.commit()


def nullable_primary_update(db, cursor):
    print("NULLABLE PRIMARY UPDATE TEST")

    table_name = f"nullable_primary_{int(time.time() * 1000000)}"
    table_created = False

    try:
        cursor.execute(
            f'''CREATE TABLE ha_lineairdb_test.{table_name} (
                title VARCHAR(50) NOT NULL,
                tag VARCHAR(50),
                PRIMARY KEY (title)
            ) ENGINE = LineairDB'''
        )
        db.commit()
        table_created = True

        cursor.execute(
            f"INSERT INTO ha_lineairdb_test.{table_name} (title, tag) VALUES (%s, %s)",
            ("epsilon", "initial"),
        )
        db.commit()

        cursor.execute(
            f"UPDATE ha_lineairdb_test.{table_name} SET tag = %s WHERE title = %s",
            (None, "epsilon"),
        )
        db.commit()

        cursor.execute(
            f"SELECT COUNT(*) FROM ha_lineairdb_test.{table_name} WHERE tag IS NULL"
        )
        after_update = cursor.fetchone()
        if after_update is None or after_update[0] != 1:
            print("\tCheck 1 Failed: expected epsilon to become NULL, got", after_update)
            return 1

        cursor.execute(
            f"UPDATE ha_lineairdb_test.{table_name} SET tag = %s WHERE title = %s",
            ("restored", "epsilon"),
        )
        db.commit()

        cursor.execute(
            f"SELECT tag FROM ha_lineairdb_test.{table_name} WHERE title = %s",
            ("epsilon",),
        )
        restored = cursor.fetchone()
        if restored is None or restored[0] != "restored":
            print("\tCheck 2 Failed: expected restored tag, got", restored)
            return 1

        cursor.execute(
            f"SELECT COUNT(*) FROM ha_lineairdb_test.{table_name} WHERE tag IS NULL"
        )
        final_null = cursor.fetchone()
        if final_null is None or final_null[0] != 0:
            print("\tCheck 3 Failed: expected no NULL rows after restore, got", final_null)
            return 1

        print("\tPassed!")
        return 0
    finally:
        if table_created:
            cursor.execute(f"DROP TABLE IF EXISTS ha_lineairdb_test.{table_name}")
            db.commit()


def nullable_secondary_index_composite(db, cursor):
    print("NULLABLE SECONDARY INDEX COMPOSITE TEST")

    table_name = f"nullable_comp_{int(time.time() * 1000000)}"
    table_created = False

    try:
        cursor.execute(
            f'''CREATE TABLE ha_lineairdb_test.{table_name} (
                category VARCHAR(50),
                tag VARCHAR(50),
                marker VARCHAR(50),
                INDEX comp_idx (category, tag)
            ) ENGINE = LineairDB'''
        )
        db.commit()
        table_created = True

        rows = [
            ("cat", None, "m1"),
            ("cat", "blue", "m2"),
            ("dog", None, "m3"),
        ]
        cursor.executemany(
            f"INSERT INTO ha_lineairdb_test.{table_name} (category, tag, marker) VALUES (%s, %s, %s)",
            rows,
        )
        db.commit()

        cursor.execute(
            f"SELECT marker FROM ha_lineairdb_test.{table_name} WHERE category = %s AND tag IS NULL ORDER BY marker",
            ("cat",),
        )
        result_cat = [row[0] for row in cursor.fetchall()]
        if result_cat != ["m1"]:
            print("\tCheck 1 Failed: expected one NULL tag in category cat, got", result_cat)
            return 1

        cursor.execute(
            f"SELECT marker FROM ha_lineairdb_test.{table_name} WHERE category = %s AND tag IS NULL ORDER BY marker",
            ("dog",),
        )
        result_dog = [row[0] for row in cursor.fetchall()]
        if result_dog != ["m3"]:
            print("\tCheck 2 Failed: expected one NULL tag in category dog, got", result_dog)
            return 1

        cursor.execute(
            f"SELECT marker FROM ha_lineairdb_test.{table_name} WHERE category = %s AND tag = %s",
            ("cat", "blue"),
        )
        blue = cursor.fetchone()
        if blue is None or blue[0] != "m2":
            print("\tCheck 3 Failed: expected blue marker in category cat, got", blue)
            return 1

        print("\tPassed!")
        return 0
    finally:
        if table_created:
            cursor.execute(f"DROP TABLE IF EXISTS ha_lineairdb_test.{table_name}")
            db.commit()


def main():
    db = mysql.connector.connect(host="localhost", user=args.user, password=args.password)
    cursor = db.cursor()
    reset(db, cursor)
    result = 0
    result |= nullable_columns(db, cursor)
    result |= nullable_secondary_index(db, cursor)
    result |= nullable_primary_update(db, cursor)
    result |= nullable_secondary_index_composite(db, cursor)

    cursor.close()
    db.close()

    sys.exit(result)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="LineairDB nullable column test")
    parser.add_argument('--user', type=str, default="root")
    parser.add_argument('--password', type=str, default="")
    args = parser.parse_args()
    main()

