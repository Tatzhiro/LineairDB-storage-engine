import sys
import mysql.connector
from utils.connection import get_connection
from utils.reset import reset
import argparse
import time


def test_delete_primary_key(db, cursor):
    print("DELETE TEST (explicit PK)")

    pk_table = f"test_delete_pk_{int(time.time() * 1000000)}"
    cursor.execute(
        f'''CREATE TABLE ha_lineairdb_test.{pk_table} (
        id INT PRIMARY KEY,
        name VARCHAR(50),
        age INT
    ) ENGINE = LineairDB'''
    )
    db.commit()

    pk_rows = [
        (1, "alice", 25),
        (2, "bob", 30),
        (3, "carol", 27),
    ]
    cursor.executemany(
        f'INSERT INTO ha_lineairdb_test.{pk_table} (id, name, age) VALUES (%s, %s, %s)',
        pk_rows,
    )
    db.commit()

    cursor.execute(f'DELETE FROM ha_lineairdb_test.{pk_table} WHERE id = 2')
    db.commit()

    cursor.execute(f'SELECT id, name FROM ha_lineairdb_test.{pk_table} ORDER BY id')
    remaining_pk = cursor.fetchall()
    if remaining_pk != [(1, "alice"), (3, "carol")]:
        print("\tCheck 3 Failed")
        print("\tRemaining PK rows:", remaining_pk)
        return 1

    print("\tExplicit PK test Passed!")
    return 0

def test_hidden_primary_key(db, cursor):
    print("DELETE TEST (hidden primary key)")
    table_name = f"test_delete_hidden_pk_{int(time.time() * 1000000)}"
    cursor.execute(f'''CREATE TABLE ha_lineairdb_test.{table_name} (
        id INT,
        name VARCHAR(50),
        age INT
    ) ENGINE = LineairDB'''
    )
    db.commit()

    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, name, age) VALUES (1, "alice", 25)')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, name, age) VALUES (2, "bob", 30)')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, name, age) VALUES (3, "carol", 27)')
    db.commit()

    cursor.execute(f'DELETE FROM ha_lineairdb_test.{table_name} WHERE id = 2')
    db.commit()

    cursor.execute(f'SELECT id, name FROM ha_lineairdb_test.{table_name} ORDER BY id')
    remaining_hidden_pk = cursor.fetchall()
    if remaining_hidden_pk != [(1, "alice"), (3, "carol")]:
        print("\tCheck 3 Failed")
        print("\tRemaining Hidden PK rows:", remaining_hidden_pk)
        return 1

    print("\tHidden PK test Passed!")
    return 0

def test_delete_secondary_index(db, cursor):
    print("DELETE TEST (Secondary Index)")
    table_name = f"test_delete_sec_idx_{int(time.time() * 1000000)}"
    cursor.execute(f'''CREATE TABLE ha_lineairdb_test.{table_name} (
        id INT PRIMARY KEY,
        name VARCHAR(50) NOT NULL,
        age INT,
        INDEX name_idx (name)
    ) ENGINE = LineairDB'''
    )
    db.commit()

    rows = [
        (1, "alice", 25),
        (2, "bob", 30),
        (3, "alice", 27),
    ]
    cursor.executemany(
        f'INSERT INTO ha_lineairdb_test.{table_name} (id, name, age) VALUES (%s, %s, %s)',
        rows,
    )
    db.commit()

    cursor.execute(f'DELETE FROM ha_lineairdb_test.{table_name} WHERE name = "alice"')
    db.commit()

    cursor.execute(f'SELECT id, name FROM ha_lineairdb_test.{table_name} ORDER BY id')
    remaining = cursor.fetchall()
    
    if remaining != [(2, "bob")]:
        print("\tCheck Failed")
        print("\tRemaining rows:", remaining)
        return 1

    print("\tSecondary Index delete test Passed!")
    return 0

def delete(db, cursor):
    reset(db, cursor)
    result = 0
    result |= test_delete_primary_key(db, cursor)
    result |= test_hidden_primary_key(db, cursor)
    result |= test_delete_secondary_index(db, cursor)

    if result == 0:
        print("ALL DELETE TESTS PASSED!")
    else:
        print("SOME DELETE TESTS FAILED!")

    return result
 
def main():
    db=get_connection(user=args.user, password=args.password)
    cursor=db.cursor()
    
    sys.exit(delete(db, cursor))



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