"""
TDD: Test for CREATE INDEX statement (separate from CREATE TABLE)
"""
import sys
import mysql.connector
from utils.connection import get_connection
import argparse

def reset(db, cursor):
    cursor.execute('DROP DATABASE IF EXISTS ha_lineairdb_test')
    cursor.execute('CREATE DATABASE ha_lineairdb_test')
    db.commit()

def test_create_index_and_use(db, cursor):
    """CREATE INDEX on an existing table, then read/write using the index"""
    print("TEST: CREATE INDEX and use it for read/write")
    
    # 1. Create table
    cursor.execute('''
        CREATE TABLE ha_lineairdb_test.t1 (
            id INT NOT NULL PRIMARY KEY,
            name VARCHAR(50) NOT NULL
        ) ENGINE = LineairDB
    ''')
    db.commit()
    
    # 2. Create index
    try:
        cursor.execute('CREATE INDEX idx_name ON ha_lineairdb_test.t1 (name)')
        db.commit()
    except mysql.connector.Error as err:
        print(f"\tFailed to create index: {err}")
        return 1
    
    # 3. Insert data
    cursor.execute("INSERT INTO ha_lineairdb_test.t1 VALUES (1, 'Alice')")
    cursor.execute("INSERT INTO ha_lineairdb_test.t1 VALUES (2, 'Bob')")
    cursor.execute("INSERT INTO ha_lineairdb_test.t1 VALUES (3, 'Charlie')")
    db.commit()
    
    # 4. Read using indexed column
    cursor.execute("SELECT * FROM ha_lineairdb_test.t1 WHERE name = 'Bob'")
    result = cursor.fetchall()
    if len(result) != 1 or result[0][0] != 2:
        print(f"\tFailed to read: expected (2, 'Bob'), got {result}")
        return 1
    
    print("\tPassed!")
    return 0

def main():
    db = get_connection(user=args.user, password=args.password)
    cursor = db.cursor()
    
    reset(db, cursor)
    result = test_create_index_and_use(db, cursor)
    
    cursor.close()
    db.close()
    sys.exit(result)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--user', default="root")
    parser.add_argument('--password', default="")
    args = parser.parse_args()
    main()
