import sys
import mysql.connector
from utils.connection import get_connection
import argparse

def reset (db, cursor) :
    cursor.execute('DROP DATABASE IF EXISTS ha_lineairdb_test')
    cursor.execute('CREATE DATABASE ha_lineairdb_test')
    db.commit()

#TODO: add null column
def test_create_index_on_int_column (db, cursor) :
    print("CREATE INDEX ON INT COLUMN TEST")
    cursor.execute('CREATE TABLE ha_lineairdb_test.test_int (\
        id int NOT NULL PRIMARY KEY,\
        score int NOT NULL,\
        name VARCHAR(10),\
        INDEX score_idx (score)\
    ) ENGINE = LineairDB')
    db.commit()
    
    print("\tPassed!")
    return 0

def test_create_index_on_string_column (db, cursor) :
    print("CREATE INDEX ON STRING COLUMN TEST")
    cursor.execute('CREATE TABLE ha_lineairdb_test.test_string (\
        id int NOT NULL PRIMARY KEY,\
        email VARCHAR(10) NOT NULL,\
        age int,\
        INDEX email_idx (email)\
    ) ENGINE = LineairDB')
    db.commit()
    
    print("\tPassed!")
    return 0

def test_create_index_on_datetime_column (db, cursor) :
    print("CREATE INDEX ON DATETIME COLUMN TEST")
    cursor.execute('CREATE TABLE ha_lineairdb_test.test_datetime (\
        id int NOT NULL PRIMARY KEY,\
        created_at DATETIME NOT NULL,\
        title VARCHAR(10),\
        INDEX created_at_idx (created_at)\
    ) ENGINE = LineairDB')
    db.commit()
    
    print("\tPassed!")
    return 0

def test_create_multiple_indexes (db, cursor) :
    print("CREATE MULTIPLE INDEXES TEST")
    cursor.execute('CREATE TABLE ha_lineairdb_test.test_multiple (\
        id int NOT NULL PRIMARY KEY,\
        name VARCHAR(10) NOT NULL,\
        age int NOT NULL,\
        city VARCHAR(10) NOT NULL,\
        INDEX name_idx (name),\
        INDEX age_idx (age),\
        INDEX city_idx (city)\
    ) ENGINE = LineairDB')
    db.commit()
    
    print("\tPassed!")
    return 0

def test_create_duplicate_index (db, cursor) :
    print("CREATE DUPLICATE INDEX TEST (should fail)")
    # 同じ名前のインデックスを複数定義しようとする（失敗するはず）
    try:
        cursor.execute('CREATE TABLE ha_lineairdb_test.test_dup (\
            id int NOT NULL PRIMARY KEY,\
            value int NOT NULL,\
            INDEX value_idx (value),\
            INDEX value_idx (value)\
        ) ENGINE = LineairDB')
        db.commit()
        print("\tFailed: duplicate index should not be created")
        return 1
    except mysql.connector.Error as err:
        print(f"\tPassed! (Expected error: {err})")
        return 0

def main():
    # test
    db=get_connection(user=args.user, password=args.password)
    cursor=db.cursor()
    
    reset(db, cursor)
    
    result = 0
    result |= test_create_index_on_int_column(db, cursor)
    result |= test_create_index_on_string_column(db, cursor)
    result |= test_create_index_on_datetime_column(db, cursor)
    result |= test_create_multiple_indexes(db, cursor)
    result |= test_create_duplicate_index(db, cursor)
    
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

