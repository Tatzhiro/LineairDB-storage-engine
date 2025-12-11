import sys
import mysql.connector
from utils.connection import get_connection
from utils.reset import reset
import argparse

def where (db, cursor) :
    cursor.execute('DROP DATABASE IF EXISTS ha_lineairdb_test')
    cursor.execute('CREATE DATABASE ha_lineairdb_test')
    cursor.execute('CREATE TABLE ha_lineairdb_test.items (\
        title int NOT NULL,\
        content TEXT,\
        INDEX title_idx (title)\
    )ENGINE = LineairDB')
    print("PRIMARY KEY INT TEST")
    cursor.execute(\
        'INSERT INTO ha_lineairdb_test.items (\
            title, content\
        ) VALUES ("1", "alice")'\
    )
    db.commit()

    cursor.execute('SELECT * FROM ha_lineairdb_test.items')
    rows = cursor.fetchall()
    if not rows or rows[0][0] != 1 :
        print("\tCheck 1 Failed")
        print("\t", rows)
        return 1
    print("\tCheck 1 Passed")
    cursor.execute('SELECT * FROM ha_lineairdb_test.items WHERE title = "1"')
    rows = cursor.fetchall()
    if not rows or rows[0][1] != "alice" :
        print("\tCheck 2 Failed")
        print("\t", rows)
        return 1

    print("\tPassed!")
    print("\t", rows)
    return 0

def main():
    # test
    db=get_connection(user=args.user, password=args.password)
    cursor=db.cursor()
    
    sys.exit(where(db, cursor))


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