import sys
import mysql.connector
from utils.connection import get_connection
import argparse

def reset (db, cursor) :
    cursor.execute('DROP DATABASE IF EXISTS ha_lineairdb_test')
    cursor.execute('CREATE DATABASE ha_lineairdb_test')
    cursor.execute('CREATE TABLE ha_lineairdb_test.test1\
                    (id int not null, col1 CHAR(100))\
                    ENGINE = LineairDB')
    cursor.execute('CREATE TABLE ha_lineairdb_test.test2\
                    (id int not null, col1 CHAR(100))\
                    ENGINE = LineairDB')
    db.commit()

def multitable (db, cursor) :
    reset(db, cursor)
    print("MULTIPLE TABLE TEST")
    cursor.execute(\
        'INSERT INTO ha_lineairdb_test.test1 (\
            id, col1\
        ) VALUES (1, "test1")'\
    )
    cursor.execute(\
        'INSERT INTO ha_lineairdb_test.test2 (\
            id, col1\
        ) VALUES (1, "test2")'\
    )
    db.commit()

    cursor.execute('SELECT * FROM ha_lineairdb_test.test1')
    rows = cursor.fetchall()
    try:
      if rows[0][1] != "test1":
          print("\tCheck 1 Failed")
          print("\t", rows)
          return 1
    except IndexError:
        print("\tCheck 1 Failed")
        print("\t", rows)
    
    cursor.execute('SELECT * FROM ha_lineairdb_test.test2')
    rows = cursor.fetchall()
    if rows[0][1] != "test2":
        print("\tCheck 2 Failed")
        print("\t", rows)
        return 1

    print("\tPassed!")
    return 0
 
def main():
    # test
    db=get_connection(user=args.user, password=args.password)
    cursor=db.cursor()
    
    sys.exit(multitable(db, cursor))


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