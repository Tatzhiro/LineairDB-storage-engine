import sys
import mysql.connector
from utils.reset import reset
import argparse

def insert (db, cursor) :
    reset(db, cursor)
    print("INSERT TEST")
    cursor.execute(\
        'INSERT INTO ha_lineairdb_test.items (\
            title, content\
        ) VALUES ("alice", "alice meets bob")'\
    )
    cursor.execute(\
        'INSERT INTO ha_lineairdb_test.items (\
            title, content\
        ) VALUES ("bob", "bob meets carol")'\
    )
    db.commit()
    cursor.execute('SELECT title FROM ha_lineairdb_test.items')
    rows = cursor.fetchall()
    if not rows: 
        print("\tFailed: list empty")
        return 1
    for row in rows:
        if row[0] != "alice" and row[0] != "bob":
            print("\tFailed")
            print("\t", rows)
            return 1
    print("\tPassed!")
    return 0

 
def main():
    # test
    db=mysql.connector.connect(host="localhost", user=args.user, password=args.password)
    cursor=db.cursor()
    
    sys.exit(insert(db, cursor))


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