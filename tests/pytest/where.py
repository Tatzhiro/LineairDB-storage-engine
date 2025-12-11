import sys
import mysql.connector
from utils.connection import get_connection
from utils.reset import reset
import argparse

def where (db, cursor) :
    reset(db, cursor)
    print("SELECT WHERE TEST")
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

    cursor.execute('SELECT * FROM ha_lineairdb_test.items WHERE content is NULL')
    rows = cursor.fetchall()
    if rows :
        print("\tCheck 1 Failed")
        print("\t", rows)
        return 1

    cursor.execute('SELECT * FROM ha_lineairdb_test.items WHERE content = "alice meets bob"')
    rows = cursor.fetchall()

    if not rows or rows[0][0] != "alice" :
        print("\tCheck 2 Failed")
        print("\t", rows)
        return 1

    cursor.execute('SELECT * FROM ha_lineairdb_test.items WHERE title = "alice"')
    rows = cursor.fetchall()
    if not rows or rows[0][0] != "alice" :
        print("\tCheck 3 Failed")
        print("\t", rows)
        return 1
        
    print("\tPassed!")
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