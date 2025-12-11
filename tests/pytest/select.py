import sys
from utils.reset import reset
from utils.connection import get_connection
import argparse

def select (db, cursor) :
    reset(db, cursor)
    print("SELECT TEST")
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

    cursor.execute('SELECT * FROM ha_lineairdb_test.items')
    rows = cursor.fetchall()
    if not rows :
        print("\tCheck 1 Failed")
        print("\t", rows)
        return 1

    cursor.execute('SELECT title, content FROM ha_lineairdb_test.items;')
    rows = cursor.fetchall()

    if len(rows[0]) != 2 :
        print("\tCheck 2 Failed")
        print("\t", rows)
        return 1
    print("\tPassed!")
    print("\t", rows)
    return 0



def main():
    # test
    db = get_connection(user=args.user, password=args.password)
    cursor=db.cursor()
    
    sys.exit(select(db, cursor))


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