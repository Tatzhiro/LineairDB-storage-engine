import sys
import mysql.connector
from utils.reset import reset
import argparse

def update (db, cursor) :
    reset(db, cursor)
    print("UPDATE TEST")
    cursor.execute(\
        'INSERT INTO ha_lineairdb_test.items (\
            title, content\
        ) VALUES ("carol", "ddd")'\
    )
    cursor.execute('UPDATE ha_lineairdb_test.items SET content="XXX"')

    db.commit()

    cursor.execute('SELECT * FROM ha_lineairdb_test.items')
    rows = cursor.fetchall()
    if not rows :
        print("\tFailed")
        print("\t", rows)
        return 1
    if rows[0][1] == "XXX" and rows[0][0] == "carol":
        print("\tPassed!")
        print("\t", rows)
        return 0
    print("\tFailed")
    print("\t", rows)
    return 1

 
def main():
    # test
    db=mysql.connector.connect(host="localhost", user=args.user, password=args.password)
    cursor=db.cursor()
    
    sys.exit(update(db, cursor))


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