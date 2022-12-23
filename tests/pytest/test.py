from time import sleep
import mysql.connector
import argparse
from update import update
from delete import delete
from select_null import selectNull
from insert import insert

def INSERT (cursor, prm) :
    cursor.execute('INSERT INTO ha_lineairdb_test.items (title, content) VALUES (%s, %s)', prm)

def main():
    # test
    db=mysql.connector.connect(host="localhost", user=args.user, password=args.password)
    cursor=db.cursor()

    insert(db, cursor)
    delete(db, cursor)
    selectNull(db, cursor)
    update(db, cursor)


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