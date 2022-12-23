import sys
import mysql.connector
from reset import reset
import argparse

def selectNull (db, cursor) :
    reset(db, cursor)
    print("NULL SELECT TEST")
    cursor.execute(\
        'INSERT INTO ha_lineairdb_test.items (\
            title, content9\
        ) VALUES ("carol", "")'\
    )
    db.commit()

    cursor.execute('SELECT * FROM ha_lineairdb_test.items')
    rows = cursor.fetchall()
    if not rows :
        print("\tCheck 1 Failed")
        print("\t", rows)
        return 1
    for i in rows[0] :
        if i != "carol" and i != None and i != "":
            print("\tCheck 2 Failed")
            print("\t", rows)
            return 1
    if (rows[0][9] == None) :
        print("\tCheck 3 Failed")
        print("\t", rows)
    print("\tPassed!")
    return 0

 
def main():
    # test
    db=mysql.connector.connect(host="localhost", user=args.user, password=args.password)
    cursor=db.cursor()
    
    sys.exit(selectNull(db, cursor))


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