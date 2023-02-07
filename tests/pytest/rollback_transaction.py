import sys
import mysql.connector
from utils.reset import reset
import argparse
import concurrent.futures
import threading

isThread2Precommit = threading.Event()

def tx2_expect_no_row () :
    db=mysql.connector.connect(host="localhost", user=args.user, password=args.password)
    cursor=db.cursor()
    print("\ttx2 BEGIN")
    cursor.execute('BEGIN')

    print("\ttx2 SELECT")
    cursor.execute('SELECT title, content FROM ha_lineairdb_test.items')
    rows = cursor.fetchall()
    
    print("\ttx2 COMMIT")
    isThread2Precommit.set()
    cursor.execute('COMMIT')

    return rows

def transaction (db, cursor) :
    reset(db, cursor)
    print("ROLLBACK TEST")

    print("\ttx1 BEGIN")
    cursor.execute('BEGIN')

    print("\ttx1 INSERT")
    cursor.execute(\
        'INSERT INTO ha_lineairdb_test.items (\
            title, content\
        ) VALUES ("alice", "alice meets bob")'\
    )

    executor = concurrent.futures.ThreadPoolExecutor()
    future = executor.submit(tx2_expect_no_row)

    isThread2Precommit.wait()
    print("\ttx1 ROLLBACK")
    cursor.execute('ROLLBACK')
    db.commit()

    rows = future.result()
    if rows :
        print("\tCheck 1 Failed")
        print("\t", rows)
        return 1

    rows = tx2_expect_no_row()
    if rows :
        print("\tCheck 2 Failed")
        print("\t", rows)
        return 1

    print("\tPassed!")
    return 0



def main():
    # test
    db=mysql.connector.connect(host="localhost", user=args.user, password=args.password)
    cursor=db.cursor()
    
    sys.exit(transaction(db, cursor))


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