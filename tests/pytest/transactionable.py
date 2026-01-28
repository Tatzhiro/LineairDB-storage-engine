import sys
import mysql.connector
from utils.connection import get_connection
from utils.reset import reset
import argparse
import concurrent.futures
import threading

hasThread2ExecutedQuery = threading.Event()

def tx3_expect_row () :
    db=get_connection(user=args.user, password=args.password)
    cursor=db.cursor()
    print("\ttx3 BEGIN")
    cursor.execute('BEGIN')

    print("\ttx3 SELECT")
    cursor.execute('SELECT title, content FROM ha_lineairdb_test.items')
    rows = cursor.fetchall()

    print("\ttx3 COMMIT")
    cursor.execute('COMMIT')

    return rows

def tx2_expect_no_row () :
    db=get_connection(user=args.user, password=args.password)
    cursor=db.cursor()
    print("\ttx2 BEGIN")
    cursor.execute('BEGIN')

    print("\ttx2 SELECT")
    try:
        cursor.execute('SELECT title, content FROM ha_lineairdb_test.items')
        rows = cursor.fetchall()
    except mysql.connector.errors.DatabaseError as e:
        # Precision Locking may abort tx2 due to range overlap with tx1's INSERT
        # This is correct behavior - tx2 cannot see uncommitted data
        print(f"\ttx2 aborted (expected due to Precision Locking): {e}")
        hasThread2ExecutedQuery.set()
        cursor.execute('ROLLBACK')
        return []  # Treat as "no rows visible" which is the expected outcome
    
    finally:
        hasThread2ExecutedQuery.set()
    
    print("\ttx2 COMMIT")
    cursor.execute('COMMIT')
    

    return rows

def transaction (db, cursor) :
    clear_data(db, cursor)
    print("TRANSACTIONABLE TEST")

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
    hasThread2ExecutedQuery.wait()

    print("\ttx1 COMMIT")
    cursor.execute('COMMIT')
    db.commit()

    rows = future.result()
    if rows :
        print("\tCheck 1 Failed")
        print("\t", rows)
        return 1

    rows = tx3_expect_row()
    if not rows :
        print("\tCheck 2 Failed")
        return 1

    print("\tPassed!")
    print("\t", rows)
    return 0


def clear_data(db, cursor):
    cursor.execute('DELETE FROM ha_lineairdb_test.items')
    db.commit()

def simple_for_update_test(db, cursor):
    clear_data(db, cursor)
    print("SIMPLE FOR UPDATE TEST")

    cursor.execute('INSERT INTO ha_lineairdb_test.items (title, content) VALUES ("alice", "alice content")')
    db.commit()

    print("\ttx BEGIN")
    cursor.execute('BEGIN')

    print("\ttx SELECT FOR UPDATE")
    cursor.execute('SELECT * FROM ha_lineairdb_test.items WHERE title="alice" FOR UPDATE')
    rows = cursor.fetchall()

    if not rows:
        print("\tFailed: rows empty")
        return 1

    print("\ttx COMMIT")
    cursor.execute('COMMIT')

    print("\tPassed!")
    return 0


def main():
    # test
    db=get_connection(user=args.user, password=args.password)
    cursor=db.cursor()
    
    reset(db, cursor)

    if transaction(db, cursor) != 0:
        sys.exit(1)

    if simple_for_update_test(db, cursor) != 0:
        sys.exit(1)

    sys.exit(0)


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