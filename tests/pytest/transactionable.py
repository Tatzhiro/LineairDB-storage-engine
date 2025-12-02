import sys
import mysql.connector
import argparse
import concurrent.futures
import threading
import time

thread1_read_done = threading.Event()
thread2_read_done = threading.Event()
both_reads_done = threading.Barrier(2)

thread1_result = {"read_value": None, "committed": False, "error": None}
thread2_result = {"read_value": None, "committed": False, "error": None}


def create_test_table(db, cursor):
    cursor.execute("DROP TABLE IF EXISTS ha_lineairdb_test.silo_test")
    cursor.execute("""
        CREATE TABLE ha_lineairdb_test.silo_test (
            k VARCHAR(10) PRIMARY KEY,
            v INT NOT NULL
        ) ENGINE=LINEAIRDB
    """)
    db.commit()


def setup_initial_state(db, cursor):
    cursor.execute("DELETE FROM ha_lineairdb_test.silo_test")
    cursor.execute("INSERT INTO ha_lineairdb_test.silo_test (k, v) VALUES ('x', 0)")
    cursor.execute("INSERT INTO ha_lineairdb_test.silo_test (k, v) VALUES ('y', 0)")
    db.commit()


def thread1_read_x_write_y():
    global thread1_result
    
    try:
        db = mysql.connector.connect(host="localhost", user=args.user, password=args.password)
        cursor = db.cursor()
        
        cursor.execute('BEGIN')
        
        cursor.execute("SELECT v FROM ha_lineairdb_test.silo_test WHERE k='x'")
        row = cursor.fetchone()
        thread1_result["read_value"] = row[0] if row else None
        print(f"\t[T1] Read x = {thread1_result['read_value']}")
        
        thread1_read_done.set()
        both_reads_done.wait()
        
        cursor.execute("UPDATE ha_lineairdb_test.silo_test SET v=1 WHERE k='y'")
        print("\t[T1] Write y = 1")
        
        time.sleep(0.01)
        
        cursor.execute('COMMIT')
        thread1_result["committed"] = True
        print("\t[T1] COMMIT - Success")
        
    except mysql.connector.Error as e:
        thread1_result["committed"] = False
        thread1_result["error"] = str(e)
        print(f"\t[T1] COMMIT - Aborted: {e}")
        try:
            cursor.execute('ROLLBACK')
        except:
            pass
    finally:
        try:
            db.close()
        except:
            pass


def thread2_read_y_write_x():
    global thread2_result
    
    try:
        db = mysql.connector.connect(host="localhost", user=args.user, password=args.password)
        cursor = db.cursor()
        
        cursor.execute('BEGIN')
        
        cursor.execute("SELECT v FROM ha_lineairdb_test.silo_test WHERE k='y'")
        row = cursor.fetchone()
        thread2_result["read_value"] = row[0] if row else None
        print(f"\t[T2] Read y = {thread2_result['read_value']}")
        
        thread2_read_done.set()
        both_reads_done.wait()
        
        cursor.execute("UPDATE ha_lineairdb_test.silo_test SET v=1 WHERE k='x'")
        print("\t[T2] Write x = 1")
        
        cursor.execute('COMMIT')
        thread2_result["committed"] = True
        print("\t[T2] COMMIT - Success")
        
    except mysql.connector.Error as e:
        thread2_result["committed"] = False
        thread2_result["error"] = str(e)
        print(f"\t[T2] COMMIT - Aborted: {e}")
        try:
            cursor.execute('ROLLBACK')
        except:
            pass
    finally:
        try:
            db.close()
        except:
            pass


def get_final_state():
    db = mysql.connector.connect(host="localhost", user=args.user, password=args.password)
    cursor = db.cursor()
    
    cursor.execute("SELECT k, v FROM ha_lineairdb_test.silo_test ORDER BY k")
    rows = cursor.fetchall()
    
    db.close()
    
    result = {}
    for k, v in rows:
        result[k] = v
    return result


def silo_serializability_test(db, cursor):
    """
    Silo Serializability Test (Write Skew Prevention)
    
    Initial state: x=0, y=0
    T1: read(x), write(y=1)
    T2: read(y), write(x=1)
    
    Non-serializable outcome (x=1, y=1) must be prevented.
    At least one transaction must abort.
    """
    global thread1_result, thread2_result, thread1_read_done, thread2_read_done, both_reads_done
    
    print("SILO SERIALIZABILITY TEST")
    
    thread1_result = {"read_value": None, "committed": False, "error": None}
    thread2_result = {"read_value": None, "committed": False, "error": None}
    thread1_read_done = threading.Event()
    thread2_read_done = threading.Event()
    both_reads_done = threading.Barrier(2)
    
    create_test_table(db, cursor)
    setup_initial_state(db, cursor)
    
    print("\t[Setup] Initial state: x=0, y=0")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        future1 = executor.submit(thread1_read_x_write_y)
        future2 = executor.submit(thread2_read_y_write_x)
        future1.result()
        future2.result()
    
    print("\t--- Results ---")
    print(f"\tT1: read x={thread1_result['read_value']}, committed={thread1_result['committed']}")
    print(f"\tT2: read y={thread2_result['read_value']}, committed={thread2_result['committed']}")
    
    final_state = get_final_state()
    print(f"\tFinal state: x={final_state.get('x')}, y={final_state.get('y')}")
    
    t1_read_0 = thread1_result["read_value"] == 0
    t2_read_0 = thread2_result["read_value"] == 0
    both_committed = thread1_result["committed"] and thread2_result["committed"]
    final_x1_y1 = final_state.get('x') == 1 and final_state.get('y') == 1
    
    if t1_read_0 and t2_read_0 and both_committed and final_x1_y1:
        print("\tFailed: Non-serializable outcome (x=1, y=1)")
        return 1
    
    if not both_committed:
        aborted = []
        if not thread1_result["committed"]:
            aborted.append("T1")
        if not thread2_result["committed"]:
            aborted.append("T2")
        print(f"\tPassed! ({', '.join(aborted)} aborted)")
        return 0
    
    if both_committed and (not t1_read_0 or not t2_read_0):
        print("\tPassed! (Both committed with serialized reads)")
        return 0
    
    print("\tPassed!")
    return 0


def main():
    db = mysql.connector.connect(host="localhost", user=args.user, password=args.password)
    cursor = db.cursor()
    
    sys.exit(silo_serializability_test(db, cursor))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Connect to MySQL')
    parser.add_argument('--user', metavar='user', type=str,
                        help='name of user', default="root")
    parser.add_argument('--password', metavar='pw', type=str,
                        help='password for the user', default="")
    args = parser.parse_args()
    main()
