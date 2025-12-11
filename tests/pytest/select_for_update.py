import sys
import mysql.connector
from utils.connection import get_connection
from utils.reset import reset
import argparse
import threading
import time

def tx1_select_for_update(user, password):
    try:
        conn = get_connection(user=user, password=password)
        cursor = conn.cursor()
        print("[T1] BEGIN")
        cursor.execute("BEGIN")
        
        print("[T1] SELECT ... FOR UPDATE (locking row 'alice')")
        cursor.execute("SELECT title, content FROM ha_lineairdb_test.items WHERE title='alice' FOR UPDATE")
        rows = cursor.fetchall()
        print(f"[T1] Result: {rows}")
        
        # ロックを保持したまま待機
        time.sleep(2)
        
        print("[T1] COMMIT")
        cursor.execute("COMMIT")
        conn.close()
    except Exception as e:
        print(f"[T1] Error: {e}")

def tx2_update(user, password):
    try:
        # T1が確実にロックを取るまで少し待つ
        time.sleep(0.5)
        
        conn = get_connection(user=user, password=password)
        cursor = conn.cursor()
        print("[T2] BEGIN")
        cursor.execute("BEGIN")
        
        print("[T2] UPDATE executing...")
        start_time = time.time()
        
        # 更新実行
        cursor.execute("UPDATE ha_lineairdb_test.items SET content='updated_by_t2' WHERE title='alice'")
        
        end_time = time.time()
        elapsed = end_time - start_time
        print(f"[T2] UPDATE finished. Elapsed time: {elapsed:.4f} sec")
        
        if elapsed < 1.0:
             print("[T2] Result: Non-blocking (Fast) - Expected for LineairDB")
        else:
             print("[T2] Result: Blocking (Slow) - Standard pessimistic locking behavior")

        cursor.execute("COMMIT")
        conn.close()
    except Exception as e:
        print(f"[T2] Error: {e}")

def test_for_update(db, cursor, args):
    # データベース初期化
    reset(db, cursor)
    
    # テストデータ挿入
    print("Initializing data...")
    cursor.execute("INSERT INTO ha_lineairdb_test.items (title, content) VALUES ('alice', 'initial_content')")
    db.commit()
    
    # スレッド作成
    t1 = threading.Thread(target=tx1_select_for_update, args=(args.user, args.password))
    t2 = threading.Thread(target=tx2_update, args=(args.user, args.password))

    # テスト実行
    t1.start()
    t2.start()

    t1.join()
    t2.join()
    
    return 0

def main():
    parser = argparse.ArgumentParser(description='Test SELECT FOR UPDATE concurrency')
    parser.add_argument('--user', type=str, default="root")
    parser.add_argument('--password', type=str, default="")
    args = parser.parse_args()

    db = get_connection(user=args.user, password=args.password)
    cursor = db.cursor()
    
    sys.exit(test_for_update(db, cursor, args))

if __name__ == "__main__":
    main()

