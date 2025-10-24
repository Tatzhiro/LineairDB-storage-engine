import sys
import mysql.connector
from utils.reset import reset
import argparse

def test_insert_with_secondary_index(db, cursor):
    """セカンダリインデックスを持つテーブルへの挿入テスト"""
    reset(db, cursor)
    print("INSERT WITH SECONDARY INDEX TEST")
    
    cursor.execute(
        'INSERT INTO ha_lineairdb_test.items (\
            title, content\
        ) VALUES ("alice", "alice meets bob")'
    )
    cursor.execute(
        'INSERT INTO ha_lineairdb_test.items (\
            title, content\
        ) VALUES ("bob", "bob meets carol")'
    )
    cursor.execute(
        'INSERT INTO ha_lineairdb_test.items (\
            title, content\
        ) VALUES ("carol", "carol meets dave")'
    )
    db.commit()
    
    # セカンダリインデックスを使った検索で確認
    cursor.execute('SELECT title FROM ha_lineairdb_test.items WHERE title = "alice"')
    rows = cursor.fetchall()
    if not rows or rows[0][0] != "alice":
        print("\tFailed: alice not found")
        return 1
    
    print("\tPassed!")
    return 0

def test_select_with_secondary_index(db, cursor):
    """セカンダリインデックスを使った検索テスト"""
    reset(db, cursor)
    print("SELECT WITH SECONDARY INDEX TEST")
    
    # テストデータを挿入
    cursor.execute('INSERT INTO ha_lineairdb_test.items (title, content) VALUES ("alice", "data1")')
    cursor.execute('INSERT INTO ha_lineairdb_test.items (title, content) VALUES ("bob", "data2")')
    cursor.execute('INSERT INTO ha_lineairdb_test.items (title, content) VALUES ("carol", "data3")')
    db.commit()
    
    # セカンダリインデックスを使った検索
    cursor.execute('SELECT * FROM ha_lineairdb_test.items WHERE title = "bob"')
    rows = cursor.fetchall()
    if not rows or rows[0][0] != "bob":
        print("\tFailed: bob not found")
        print("\t", rows)
        return 1
    
    # 複数行の検索
    cursor.execute('SELECT title, content FROM ha_lineairdb_test.items WHERE title IN ("alice", "carol")')
    rows = cursor.fetchall()
    if len(rows) != 2:
        print("\tFailed: expected 2 rows")
        print("\t", rows)
        return 1
    
    titles = [row[0] for row in rows]
    if "alice" not in titles or "carol" not in titles:
        print("\tFailed: alice or carol not found")
        print("\t", rows)
        return 1
    
    print("\tPassed!")
    return 0

def test_range_query_with_secondary_index(db, cursor):
    """セカンダリインデックスを使った範囲検索テスト"""
    reset(db, cursor)
    print("RANGE QUERY WITH SECONDARY INDEX TEST")
    
    # 数値のセカンダリインデックスを持つテーブルを作成
    cursor.execute('CREATE TABLE ha_lineairdb_test.scores (\
        id INT NOT NULL PRIMARY KEY,\
        name VARCHAR(50) NOT NULL,\
        score INT NOT NULL,\
        INDEX score_idx (score)\
    ) ENGINE = LineairDB')
    db.commit()
    
    # テストデータを挿入
    cursor.execute('INSERT INTO ha_lineairdb_test.scores (id, name, score) VALUES (1, "alice", 85)')
    cursor.execute('INSERT INTO ha_lineairdb_test.scores (id, name, score) VALUES (2, "bob", 92)')
    cursor.execute('INSERT INTO ha_lineairdb_test.scores (id, name, score) VALUES (3, "carol", 78)')
    cursor.execute('INSERT INTO ha_lineairdb_test.scores (id, name, score) VALUES (4, "dave", 95)')
    db.commit()
    
    # 範囲検索
    cursor.execute('SELECT name FROM ha_lineairdb_test.scores WHERE score >= 90')
    rows = cursor.fetchall()
    if len(rows) != 2:
        print("\tFailed: expected 2 rows with score >= 90")
        print("\t", rows)
        return 1
    
    names = [row[0] for row in rows]
    if "bob" not in names or "dave" not in names:
        print("\tFailed: bob and dave should be found")
        print("\t", rows)
        return 1
    
    print("\tPassed!")
    return 0

def main():
    # test
    db = mysql.connector.connect(host="localhost", user=args.user, password=args.password)
    cursor = db.cursor()
    
    result = 0
    result |= test_insert_with_secondary_index(db, cursor)
    result |= test_select_with_secondary_index(db, cursor)
    result |= test_range_query_with_secondary_index(db, cursor)
    
    if result == 0:
        print("\nALL TESTS PASSED!")
    else:
        print("\nSOME TESTS FAILED!")
    
    cursor.close()
    db.close()
    
    sys.exit(result)


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

