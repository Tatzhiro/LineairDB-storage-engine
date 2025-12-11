import sys
import mysql.connector
from utils.connection import get_connection
import argparse


def test_primary_key_exact_match(db, cursor):
    """PRIMARY KEYでの完全一致検索テスト"""
    print("PRIMARY KEY EXACT MATCH TEST")
    
    cursor.execute('DROP DATABASE IF EXISTS ha_lineairdb_test')
    cursor.execute('CREATE DATABASE ha_lineairdb_test')
    cursor.execute('''
        CREATE TABLE ha_lineairdb_test.users (
            id INT NOT NULL PRIMARY KEY,
            name VARCHAR(50) NOT NULL,
            age INT NOT NULL
        ) ENGINE=LineairDB
    ''')
    
    # テストデータ挿入
    test_data = [
        (1, 'alice', 25),
        (3, 'bob', 30),
        (5, 'carol', 28),
        (10, 'dave', 35),
        (15, 'eve', 22)
    ]
    
    for id_val, name, age in test_data:
        cursor.execute(
            f'INSERT INTO ha_lineairdb_test.users VALUES ({id_val}, "{name}", {age})'
        )
    db.commit()
    
    # 完全一致検索テスト
    print("\t完全一致検索: id=5")
    cursor.execute('SELECT * FROM ha_lineairdb_test.users WHERE id = 5')
    rows = cursor.fetchall()
    
    if len(rows) != 1 or rows[0][0] != 5 or rows[0][1] != 'carol':
        print(f"\t❌ Failed: Expected (5, 'carol', 28), got {rows}")
        return 1
    print(f"\t✅ Passed: {rows[0]}")
    
    # 存在しないキー
    print("\t存在しないキー: id=100")
    cursor.execute('SELECT * FROM ha_lineairdb_test.users WHERE id = 100')
    rows = cursor.fetchall()
    
    if len(rows) != 0:
        print(f"\t❌ Failed: Expected 0 rows, got {len(rows)}")
        return 1
    print(f"\t✅ Passed: 0 rows (correct)")
    
    # 最小値
    print("\t最小値: id=1")
    cursor.execute('SELECT * FROM ha_lineairdb_test.users WHERE id = 1')
    rows = cursor.fetchall()
    
    if len(rows) != 1 or rows[0][1] != 'alice':
        print(f"\t❌ Failed: Expected alice, got {rows}")
        return 1
    print(f"\t✅ Passed: {rows[0]}")
    
    # 最大値
    print("\t最大値: id=15")
    cursor.execute('SELECT * FROM ha_lineairdb_test.users WHERE id = 15')
    rows = cursor.fetchall()
    
    if len(rows) != 1 or rows[0][1] != 'eve':
        print(f"\t❌ Failed: Expected eve, got {rows}")
        return 1
    print(f"\t✅ Passed: {rows[0]}")
    
    return 0


def test_primary_key_range_queries(db, cursor):
    """PRIMARY KEYでの範囲検索テスト"""
    print("\nPRIMARY KEY RANGE QUERY TEST")
    
    cursor.execute('DROP DATABASE IF EXISTS ha_lineairdb_test')
    cursor.execute('CREATE DATABASE ha_lineairdb_test')
    cursor.execute('''
        CREATE TABLE ha_lineairdb_test.products (
            id INT NOT NULL PRIMARY KEY,
            name VARCHAR(50) NOT NULL,
            price INT NOT NULL
        ) ENGINE=LineairDB
    ''')
    
    # テストデータ挿入
    test_data = [
        (1, 'product_a', 100),
        (5, 'product_b', 200),
        (10, 'product_c', 300),
        (15, 'product_d', 400),
        (20, 'product_e', 500),
        (25, 'product_f', 600),
        (30, 'product_g', 700)
    ]
    
    for id_val, name, price in test_data:
        cursor.execute(
            f'INSERT INTO ha_lineairdb_test.products VALUES ({id_val}, "{name}", {price})'
        )
    db.commit()
    
    print("\t全データ:")
    cursor.execute('SELECT * FROM ha_lineairdb_test.products')
    all_rows = cursor.fetchall()
    for row in all_rows:
        print(f"\t  {row}")
    
    # 範囲検索: id > 15
    print("\n\t範囲検索: id > 15")
    cursor.execute('SELECT id, name FROM ha_lineairdb_test.products WHERE id > 15')
    rows = cursor.fetchall()
    print(f"\t  結果: {rows}")
    
    expected_ids = [20, 25, 30]
    result_ids = [row[0] for row in rows]
    if result_ids != expected_ids:
        print(f"\t❌ Failed: Expected {expected_ids}, got {result_ids}")
        return 1
    print(f"\t✅ Passed")
    
    # 範囲検索: id < 15
    print("\n\t範囲検索: id < 15")
    cursor.execute('SELECT id, name FROM ha_lineairdb_test.products WHERE id < 15')
    rows = cursor.fetchall()
    print(f"\t  結果: {rows}")
    
    expected_ids = [1, 5, 10]
    result_ids = [row[0] for row in rows]
    if result_ids != expected_ids:
        print(f"\t❌ Failed: Expected {expected_ids}, got {result_ids}")
        return 1
    print(f"\t✅ Passed")
    
    # 範囲検索: id >= 10 AND id <= 20
    print("\n\t範囲検索: id >= 10 AND id <= 20")
    cursor.execute('SELECT id, name FROM ha_lineairdb_test.products WHERE id >= 10 AND id <= 20')
    rows = cursor.fetchall()
    print(f"\t  結果: {rows}")
    
    expected_ids = [10, 15, 20]
    result_ids = [row[0] for row in rows]
    if result_ids != expected_ids:
        print(f"\t❌ Failed: Expected {expected_ids}, got {result_ids}")
        return 1
    print(f"\t✅ Passed")
    
    # BETWEEN
    print("\n\t範囲検索: id BETWEEN 5 AND 15")
    cursor.execute('SELECT id, name FROM ha_lineairdb_test.products WHERE id BETWEEN 5 AND 15')
    rows = cursor.fetchall()
    print(f"\t  結果: {rows}")
    
    expected_ids = [5, 10, 15]
    result_ids = [row[0] for row in rows]
    if result_ids != expected_ids:
        print(f"\t❌ Failed: Expected {expected_ids}, got {result_ids}")
        return 1
    print(f"\t✅ Passed")
    
    return 0




def test_primary_key_exclusive_range(db, cursor):
    """PRIMARY KEY exclusive range boundary test (< and >)"""
    print("\nPRIMARY KEY EXCLUSIVE RANGE BOUNDARY TEST")
    
    cursor.execute('DROP DATABASE IF EXISTS ha_lineairdb_test')
    cursor.execute('CREATE DATABASE ha_lineairdb_test')
    cursor.execute('''
        CREATE TABLE ha_lineairdb_test.items (
            id INT NOT NULL PRIMARY KEY,
            name VARCHAR(50) NOT NULL
        ) ENGINE=LineairDB
    ''')
    
    # Insert values 1, 2, 3, 4, 5
    for i in range(1, 6):
        cursor.execute(f'INSERT INTO ha_lineairdb_test.items VALUES ({i}, "item_{i}")')
    db.commit()
    
    # Test: id < 3 should return 1, 2 (NOT 3)
    print("\t範囲検索: id < 3 (exclusive)")
    cursor.execute('SELECT id FROM ha_lineairdb_test.items WHERE id < 3 ORDER BY id')
    rows = cursor.fetchall()
    result_ids = [row[0] for row in rows]
    
    if result_ids != [1, 2]:
        print(f"\t❌ Failed: Expected [1, 2], got {result_ids}")
        if 3 in result_ids:
            print("\t*** BUG: end_range->flag (HA_READ_BEFORE_KEY) not honored! ***")
        return 1
    print(f"\t✅ Passed: {result_ids}")
    
    # Test: id > 3 should return 4, 5 (NOT 3)
    print("\t範囲検索: id > 3 (exclusive)")
    cursor.execute('SELECT id FROM ha_lineairdb_test.items WHERE id > 3 ORDER BY id')
    rows = cursor.fetchall()
    result_ids = [row[0] for row in rows]
    
    if result_ids != [4, 5]:
        print(f"\t❌ Failed: Expected [4, 5], got {result_ids}")
        if 3 in result_ids:
            print("\t*** BUG: find_flag (HA_READ_AFTER_KEY) not working! ***")
        return 1
    print(f"\t✅ Passed: {result_ids}")
    
    # Test: id <= 3 should return 1, 2, 3
    print("\t範囲検索: id <= 3 (inclusive)")
    cursor.execute('SELECT id FROM ha_lineairdb_test.items WHERE id <= 3 ORDER BY id')
    rows = cursor.fetchall()
    result_ids = [row[0] for row in rows]
    
    if result_ids != [1, 2, 3]:
        print(f"\t❌ Failed: Expected [1, 2, 3], got {result_ids}")
        return 1
    print(f"\t✅ Passed: {result_ids}")
    
    # Test: id >= 3 should return 3, 4, 5
    print("\t範囲検索: id >= 3 (inclusive)")
    cursor.execute('SELECT id FROM ha_lineairdb_test.items WHERE id >= 3 ORDER BY id')
    rows = cursor.fetchall()
    result_ids = [row[0] for row in rows]
    
    if result_ids != [3, 4, 5]:
        print(f"\t❌ Failed: Expected [3, 4, 5], got {result_ids}")
        return 1
    print(f"\t✅ Passed: {result_ids}")
    
    # Test: 2 < id < 4 should return only 3
    print("\t範囲検索: 2 < id < 4 (both exclusive)")
    cursor.execute('SELECT id FROM ha_lineairdb_test.items WHERE id > 2 AND id < 4 ORDER BY id')
    rows = cursor.fetchall()
    result_ids = [row[0] for row in rows]
    
    if result_ids != [3]:
        print(f"\t❌ Failed: Expected [3], got {result_ids}")
        return 1
    print(f"\t✅ Passed: {result_ids}")
    
    return 0


def test_composite_primary_key_exclusive_range(db, cursor):
    """Composite PRIMARY KEY exclusive range boundary test"""
    print("\nCOMPOSITE PRIMARY KEY EXCLUSIVE RANGE TEST")
    
    cursor.execute('DROP DATABASE IF EXISTS ha_lineairdb_test')
    cursor.execute('CREATE DATABASE ha_lineairdb_test')
    cursor.execute('''
        CREATE TABLE ha_lineairdb_test.sales (
            year INT NOT NULL,
            month INT NOT NULL,
            amount INT NOT NULL,
            PRIMARY KEY (year, month)
        ) ENGINE=LineairDB
    ''')
    
    # Insert test data
    test_data = [
        (2024, 1, 100),
        (2024, 3, 200),
        (2024, 6, 300),
        (2024, 9, 400),
        (2024, 12, 500),
    ]
    
    for year, month, amount in test_data:
        cursor.execute(f'INSERT INTO ha_lineairdb_test.sales VALUES ({year}, {month}, {amount})')
    db.commit()
    
    # Test: year=2024 AND month < 6 should return months 1, 3 (NOT 6)
    print("\t複合キー範囲: year=2024 AND month < 6")
    cursor.execute('SELECT month FROM ha_lineairdb_test.sales WHERE year = 2024 AND month < 6 ORDER BY month')
    rows = cursor.fetchall()
    result_months = [row[0] for row in rows]
    
    if result_months != [1, 3]:
        print(f"\t❌ Failed: Expected [1, 3], got {result_months}")
        if 6 in result_months:
            print("\t*** BUG: month=6 should be excluded! ***")
        return 1
    print(f"\t✅ Passed: {result_months}")
    
    # Test: year=2024 AND month > 6 should return months 9, 12 (NOT 6)
    print("\t複合キー範囲: year=2024 AND month > 6")
    cursor.execute('SELECT month FROM ha_lineairdb_test.sales WHERE year = 2024 AND month > 6 ORDER BY month')
    rows = cursor.fetchall()
    result_months = [row[0] for row in rows]
    
    if result_months != [9, 12]:
        print(f"\t❌ Failed: Expected [9, 12], got {result_months}")
        if 6 in result_months:
            print("\t*** BUG: month=6 should be excluded! ***")
        return 1
    print(f"\t✅ Passed: {result_months}")
    
    # Test: year=2024 AND month <= 6 should return months 1, 3, 6
    print("\t複合キー範囲: year=2024 AND month <= 6")
    cursor.execute('SELECT month FROM ha_lineairdb_test.sales WHERE year = 2024 AND month <= 6 ORDER BY month')
    rows = cursor.fetchall()
    result_months = [row[0] for row in rows]
    
    if result_months != [1, 3, 6]:
        print(f"\t❌ Failed: Expected [1, 3, 6], got {result_months}")
        return 1
    print(f"\t✅ Passed: {result_months}")
    
    # Test: year=2024 AND month >= 6 should return months 6, 9, 12
    print("\t複合キー範囲: year=2024 AND month >= 6")
    cursor.execute('SELECT month FROM ha_lineairdb_test.sales WHERE year = 2024 AND month >= 6 ORDER BY month')
    rows = cursor.fetchall()
    result_months = [row[0] for row in rows]
    
    if result_months != [6, 9, 12]:
        print(f"\t❌ Failed: Expected [6, 9, 12], got {result_months}")
        return 1
    print(f"\t✅ Passed: {result_months}")
    
    # Test: year=2024 AND 3 < month < 9 should return only 6
    print("\t複合キー範囲: year=2024 AND 3 < month < 9")
    cursor.execute('SELECT month FROM ha_lineairdb_test.sales WHERE year = 2024 AND month > 3 AND month < 9 ORDER BY month')
    rows = cursor.fetchall()
    result_months = [row[0] for row in rows]
    
    if result_months != [6]:
        print(f"\t❌ Failed: Expected [6], got {result_months}")
        return 1
    print(f"\t✅ Passed: {result_months}")
    
    return 0


def test_primary_key_composite(db, cursor):
    """複合PRIMARY KEYテスト"""
    print("\nCOMPOSITE PRIMARY KEY TEST")
    
    cursor.execute('DROP DATABASE IF EXISTS ha_lineairdb_test')
    cursor.execute('CREATE DATABASE ha_lineairdb_test')
    
    # 複合PRIMARY KEY
    cursor.execute('''
        CREATE TABLE ha_lineairdb_test.order_items (
            order_id INT NOT NULL,
            item_id INT NOT NULL,
            quantity INT NOT NULL,
            PRIMARY KEY (order_id, item_id)
        ) ENGINE=LineairDB
    ''')
    
    # テストデータ挿入
    test_data = [
        (1, 1, 10),
        (1, 2, 20),
        (1, 3, 15),
        (2, 1, 5),
        (2, 2, 25),
        (3, 1, 30)
    ]
    
    for order_id, item_id, quantity in test_data:
        cursor.execute(
            f'INSERT INTO ha_lineairdb_test.order_items VALUES ({order_id}, {item_id}, {quantity})'
        )
    db.commit()
    
    print("\t全データ:")
    cursor.execute('SELECT * FROM ha_lineairdb_test.order_items')
    all_rows = cursor.fetchall()
    for row in all_rows:
        print(f"\t  {row}")
    
    # 複合キー完全一致
    print("\n\t複合キー完全一致: order_id=1 AND item_id=2")
    cursor.execute('SELECT * FROM ha_lineairdb_test.order_items WHERE order_id = 1 AND item_id = 2')
    rows = cursor.fetchall()
    print(f"\t  結果: {rows}")
    
    if len(rows) != 1 or rows[0][2] != 20:
        print(f"\t❌ Failed: Expected quantity=20, got {rows}")
        return 1
    print(f"\t✅ Passed")
    
    # 複合キー前方一致
    print("\n\t複合キー前方一致: order_id=1")
    cursor.execute('SELECT * FROM ha_lineairdb_test.order_items WHERE order_id = 1')
    rows = cursor.fetchall()
    print(f"\t  結果: {rows}")
    
    if len(rows) != 3:
        print(f"\t❌ Failed: Expected 3 rows, got {len(rows)}")
        return 1
    print(f"\t✅ Passed")
    
    return 0


def main():
    db = get_connection(user=args.user, password=args.password)
    cursor = db.cursor()
    
    result = 0
    
    # 各テストを実行
    result |= test_primary_key_exact_match(db, cursor)
    result |= test_primary_key_range_queries(db, cursor)
    result |= test_primary_key_exclusive_range(db, cursor)
    result |= test_composite_primary_key_exclusive_range(db, cursor)
    result |= test_primary_key_composite(db, cursor)
    
    if result == 0:
        print("\n" + "="*50)
        print("🎉 ALL PRIMARY KEY TESTS PASSED!")
        print("="*50)
    else:
        print("\n" + "="*50)
        print("❌ SOME TESTS FAILED")
        print("="*50)
    
    sys.exit(result)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='PRIMARY KEY comprehensive tests for LineairDB')
    parser.add_argument('--user', metavar='user', type=str,
                        help='MySQL user name',
                        default="root")
    parser.add_argument('--password', metavar='pw', type=str,
                        help='MySQL password',
                        default="")
    args = parser.parse_args()
    main()

