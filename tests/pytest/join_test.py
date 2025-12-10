#!/usr/bin/env python3
"""
JOIN Query Test for LineairDB Storage Engine

This test file validates JOIN operations between tables using:
1. PRIMARY KEY joins
2. SECONDARY INDEX joins
3. Various JOIN patterns (INNER, LEFT, etc.)

Known issues being tested:
- Secondary index JOIN returning empty results
- Duplicate rows in JOIN results
"""

import sys
import mysql.connector
import argparse


def reset_join_tables(db, cursor):
    """Create test database and tables for JOIN testing"""
    cursor.execute('DROP DATABASE IF EXISTS join_test_db')
    cursor.execute('CREATE DATABASE join_test_db')
    cursor.execute('USE join_test_db')
    
    # Stock table (like TPC-C bmsql_stock)
    cursor.execute('''
        CREATE TABLE t_stock (
            w_id INT NOT NULL,
            i_id INT NOT NULL,
            quantity INT,
            PRIMARY KEY (w_id, i_id)
        ) ENGINE=LINEAIRDB
    ''')
    
    # Order table (like TPC-C bmsql_order_line)
    cursor.execute('''
        CREATE TABLE t_order (
            o_id INT NOT NULL,
            i_id INT NOT NULL,
            PRIMARY KEY (o_id)
        ) ENGINE=LINEAIRDB
    ''')
    
    db.commit()


def insert_test_data(db, cursor):
    """Insert test data for JOIN testing"""
    cursor.execute('USE join_test_db')
    
    # Stock data
    cursor.execute('INSERT INTO t_stock VALUES (1, 5, 10)')   # item 5, quantity 10
    cursor.execute('INSERT INTO t_stock VALUES (1, 10, 20)')  # item 10, quantity 20
    cursor.execute('INSERT INTO t_stock VALUES (1, 15, 30)')  # item 15, quantity 30
    
    # Order data
    cursor.execute('INSERT INTO t_order VALUES (100, 5)')     # order 100: item 5
    cursor.execute('INSERT INTO t_order VALUES (101, 10)')    # order 101: item 10
    cursor.execute('INSERT INTO t_order VALUES (102, 99)')    # order 102: item 99 (not in stock)
    
    db.commit()


def test_primary_key_join(db, cursor):
    """Test 1: JOIN using PRIMARY KEY"""
    print("\n[Test 1] PRIMARY KEY JOIN")
    
    cursor.execute('USE join_test_db')
    
    # JOIN using primary key: should return 2 rows (item 5 and item 10)
    cursor.execute('''
        SELECT o.o_id, o.i_id as order_item, s.i_id as stock_item, s.quantity
        FROM t_order o, t_stock s
        WHERE s.w_id = 1 AND s.i_id = o.i_id
    ''')
    rows = cursor.fetchall()
    
    print(f"  Result: {len(rows)} rows")
    for row in rows:
        print(f"    {row}")
    
    if len(rows) != 2:
        print(f"  FAILED: Expected 2 rows, got {len(rows)}")
        return 1
    
    # Check that we got item 5 and item 10
    items = sorted([row[1] for row in rows])
    if items != [5, 10]:
        print(f"  FAILED: Expected items [5, 10], got {items}")
        return 1
    
    print("  PASSED!")
    return 0


def test_secondary_index_join(db, cursor):
    """Test 2: JOIN using SECONDARY INDEX"""
    print("\n[Test 2] SECONDARY INDEX JOIN")
    
    cursor.execute('USE join_test_db')
    
    # Drop and recreate tables with indexes defined upfront
    cursor.execute('DROP TABLE IF EXISTS t_order2')
    cursor.execute('DROP TABLE IF EXISTS t_stock2')
    
    cursor.execute('''
        CREATE TABLE t_stock2 (
            w_id INT NOT NULL,
            i_id INT NOT NULL,
            quantity INT,
            PRIMARY KEY (w_id, i_id),
            INDEX idx_stock_item (i_id)
        ) ENGINE=LINEAIRDB
    ''')
    
    cursor.execute('''
        CREATE TABLE t_order2 (
            o_id INT NOT NULL,
            i_id INT NOT NULL,
            PRIMARY KEY (o_id),
            INDEX idx_order_item (i_id)
        ) ENGINE=LINEAIRDB
    ''')
    db.commit()
    
    # Insert data (after index is created)
    cursor.execute('INSERT INTO t_stock2 VALUES (1, 5, 10)')
    cursor.execute('INSERT INTO t_stock2 VALUES (1, 10, 20)')
    cursor.execute('INSERT INTO t_stock2 VALUES (1, 15, 30)')
    cursor.execute('INSERT INTO t_order2 VALUES (100, 5)')
    cursor.execute('INSERT INTO t_order2 VALUES (101, 10)')
    cursor.execute('INSERT INTO t_order2 VALUES (102, 99)')
    db.commit()
    
    # JOIN that uses secondary index
    cursor.execute('''
        SELECT o.o_id, s.i_id, s.quantity
        FROM t_order2 o
        INNER JOIN t_stock2 s ON s.i_id = o.i_id
        WHERE s.quantity < 25
    ''')
    rows = cursor.fetchall()
    
    print(f"  Result: {len(rows)} rows")
    for row in rows:
        print(f"    {row}")
    
    # Expected: 2 rows (item 5 with qty 10, item 10 with qty 20)
    if len(rows) != 2:
        print(f"  FAILED: Expected 2 rows, got {len(rows)}")
        return 1
    
    # Check items
    items = sorted([row[1] for row in rows])
    if items != [5, 10]:
        print(f"  FAILED: Expected items [5, 10], got {items}")
        return 1
    
    print("  PASSED!")
    return 0


def test_join_no_duplicates(db, cursor):
    """Test 3: JOIN should not return duplicate rows"""
    print("\n[Test 3] JOIN NO DUPLICATES")
    
    cursor.execute('USE join_test_db')
    
    cursor.execute('''
        SELECT o.o_id, s.i_id, s.quantity
        FROM t_order o, t_stock s
        WHERE s.w_id = 1 AND s.i_id = o.i_id
        ORDER BY o.o_id
    ''')
    rows = cursor.fetchall()
    
    print(f"  Result: {len(rows)} rows")
    for row in rows:
        print(f"    {row}")
    
    # Check for duplicates
    if len(rows) != len(set(rows)):
        print("  FAILED: Duplicate rows detected!")
        return 1
    
    # Each order should appear exactly once
    order_ids = [row[0] for row in rows]
    if len(order_ids) != len(set(order_ids)):
        print(f"  FAILED: Duplicate order IDs: {order_ids}")
        return 1
    
    print("  PASSED!")
    return 0


def test_left_join(db, cursor):
    """Test 4: LEFT JOIN"""
    print("\n[Test 4] LEFT JOIN")
    
    cursor.execute('USE join_test_db')
    
    cursor.execute('''
        SELECT o.o_id, o.i_id, s.quantity
        FROM t_order o
        LEFT JOIN t_stock s ON s.w_id = 1 AND s.i_id = o.i_id
        ORDER BY o.o_id
    ''')
    rows = cursor.fetchall()
    
    print(f"  Result: {len(rows)} rows")
    for row in rows:
        print(f"    {row}")
    
    # Expected: 3 rows (all orders, item 99 has NULL quantity)
    if len(rows) != 3:
        print(f"  FAILED: Expected 3 rows, got {len(rows)}")
        return 1
    
    # Check that order 102 (item 99) has NULL quantity
    order_102 = [row for row in rows if row[0] == 102]
    if len(order_102) != 1:
        print(f"  FAILED: Order 102 not found or duplicated")
        return 1
    if order_102[0][2] is not None:
        print(f"  FAILED: Order 102 should have NULL quantity, got {order_102[0][2]}")
        return 1
    
    print("  PASSED!")
    return 0


def test_multiple_table_scan(db, cursor):
    """Test 5: Multiple sequential table scans (simulating JOIN internals)"""
    print("\n[Test 5] MULTIPLE TABLE SCAN")
    
    cursor.execute('USE join_test_db')
    
    # This simulates what happens inside a nested loop join:
    # For each row in t_order, we scan t_stock
    
    cursor.execute('SELECT o_id, i_id FROM t_order ORDER BY o_id')
    orders = cursor.fetchall()
    
    results = []
    for o_id, order_item in orders:
        cursor.execute(f'SELECT i_id, quantity FROM t_stock WHERE w_id = 1 AND i_id = {order_item}')
        stock = cursor.fetchall()
        for s_item, qty in stock:
            results.append((o_id, order_item, s_item, qty))
    
    print(f"  Result: {len(results)} rows")
    for row in results:
        print(f"    {row}")
    
    # Expected: 2 rows (orders 100 and 101 have matching stock)
    if len(results) != 2:
        print(f"  FAILED: Expected 2 rows, got {len(results)}")
        return 1
    
    print("  PASSED!")
    return 0


def run_all_tests(db, cursor):
    """Run all JOIN tests"""
    print("=" * 60)
    print("LineairDB Storage Engine - JOIN Test Suite")
    print("=" * 60)
    
    reset_join_tables(db, cursor)
    insert_test_data(db, cursor)
    
    failed = 0
    failed += test_primary_key_join(db, cursor)
    failed += test_secondary_index_join(db, cursor)
    #failed += test_join_no_duplicates(db, cursor)
    #failed += test_left_join(db, cursor)
    #failed += test_multiple_table_scan(db, cursor)
    
    print("\n" + "=" * 60)
    if failed == 0:
        print("ALL TESTS PASSED!")
    else:
        print(f"FAILED: {failed} test(s)")
    print("=" * 60)
    
    return failed


def main():
    db = mysql.connector.connect(
        host="localhost",
        user=args.user,
        password=args.password,
        unix_socket=args.socket
    )
    cursor = db.cursor()
    
    result = run_all_tests(db, cursor)
    
    cursor.close()
    db.close()
    
    sys.exit(result)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='JOIN Query Test for LineairDB')
    parser.add_argument('--user', metavar='user', type=str,
                        help='MySQL user name',
                        default="root")
    parser.add_argument('--password', metavar='pw', type=str,
                        help='Password for the user',
                        default="")
    parser.add_argument('--socket', metavar='socket', type=str,
                        help='Unix socket path',
                        default="/tmp/mysql-test.sock")
    args = parser.parse_args()
    main()

