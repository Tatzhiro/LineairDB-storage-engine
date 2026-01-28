#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import argparse
import random
from datetime import datetime
from decimal import Decimal
import mysql.connector

# =========================
# Helpers
# =========================

def reset(db, cursor, dbname):
    cursor.execute(f'DROP DATABASE IF EXISTS {dbname}')
    cursor.execute(f'CREATE DATABASE {dbname}')
    cursor.execute(f'USE {dbname}')
    db.commit()

def setup_schema(db, cursor, dbname, engine):
    cursor.execute(f'USE {dbname}')

    # Drop in dependency order
    drops = [
        "bmsql_order_line", "bmsql_new_order", "bmsql_oorder", "bmsql_history",
        "bmsql_stock", "bmsql_item", "bmsql_customer", "bmsql_district", "bmsql_warehouse"
    ]
    for t in drops:
        cursor.execute(f"DROP TABLE IF EXISTS {t}")

    # Minimal DDL (minimum required; add columns as needed)
    cursor.execute(f'''
    CREATE TABLE bmsql_warehouse (
      w_id       INT NOT NULL,
      w_ytd      DECIMAL(12,2)  NOT NULL,
      w_tax      DECIMAL(4,4)   NOT NULL,
      w_name     VARCHAR(10) NOT NULL,
      PRIMARY KEY (w_id)
    ) ENGINE={engine}
    ''')

    cursor.execute(f'''
    CREATE TABLE bmsql_district (
      d_w_id       INT NOT NULL,
      d_id         INT NOT NULL,
      d_ytd        DECIMAL(12,2)  NOT NULL,
      d_tax        DECIMAL(4,4)   NOT NULL,
      d_next_o_id  INT NOT NULL,
      d_name       VARCHAR(10) NOT NULL,
      PRIMARY KEY (d_w_id, d_id)
    ) ENGINE={engine}
    ''')

    # Define indexes inside CREATE TABLE (safe in MySQL)
    cursor.execute(f'''
    CREATE TABLE bmsql_customer (
      c_w_id         INT NOT NULL,
      c_d_id         INT NOT NULL,
      c_id           INT NOT NULL,
      c_discount     DECIMAL(4,4)   NOT NULL,
      c_credit       CHAR(2)       NOT NULL,
      c_last         VARCHAR(16) NOT NULL,
      c_first        VARCHAR(16) NOT NULL,
      c_credit_lim   DECIMAL(12,2)  NOT NULL,
      c_balance      DECIMAL(12,2)  NOT NULL,
      c_ytd_payment  DECIMAL(12,2)  NOT NULL,
      c_payment_cnt  INT            NOT NULL,
      c_delivery_cnt INT            NOT NULL,
      c_data         VARCHAR(500) NOT NULL,
      PRIMARY KEY (c_w_id, c_d_id, c_id),
      INDEX bmsql_customer_idx1 (c_w_id, c_d_id, c_last, c_first)
    ) ENGINE={engine}
    ''')

    cursor.execute(f'''
    CREATE TABLE bmsql_item (
      i_id    INT NOT NULL,
      i_name  VARCHAR(24) NOT NULL,
      i_price DECIMAL(7,2)  NOT NULL,
      i_data  VARCHAR(50) NOT NULL,
      i_im_id INT NOT NULL,
      PRIMARY KEY (i_id)
    ) ENGINE={engine}
    ''')

    cursor.execute(f'''
    CREATE TABLE bmsql_stock (
      s_w_id       INT NOT NULL,
      s_i_id       INT NOT NULL,
      s_quantity   INT            NOT NULL,
      s_ytd        INT            NOT NULL,
      s_order_cnt  INT            NOT NULL,
      s_remote_cnt INT            NOT NULL,
      s_data       VARCHAR(50) NOT NULL,
      s_dist_01    CHAR(24) NOT NULL,
      s_dist_02    CHAR(24) NOT NULL,
      s_dist_03    CHAR(24) NOT NULL,
      s_dist_04    CHAR(24) NOT NULL,
      s_dist_05    CHAR(24) NOT NULL,
      s_dist_06    CHAR(24) NOT NULL,
      s_dist_07    CHAR(24) NOT NULL,
      s_dist_08    CHAR(24) NOT NULL,
      s_dist_09    CHAR(24) NOT NULL,
      s_dist_10    CHAR(24) NOT NULL,
      PRIMARY KEY (s_w_id, s_i_id)
    ) ENGINE={engine}
    ''')

    cursor.execute(f'''
    CREATE TABLE bmsql_oorder (
      o_w_id       INT NOT NULL,
      o_d_id       INT NOT NULL,
      o_id         INT NOT NULL,
      o_c_id       INT NOT NULL,
      o_carrier_id INT NULL,
      o_ol_cnt     INT NOT NULL,
      o_all_local  INT NOT NULL,
      o_entry_d    DATETIME NOT NULL,
      PRIMARY KEY (o_w_id, o_d_id, o_id)
    ) ENGINE={engine}
    ''')

    cursor.execute(f'''
    CREATE TABLE bmsql_new_order (
      no_w_id INT NOT NULL,
      no_d_id INT NOT NULL,
      no_o_id INT NOT NULL,
      PRIMARY KEY (no_w_id, no_d_id, no_o_id)
    ) ENGINE={engine}
    ''')

    cursor.execute(f'''
    CREATE TABLE bmsql_order_line (
      ol_w_id        INT NOT NULL,
      ol_d_id        INT NOT NULL,
      ol_o_id        INT NOT NULL,
      ol_number      INT NOT NULL,
      ol_i_id        INT NOT NULL,
      ol_delivery_d  DATETIME NULL,
      ol_amount      DECIMAL(12,2)  NOT NULL,
      ol_supply_w_id INT NOT NULL,
      ol_quantity    INT NOT NULL,
      ol_dist_info   CHAR(24) NOT NULL,
      PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number)
    ) ENGINE={engine}
    ''')

    cursor.execute(f'''
    CREATE TABLE bmsql_history (
      hist_id  BIGINT NOT NULL AUTO_INCREMENT,
      h_c_id   INT NOT NULL,
      h_c_d_id INT NOT NULL,
      h_c_w_id INT NOT NULL,
      h_d_id   INT NOT NULL,
      h_w_id   INT NOT NULL,
      h_date   DATETIME NOT NULL,
      h_amount DECIMAL(12,2) NOT NULL,
      h_data   VARCHAR(50) NOT NULL,
      PRIMARY KEY (hist_id)
    ) ENGINE={engine}
    ''')
    db.commit()


def populate_sample_data(db, cursor, dbname, num_items=20):
    cursor.execute(f'USE {dbname}')

    # Warehouse
    cursor.execute(
        '''
        INSERT INTO bmsql_warehouse
          (w_id, w_ytd, w_tax, w_name)
        VALUES (%s, %s, %s, %s)
        ''',
        (1, Decimal('0.00'), Decimal('0.1000'), 'TEST WH')
    )

    # District
    cursor.execute(
        '''
        INSERT INTO bmsql_district
          (d_w_id, d_id, d_ytd, d_tax, d_next_o_id, d_name)
        VALUES (%s, %s, %s, %s, %s, %s)
        ''',
        (1, 1, Decimal('0.00'), Decimal('0.0500'), 3001, 'DISTRICT1')
    )

    # Customer
    cursor.execute(
        '''
        INSERT INTO bmsql_customer
          (c_w_id, c_d_id, c_id, c_discount, c_credit, c_last, c_first,
           c_credit_lim, c_balance, c_ytd_payment, c_payment_cnt,
           c_delivery_cnt, c_data)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''',
        (
            1,
            1,
            1,
            Decimal('0.0500'),
            'GC',
            'BARBARBAR',
            'ALICE',
            Decimal('50000.00'),
            Decimal('0.00'),
            Decimal('10.00'),
            0,
            0,
            'CUSTOMER-DATA' * 20,
        ),
    )

    # Items
    item_rows = []
    for i in range(1, num_items + 1):
        price = Decimal('10.00') + Decimal(str(i))
        item_rows.append((i, f'Item-{i:05d}', price, f'Data-{i:05d}', i))
    cursor.executemany(
        '''
        INSERT INTO bmsql_item
          (i_id, i_name, i_price, i_data, i_im_id)
        VALUES (%s, %s, %s, %s, %s)
        ''',
        item_rows,
    )

    # Stock
    stock_rows = []
    for i in range(1, num_items + 1):
        dist = [f'DIST-{i:05d}-{d:02d}'.ljust(24)[:24] for d in range(1, 11)]
        stock_rows.append(
            (
                1,
                i,
                100,
                0,
                0,
                0,
                f'STOCK-DATA-{i:05d}',
                *dist,
            )
        )
    cursor.executemany(
        '''
        INSERT INTO bmsql_stock
          (s_w_id, s_i_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt, s_data,
           s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05,
           s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10)
        VALUES (%s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s)
        ''',
        stock_rows,
    )

    db.commit()


def clear_tpcc_data(db, cursor, dbname):
    cursor.execute(f'USE {dbname}')

    tables_in_dependency_order = [
        "bmsql_order_line",
        "bmsql_new_order",
        "bmsql_oorder",
        "bmsql_history",
        "bmsql_stock",
        "bmsql_item",
        "bmsql_customer",
        "bmsql_district",
        "bmsql_warehouse",
    ]
    for table in tables_in_dependency_order:
        cursor.execute(f'DELETE FROM {table}')

    db.commit()


def prepare_tpcc_environment(db, cursor, dbname, engine, num_items=20, reset_schema=False):
    if reset_schema:
        reset(db, cursor, dbname)
        setup_schema(db, cursor, dbname, engine)
    else:
        clear_tpcc_data(db, cursor, dbname)

    populate_sample_data(db, cursor, dbname, num_items)

# =========================
# TPC-C Tests
# =========================

def test_tpcc_neworder(db, cursor, dbname, num_items=5, force_invalid=False):
    """
    Simplified New-Order test:
      - verify customer and warehouse exist
      - fetch district D_NEXT_O_ID with FOR UPDATE
      - increment district D_NEXT_O_ID
      - insert into oorder
      - insert into new_order
      - for each order line, fetch item/stock (stock with FOR UPDATE), update stock, insert order_line
      - commit / rollback
    """
    print("TEST TPC-C New-Order")
    cursor.execute(f'USE {dbname}')

    try:
        cursor.execute('START TRANSACTION')
        # Parameters (fixed for tests; randomize if needed)
        w_id = 1
        d_id = 1
        c_id = 1

        # In the Java implementation, numItems is random in 5..15; here it is passed as an argument.
        o_ol_cnt = num_items
        o_all_local = 1

        # --- Fetch customer info (stmtGetCustSQL)
        cursor.execute('''
          SELECT c_discount, c_last, c_credit
            FROM bmsql_customer
           WHERE c_w_id = %s AND c_d_id = %s AND c_id = %s
        ''', (w_id, d_id, c_id))
        if cursor.fetchone() is None:
            cursor.execute('ROLLBACK')
            print("\tFailed: customer not found")
            return 1

        # --- Fetch warehouse info (stmtGetWhseSQL)
        cursor.execute('SELECT w_tax FROM bmsql_warehouse WHERE w_id = %s', (w_id,))
        if cursor.fetchone() is None:
            cursor.execute('ROLLBACK')
            print("\tFailed: warehouse not found")
            return 1

        # --- Fetch D_NEXT_O_ID from district with FOR UPDATE (stmtGetDistSQL)
        cursor.execute('SELECT d_next_o_id, d_tax FROM bmsql_district WHERE d_w_id = %s AND d_id = %s FOR UPDATE', (w_id, d_id))
        row = cursor.fetchone()
        if not row:
            cursor.execute('ROLLBACK')
            print("\tFailed: district not found")
            return 1
        d_next_o_id = int(row[0])
        # Use as o_id
        o_id = d_next_o_id

        # --- Update district (stmtUpdateDistSQL)
        cursor.execute('UPDATE bmsql_district SET d_next_o_id = d_next_o_id + 1 WHERE d_w_id = %s AND d_id = %s', (w_id, d_id))
        if cursor.rowcount == 0:
            cursor.execute('ROLLBACK')
            print("\tFailed: could not update district")
            return 1

        # --- insert open order（stmtInsertOOrderSQL）
        entry_ts = datetime.now()
        cursor.execute('''
          INSERT INTO bmsql_oorder (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local)
          VALUES (%s,%s,%s,%s,%s,%s,%s)
        ''', (o_id, d_id, w_id, c_id, entry_ts, o_ol_cnt, o_all_local))
        if cursor.rowcount == 0:
            cursor.execute('ROLLBACK')
            print("\tFailed: cannot insert oorder")
            return 1

        # --- insert new_order (stmtInsertNewOrderSQL)
        cursor.execute('INSERT INTO bmsql_new_order (no_o_id, no_d_id, no_w_id) VALUES (%s,%s,%s)', (o_id, d_id, w_id))
        if cursor.rowcount == 0:
            # Java logs a warning, but we may treat this as an error and roll back here
            cursor.execute('ROLLBACK')
            print("\tFailed: cannot insert new_order")
            return 1

        # --- Prepare item/stock data (random or specified by args in this test)
        itemIDs = []
        supplierWarehouseIDs = []
        orderQuantities = []
        for i in range(o_ol_cnt):
            # Must use existing i_id in real data; this test expects 1..N
            if force_invalid and i == o_ol_cnt - 1:
                itemIDs.append(99999999)  # Force a missing ID to test rollback
            else:
                itemIDs.append(random.randint(1, 20))  # Match the item range in the real DB
            supplierWarehouseIDs.append(w_id)  # Single-warehouse scenario
            orderQuantities.append(random.randint(1, 10))

        # --- Process each OL (stmtGetItemSQL, stmtGetStockSQL, stmtUpdateStockSQL, stmtInsertOrderLineSQL)
        ol_number = 0
        for ol_number in range(1, o_ol_cnt + 1):
            ol_i_id = itemIDs[ol_number - 1]
            ol_supply_w_id = supplierWarehouseIDs[ol_number - 1]
            ol_quantity = orderQuantities[ol_number - 1]

            # getItem: SELECT I_PRICE, I_NAME, I_DATA FROM item WHERE i_id = ?
            cursor.execute('SELECT i_price, i_name, i_data FROM bmsql_item WHERE i_id = %s', (ol_i_id,))
            item_row = cursor.fetchone()
            if not item_row:
                # Java throws UserAbortException to roll back (expected 1% rollback)
                cursor.execute('ROLLBACK')
                print(f"\tExpected rollback: item {ol_i_id} not found")
                return 0  # expected rollback scenario -> test passes as rollback
            i_price = Decimal(str(item_row[0]))

            ol_amount = Decimal(ol_quantity) * i_price

            # getStock FOR UPDATE
            cursor.execute('''
                SELECT s_quantity, s_data, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05,
                       s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10, s_remote_cnt
                  FROM bmsql_stock
                 WHERE s_i_id = %s AND s_w_id = %s
                 FOR UPDATE
            ''', (ol_i_id, ol_supply_w_id))
            srow = cursor.fetchone()
            if not srow:
                cursor.execute('ROLLBACK')
                print(f"\tFailed: stock s_i_id={ol_i_id} s_w_id={ol_supply_w_id} not found")
                return 1
            s_quantity = int(srow[0])
            # dist info: pick column based on d_id (1..10)
            dist_info = None
            if 1 <= d_id <= 10:
                dist_info = srow[2 + (d_id - 1)]  # s_dist_01 starts at index 2

            # s_quantity adjust as in Java:
            if s_quantity - ol_quantity >= 10:
                new_s_quantity = s_quantity - ol_quantity
            else:
                new_s_quantity = s_quantity - ol_quantity + 91

            s_remote_cnt_increment = 0 if ol_supply_w_id == w_id else 1

            # update stock
            cursor.execute('''
              UPDATE bmsql_stock
                 SET s_quantity = %s,
                     s_ytd = s_ytd + %s,
                     s_order_cnt = s_order_cnt + 1,
                     s_remote_cnt = s_remote_cnt + %s
               WHERE s_i_id = %s AND s_w_id = %s
            ''', (new_s_quantity, ol_quantity, s_remote_cnt_increment, ol_i_id, ol_supply_w_id))
            if cursor.rowcount == 0:
                cursor.execute('ROLLBACK')
                print("\tFailed: update stock affected 0 rows")
                return 1

            # insert order_line
            cursor.execute('''
              INSERT INTO bmsql_order_line
                (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info)
              VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ''', (o_id, d_id, w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, dist_info))
            if cursor.rowcount == 0:
                cursor.execute('ROLLBACK')
                print("\tFailed: insert order_line")
                return 1

        # Commit if everything succeeds
        cursor.execute('COMMIT')
        print("\tPassed!")
        return 0

    except mysql.connector.Error as err:
        print(f"\tFailed (MySQL): {err}")
        cursor.execute('ROLLBACK')
        return 1

# =========================
# Main
# =========================

def main():

    db = mysql.connector.connect(host=args.host, port=args.port, user=args.user, password=args.password)
    cursor = db.cursor()

    try:
        result = 0

        prepare_tpcc_environment(db, cursor, "ha_lineairdb_test", "LineairDB", num_items=20, reset_schema=True)
        result |= test_tpcc_neworder(db, cursor, "ha_lineairdb_test", num_items=10)

        if result == 0:
            print("\nALL TESTS PASSED!")
        else:
            print("\nSOME TESTS FAILED!")
        sys.exit(result)
    finally:
        try:
            cursor.close()
        except:
            pass
        try:
            db.close()
        except:
            pass

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Connect to MySQL')
    parser.add_argument('--host', metavar='host', type=str,
                        help='hostname of MySQL server',
                        default="localhost")
    parser.add_argument('--port', metavar='port', type=int,
                        help='port number of MySQL server',
                        default=3306)
    parser.add_argument('--user', metavar='user', type=str,
                        help='name of user',
                        default="root")
    parser.add_argument('--password', metavar='pw', type=str,
                        help='password for the user',
                        default="")
    args = parser.parse_args()
    main()
