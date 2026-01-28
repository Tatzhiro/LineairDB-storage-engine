#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import argparse
from datetime import datetime
from decimal import Decimal
import random
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


    cursor.execute(f'''
    CREATE TABLE bmsql_warehouse (
      w_id       INT NOT NULL,
      w_ytd      DECIMAL(12,2) NOT NULL,
      w_tax      DECIMAL(4,4)  NOT NULL,
      w_name     VARCHAR(10) NOT NULL,
      w_street_1 VARCHAR(20) NOT NULL,
      w_street_2 VARCHAR(20) NOT NULL,
      w_city     VARCHAR(20) NOT NULL,
      w_state    CHAR(2) NOT NULL,
      w_zip      CHAR(9) NOT NULL,
      PRIMARY KEY (w_id)
    ) ENGINE={engine}
    ''')

    cursor.execute(f'''
    CREATE TABLE bmsql_district (
      d_w_id       INT NOT NULL,
      d_id         INT NOT NULL,
      d_ytd        DECIMAL(12,2) NOT NULL,
      d_tax        DECIMAL(4,4)  NOT NULL,
      d_next_o_id  INT NOT NULL,
      d_name       VARCHAR(10) NOT NULL,
      d_street_1   VARCHAR(20) NOT NULL,
      d_street_2   VARCHAR(20) NOT NULL,
      d_city       VARCHAR(20) NOT NULL,
      d_state      CHAR(2) NOT NULL,
      d_zip        CHAR(9) NOT NULL,
      PRIMARY KEY (d_w_id, d_id)
    ) ENGINE={engine}
    ''')

    # Define indexes inside CREATE TABLE (safe in MySQL)
    cursor.execute(f'''
    CREATE TABLE bmsql_customer (
      c_w_id         INT NOT NULL,
      c_d_id         INT NOT NULL,
      c_id           INT NOT NULL,
      c_discount     DECIMAL(4,4)  NOT NULL,
      c_credit       CHAR(2)       NOT NULL,
      c_last         VARCHAR(16) NOT NULL,
      c_first        VARCHAR(16) NOT NULL,
      c_credit_lim   DECIMAL(12,2) NOT NULL,
      c_balance      DECIMAL(12,2) NOT NULL,
      c_ytd_payment  REAL NOT NULL,
      c_payment_cnt  INT           NOT NULL,
      c_delivery_cnt INT           NOT NULL,
      c_street_1     VARCHAR(20) NOT NULL,
      c_street_2     VARCHAR(20) NOT NULL,
      c_city         VARCHAR(20) NOT NULL,
      c_state        CHAR(2) NOT NULL,
      c_zip          CHAR(9) NOT NULL,
      c_phone        CHAR(16) NOT NULL,
      c_since        DATETIME NULL,
      c_middle       CHAR(2) NOT NULL,
      c_data         VARCHAR(500) NOT NULL,
      PRIMARY KEY (c_w_id, c_d_id, c_id),
      INDEX bmsql_customer_idx1 (c_w_id, c_d_id, c_last, c_first)
    ) ENGINE={engine}
    ''')

    cursor.execute(f'''
    CREATE TABLE bmsql_item (
      i_id    INT NOT NULL,
      i_name  VARCHAR(24) NOT NULL,
      i_price DECIMAL(5,2) NOT NULL,
      i_data  VARCHAR(50) NOT NULL,
      i_im_id INT NOT NULL,
      PRIMARY KEY (i_id)
    ) ENGINE={engine}
    ''')

    cursor.execute(f'''
    CREATE TABLE bmsql_stock (
      s_w_id       INT NOT NULL,
      s_i_id       INT NOT NULL,
      s_quantity   DECIMAL(4,0) NOT NULL,
      s_ytd        DECIMAL(8,2) NOT NULL,
      s_order_cnt  INT           NOT NULL,
      s_remote_cnt INT           NOT NULL,
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
      o_ol_cnt     DECIMAL(2,0) NOT NULL,
      o_all_local  DECIMAL(1,0) NOT NULL,
      o_entry_d    DATETIME NULL,
      PRIMARY KEY (o_w_id, o_d_id, o_id),
      UNIQUE (o_w_id, o_d_id, o_c_id, o_id)
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
      ol_amount      DECIMAL(6,2) NOT NULL,
      ol_supply_w_id INT NOT NULL,
      ol_quantity    DECIMAL(2,0) NOT NULL,
      ol_dist_info   CHAR(24) NOT NULL,
      PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number)
    ) ENGINE={engine}
    ''')

    cursor.execute(f'''
    CREATE TABLE bmsql_history (
      h_c_id   INT NOT NULL,
      h_c_d_id INT NOT NULL,
      h_c_w_id INT NOT NULL,
      h_d_id   INT NOT NULL,
      h_w_id   INT NOT NULL,
      h_date   DATETIME NULL,
      h_amount DECIMAL(6,2) NOT NULL,
      h_data   VARCHAR(24) NOT NULL
    ) ENGINE={engine}
    ''')
    db.commit()
    return 0


def populate_delivery_fixture(db, cursor, dbname):
    cursor.execute(f'USE {dbname}')

    warehouse_rows = [
        (
            1,
            Decimal('0.00'),
            Decimal('0.0700'),
            'WARE1',
            'STREET1',
            'STREET2',
            'CITY',
            'ST',
            '123456789',
        )
    ]
    cursor.executemany(
        '''
        INSERT INTO bmsql_warehouse
          (w_id, w_ytd, w_tax, w_name, w_street_1, w_street_2, w_city, w_state, w_zip)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ''',
        warehouse_rows,
    )

    district_rows = [
        (
            1,
            d_id,
            Decimal('0.00'),
            Decimal('0.0500'),
            3001,
            f'DIST{d_id}',
            'D_STREET1',
            'D_STREET2',
            'D_CITY',
            'DS',
            '987654321',
        )
        for d_id in range(1, 3)
    ]
    cursor.executemany(
        '''
        INSERT INTO bmsql_district
          (d_w_id, d_id, d_ytd, d_tax, d_next_o_id, d_name,
           d_street_1, d_street_2, d_city, d_state, d_zip)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ''',
        district_rows,
    )

    customer_rows = [
        (
            1,
            1,
            1,
            Decimal('0.0500'),
            'GC',
            'BARBARBAR',
            'ALICE',
            Decimal('50000.00'),
            Decimal('10.00'),
            500.0,
            5,
            3,
            'STREET1',
            'STREET2',
            'CITY',
            'ST',
            '135792468',
            '0123456789',
            datetime.now(),
            'MN',
            'CUSTOMER-DATA',
        )
    ]
    cursor.executemany(
        '''
        INSERT INTO bmsql_customer
          (c_w_id, c_d_id, c_id, c_discount, c_credit, c_last, c_first,
           c_credit_lim, c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt,
           c_street_1, c_street_2, c_city, c_state, c_zip, c_phone,
           c_since, c_middle, c_data)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ''',
        customer_rows,
    )

    item_rows = [
        (i, f'ITEM-{i:05d}', Decimal('10.00') + Decimal(str(i)), f'ITEM-DATA-{i:05d}', i)
        for i in range(1, 4)
    ]
    cursor.executemany(
        '''
        INSERT INTO bmsql_item
          (i_id, i_name, i_price, i_data, i_im_id)
        VALUES (%s,%s,%s,%s,%s)
        ''',
        item_rows,
    )

    stock_rows = []
    for i in range(1, 4):
        dist_cols = [f'DIST-{i}-{d}'.ljust(24)[:24] for d in range(1, 11)]
        stock_rows.append(
            (
                1,
                i,
                Decimal('50'),
                Decimal('0.00'),
                0,
                0,
                f'STOCK-DATA-{i:05d}',
                *dist_cols,
            )
        )
    cursor.executemany(
        '''
        INSERT INTO bmsql_stock
          (s_w_id, s_i_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt, s_data,
           s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05,
           s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10)
        VALUES (%s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ''',
        stock_rows,
    )

    order_id = 3000
    cursor.execute(
        '''
        INSERT INTO bmsql_oorder
          (o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_ol_cnt, o_all_local, o_entry_d)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        ''',
        (1, 1, order_id, 1, None, Decimal('3'), Decimal('1'), datetime.now()),
    )

    cursor.execute(
        '''
        INSERT INTO bmsql_new_order
          (no_w_id, no_d_id, no_o_id)
        VALUES (%s,%s,%s)
        ''',
        (1, 1, order_id),
    )

    ol_rows = []
    for line_number in range(1, 4):
        amount = Decimal('15.00') + Decimal(line_number)
        ol_rows.append(
            (
                1,
                1,
                order_id,
                line_number,
                line_number,
                datetime(2000, 1, 1),  # Use a clearly different date to ensure UPDATE changes the value
                amount,
                1,
                Decimal('5'),
                f'DIST-INFO-{line_number}'.ljust(24)[:24],
            )
        )
    cursor.executemany(
        '''
        INSERT INTO bmsql_order_line
          (ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_delivery_d,
           ol_amount, ol_supply_w_id, ol_quantity, ol_dist_info)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ''',
        ol_rows,
    )

    db.commit()

# =========================
# TPC-C Tests
# =========================
def test_tpcc_delivery(db, cursor, dbname):
    """Delivery (simple version without JOIN/multi-loop, revised)"""
    print("TEST TPC-C Delivery")
    cursor.execute(f'USE {dbname}')
    try:
        # Optional: set a higher isolation level explicitly here
        # cursor.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")

        cursor.execute('START TRANSACTION')
        w_id, d_id = 1, 1

        cursor.execute(
            '''
            SELECT c_balance, c_delivery_cnt
              FROM bmsql_customer
             WHERE c_w_id = %s AND c_d_id = %s AND c_id = %s
            ''',
            (w_id, d_id, 1),
        )
        cust_row = cursor.fetchone()
        if not cust_row:
            cursor.execute('ROLLBACK')
            print("\tFailed: customer not found")
            return 1
        initial_balance = Decimal(str(cust_row[0]))
        initial_delivery_cnt = int(cust_row[1])

        # Lock the oldest new_order for this (w,d)
        cursor.execute('''
          SELECT no_o_id
            FROM bmsql_new_order
           WHERE no_w_id = %s AND no_d_id = %s
           ORDER BY no_o_id ASC
           LIMIT 1
           FOR UPDATE
        ''', (w_id, d_id))
        row = cursor.fetchone()
        if not row:
            cursor.execute('ROLLBACK')
            print("\tSkipped (no new_order rows)")
            return 0

        no_o_id = int(row[0])

        # Delete the entry from new_order
        cursor.execute('DELETE FROM bmsql_new_order WHERE no_o_id = %s AND no_d_id = %s AND no_w_id = %s',
                       (no_o_id, d_id, w_id))
        if cursor.rowcount != 1:
            cursor.execute('ROLLBACK')
            print(f"\tFailed: expected to delete 1 new_order row, deleted {cursor.rowcount}")
            return 1

        # Get customer id from oorder (open order)
        cursor.execute('SELECT o_c_id FROM bmsql_oorder WHERE o_id = %s AND o_d_id = %s AND o_w_id = %s',
                       (no_o_id, d_id, w_id))
        row = cursor.fetchone()
        if not row:
            cursor.execute('ROLLBACK')
            print("\tFailed (open order not found)")
            return 1
        c_id = int(row[0])

        # set carrier id (random 1..10 as in Java code)
        o_carrier_id = random.randint(1, 10)
        cursor.execute('UPDATE bmsql_oorder SET o_carrier_id = %s WHERE o_id = %s AND o_d_id = %s AND o_w_id = %s',
                       (o_carrier_id, no_o_id, d_id, w_id))
        if cursor.rowcount != 1:
            cursor.execute('ROLLBACK')
            print("\tFailed to update oorder carrier")
            return 1

        # set delivery date on order lines
        cursor.execute('UPDATE bmsql_order_line SET ol_delivery_d = NOW() WHERE ol_o_id = %s AND ol_d_id = %s AND ol_w_id = %s',
                       (no_o_id, d_id, w_id))
        if cursor.rowcount == 0:
            cursor.execute('ROLLBACK')
            print("\tFailed: no order_line rows updated")
            return 1

        # sum the order line amounts
        cursor.execute('SELECT SUM(ol_amount) AS ol_total FROM bmsql_order_line WHERE ol_o_id = %s AND ol_d_id = %s AND ol_w_id = %s',
                       (no_o_id, d_id, w_id))
        row = cursor.fetchone()
        sum_amount = Decimal('0.00')
        if row and row[0] is not None:
            # cursor returns Decimal/str depending on connector, coerce to Decimal
            sum_amount = Decimal(str(row[0]))

        # update customer balance and delivery count
        cursor.execute('''
          UPDATE bmsql_customer
             SET c_balance = c_balance + %s,
                 c_delivery_cnt = c_delivery_cnt + 1
           WHERE c_w_id = %s AND c_d_id = %s AND c_id = %s
        ''', (sum_amount, w_id, d_id, c_id))
        if cursor.rowcount == 0:
            cursor.execute('ROLLBACK')
            print("\tFailed to update customer")
            return 1

        cursor.execute('COMMIT')
        print("\tPassed!")

        expected_balance = initial_balance + sum_amount
        expected_delivery_cnt = initial_delivery_cnt + 1

        # Verify the side effects
        cursor.execute(
            '''
            SELECT COUNT(*)
              FROM bmsql_new_order
             WHERE no_o_id = %s AND no_d_id = %s AND no_w_id = %s
            ''',
            (no_o_id, d_id, w_id),
        )
        remaining_new_orders = cursor.fetchone()[0]
        if remaining_new_orders != 0:
            print("\tFailed: new_order entry still exists after delivery")
            return 1

        cursor.execute(
            '''
            SELECT o_carrier_id
              FROM bmsql_oorder
             WHERE o_id = %s AND o_d_id = %s AND o_w_id = %s
            ''',
            (no_o_id, d_id, w_id),
        )
        carrier_row = cursor.fetchone()
        if not carrier_row or carrier_row[0] != o_carrier_id:
            print("\tFailed: carrier id not updated as expected")
            return 1

        cursor.execute(
            '''
            SELECT COUNT(*)
              FROM bmsql_order_line
             WHERE ol_o_id = %s AND ol_d_id = %s AND ol_w_id = %s
               AND ol_delivery_d IS NULL
            ''',
            (no_o_id, d_id, w_id),
        )
        null_delivery = cursor.fetchone()[0]
        if null_delivery != 0:
            print("\tFailed: some order_line rows still have NULL delivery date")
            return 1

        cursor.execute(
            '''
            SELECT c_balance, c_delivery_cnt
              FROM bmsql_customer
             WHERE c_w_id = %s AND c_d_id = %s AND c_id = %s
            ''',
            (w_id, d_id, c_id),
        )
        post_cust = cursor.fetchone()
        if not post_cust:
            print("\tFailed: customer row missing after delivery")
            return 1

        updated_balance = Decimal(str(post_cust[0]))
        updated_delivery_cnt = int(post_cust[1])

        if updated_balance != expected_balance or updated_delivery_cnt != expected_delivery_cnt:
            print("\tFailed: customer aggregates not updated as expected")
            return 1

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
        reset(db, cursor, "ha_lineairdb_test")
        setup_schema(db, cursor, "ha_lineairdb_test", "LineairDB")
        populate_delivery_fixture(db, cursor, "ha_lineairdb_test")

        result = 0
        result |= test_tpcc_delivery(db, cursor, "ha_lineairdb_test")
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