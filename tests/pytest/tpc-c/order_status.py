#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import argparse
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

    # Minimal DDL (必要最小限。必要に応じて列追加OK)
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

    # ★ インデックスは CREATE TABLE の中で同時定義（MySQLで安全）
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
      c_middle       CHAR(2) NOT NULL,
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


def populate_order_status_fixture(db, cursor, dbname):
    cursor.execute(f'USE {dbname}')

    # Warehouse
    cursor.execute(
        '''
        INSERT INTO bmsql_warehouse
          (w_id, w_ytd, w_tax, w_name)
        VALUES (%s,%s,%s,%s)
        ''',
        (1, Decimal('0.00'), Decimal('0.0700'), 'WH1'),
    )

    # District
    cursor.execute(
        '''
        INSERT INTO bmsql_district
          (d_w_id, d_id, d_ytd, d_tax, d_next_o_id, d_name)
        VALUES (%s,%s,%s,%s,%s,%s)
        ''',
        (1, 1, Decimal('0.00'), Decimal('0.0500'), 3005, 'DIST1'),
    )

    # Customers sharing the same last name to exercise "search by name"
    customers = [
        (
            1,
            1,
            1,
            Decimal('0.0500'),
            'GC',
            'BARBARBAR',
            'ALICE',
            'AA',
            Decimal('50000.00'),
            Decimal('100.00'),
            Decimal('500.00'),
            5,
            3,
            'CUSTOMER-DATA-1',
        ),
        (
            1,
            1,
            2,
            Decimal('0.0400'),
            'BC',
            'BARBARBAR',
            'BETTY',
            'BB',
            Decimal('45000.00'),
            Decimal('150.00'),
            Decimal('450.00'),
            4,
            2,
            'CUSTOMER-DATA-2',
        ),
        (
            1,
            1,
            3,
            Decimal('0.0300'),
            'PC',
            'BARBARBAR',
            'CAROL',
            'CC',
            Decimal('47000.00'),
            Decimal('120.00'),
            Decimal('300.00'),
            6,
            1,
            'CUSTOMER-DATA-3',
        ),
    ]
    cursor.executemany(
        '''
        INSERT INTO bmsql_customer
          (c_w_id, c_d_id, c_id, c_discount, c_credit, c_last, c_first, c_middle,
           c_credit_lim, c_balance, c_ytd_payment, c_payment_cnt,
           c_delivery_cnt, c_data)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ''',
        customers,
    )

    # Items (minimal subset)
    items = [
        (i, f'ITEM-{i:05d}', Decimal('10.00') + Decimal(str(i)), f'ITEM-DATA-{i:05d}', i)
        for i in range(1, 4)
    ]
    cursor.executemany(
        '''
        INSERT INTO bmsql_item
          (i_id, i_name, i_price, i_data, i_im_id)
        VALUES (%s,%s,%s,%s,%s)
        ''',
        items,
    )

    # Stock for the inserted items
    stock_rows = []
    for i in range(1, 4):
        dist_cols = [f'DIST-{i}-{d}'.ljust(24)[:24] for d in range(1, 11)]
        stock_rows.append(
            (
                1,
                i,
                50,
                0,
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

    now = datetime.now()
    # Orders for customer 2 (BETTY) to ensure latest order detection
    orders = [
        (1, 1, 2000, 2, 1, 2, 1, now),
        (1, 1, 2001, 2, 2, 2, 1, now),
    ]
    cursor.executemany(
        '''
        INSERT INTO bmsql_oorder
          (o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_ol_cnt, o_all_local, o_entry_d)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        ''',
        orders,
    )

    order_lines = [
        (1, 1, 2000, 1, 1, now, Decimal('15.00'), 1, 5, 'DIST-A'.ljust(24)[:24]),
        (1, 1, 2000, 2, 2, now, Decimal('16.50'), 1, 3, 'DIST-B'.ljust(24)[:24]),
        (1, 1, 2001, 1, 2, None, Decimal('17.25'), 1, 4, 'DIST-C'.ljust(24)[:24]),
        (1, 1, 2001, 2, 3, None, Decimal('18.75'), 1, 6, 'DIST-D'.ljust(24)[:24]),
    ]
    cursor.executemany(
        '''
        INSERT INTO bmsql_order_line
          (ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_delivery_d,
           ol_amount, ol_supply_w_id, ol_quantity, ol_dist_info)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ''',
        order_lines,
    )

    db.commit()
def pick_middle_index(n):
    # Java 実装と同じロジック:
    # index = n / 2 ; if n % 2 == 0 then index -= 1
    if n <= 0:
        return None
    index = n // 2
    if n % 2 == 0:
        index -= 1
    return index

def test_tpcc_orderstatus(db, cursor, dbname):
    print("TEST TPC-C Order-Status")
    cursor.execute(f'USE {dbname}')
    try:
        cursor.execute('START TRANSACTION')

        w_id = 1
        d_id = 1
        c_last = "BARBARBAR"

        cursor.execute(
            '''
            SELECT c_first, c_middle, c_id, c_balance
              FROM bmsql_customer
             WHERE c_w_id = %s AND c_d_id = %s AND c_last = %s
             ORDER BY c_first
            ''',
            (w_id, d_id, c_last),
        )
        rows = cursor.fetchall()
        if not rows:
            cursor.execute('ROLLBACK')
            print("\tOrder-Status: Failed - no customers found for last name BARBARBAR")
            return 1

        idx = pick_middle_index(len(rows))
        if idx is None:
            cursor.execute('ROLLBACK')
            print("\tOrder-Status: Failed - cannot determine middle customer")
            return 1

        c_first, c_middle, c_id, balance_value = rows[idx]
        if c_id != 2:
            cursor.execute('ROLLBACK')
            print(f"\tOrder-Status: Failed - expected middle customer id 2, got {c_id}")
            return 1
        c_balance = Decimal(str(balance_value))

        # --- 最新の注文を取得（ordStatGetNewestOrdSQL）
        cursor.execute('''
          SELECT o_id, o_carrier_id, o_entry_d
            FROM bmsql_oorder
           WHERE o_w_id = %s AND o_d_id = %s AND o_c_id = %s
           ORDER BY o_id DESC LIMIT 1
        ''', (w_id, d_id, c_id))
        row = cursor.fetchone()
        if not row:
            cursor.execute('ROLLBACK')
            print("\tOrder-Status: Failed - customer has no orders")
            return 1

        o_id = int(row[0])
        o_carrier_id = row[1]
        o_entry_d = row[2]

        if o_id != 2001:
            cursor.execute('ROLLBACK')
            print(f"\tOrder-Status: Failed - expected latest order 2001, got {o_id}")
            return 1

        # --- 注文明細（order lines）を取得（ordStatGetOrderLinesSQL）
        cursor.execute('''
          SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d
            FROM bmsql_order_line
           WHERE ol_o_id = %s AND ol_d_id = %s AND ol_w_id = %s
        ''', (o_id, d_id, w_id))
        order_lines = cursor.fetchall()

        if len(order_lines) != 2:
            cursor.execute('ROLLBACK')
            print(f"\tOrder-Status: Failed - expected 2 order lines, got {len(order_lines)}")
            return 1

        expected_lines = [
            (2, 1, 4, Decimal('17.25')),
            (3, 1, 6, Decimal('18.75')),
        ]
        for idx, (ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d) in enumerate(order_lines):
            exp_i_id, exp_supply_w_id, exp_qty, exp_amount = expected_lines[idx]
            if (
                ol_i_id != exp_i_id
                or ol_supply_w_id != exp_supply_w_id
                or ol_quantity != exp_qty
                or Decimal(str(ol_amount)) != exp_amount
            ):
                cursor.execute('ROLLBACK')
                print("\tOrder-Status: Failed - order line content mismatch")
                return 1
            if ol_delivery_d is not None:
                cursor.execute('ROLLBACK')
                print("\tOrder-Status: Failed - expected NULL delivery date for latest order")
                return 1

        print(f"\tCustomer OK: id={c_id}, name={c_first} {c_middle}, balance={c_balance}")
        print(f"\tOrder OK: id={o_id}, carrier={o_carrier_id}, entry_d={o_entry_d}")
        print("\tOrder lines OK")

        cursor.execute('COMMIT')
        print("\tOrder-Status: Passed")
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
        populate_order_status_fixture(db, cursor, "ha_lineairdb_test")

        result = 0
        result |= test_tpcc_orderstatus(db, cursor, "ha_lineairdb_test")

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
