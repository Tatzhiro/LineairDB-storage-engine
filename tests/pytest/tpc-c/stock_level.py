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

    drops = [
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
    for t in drops:
        cursor.execute(f"DROP TABLE IF EXISTS {t}")

    cursor.execute(
        f'''
    CREATE TABLE bmsql_warehouse (
      w_id       INT NOT NULL,
      w_ytd      DECIMAL(12,2)  NOT NULL,
      w_tax      DECIMAL(4,4)   NOT NULL,
      w_name     VARCHAR(10) NOT NULL,
      PRIMARY KEY (w_id)
    ) ENGINE={engine}
    '''
    )

    cursor.execute(
        f'''
    CREATE TABLE bmsql_district (
      d_w_id       INT NOT NULL,
      d_id         INT NOT NULL,
      d_ytd        DECIMAL(12,2)  NOT NULL,
      d_tax        DECIMAL(4,4)   NOT NULL,
      d_next_o_id  INT NOT NULL,
      d_name       VARCHAR(10) NOT NULL,
      PRIMARY KEY (d_w_id, d_id)
    ) ENGINE={engine}
    '''
    )

    cursor.execute(
        f'''
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
    '''
    )

    cursor.execute(
        f'''
    CREATE TABLE bmsql_item (
      i_id    INT NOT NULL,
      i_name  VARCHAR(24) NOT NULL,
      i_price DECIMAL(7,2)  NOT NULL,
      i_data  VARCHAR(50) NOT NULL,
      i_im_id INT NOT NULL,
      PRIMARY KEY (i_id)
    ) ENGINE={engine}
    '''
    )

    cursor.execute(
        f'''
    CREATE TABLE bmsql_stock (
      s_w_id       INT NOT NULL,
      s_i_id       INT NOT NULL,
      s_quantity   INT            NOT NULL,
      s_ytd        INT            NOT NULL,
      s_order_cnt  INT            NOT NULL,
      s_remote_cnt INT            Not NULL,
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
    '''
    )

    cursor.execute(
        f'''
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
    '''
    )

    cursor.execute(
        f'''
    CREATE TABLE bmsql_new_order (
      no_w_id INT NOT NULL,
      no_d_id INT NOT NULL,
      no_o_id INT NOT NULL,
      PRIMARY KEY (no_w_id, no_d_id, no_o_id)
    ) ENGINE={engine}
    '''
    )

    cursor.execute(
        f'''
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
    '''
    )

    cursor.execute(
        f'''
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
    '''
    )
    db.commit()


def populate_stock_level_fixture(db, cursor, dbname):
    cursor.execute(f'USE {dbname}')

    cursor.execute(
        '''
        INSERT INTO bmsql_warehouse
          (w_id, w_ytd, w_tax, w_name)
        VALUES (1, 0.00, 0.0700, 'W1')
        '''
    )

    district_rows = []
    for d_id in range(1, 3):
        district_rows.append(
            (1, d_id, 0.0, 0.0500, 3005 + d_id, f'D{d_id}')
        )
    cursor.executemany(
        '''
        INSERT INTO bmsql_district
          (d_w_id, d_id, d_ytd, d_tax, d_next_o_id, d_name)
        VALUES (%s, %s, %s, %s, %s, %s)
        ''',
        district_rows,
    )

    cursor.execute(
        '''
        INSERT INTO bmsql_customer
          (c_w_id, c_d_id, c_id, c_discount, c_credit, c_last, c_first,
           c_credit_lim, c_balance, c_ytd_payment, c_payment_cnt,
           c_delivery_cnt, c_data, c_middle)
        VALUES (1, 1, 1001, 0.0500, 'GC', 'DOE', 'JOHN',
                50000.00, 0.00, 0.00, 0, 0, 'cust-data', 'OE')
        '''
    )

    items = []
    for i in range(1, 31):
        items.append((i, f'ITEM-{i:03d}', Decimal('10.00') + Decimal(str(i)), f'data-{i:03d}', i))
    cursor.executemany(
        '''
        INSERT INTO bmsql_item
          (i_id, i_name, i_price, i_data, i_im_id)
        VALUES (%s,%s,%s,%s,%s)
        ''',
        items,
    )

    stock_rows = []
    for i in range(1, 31):
        quantity = 5 if i % 5 == 0 else 100
        dist_cols = [f'DIST-{i:03d}-{d:02d}'.ljust(24)[:24] for d in range(1, 11)]
        stock_rows.append((1, i, quantity, 0, 0, 0, f'sdata-{i:03d}', *dist_cols))

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

    order_rows = []
    order_line_rows = []
    current_o_id = 2950
    for d_id in range(1, 3):
        for offset in range(25):
            o_id = current_o_id + offset
            order_rows.append((1, d_id, o_id, 1001, None, 5, 1, datetime.now()))
            for number in range(1, 6):
                item_id = ((offset * 5) + number)
                if item_id > 30:
                    item_id = item_id % 30 or 30
                quantity = random.randint(1, 10)
                order_line_rows.append(
                    (
                        1,
                        d_id,
                        o_id,
                        number,
                        item_id,
                        None,
                        Decimal('5.00'),
                        1,
                        quantity,
                        f'OL-DIST-{number:02d}'.ljust(24)[:24],
                    )
                )
        current_o_id += 25

    cursor.executemany(
        '''
        INSERT INTO bmsql_oorder
          (o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_ol_cnt, o_all_local, o_entry_d)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        ''',
        order_rows,
    )

    cursor.executemany(
        '''
        INSERT INTO bmsql_order_line
          (ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_delivery_d,
           ol_amount, ol_supply_w_id, ol_quantity, ol_dist_info)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ''',
        order_line_rows,
    )

    cursor.executemany(
        '''
        INSERT INTO bmsql_new_order
          (no_w_id, no_d_id, no_o_id)
        VALUES (%s,%s,%s)
        ''',
        [(1, 1, 3030), (1, 2, 3035)],
    )

    db.commit()

def test_tpcc_stocklevel(db, cursor, dbname, w_id=1, terminalDistrictLowerID=1, terminalDistrictUpperID=10, threshold=None):
    print("TEST TPC-C Stock-Level")
    cursor.execute(f'USE {dbname}')
    try:
        cursor.execute('START TRANSACTION')

        if threshold is None:
            threshold = random.randint(10, 20)
        # choose district randomly in the provided range
        d_id = random.randint(terminalDistrictLowerID, terminalDistrictUpperID)

        # 1) get D_NEXT_O_ID (stockGetDistOrderIdSQL)
        cursor.execute('SELECT d_next_o_id FROM bmsql_district WHERE d_w_id = %s AND d_id = %s', (w_id, d_id))
        row = cursor.fetchone()
        if not row:
            cursor.execute('ROLLBACK')
            print(f"\tFailed: district not found [w_id={w_id}, d_id={d_id}]")
            return 1
        o_id = int(row[0])

        # 2) count distinct low-stock items in last 20 orders (stockGetCountStockSQL)
        # Java sets params in this order:
        # 1: OL_W_ID = w_id
        # 2: OL_D_ID = d_id
        # 3: OL_O_ID < o_id
        # 4: OL_O_ID >= o_id - 20
        # 5: S_W_ID = w_id
        # 6: S_QUANTITY < threshold
        cursor.execute('''
          SELECT COUNT(DISTINCT(s_i_id)) AS stock_count
            FROM bmsql_order_line ol, bmsql_stock s
           WHERE ol.ol_w_id = %s
             AND ol.ol_d_id = %s
             AND ol.ol_o_id < %s
             AND ol.ol_o_id >= %s
             AND s.s_w_id = %s
             AND s.s_i_id = ol.ol_i_id
             AND s.s_quantity < %s
        ''', (w_id, d_id, o_id, max(o_id - 20, 0), w_id, threshold))
        row = cursor.fetchone()
        if not row:
            cursor.execute('ROLLBACK')
            print(f"\tFailed: count query returned no row [w_id={w_id}, d_id={d_id}, o_id={o_id}]")
            return 1
        stock_count = int(row[0] or 0)

        cursor.execute('COMMIT')

        print(f"\tWarehouse: {w_id}, District: {d_id}, Threshold: {threshold}, D_NEXT_O_ID: {o_id}")
        print(f"\tLow stock distinct items in last 20 orders: {stock_count}")
        print("\tPassed!")
        return 0

    except mysql.connector.Error as err:
        print(f"\tFailed (MySQL): {err}")
        try:
            cursor.execute('ROLLBACK')
        except:
            pass
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
        populate_stock_level_fixture(db, cursor, "ha_lineairdb_test")

        result = 0
        result |= test_tpcc_stocklevel(db, cursor, "ha_lineairdb_test", threshold=50)

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
