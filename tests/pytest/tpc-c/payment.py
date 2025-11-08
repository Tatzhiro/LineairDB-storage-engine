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

def seed_minimal_data(db, cursor, dbname, item_count=100, initial_next_o_id=3001):
    cursor.execute(f'USE {dbname}')

    # 1 warehouse, 1 district, 1 customer, items 1..item_count, stock for w_id=1
    cursor.execute('INSERT INTO bmsql_warehouse (w_id, w_ytd, w_tax, w_name) VALUES (1, 0.00, 0.0700, "W1")')

    cursor.execute('INSERT INTO bmsql_district (d_w_id, d_id, d_ytd, d_tax, d_next_o_id, d_name) '
                   'VALUES (1, 1, 0.00, 0.0500, %s, "D1")', (initial_next_o_id,))

    cursor.execute('INSERT INTO bmsql_customer (c_w_id, c_d_id, c_id, c_discount, c_credit, c_last, c_first, '
                   'c_credit_lim, c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data, c_middle) '
                   'VALUES (1, 1, 1001, 0.0500, "GC", "DOE", "JOHN", 50000.00, 0.00, 0.00, 0, 0, "init", "OE")')

    # Items and stock
    random.seed(42)
    for i in range(1, item_count + 1):
        price = round(1.00 + (i % 100) * 0.25, 2)  # 1.00..25.75
        cursor.execute(
            'INSERT INTO bmsql_item (i_id, i_name, i_price, i_data, i_im_id) VALUES (%s, %s, %s, %s, %s)',
            (i, f'ITEM-{i}', price, 'data', i),
        )
        dist_cols = [f'DIST-{i:05d}-{d:02d}'.ljust(24)[:24] for d in range(1, 11)]
        cursor.execute(
            '''
            INSERT INTO bmsql_stock
              (s_w_id, s_i_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt, s_data,
               s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05,
               s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10)
            VALUES (%s, %s, %s, 0, 0, 0, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s)
            ''',
            (1, i, 100, 'sdata', *dist_cols),
        )
    db.commit()

# =========================
# TPC-C Tests
# =========================

def test_tpcc_new_order(db, cursor, dbname, ol_cnt=10):
    """JOIN + 複数明細ループを含む New-Order テスト"""
    print("TEST TPC-C New-Order (JOIN & multi-line)")
    cursor.execute(f'USE {dbname}')
    try:
        cursor.execute('START TRANSACTION')

        w_id, d_id, c_id = 1, 1, 1001

        # 倉庫税 + 顧客割引を JOIN で同時取得
        cursor.execute('''
          SELECT w.w_tax, c.c_discount
            FROM bmsql_warehouse w
            JOIN bmsql_customer c
              ON w.w_id = c.c_w_id
           WHERE w.w_id = %s AND c.c_w_id = %s AND c.c_d_id = %s AND c.c_id = %s
        ''', (w_id, w_id, d_id, c_id))
        row = cursor.fetchone()
        if not row:
            raise RuntimeError("warehouse/customer not seeded")
        w_tax, c_discount = float(row[0]), float(row[1])

        # 次注文IDをロック取得
        cursor.execute('''
          SELECT d_next_o_id
            FROM bmsql_district
           WHERE d_w_id = %s AND d_id = %s
           FOR UPDATE
        ''', (w_id, d_id))
        d_next_o_id = int(cursor.fetchone()[0])
        new_o_id = d_next_o_id

        # 採番更新
        cursor.execute('UPDATE bmsql_district SET d_next_o_id = d_next_o_id + 1 WHERE d_w_id = %s AND d_id = %s',
                       (w_id, d_id))

        # 受注ヘッダ
        all_local_flag = 1  # 本テンプレではリモート倉庫なし
        cursor.execute('''
          INSERT INTO bmsql_oorder (o_w_id, o_d_id, o_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local)
          VALUES (%s, %s, %s, %s, NOW(), %s, %s)
        ''', (w_id, d_id, new_o_id, c_id, ol_cnt, all_local_flag))

        # NEW_ORDER
        cursor.execute('INSERT INTO bmsql_new_order (no_w_id, no_d_id, no_o_id) VALUES (%s, %s, %s)',
                       (w_id, d_id, new_o_id))

        # 明細ループ
        total_amount = 0.0
        item_max_id = 100  # seed_minimal_data と合わせる
        for ol_number in range(1, ol_cnt + 1):
            i_id = random.randint(1, item_max_id)
            supply_w_id = w_id
            quantity = random.randint(1, 10)

            # ITEM
            cursor.execute('SELECT i_price, i_name, i_data FROM bmsql_item WHERE i_id = %s', (i_id,))
            res = cursor.fetchone()
            if not res:
                raise RuntimeError(f"item {i_id} missing")
            i_price = float(res[0])

            # STOCK (FOR UPDATE)
            cursor.execute('''
              SELECT s_quantity, s_ytd, s_order_cnt, s_remote_cnt, s_data, s_dist_01
                FROM bmsql_stock
               WHERE s_w_id = %s AND s_i_id = %s
               FOR UPDATE
            ''', (supply_w_id, i_id))
            srow = cursor.fetchone()
            if not srow:
                raise RuntimeError(f"stock (w={supply_w_id}, i={i_id}) missing")
            s_quantity = int(srow[0])
            s_dist_info = srow[5]

            # 在庫更新ロジック（TPC-C準拠の簡易版）
            if s_quantity < quantity + 10:
                new_s_quantity = s_quantity - quantity + 91
            else:
                new_s_quantity = s_quantity - quantity

            cursor.execute('''
              UPDATE bmsql_stock
                 SET s_quantity   = %s,
                     s_ytd        = s_ytd + %s,
                     s_order_cnt  = s_order_cnt + 1,
                     s_remote_cnt = s_remote_cnt + 0
               WHERE s_w_id = %s AND s_i_id = %s
            ''', (new_s_quantity, quantity, supply_w_id, i_id))

            ol_amount = round(i_price * quantity * (1 - c_discount) * (1 + w_tax), 2)
            total_amount += ol_amount

            cursor.execute('''
              INSERT INTO bmsql_order_line
                (ol_w_id, ol_d_id, ol_o_id, ol_number,
                 ol_i_id, ol_supply_w_id, ol_quantity, ol_amount,
                 ol_dist_info, ol_delivery_d)
              VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NULL)
            ''', (w_id, d_id, new_o_id, ol_number, i_id, supply_w_id, quantity, ol_amount, s_dist_info))

        cursor.execute('COMMIT')
        print("\tPassed!")
        return 0

    except mysql.connector.Error as err:
        print(f"\tFailed (MySQL): {err}")
        cursor.execute('ROLLBACK')
        return 1
    except Exception as ex:
        print(f"\tFailed: {ex}")
        cursor.execute('ROLLBACK')
        return 1

def test_tpcc_payment(db, cursor, dbname, amount=200.00):
    """Payment（JOIN/複数ループ不要のシンプル版）"""
    print("TEST TPC-C Payment")
    cursor.execute(f'USE {dbname}')
    try:
        cursor.execute('START TRANSACTION')
        w_id, d_id, c_id = 1, 1, 1001

        cursor.execute('SELECT w_ytd FROM bmsql_warehouse WHERE w_id = %s', (w_id,))
        initial_w_ytd = Decimal(str(cursor.fetchone()[0]))

        cursor.execute('SELECT d_ytd FROM bmsql_district WHERE d_w_id = %s AND d_id = %s', (w_id, d_id))
        initial_d_ytd = Decimal(str(cursor.fetchone()[0]))

        cursor.execute('''
          SELECT c_balance, c_ytd_payment, c_payment_cnt
            FROM bmsql_customer
           WHERE c_w_id = %s AND c_d_id = %s AND c_id = %s
        ''', (w_id, d_id, c_id))
        cust_row = cursor.fetchone()
        if not cust_row:
            raise RuntimeError("customer not seeded")
        initial_c_balance = Decimal(str(cust_row[0]))
        initial_c_ytd = Decimal(str(cust_row[1]))
        initial_c_paycnt = int(cust_row[2])

        cursor.execute('SELECT COUNT(*) FROM bmsql_history')
        history_before = int(cursor.fetchone()[0])

        cursor.execute('UPDATE bmsql_warehouse SET w_ytd = w_ytd + %s WHERE w_id = %s', (amount, w_id))
        cursor.execute('UPDATE bmsql_district  SET d_ytd = d_ytd + %s WHERE d_w_id = %s AND d_id = %s',
                       (amount, w_id, d_id))

        cursor.execute('SELECT c_balance FROM bmsql_customer WHERE c_w_id = %s AND c_d_id = %s AND c_id = %s FOR UPDATE',
                       (w_id, d_id, c_id))
        bal = Decimal(str(cursor.fetchone()[0]))
        cursor.execute('''
          UPDATE bmsql_customer
             SET c_balance     = %s,
                 c_ytd_payment = c_ytd_payment + %s,
                 c_payment_cnt = c_payment_cnt + 1
           WHERE c_w_id = %s AND c_d_id = %s AND c_id = %s
        ''', (bal - Decimal(str(amount)), amount, w_id, d_id, c_id))

        cursor.execute('''
          INSERT INTO bmsql_history
            (h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data)
          VALUES (%s, %s, %s, %s, %s, NOW(), %s, %s)
        ''', (c_id, d_id, w_id, d_id, w_id, amount, 'payment'))

        cursor.execute('COMMIT')

        expected_w_ytd = initial_w_ytd + Decimal(str(amount))
        cursor.execute('SELECT w_ytd FROM bmsql_warehouse WHERE w_id = %s', (w_id,))
        new_w_ytd = Decimal(str(cursor.fetchone()[0]))
        if new_w_ytd != expected_w_ytd:
            print("\tFailed: warehouse YTD mismatch")
            return 1

        expected_d_ytd = initial_d_ytd + Decimal(str(amount))
        cursor.execute('SELECT d_ytd FROM bmsql_district WHERE d_w_id = %s AND d_id = %s', (w_id, d_id))
        new_d_ytd = Decimal(str(cursor.fetchone()[0]))
        if new_d_ytd != expected_d_ytd:
            print("\tFailed: district YTD mismatch")
            return 1

        cursor.execute('''
          SELECT c_balance, c_ytd_payment, c_payment_cnt
            FROM bmsql_customer
           WHERE c_w_id = %s AND c_d_id = %s AND c_id = %s
        ''', (w_id, d_id, c_id))
        post_cust = cursor.fetchone()
        if not post_cust:
            print("\tFailed: customer missing after payment")
            return 1

        updated_balance = Decimal(str(post_cust[0]))
        updated_ytd = Decimal(str(post_cust[1]))
        updated_paycnt = int(post_cust[2])

        if updated_balance != initial_c_balance - Decimal(str(amount)):
            print("\tFailed: customer balance mismatch")
            return 1
        if updated_ytd != initial_c_ytd + Decimal(str(amount)):
            print("\tFailed: customer YTD payment mismatch")
            return 1
        if updated_paycnt != initial_c_paycnt + 1:
            print("\tFailed: customer payment count mismatch")
            return 1

        cursor.execute('SELECT COUNT(*) FROM bmsql_history')
        history_after = int(cursor.fetchone()[0])
        if history_after != history_before + 1:
            print("\tFailed: history record not inserted")
            return 1

        cursor.execute('''
          SELECT h_amount, h_data
            FROM bmsql_history
           WHERE h_c_id = %s AND h_c_d_id = %s AND h_c_w_id = %s
           ORDER BY h_date DESC
           LIMIT 1
        ''', (c_id, d_id, w_id))
        hist_row = cursor.fetchone()
        if not hist_row or Decimal(str(hist_row[0])) != Decimal(str(amount)) or hist_row[1] != 'payment':
            print("\tFailed: history record content mismatch")
            return 1

        print("\tPassed!")
        return 0
    except mysql.connector.Error as err:
        print(f"\tFailed (MySQL): {err}")
        cursor.execute('ROLLBACK')
        return 1

def test_tpcc_delivery(db, cursor, dbname):
    """Delivery（JOIN/複数ループ不要のシンプル版）"""
    print("TEST TPC-C Delivery")
    cursor.execute(f'USE {dbname}')
    try:
        cursor.execute('START TRANSACTION')
        w_id, d_id = 1, 1

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

        cursor.execute('DELETE FROM bmsql_new_order WHERE no_w_id = %s AND no_d_id = %s AND no_o_id = %s',
                       (w_id, d_id, no_o_id))

        cursor.execute('SELECT o_c_id FROM bmsql_oorder WHERE o_w_id = %s AND o_d_id = %s AND o_id = %s',
                       (w_id, d_id, no_o_id))
        c_id = int(cursor.fetchone()[0])

        cursor.execute('UPDATE bmsql_oorder SET o_carrier_id = %s WHERE o_w_id = %s AND o_d_id = %s AND o_id = %s',
                       (7, w_id, d_id, no_o_id))

        cursor.execute('UPDATE bmsql_order_line SET ol_delivery_d = NOW() WHERE ol_w_id = %s AND ol_d_id = %s AND ol_o_id = %s',
                       (w_id, d_id, no_o_id))

        cursor.execute('SELECT SUM(ol_amount) FROM bmsql_order_line WHERE ol_w_id = %s AND ol_d_id = %s AND ol_o_id = %s',
                       (w_id, d_id, no_o_id))
        sum_amount = float(cursor.fetchone()[0] or 0.0)

        cursor.execute('SELECT c_balance FROM bmsql_customer WHERE c_w_id = %s AND c_d_id = %s AND c_id = %s FOR UPDATE',
                       (w_id, d_id, c_id))
        bal = float(cursor.fetchone()[0])

        cursor.execute('''
          UPDATE bmsql_customer
             SET c_balance = %s,
                 c_delivery_cnt = c_delivery_cnt + 1
           WHERE c_w_id = %s AND c_d_id = %s AND c_id = %s
        ''', (bal + sum_amount, w_id, d_id, c_id))

        cursor.execute('COMMIT')
        print("\tPassed!")
        return 0
    except mysql.connector.Error as err:
        print(f"\tFailed (MySQL): {err}")
        cursor.execute('ROLLBACK')
        return 1

def test_tpcc_order_status(db, cursor, dbname):
    """Order-Status（JOIN/複数ループ不要のシンプル版）"""
    print("TEST TPC-C Order-Status")
    cursor.execute(f'USE {dbname}')
    try:
        cursor.execute('START TRANSACTION')
        w_id, d_id, c_id = 1, 1, 1001

        cursor.execute('SELECT c_first, c_last, c_balance FROM bmsql_customer WHERE c_w_id = %s AND c_d_id = %s AND c_id = %s',
                       (w_id, d_id, c_id))

        cursor.execute('''
          SELECT o_id
            FROM bmsql_oorder
           WHERE o_w_id = %s AND o_d_id = %s AND o_c_id = %s
             AND o_id = (SELECT MAX(o_id) FROM bmsql_oorder WHERE o_w_id = %s AND o_d_id = %s AND o_c_id = %s)
        ''', (w_id, d_id, c_id, w_id, d_id, c_id))
        row = cursor.fetchone()
        if row:
            o_id = int(row[0])
            cursor.execute('SELECT ol_i_id, ol_amount, ol_delivery_d FROM bmsql_order_line WHERE ol_w_id = %s AND ol_d_id = %s AND ol_o_id = %s ORDER BY ol_number',
                           (w_id, d_id, o_id))

        cursor.execute('COMMIT')
        print("\tPassed!")
        return 0
    except mysql.connector.Error as err:
        print(f"\tFailed (MySQL): {err}")
        cursor.execute('ROLLBACK')
        return 1

def test_tpcc_stock_level(db, cursor, dbname, threshold=50):
    """Stock-Level（JOIN/複数ループ不要のシンプル版）"""
    print("TEST TPC-C Stock-Level")
    cursor.execute(f'USE {dbname}')
    try:
        w_id, d_id = 1, 1
        cursor.execute('''
          SELECT COUNT(*) AS low_stock
            FROM (
              SELECT s_w_id, s_i_id, s_quantity
                FROM bmsql_stock
               WHERE s_w_id = %s
                 AND s_quantity < %s
                 AND s_i_id IN (
                   SELECT ol_i_id
                     FROM bmsql_district d
                     JOIN bmsql_order_line ol
                       ON ol.ol_w_id = d.d_w_id
                      AND ol.ol_d_id = d.d_id
                      AND ol.ol_o_id >= d.d_next_o_id - 20
                      AND ol.ol_o_id <  d.d_next_o_id
                    WHERE d.d_w_id = %s AND d.d_id = %s
                 )
            ) AS L
        ''', (w_id, threshold, w_id, d_id))
        low = int(cursor.fetchone()[0])
        print(f"\tLow stock count = {low}")
        print("\tPassed!")
        return 0
    except mysql.connector.Error as err:
        print(f"\tFailed (MySQL): {err}")
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
        seed_minimal_data(db, cursor, "ha_lineairdb_test")

        result = 0
        result |= test_tpcc_payment(db, cursor, "ha_lineairdb_test", amount=200.00)

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
