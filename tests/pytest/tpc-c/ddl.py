#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import argparse
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
      w_tax      DECIMAL(4,4) NOT NULL,
      w_name     VARCHAR(10) NOT NULL,
      w_street_1 VARCHAR(20) NOT NULL,
      w_street_2 VARCHAR(20) NOT NULL,
      w_city     VARCHAR(20) NOT NULL,
      w_state    CHAR(2) NOT NULL,
      w_zip      CHAR(9) NOT NULL,
      PRIMARY KEY (w_id)
    ) ENGINE=LineairDB
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
    ) ENGINE=LineairDB
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
    ) ENGINE=LineairDB
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
    ) ENGINE=LineairDB
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
    ) ENGINE=LineairDB
    ''')

    cursor.execute(f'''
    CREATE TABLE bmsql_new_order (
      no_w_id INT NOT NULL,
      no_d_id INT NOT NULL,
      no_o_id INT NOT NULL,
      PRIMARY KEY (no_w_id, no_d_id, no_o_id)
    ) ENGINE=LineairDB
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
    ) ENGINE=LineairDB
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
    ) ENGINE=LineairDB
    ''')
    db.commit()
    return 0

def main():
    parser = argparse.ArgumentParser(description='TPC-C style tests (MySQL)')
    parser.add_argument('--host', type=str, default='localhost')
    parser.add_argument('--port', type=int, default=3306)
    parser.add_argument('--user', type=str, default='root')
    parser.add_argument('--password', type=str, default='')
    parser.add_argument('--dbname', type=str, default='ha_lineairdb_test')
    parser.add_argument('--engine', type=str, default='LineairDB', help='Storage engine (LineairDB/InnoDB/...)')
    parser.add_argument('--ol_cnt', type=int, default=10, help='Order lines count for New-Order test')
    args = parser.parse_args()

    db = mysql.connector.connect(host=args.host, port=args.port, user=args.user, password=args.password)
    cursor = db.cursor()

    try:
        reset(db, cursor, args.dbname)
        result = 0
        result |= setup_schema(db, cursor, args.dbname, args.engine)

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
    main()
