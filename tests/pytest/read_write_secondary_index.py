import sys
import mysql.connector
import argparse
import time

def test_write_operation(db, cursor):
    """基本的なWRITE操作のテスト（INSERT）"""
    print("WRITE OPERATION TEST")
    
    # ユニークなテーブル名を生成
    table_name = f"test_write_{int(time.time() * 1000000)}"
    
    # テーブルを作成（PRIMARY KEYなし、INDEXのみ）
    cursor.execute(f'''CREATE TABLE ha_lineairdb_test.{table_name} (
        title VARCHAR(50) NOT NULL,
        content TEXT,
        INDEX title_idx (title)
    ) ENGINE = LineairDB''')
    db.commit()
    
    # データを挿入
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (title, content) VALUES ("alice", "test data 1")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (title, content) VALUES ("bob", "test data 2")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (title, content) VALUES ("carol", "test data 3")')
    db.commit()
    
    # 挿入確認
    cursor.execute(f'SELECT COUNT(*) FROM ha_lineairdb_test.{table_name}')
    count = cursor.fetchone()[0]
    if count != 3:
        print(f"\tFailed: expected 3 rows, got {count}")
        cursor.execute(f'SELECT title, content FROM ha_lineairdb_test.{table_name}')
        print("\tActual data:", cursor.fetchall())
        return 1
    
    print("\tPassed!")
    return 0

def test_read_operation(db, cursor):
    """基本的なREAD操作のテスト（SELECT）"""
    print("READ OPERATION TEST")
    
    # ユニークなテーブル名を生成
    table_name = f"test_read_{int(time.time() * 1000000)}"
    
    # テーブルを作成（PRIMARY KEYなし、INDEXのみ）
    cursor.execute(f'''CREATE TABLE ha_lineairdb_test.{table_name} (
        title VARCHAR(50) NOT NULL,
        content TEXT,
        INDEX title_idx (title)
    ) ENGINE = LineairDB''')
    db.commit()
    
    # テストデータを挿入
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (title, content) VALUES ("alice", "data1")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (title, content) VALUES ("bob", "data2")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (title, content) VALUES ("carol", "data3")')
    db.commit()
    
    # 単一行の読み取り
    cursor.execute(f'SELECT title, content FROM ha_lineairdb_test.{table_name} WHERE title = "bob"')
    rows = cursor.fetchall()
    if len(rows) != 1 or rows[0][0] != "bob" or rows[0][1] != "data2":
        print("\tFailed: expected ('bob', 'data2')")
        print("\tGot:", rows)
        return 1
    
    # 全行の読み取り
    cursor.execute(f'SELECT title FROM ha_lineairdb_test.{table_name}')
    rows = cursor.fetchall()
    if len(rows) != 3:
        print("\tFailed: expected 3 rows")
        print("\tGot:", rows)
        return 1
    
    titles = [row[0] for row in rows]
    if "alice" not in titles or "bob" not in titles or "carol" not in titles:
        print("\tFailed: expected alice, bob, and carol")
        print("\tGot:", titles)
        return 1
    
    print("\tPassed!")
    return 0

def test_secondary_index_multiple_values(db, cursor):
    """セカンダリインデックスで同じキーに対して複数の値を格納・読み取るテスト"""
    print("SECONDARY INDEX MULTIPLE VALUES TEST")
    
    # ユニークなテーブル名を生成
    table_name = f"test_multi_{int(time.time() * 1000000)}"
    
    # テーブルを作成（年齢のセカンダリインデックス付き）
    cursor.execute(f'''CREATE TABLE ha_lineairdb_test.{table_name} (
        name VARCHAR(50) NOT NULL,
        age INT NOT NULL,
        department VARCHAR(50),
        INDEX age_idx (age)
    ) ENGINE = LineairDB''')
    db.commit()
    
    # 同じ年齢を持つ複数のレコードを挿入
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (name, age, department) VALUES ("alice", 25, "engineering")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (name, age, department) VALUES ("bob", 30, "sales")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (name, age, department) VALUES ("carol", 25, "marketing")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (name, age, department) VALUES ("dave", 25, "engineering")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (name, age, department) VALUES ("john", 25, "student")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (name, age, department) VALUES ("eve", 30, "hr")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (name, age, department) VALUES ("gariman", 30, "programmer")')
    db.commit()
    
    # 全データを確認
    print("\t全データ:")
    cursor.execute(f'SELECT name, age, department FROM ha_lineairdb_test.{table_name}')
    all_rows = cursor.fetchall()
    for row in all_rows:
        print(f"\t  {row}")
    
    # age=25のレコードを検索（3件期待）
    cursor.execute(f'SELECT name, age, department FROM ha_lineairdb_test.{table_name} WHERE age = 25')
    rows_25 = cursor.fetchall()
    
    print(f"\tage=25のレコード（{len(rows_25)}件）:")
    for row in rows_25:
        print(f"\t  {row}")
    
    if len(rows_25) != 4:
        print(f"\tFailed: expected 3 rows with age=25, got {len(rows_25)}")
        return 1
    
    names_25 = [row[0] for row in rows_25]
    if "alice" not in names_25 or "carol" not in names_25 or "dave" not in names_25:
        print(f"\tFailed: expected alice, carol, and dave")
        print(f"\tGot: {names_25}")
        return 1
    
    # age=30のレコードを検索（2件期待）
    cursor.execute(f'SELECT name, age, department FROM ha_lineairdb_test.{table_name} WHERE age = 30')
    rows_30 = cursor.fetchall()
    
    print(f"\tage=30のレコード（{len(rows_30)}件）:")
    for row in rows_30:
        print(f"\t  {row}")
    
    # もし0件なら、全データで30のレコードを確認
    if len(rows_30) == 0:
        print("\t警告: age=30が0件。全データから30を探します:")
        cursor.execute(f'SELECT name, age, department FROM ha_lineairdb_test.{table_name}')
        all_check = cursor.fetchall()
        for row in all_check:
            if row[1] == 30:
                print(f"\t  見つかった: {row}")
    
    if len(rows_30) != 3:
        print(f"\tFailed: expected 2 rows with age=30, got {len(rows_30)}")
        return 1
    
    names_30 = [row[0] for row in rows_30]
    if "bob" not in names_30 or "eve" not in names_30:
        print(f"\tFailed: expected bob and eve")
        print(f"\tGot: {names_30}")
        return 1
    
    # 存在しない年齢を検索（0件期待）
    cursor.execute(f'SELECT name, age, department FROM ha_lineairdb_test.{table_name} WHERE age = 99')
    rows_99 = cursor.fetchall()
    if len(rows_99) != 0:
        print(f"\tFailed: expected 0 rows with age=99, got {len(rows_99)}")
        return 1
    
    print("\tPassed!")
    return 0

def test_secondary_index_range_query(db, cursor):
    """セカンダリインデックスを使った範囲検索のテスト"""
    print("SECONDARY INDEX RANGE QUERY TEST")
    
    # ユニークなテーブル名を生成
    table_name = f"test_range_{int(time.time() * 1000000)}"
    
    # テーブルを作成（年齢のセカンダリインデックス付き）
    cursor.execute(f'''CREATE TABLE ha_lineairdb_test.{table_name} (
        name VARCHAR(50) NOT NULL,
        age INT NOT NULL,
        department VARCHAR(50),
        INDEX age_idx (age)
    ) ENGINE = LineairDB''')
    db.commit()
    
    # データを挿入（年齢に幅を持たせる）
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (name, age, department) VALUES ("alice", 22, "engineering")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (name, age, department) VALUES ("bob", 30, "sales")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (name, age, department) VALUES ("carol", 25, "marketing")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (name, age, department) VALUES ("dave", 27, "engineering")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (name, age, department) VALUES ("john", 25, "student")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (name, age, department) VALUES ("eve", 31, "hr")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (name, age, department) VALUES ("gariman", 35, "programmer")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (name, age, department) VALUES ("haru", 29, "finance")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (name, age, department) VALUES ("ken", 40, "design")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (name, age, department) VALUES ("lisa", 24, "research")')
    db.commit()
    
    # 全データを確認
    print("\t全データ:")
    cursor.execute(f'SELECT name, age, department FROM ha_lineairdb_test.{table_name}')
    all_rows = cursor.fetchall()
    for row in all_rows:
        print(f"\t  {row}")
    
    if len(all_rows) != 10:
        print(f"\tFailed: expected 10 rows, got {len(all_rows)}")
        return 1
    
    # 特定値検索: age = 25（2件期待: carol, john）
    print("\n\t特定値検索: age = 25")
    cursor.execute(f'SELECT name, age, department FROM ha_lineairdb_test.{table_name} WHERE age = 25')
    rows_25 = cursor.fetchall()
    for row in rows_25:
        print(f"\t  {row}")
    
    if len(rows_25) != 2:
        print(f"\tFailed: expected 2 rows with age=25, got {len(rows_25)}")
        return 1
    
    names_25 = [row[0] for row in rows_25]
    if "carol" not in names_25 or "john" not in names_25:
        print(f"\tFailed: expected carol and john")
        print(f"\tGot: {names_25}")
        return 1
    
    # 特定値検索: age = 30（1件期待: bob）
    print("\n\t特定値検索: age = 30")
    cursor.execute(f'SELECT name, age, department FROM ha_lineairdb_test.{table_name} WHERE age = 30')
    rows_30 = cursor.fetchall()
    for row in rows_30:
        print(f"\t  {row}")
    
    if len(rows_30) != 1:
        print(f"\tFailed: expected 1 row with age=30, got {len(rows_30)}")
        return 1
    
    if rows_30[0][0] != "bob":
        print(f"\tFailed: expected bob")
        print(f"\tGot: {rows_30[0][0]}")
        return 1
    
    # 範囲検索: age < 30（6件期待: alice(22), lisa(24), carol(25), john(25), dave(27), haru(29)）
    print("\n\t範囲検索: age < 30")
    cursor.execute(f'SELECT name, age, department FROM ha_lineairdb_test.{table_name} WHERE age < 30')
    rows_lt_30 = cursor.fetchall()
    for row in rows_lt_30:
        print(f"\t  {row}")
    
    if len(rows_lt_30) != 6:
        print(f"\tFailed: expected 6 rows with age<30, got {len(rows_lt_30)}")
        return 1
    
    names_lt_30 = [row[0] for row in rows_lt_30]
    expected_names = ["alice", "lisa", "carol", "john", "dave", "haru"]
    for name in expected_names:
        if name not in names_lt_30:
            print(f"\tFailed: expected {name} in age<30 results")
            print(f"\tGot: {names_lt_30}")
            return 1
    
    # 範囲検索: age <= 30（7件期待: 上記6件 + bob(30)）
    print("\n\t範囲検索: age <= 30")
    cursor.execute(f'SELECT name, age, department FROM ha_lineairdb_test.{table_name} WHERE age <= 30')
    rows_lte_30 = cursor.fetchall()
    for row in rows_lte_30:
        print(f"\t  {row}")
    
    if len(rows_lte_30) != 7:
        print(f"\tFailed: expected 7 rows with age<=30, got {len(rows_lte_30)}")
        return 1
    
    # 範囲検索: age > 30（3件期待: eve(31), gariman(35), ken(40)）
    print("\n\t範囲検索: age > 30")
    cursor.execute(f'SELECT name, age, department FROM ha_lineairdb_test.{table_name} WHERE age > 30')
    rows_gt_30 = cursor.fetchall()
    for row in rows_gt_30:
        print(f"\t  {row}")
    
    if len(rows_gt_30) != 3:
        print(f"\tFailed: expected 3 rows with age>30, got {len(rows_gt_30)}")
        return 1
    
    names_gt_30 = [row[0] for row in rows_gt_30]
    expected_names_gt = ["eve", "gariman", "ken"]
    for name in expected_names_gt:
        if name not in names_gt_30:
            print(f"\tFailed: expected {name} in age>30 results")
            print(f"\tGot: {names_gt_30}")
            return 1
    
    # 範囲検索: age >= 30（4件期待: bob(30), eve(31), gariman(35), ken(40)）
    print("\n\t範囲検索: age >= 30")
    cursor.execute(f'SELECT name, age, department FROM ha_lineairdb_test.{table_name} WHERE age >= 30')
    rows_gte_30 = cursor.fetchall()
    for row in rows_gte_30:
        print(f"\t  {row}")
    
    if len(rows_gte_30) != 4:
        print(f"\tFailed: expected 4 rows with age>=30, got {len(rows_gte_30)}")
        return 1
    
    # 範囲検索: age BETWEEN 25 AND 30（5件期待: carol(25), john(25), dave(27), haru(29), bob(30)）
    print("\n\t範囲検索: age BETWEEN 25 AND 30")
    cursor.execute(f'SELECT name, age, department FROM ha_lineairdb_test.{table_name} WHERE age BETWEEN 25 AND 30')
    rows_between = cursor.fetchall()
    for row in rows_between:
        print(f"\t  {row}")
    
    if len(rows_between) != 5:
        print(f"\tFailed: expected 5 rows with age BETWEEN 25 AND 30, got {len(rows_between)}")
        return 1
    
    names_between = [row[0] for row in rows_between]
    expected_names_between = ["carol", "john", "dave", "haru", "bob"]
    for name in expected_names_between:
        if name not in names_between:
            print(f"\tFailed: expected {name} in BETWEEN results")
            print(f"\tGot: {names_between}")
            return 1
    
    print("\n\tPassed!")
    return 0

def test_string_range_query(db, cursor):
    """文字列カラムのセカンダリインデックスを使った範囲検索のテスト"""
    print("STRING RANGE QUERY TEST")
    
    # ユニークなテーブル名を生成
    table_name = f"test_str_{int(time.time() * 1000000)}"
    
    # テーブルを作成（短いコードカラムにセカンダリインデックス）
    cursor.execute(f'''CREATE TABLE ha_lineairdb_test.{table_name} (
        id INT NOT NULL,
        code VARCHAR(5) NOT NULL,
        name VARCHAR(20),
        INDEX code_idx (code)
    ) ENGINE = LineairDB''')
    db.commit()
    
    # データを挿入（短いコード: A1, B2, C3, D4, E5）
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, code, name) VALUES (1, "A1", "alpha")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, code, name) VALUES (2, "B2", "beta")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, code, name) VALUES (3, "C3", "gamma")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, code, name) VALUES (4, "D4", "delta")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, code, name) VALUES (5, "E5", "epsilon")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, code, name) VALUES (6, "AA", "test1")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, code, name) VALUES (7, "BB", "test2")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, code, name) VALUES (8, "CC", "test3")')
    db.commit()
    
    # 全データを確認
    print("\t全データ:")
    cursor.execute(f'SELECT id, code, name FROM ha_lineairdb_test.{table_name}')
    all_rows = cursor.fetchall()
    for row in all_rows:
        print(f"\t  {row}")
    
    if len(all_rows) != 8:
        print(f"\tFailed: expected 8 rows, got {len(all_rows)}")
        return 1
    
    # 文字列範囲検索: code < "C3" (A1, AA, B2, BB期待)
    print("\n\t範囲検索: code < 'C3'")
    cursor.execute(f'SELECT id, code, name FROM ha_lineairdb_test.{table_name} WHERE code < "C3"')
    rows_lt = cursor.fetchall()
    for row in rows_lt:
        print(f"\t  {row}")
    
    if len(rows_lt) != 4:
        print(f"\tFailed: expected 4 rows with code < 'C3', got {len(rows_lt)}")
        return 1
    
    codes_lt = [row[1] for row in rows_lt]
    expected_codes = ["A1", "AA", "B2", "BB"]
    for code in expected_codes:
        if code not in codes_lt:
            print(f"\tFailed: expected {code} in code < 'C3' results")
            print(f"\tGot: {codes_lt}")
            return 1
    
    # 文字列範囲検索: code >= "C3" (C3, CC, D4, E5期待)
    print("\n\t範囲検索: code >= 'C3'")
    cursor.execute(f'SELECT id, code, name FROM ha_lineairdb_test.{table_name} WHERE code >= "C3"')
    rows_gte = cursor.fetchall()
    for row in rows_gte:
        print(f"\t  {row}")
    
    if len(rows_gte) != 4:
        print(f"\tFailed: expected 4 rows with code >= 'C3', got {len(rows_gte)}")
        return 1
    
    codes_gte = [row[1] for row in rows_gte]
    expected_codes_gte = ["C3", "CC", "D4", "E5"]
    for code in expected_codes_gte:
        if code not in codes_gte:
            print(f"\tFailed: expected {code} in code >= 'C3' results")
            print(f"\tGot: {codes_gte}")
            return 1
    
    # 文字列範囲検索: code BETWEEN "B2" AND "D4" (B2, BB, C3, CC, D4期待)
    print("\n\t範囲検索: code BETWEEN 'B2' AND 'D4'")
    cursor.execute(f'SELECT id, code, name FROM ha_lineairdb_test.{table_name} WHERE code BETWEEN "B2" AND "D4"')
    rows_between = cursor.fetchall()
    for row in rows_between:
        print(f"\t  {row}")
    
    if len(rows_between) != 5:
        print(f"\tFailed: expected 5 rows with code BETWEEN 'B2' AND 'D4', got {len(rows_between)}")
        return 1
    
    codes_between = [row[1] for row in rows_between]
    expected_codes_between = ["B2", "BB", "C3", "CC", "D4"]
    for code in expected_codes_between:
        if code not in codes_between:
            print(f"\tFailed: expected {code} in BETWEEN results")
            print(f"\tGot: {codes_between}")
            return 1
    
    print("\n\tPassed!")
    return 0

def test_datetime_range_query(db, cursor):
    """DATETIME型カラムのセカンダリインデックスを使った範囲検索のテスト"""
    print("DATETIME RANGE QUERY TEST")
    
    # ユニークなテーブル名を生成
    table_name = f"test_dt_{int(time.time() * 1000000)}"
    
    # テーブルを作成（登録日時のセカンダリインデックス）
    cursor.execute(f'''CREATE TABLE ha_lineairdb_test.{table_name} (
        id INT NOT NULL,
        user VARCHAR(10),
        reg_date DATETIME NOT NULL,
        INDEX date_idx (reg_date)
    ) ENGINE = LineairDB''')
    db.commit()
    
    # データを挿入（2024年1月〜2024年12月の様々な日付）
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, user, reg_date) VALUES (1, "alice", "2024-01-15 10:00:00")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, user, reg_date) VALUES (2, "bob", "2024-03-20 14:30:00")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, user, reg_date) VALUES (3, "carol", "2024-06-10 09:15:00")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, user, reg_date) VALUES (4, "dave", "2024-06-25 16:45:00")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, user, reg_date) VALUES (5, "eve", "2024-09-05 11:20:00")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, user, reg_date) VALUES (6, "frank", "2024-12-01 08:00:00")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, user, reg_date) VALUES (7, "grace", "2024-12-15 13:30:00")')
    db.commit()
    
    # 全データを確認
    print("\t全データ:")
    cursor.execute(f'SELECT id, user, reg_date FROM ha_lineairdb_test.{table_name}')
    all_rows = cursor.fetchall()
    for row in all_rows:
        print(f"\t  {row}")
    
    if len(all_rows) != 7:
        print(f"\tFailed: expected 7 rows, got {len(all_rows)}")
        return 1
    
    # 日付範囲検索: reg_date < "2024-06-01" (alice, bob期待)
    print("\n\t範囲検索: reg_date < '2024-06-01'")
    cursor.execute(f'SELECT id, user, reg_date FROM ha_lineairdb_test.{table_name} WHERE reg_date < "2024-06-01"')
    rows_lt = cursor.fetchall()
    for row in rows_lt:
        print(f"\t  {row}")
    
    if len(rows_lt) != 2:
        print(f"\tFailed: expected 2 rows with reg_date < '2024-06-01', got {len(rows_lt)}")
        return 1
    
    users_lt = [row[1] for row in rows_lt]
    if "alice" not in users_lt or "bob" not in users_lt:
        print(f"\tFailed: expected alice and bob")
        print(f"\tGot: {users_lt}")
        return 1
    
    # 日付範囲検索: reg_date >= "2024-09-01" (eve, frank, grace期待)
    print("\n\t範囲検索: reg_date >= '2024-09-01'")
    cursor.execute(f'SELECT id, user, reg_date FROM ha_lineairdb_test.{table_name} WHERE reg_date >= "2024-09-01"')
    rows_gte = cursor.fetchall()
    for row in rows_gte:
        print(f"\t  {row}")
    
    if len(rows_gte) != 3:
        print(f"\tFailed: expected 3 rows with reg_date >= '2024-09-01', got {len(rows_gte)}")
        return 1
    
    users_gte = [row[1] for row in rows_gte]
    expected_users = ["eve", "frank", "grace"]
    for user in expected_users:
        if user not in users_gte:
            print(f"\tFailed: expected {user} in results")
            print(f"\tGot: {users_gte}")
            return 1
    
    # 日付範囲検索: reg_date BETWEEN "2024-06-01" AND "2024-09-30" (carol, dave, eve期待)
    print("\n\t範囲検索: reg_date BETWEEN '2024-06-01' AND '2024-09-30'")
    cursor.execute(f'SELECT id, user, reg_date FROM ha_lineairdb_test.{table_name} WHERE reg_date BETWEEN "2024-06-01" AND "2024-09-30"')
    rows_between = cursor.fetchall()
    for row in rows_between:
        print(f"\t  {row}")
    
    if len(rows_between) != 3:
        print(f"\tFailed: expected 3 rows with reg_date BETWEEN '2024-06-01' AND '2024-09-30', got {len(rows_between)}")
        return 1
    
    users_between = [row[1] for row in rows_between]
    expected_users_between = ["carol", "dave", "eve"]
    for user in expected_users_between:
        if user not in users_between:
            print(f"\tFailed: expected {user} in BETWEEN results")
            print(f"\tGot: {users_between}")
            return 1
    
    # 時刻込みの範囲検索: reg_date > "2024-06-10 12:00:00" (dave, eve, frank, grace期待)
    print("\n\t範囲検索: reg_date > '2024-06-10 12:00:00' (時刻指定)")
    cursor.execute(f'SELECT id, user, reg_date FROM ha_lineairdb_test.{table_name} WHERE reg_date > "2024-06-10 12:00:00"')
    rows_time = cursor.fetchall()
    for row in rows_time:
        print(f"\t  {row}")
    
    if len(rows_time) != 4:
        print(f"\tFailed: expected 4 rows with reg_date > '2024-06-10 12:00:00', got {len(rows_time)}")
        return 1
    
    users_time = [row[1] for row in rows_time]
    expected_users_time = ["dave", "eve", "frank", "grace"]
    for user in expected_users_time:
        if user not in users_time:
            print(f"\tFailed: expected {user} in results")
            print(f"\tGot: {users_time}")
            return 1
    
    print("\n\tPassed!")
    return 0

def main():
    # データベースに接続
    db = mysql.connector.connect(host="localhost", user=args.user, password=args.password)
    cursor = db.cursor()
    
    # データベースを作成（存在しない場合）
    cursor.execute('CREATE DATABASE IF NOT EXISTS ha_lineairdb_test')
    db.commit()
    
    result = 0
    result |= test_write_operation(db, cursor)
    result |= test_read_operation(db, cursor)
    result |= test_secondary_index_multiple_values(db, cursor)
    result |= test_secondary_index_range_query(db, cursor)
    result |= test_string_range_query(db, cursor)
    result |= test_datetime_range_query(db, cursor)
    
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

