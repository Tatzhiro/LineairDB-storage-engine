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

def test_composite_index_int_string(db, cursor):
    """複合インデックス（INT + STRING）のテスト"""
    print("COMPOSITE INDEX (INT + STRING) TEST")
    
    # ユニークなテーブル名を生成
    table_name = f"test_comp_int_str_{int(time.time() * 1000000)}"
    
    # テーブルを作成（age + department の複合インデックス）
    cursor.execute(f'''CREATE TABLE ha_lineairdb_test.{table_name} (
        name VARCHAR(50) NOT NULL,
        age INT NOT NULL,
        department VARCHAR(50) NOT NULL,
        salary INT,
        INDEX age_dept_idx (age, department)
    ) ENGINE = LineairDB''')
    db.commit()
    
    # データを挿入
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (name, age, department, salary) VALUES ("alice", 25, "engineering", 5000)')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (name, age, department, salary) VALUES ("bob", 25, "sales", 4500)')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (name, age, department, salary) VALUES ("carol", 30, "engineering", 6000)')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (name, age, department, salary) VALUES ("dave", 25, "engineering", 5200)')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (name, age, department, salary) VALUES ("eve", 30, "sales", 5500)')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (name, age, department, salary) VALUES ("frank", 25, "marketing", 4800)')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (name, age, department, salary) VALUES ("grace", 30, "engineering", 6200)')
    db.commit()
    
    # 全データを確認
    print("\t全データ:")
    cursor.execute(f'SELECT name, age, department, salary FROM ha_lineairdb_test.{table_name}')
    all_rows = cursor.fetchall()
    for row in all_rows:
        print(f"\t  {row}")
    
    if len(all_rows) != 7:
        print(f"\tFailed: expected 7 rows, got {len(all_rows)}")
        return 1
    
    # 完全一致検索: age=25 AND department="engineering" (alice, dave期待)
    print("\n\t複合キー完全一致: age=25 AND department='engineering'")
    cursor.execute(f'SELECT name, age, department, salary FROM ha_lineairdb_test.{table_name} WHERE age = 25 AND department = "engineering"')
    rows_exact = cursor.fetchall()
    for row in rows_exact:
        print(f"\t  {row}")
    
    if len(rows_exact) != 2:
        print(f"\tFailed: expected 2 rows, got {len(rows_exact)}")
        return 1
    
    names_exact = [row[0] for row in rows_exact]
    if "alice" not in names_exact or "dave" not in names_exact:
        print(f"\tFailed: expected alice and dave")
        print(f"\tGot: {names_exact}")
        return 1
    
    # 前方一致検索: age=25 (alice, bob, dave, frank期待)
    print("\n\t複合キー前方一致: age=25")
    cursor.execute(f'SELECT name, age, department FROM ha_lineairdb_test.{table_name} WHERE age = 25')
    rows_prefix = cursor.fetchall()
    for row in rows_prefix:
        print(f"\t  {row}")
    
    if len(rows_prefix) != 4:
        print(f"\tFailed: expected 4 rows with age=25, got {len(rows_prefix)}")
        return 1
    
    names_prefix = [row[0] for row in rows_prefix]
    expected_names = ["alice", "bob", "dave", "frank"]
    for name in expected_names:
        if name not in names_prefix:
            print(f"\tFailed: expected {name} in results")
            print(f"\tGot: {names_prefix}")
            return 1
    
    # 範囲検索: age=30 AND department<="engineering" (carol, grace期待)
    # 注: 'sales' > 'engineering' なので、eve (30, 'sales') は除外される
    print("\n\t複合キー範囲検索: age=30 AND department<='engineering'")
    cursor.execute(f'SELECT name, age, department FROM ha_lineairdb_test.{table_name} WHERE age = 30 AND department <= "engineering"')
    rows_range = cursor.fetchall()
    for row in rows_range:
        print(f"\t  {row}")
    
    if len(rows_range) != 2:
        print(f"\tFailed: expected 2 rows, got {len(rows_range)}")
        return 1
    
    names_range = [row[0] for row in rows_range]
    if "carol" not in names_range or "grace" not in names_range:
        print(f"\tFailed: expected carol and grace")
        print(f"\tGot: {names_range}")
        return 1
    
    print("\n\tPassed!")
    return 0

def test_composite_index_string_datetime(db, cursor):
    """複合インデックス（STRING + DATETIME）のテスト"""
    print("COMPOSITE INDEX (STRING + DATETIME) TEST")
    
    # ユニークなテーブル名を生成
    table_name = f"test_comp_str_dt_{int(time.time() * 1000000)}"
    
    # テーブルを作成（status + created_at の複合インデックス）
    cursor.execute(f'''CREATE TABLE ha_lineairdb_test.{table_name} (
        id INT NOT NULL,
        status VARCHAR(20) NOT NULL,
        created_at DATETIME NOT NULL,
        description VARCHAR(50),
        INDEX status_date_idx (status, created_at)
    ) ENGINE = LineairDB''')
    db.commit()
    
    # データを挿入
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, status, created_at, description) VALUES (1, "active", "2024-01-15 10:00:00", "task1")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, status, created_at, description) VALUES (2, "active", "2024-03-20 14:30:00", "task2")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, status, created_at, description) VALUES (3, "pending", "2024-02-10 09:15:00", "task3")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, status, created_at, description) VALUES (4, "active", "2024-06-25 16:45:00", "task4")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, status, created_at, description) VALUES (5, "completed", "2024-05-05 11:20:00", "task5")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, status, created_at, description) VALUES (6, "pending", "2024-07-01 08:00:00", "task6")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, status, created_at, description) VALUES (7, "active", "2024-02-15 13:30:00", "task7")')
    db.commit()
    
    # 全データを確認
    print("\t全データ:")
    cursor.execute(f'SELECT id, status, created_at, description FROM ha_lineairdb_test.{table_name}')
    all_rows = cursor.fetchall()
    for row in all_rows:
        print(f"\t  {row}")
    
    if len(all_rows) != 7:
        print(f"\tFailed: expected 7 rows, got {len(all_rows)}")
        return 1
    
    # 完全一致検索: status="active" AND created_at="2024-03-20 14:30:00" (id=2期待)
    print("\n\t複合キー完全一致: status='active' AND created_at='2024-03-20 14:30:00'")
    cursor.execute(f'SELECT id, status, created_at FROM ha_lineairdb_test.{table_name} WHERE status = "active" AND created_at = "2024-03-20 14:30:00"')
    rows_exact = cursor.fetchall()
    for row in rows_exact:
        print(f"\t  {row}")
    
    if len(rows_exact) != 1:
        print(f"\tFailed: expected 1 row, got {len(rows_exact)}")
        return 1
    
    if rows_exact[0][0] != 2:
        print(f"\tFailed: expected id=2")
        print(f"\tGot: {rows_exact[0][0]}")
        return 1
    
    # 前方一致検索: status="active" (id=1,2,4,7期待)
    print("\n\t複合キー前方一致: status='active'")
    cursor.execute(f'SELECT id, status, created_at FROM ha_lineairdb_test.{table_name} WHERE status = "active"')
    rows_prefix = cursor.fetchall()
    for row in rows_prefix:
        print(f"\t  {row}")
    
    if len(rows_prefix) != 4:
        print(f"\tFailed: expected 4 rows with status='active', got {len(rows_prefix)}")
        return 1
    
    ids_prefix = [row[0] for row in rows_prefix]
    expected_ids = [1, 2, 4, 7]
    for id_val in expected_ids:
        if id_val not in ids_prefix:
            print(f"\tFailed: expected id={id_val} in results")
            print(f"\tGot: {ids_prefix}")
            return 1
    
    # 範囲検索: status="active" AND created_at < "2024-03-01" (id=1,7期待)
    print("\n\t複合キー範囲検索: status='active' AND created_at < '2024-03-01'")
    cursor.execute(f'SELECT id, status, created_at FROM ha_lineairdb_test.{table_name} WHERE status = "active" AND created_at < "2024-03-01"')
    rows_range = cursor.fetchall()
    for row in rows_range:
        print(f"\t  {row}")
    
    if len(rows_range) != 2:
        print(f"\tFailed: expected 2 rows, got {len(rows_range)}")
        return 1
    
    ids_range = [row[0] for row in rows_range]
    if 1 not in ids_range or 7 not in ids_range:
        print(f"\tFailed: expected id=1 and id=7")
        print(f"\tGot: {ids_range}")
        return 1
    
    print("\n\tPassed!")
    return 0

def test_composite_index_int_int(db, cursor):
    """複合インデックス（INT + INT）のテスト"""
    print("COMPOSITE INDEX (INT + INT) TEST")
    
    # ユニークなテーブル名を生成
    table_name = f"test_comp_int_int_{int(time.time() * 1000000)}"
    
    # テーブルを作成（year + month の複合インデックス）
    cursor.execute(f'''CREATE TABLE ha_lineairdb_test.{table_name} (
        id INT NOT NULL,
        year INT NOT NULL,
        month INT NOT NULL,
        sales INT,
        INDEX year_month_idx (year, month)
    ) ENGINE = LineairDB''')
    db.commit()
    
    # データを挿入
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, year, month, sales) VALUES (1, 2023, 1, 1000)')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, year, month, sales) VALUES (2, 2023, 6, 1500)')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, year, month, sales) VALUES (3, 2023, 12, 2000)')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, year, month, sales) VALUES (4, 2024, 1, 1800)')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, year, month, sales) VALUES (5, 2024, 3, 2200)')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, year, month, sales) VALUES (6, 2024, 6, 2500)')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, year, month, sales) VALUES (7, 2024, 12, 3000)')
    db.commit()
    
    # 全データを確認
    print("\t全データ:")
    cursor.execute(f'SELECT id, year, month, sales FROM ha_lineairdb_test.{table_name}')
    all_rows = cursor.fetchall()
    for row in all_rows:
        print(f"\t  {row}")
    
    if len(all_rows) != 7:
        print(f"\tFailed: expected 7 rows, got {len(all_rows)}")
        return 1
    
    # 完全一致検索: year=2024 AND month=6 (id=6期待)
    print("\n\t複合キー完全一致: year=2024 AND month=6")
    cursor.execute(f'SELECT id, year, month, sales FROM ha_lineairdb_test.{table_name} WHERE year = 2024 AND month = 6')
    rows_exact = cursor.fetchall()
    for row in rows_exact:
        print(f"\t  {row}")
    
    if len(rows_exact) != 1:
        print(f"\tFailed: expected 1 row, got {len(rows_exact)}")
        return 1
    
    if rows_exact[0][0] != 6:
        print(f"\tFailed: expected id=6")
        print(f"\tGot: {rows_exact[0][0]}")
        return 1
    
    # 前方一致検索: year=2024 (id=4,5,6,7期待)
    print("\n\t複合キー前方一致: year=2024")
    cursor.execute(f'SELECT id, year, month FROM ha_lineairdb_test.{table_name} WHERE year = 2024')
    rows_prefix = cursor.fetchall()
    for row in rows_prefix:
        print(f"\t  {row}")
    
    if len(rows_prefix) != 4:
        print(f"\tFailed: expected 4 rows with year=2024, got {len(rows_prefix)}")
        return 1
    
    ids_prefix = [row[0] for row in rows_prefix]
    expected_ids = [4, 5, 6, 7]
    for id_val in expected_ids:
        if id_val not in ids_prefix:
            print(f"\tFailed: expected id={id_val} in results")
            print(f"\tGot: {ids_prefix}")
            return 1
    
    # 範囲検索: year=2024 AND month>=6 (id=6,7期待)
    print("\n\t複合キー範囲検索: year=2024 AND month>=6")
    cursor.execute(f'SELECT id, year, month FROM ha_lineairdb_test.{table_name} WHERE year = 2024 AND month >= 6')
    rows_range = cursor.fetchall()
    for row in rows_range:
        print(f"\t  {row}")
    
    if len(rows_range) != 2:
        print(f"\tFailed: expected 2 rows, got {len(rows_range)}")
        return 1
    
    ids_range = [row[0] for row in rows_range]
    if 6 not in ids_range or 7 not in ids_range:
        print(f"\tFailed: expected id=6 and id=7")
        print(f"\tGot: {ids_range}")
        return 1
    
    # 範囲検索: year=2023 AND month BETWEEN 6 AND 12 (id=2,3期待)
    print("\n\t複合キー範囲検索: year=2023 AND month BETWEEN 6 AND 12")
    cursor.execute(f'SELECT id, year, month FROM ha_lineairdb_test.{table_name} WHERE year = 2023 AND month BETWEEN 6 AND 12')
    rows_between = cursor.fetchall()
    for row in rows_between:
        print(f"\t  {row}")
    
    if len(rows_between) != 2:
        print(f"\tFailed: expected 2 rows, got {len(rows_between)}")
        return 1
    
    ids_between = [row[0] for row in rows_between]
    if 2 not in ids_between or 3 not in ids_between:
        print(f"\tFailed: expected id=2 and id=3")
        print(f"\tGot: {ids_between}")
        return 1
    
    print("\n\tPassed!")
    return 0

def test_composite_index_skip_middle_key(db, cursor):
    """複合インデックス（中間キー欠落）のテスト - 2番目のキーを省略した場合"""
    print("COMPOSITE INDEX SKIP MIDDLE KEY TEST")
    
    # ユニークなテーブル名を生成
    table_name = f"test_skip_middle_{int(time.time() * 1000000)}"
    
    # 3つのカラムの複合インデックスを作成
    cursor.execute(f'''CREATE TABLE ha_lineairdb_test.{table_name} (
        id INT NOT NULL,
        category VARCHAR(20) NOT NULL,
        status VARCHAR(20) NOT NULL,
        priority INT NOT NULL,
        description VARCHAR(50),
        INDEX cat_stat_pri_idx (category, status, priority)
    ) ENGINE = LineairDB''')
    db.commit()
    
    # データを挿入
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, category, status, priority, description) VALUES (1, "bug", "open", 1, "critical bug")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, category, status, priority, description) VALUES (2, "bug", "open", 3, "minor bug")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, category, status, priority, description) VALUES (3, "bug", "closed", 1, "fixed bug")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, category, status, priority, description) VALUES (4, "bug", "closed", 3, "wontfix")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, category, status, priority, description) VALUES (5, "feature", "open", 1, "new feature")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, category, status, priority, description) VALUES (6, "feature", "open", 2, "enhancement")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, category, status, priority, description) VALUES (7, "feature", "closed", 1, "implemented")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, category, status, priority, description) VALUES (8, "bug", "open", 2, "normal bug")')
    db.commit()
    
    # 全データを確認
    print("\t全データ:")
    cursor.execute(f'SELECT id, category, status, priority, description FROM ha_lineairdb_test.{table_name}')
    all_rows = cursor.fetchall()
    for row in all_rows:
        print(f"\t  {row}")
    
    if len(all_rows) != 8:
        print(f"\tFailed: expected 8 rows, got {len(all_rows)}")
        return 1
    
    # 完全一致: category="bug" AND status="open" AND priority=1
    print("\n\t完全一致: category='bug' AND status='open' AND priority=1")
    cursor.execute(f'SELECT id, category, status, priority FROM ha_lineairdb_test.{table_name} WHERE category = "bug" AND status = "open" AND priority = 1')
    rows_full = cursor.fetchall()
    for row in rows_full:
        print(f"\t  {row}")
    
    if len(rows_full) != 1 or rows_full[0][0] != 1:
        print(f"\tFailed: expected id=1")
        print(f"\tGot: {rows_full}")
        return 1
    
    # 前方一致: category="bug" のみ（status, priorityは指定なし）
    # → インデックスの最初の部分だけ使用、4件期待: id=1,2,3,4,8
    print("\n\t前方一致: category='bug' のみ")
    cursor.execute(f'SELECT id, category, status, priority FROM ha_lineairdb_test.{table_name} WHERE category = "bug"')
    rows_prefix1 = cursor.fetchall()
    for row in rows_prefix1:
        print(f"\t  {row}")
    
    if len(rows_prefix1) != 5:
        print(f"\tFailed: expected 5 rows with category='bug', got {len(rows_prefix1)}")
        return 1
    
    ids_prefix1 = [row[0] for row in rows_prefix1]
    expected_ids_1 = [1, 2, 3, 4, 8]
    for id_val in expected_ids_1:
        if id_val not in ids_prefix1:
            print(f"\tFailed: expected id={id_val} in results")
            print(f"\tGot: {ids_prefix1}")
            return 1
    
    # 前方一致: category="bug" AND status="open"（priorityは指定なし）
    # → インデックスの最初2つを使用、3件期待: id=1,2,8
    print("\n\t前方一致: category='bug' AND status='open'")
    cursor.execute(f'SELECT id, category, status, priority FROM ha_lineairdb_test.{table_name} WHERE category = "bug" AND status = "open"')
    rows_prefix2 = cursor.fetchall()
    for row in rows_prefix2:
        print(f"\t  {row}")
    
    if len(rows_prefix2) != 3:
        print(f"\tFailed: expected 3 rows, got {len(rows_prefix2)}")
        return 1
    
    ids_prefix2 = [row[0] for row in rows_prefix2]
    expected_ids_2 = [1, 2, 8]
    for id_val in expected_ids_2:
        if id_val not in ids_prefix2:
            print(f"\tFailed: expected id={id_val} in results")
            print(f"\tGot: {ids_prefix2}")
            return 1
    
    # 中間キー省略: category="bug" AND priority=1（statusを省略）
    # → MySQLはcategoryのみインデックス使用、priorityは後からフィルタリング
    # → 2件期待: id=1,3（どちらもcategory='bug' AND priority=1）
    print("\n\t中間キー省略: category='bug' AND priority=1 (statusを省略)")
    print("\t  ※MySQLはcategoryのみインデックス使用、priorityはWHEREフィルタ")
    cursor.execute(f'SELECT id, category, status, priority FROM ha_lineairdb_test.{table_name} WHERE category = "bug" AND priority = 1')
    rows_skip = cursor.fetchall()
    for row in rows_skip:
        print(f"\t  {row}")
    
    if len(rows_skip) != 2:
        print(f"\tFailed: expected 2 rows (id=1,3), got {len(rows_skip)}")
        return 1
    
    ids_skip = [row[0] for row in rows_skip]
    if 1 not in ids_skip or 3 not in ids_skip:
        print(f"\tFailed: expected id=1 and id=3")
        print(f"\tGot: {ids_skip}")
        return 1
    
    # 範囲検索との組み合わせ: category="feature" AND priority>=2
    # → categoryのみインデックス使用、priority>=2は後からフィルタリング
    # → 1件期待: id=6（category='feature' AND priority=2）
    print("\n\t中間キー省略 + 範囲: category='feature' AND priority>=2")
    cursor.execute(f'SELECT id, category, status, priority FROM ha_lineairdb_test.{table_name} WHERE category = "feature" AND priority >= 2')
    rows_range = cursor.fetchall()
    for row in rows_range:
        print(f"\t  {row}")
    
    if len(rows_range) != 1:
        print(f"\tFailed: expected 1 row (id=6), got {len(rows_range)}")
        return 1
    
    if rows_range[0][0] != 6:
        print(f"\tFailed: expected id=6")
        print(f"\tGot: {rows_range[0][0]}")
        return 1
    
    print("\n\tPassed!")
    return 0

def test_composite_primary_key_basic(db, cursor):
    """複合PRIMARY KEYの基本テスト"""
    print("COMPOSITE PRIMARY KEY BASIC TEST")
    
    # ユニークなテーブル名を生成
    table_name = f"test_comp_pk_basic_{int(time.time() * 1000000)}"
    
    # 複合PRIMARY KEY（year + month）を持つテーブルを作成
    cursor.execute(f'''CREATE TABLE ha_lineairdb_test.{table_name} (
        year INT NOT NULL,
        month INT NOT NULL,
        sales INT,
        region VARCHAR(20),
        PRIMARY KEY (year, month)
    ) ENGINE = LineairDB''')
    db.commit()
    
    # データを挿入
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (year, month, sales, region) VALUES (2023, 1, 1000, "Tokyo")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (year, month, sales, region) VALUES (2023, 3, 1200, "Osaka")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (year, month, sales, region) VALUES (2023, 6, 1500, "Tokyo")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (year, month, sales, region) VALUES (2023, 12, 2000, "Nagoya")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (year, month, sales, region) VALUES (2024, 1, 1800, "Tokyo")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (year, month, sales, region) VALUES (2024, 3, 2200, "Osaka")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (year, month, sales, region) VALUES (2024, 6, 2500, "Tokyo")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (year, month, sales, region) VALUES (2024, 9, 2700, "Nagoya")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (year, month, sales, region) VALUES (2024, 12, 3000, "Tokyo")')
    db.commit()
    
    # 全データを確認
    print("\t全データ:")
    cursor.execute(f'SELECT year, month, sales, region FROM ha_lineairdb_test.{table_name}')
    all_rows = cursor.fetchall()
    for row in all_rows:
        print(f"\t  {row}")
    
    if len(all_rows) != 9:
        print(f"\tFailed: expected 9 rows, got {len(all_rows)}")
        return 1
    
    # 完全一致検索: year=2024 AND month=6
    print("\n\t複合PRIMARY KEY完全一致: year=2024 AND month=6")
    cursor.execute(f'SELECT year, month, sales, region FROM ha_lineairdb_test.{table_name} WHERE year = 2024 AND month = 6')
    rows_exact = cursor.fetchall()
    for row in rows_exact:
        print(f"\t  {row}")
    
    if len(rows_exact) != 1:
        print(f"\tFailed: expected 1 row, got {len(rows_exact)}")
        return 1
    
    if rows_exact[0][2] != 2500 or rows_exact[0][3] != "Tokyo":
        print(f"\tFailed: expected sales=2500, region='Tokyo'")
        print(f"\tGot: sales={rows_exact[0][2]}, region={rows_exact[0][3]}")
        return 1
    
    # 前方一致検索: year=2023のみ（monthは指定なし）
    print("\n\t複合PRIMARY KEY前方一致: year=2023")
    cursor.execute(f'SELECT year, month, sales FROM ha_lineairdb_test.{table_name} WHERE year = 2023')
    rows_prefix = cursor.fetchall()
    for row in rows_prefix:
        print(f"\t  {row}")
    
    if len(rows_prefix) != 4:
        print(f"\tFailed: expected 4 rows with year=2023, got {len(rows_prefix)}")
        return 1
    
    months_2023 = [row[1] for row in rows_prefix]
    expected_months = [1, 3, 6, 12]
    for month in expected_months:
        if month not in months_2023:
            print(f"\tFailed: expected month={month} in results")
            print(f"\tGot: {months_2023}")
            return 1
    
    print("\n\tPassed!")
    return 0

def test_composite_primary_key_range(db, cursor):
    """複合PRIMARY KEYの範囲検索テスト"""
    print("COMPOSITE PRIMARY KEY RANGE TEST")
    
    # ユニークなテーブル名を生成
    table_name = f"test_comp_pk_range_{int(time.time() * 1000000)}"
    
    # 複合PRIMARY KEY（category + item_id）を持つテーブルを作成
    cursor.execute(f'''CREATE TABLE ha_lineairdb_test.{table_name} (
        category VARCHAR(20) NOT NULL,
        item_id INT NOT NULL,
        item_name VARCHAR(50),
        price INT,
        PRIMARY KEY (category, item_id)
    ) ENGINE = LineairDB''')
    db.commit()
    
    # データを挿入
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (category, item_id, item_name, price) VALUES ("book", 1, "Novel A", 1000)')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (category, item_id, item_name, price) VALUES ("book", 5, "Novel B", 1500)')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (category, item_id, item_name, price) VALUES ("book", 10, "Novel C", 2000)')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (category, item_id, item_name, price) VALUES ("book", 15, "Novel D", 1800)')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (category, item_id, item_name, price) VALUES ("electronics", 2, "Phone", 50000)')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (category, item_id, item_name, price) VALUES ("electronics", 8, "Tablet", 30000)')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (category, item_id, item_name, price) VALUES ("electronics", 12, "Laptop", 80000)')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (category, item_id, item_name, price) VALUES ("food", 3, "Apple", 200)')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (category, item_id, item_name, price) VALUES ("food", 7, "Banana", 150)')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (category, item_id, item_name, price) VALUES ("food", 11, "Orange", 180)')
    db.commit()
    
    # 全データを確認
    print("\t全データ:")
    cursor.execute(f'SELECT category, item_id, item_name, price FROM ha_lineairdb_test.{table_name}')
    all_rows = cursor.fetchall()
    for row in all_rows:
        print(f"\t  {row}")
    
    if len(all_rows) != 10:
        print(f"\tFailed: expected 10 rows, got {len(all_rows)}")
        return 1
    
    # 範囲検索: category="book" AND item_id >= 5 AND item_id <= 12
    print("\n\t複合PRIMARY KEY範囲検索: category='book' AND item_id BETWEEN 5 AND 12")
    cursor.execute(f'SELECT category, item_id, item_name FROM ha_lineairdb_test.{table_name} WHERE category = "book" AND item_id BETWEEN 5 AND 12')
    rows_range = cursor.fetchall()
    for row in rows_range:
        print(f"\t  {row}")
    
    if len(rows_range) != 2:
        print(f"\tFailed: expected 2 rows, got {len(rows_range)}")
        return 1
    
    item_ids = [row[1] for row in rows_range]
    if 5 not in item_ids or 10 not in item_ids:
        print(f"\tFailed: expected item_id=5 and item_id=10")
        print(f"\tGot: {item_ids}")
        return 1
    
    # 範囲検索: category="electronics" AND item_id < 10
    print("\n\t複合PRIMARY KEY範囲検索: category='electronics' AND item_id < 10")
    cursor.execute(f'SELECT category, item_id, item_name FROM ha_lineairdb_test.{table_name} WHERE category = "electronics" AND item_id < 10')
    rows_lt = cursor.fetchall()
    for row in rows_lt:
        print(f"\t  {row}")
    
    if len(rows_lt) != 2:
        print(f"\tFailed: expected 2 rows, got {len(rows_lt)}")
        return 1
    
    item_ids_lt = [row[1] for row in rows_lt]
    if 2 not in item_ids_lt or 8 not in item_ids_lt:
        print(f"\tFailed: expected item_id=2 and item_id=8")
        print(f"\tGot: {item_ids_lt}")
        return 1
    
    # 範囲検索: category="food" AND item_id > 5
    print("\n\t複合PRIMARY KEY範囲検索: category='food' AND item_id > 5")
    cursor.execute(f'SELECT category, item_id, item_name FROM ha_lineairdb_test.{table_name} WHERE category = "food" AND item_id > 5')
    rows_gt = cursor.fetchall()
    for row in rows_gt:
        print(f"\t  {row}")
    
    if len(rows_gt) != 2:
        print(f"\tFailed: expected 2 rows, got {len(rows_gt)}")
        return 1
    
    item_ids_gt = [row[1] for row in rows_gt]
    if 7 not in item_ids_gt or 11 not in item_ids_gt:
        print(f"\tFailed: expected item_id=7 and item_id=11")
        print(f"\tGot: {item_ids_gt}")
        return 1
    
    # カテゴリの範囲検索: category >= "electronics" (electronics, food期待)
    print("\n\tPRIMARY KEY第1カラムの範囲検索: category >= 'electronics'")
    cursor.execute(f'SELECT category, item_id, item_name FROM ha_lineairdb_test.{table_name} WHERE category >= "electronics"')
    rows_cat = cursor.fetchall()
    for row in rows_cat:
        print(f"\t  {row}")
    
    if len(rows_cat) != 6:  # electronics 3件 + food 3件
        print(f"\tFailed: expected 6 rows, got {len(rows_cat)}")
        return 1
    
    categories = set([row[0] for row in rows_cat])
    if "electronics" not in categories or "food" not in categories:
        print(f"\tFailed: expected electronics and food")
        print(f"\tGot: {categories}")
        return 1
    
    print("\n\tPassed!")
    return 0

def test_composite_primary_key_three_columns(db, cursor):
    """3カラム複合PRIMARY KEYのテスト"""
    print("COMPOSITE PRIMARY KEY (3 COLUMNS) TEST")
    
    # ユニークなテーブル名を生成
    table_name = f"test_comp_pk_3col_{int(time.time() * 1000000)}"
    
    # 3カラム複合PRIMARY KEY（year + month + day）を持つテーブルを作成
    cursor.execute(f'''CREATE TABLE ha_lineairdb_test.{table_name} (
        year INT NOT NULL,
        month INT NOT NULL,
        day INT NOT NULL,
        temperature FLOAT,
        weather VARCHAR(20),
        PRIMARY KEY (year, month, day)
    ) ENGINE = LineairDB''')
    db.commit()
    
    # データを挿入
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (year, month, day, temperature, weather) VALUES (2024, 1, 1, 5.5, "sunny")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (year, month, day, temperature, weather) VALUES (2024, 1, 15, 3.2, "cloudy")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (year, month, day, temperature, weather) VALUES (2024, 1, 31, 6.8, "rainy")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (year, month, day, temperature, weather) VALUES (2024, 3, 1, 12.5, "sunny")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (year, month, day, temperature, weather) VALUES (2024, 3, 15, 15.2, "sunny")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (year, month, day, temperature, weather) VALUES (2024, 3, 31, 18.5, "cloudy")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (year, month, day, temperature, weather) VALUES (2024, 6, 1, 22.5, "sunny")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (year, month, day, temperature, weather) VALUES (2024, 6, 15, 25.8, "sunny")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (year, month, day, temperature, weather) VALUES (2024, 6, 30, 28.2, "hot")')
    db.commit()
    
    # 全データを確認
    print("\t全データ:")
    cursor.execute(f'SELECT year, month, day, temperature, weather FROM ha_lineairdb_test.{table_name}')
    all_rows = cursor.fetchall()
    for row in all_rows:
        print(f"\t  {row}")
    
    if len(all_rows) != 9:
        print(f"\tFailed: expected 9 rows, got {len(all_rows)}")
        return 1
    
    # 完全一致検索: year=2024 AND month=3 AND day=15
    print("\n\t3カラム完全一致: year=2024 AND month=3 AND day=15")
    cursor.execute(f'SELECT year, month, day, temperature, weather FROM ha_lineairdb_test.{table_name} WHERE year = 2024 AND month = 3 AND day = 15')
    rows_exact = cursor.fetchall()
    for row in rows_exact:
        print(f"\t  {row}")
    
    if len(rows_exact) != 1:
        print(f"\tFailed: expected 1 row, got {len(rows_exact)}")
        return 1
    
    if rows_exact[0][3] != 15.2:
        print(f"\tFailed: expected temperature=15.2")
        print(f"\tGot: {rows_exact[0][3]}")
        return 1
    
    # 前方一致検索（1カラム）: year=2024
    print("\n\t前方一致（1カラム）: year=2024")
    cursor.execute(f'SELECT year, month, day FROM ha_lineairdb_test.{table_name} WHERE year = 2024')
    rows_prefix1 = cursor.fetchall()
    for row in rows_prefix1:
        print(f"\t  {row}")
    
    if len(rows_prefix1) != 9:
        print(f"\tFailed: expected 9 rows, got {len(rows_prefix1)}")
        return 1
    
    # 前方一致検索（2カラム）: year=2024 AND month=1
    print("\n\t前方一致（2カラム）: year=2024 AND month=1")
    cursor.execute(f'SELECT year, month, day FROM ha_lineairdb_test.{table_name} WHERE year = 2024 AND month = 1')
    rows_prefix2 = cursor.fetchall()
    for row in rows_prefix2:
        print(f"\t  {row}")
    
    if len(rows_prefix2) != 3:
        print(f"\tFailed: expected 3 rows, got {len(rows_prefix2)}")
        return 1
    
    days_jan = [row[2] for row in rows_prefix2]
    expected_days = [1, 15, 31]
    for day in expected_days:
        if day not in days_jan:
            print(f"\tFailed: expected day={day}")
            print(f"\tGot: {days_jan}")
            return 1
    
    # 範囲検索: year=2024 AND month=3 AND day >= 15
    print("\n\t範囲検索: year=2024 AND month=3 AND day >= 15")
    cursor.execute(f'SELECT year, month, day FROM ha_lineairdb_test.{table_name} WHERE year = 2024 AND month = 3 AND day >= 15')
    rows_range = cursor.fetchall()
    for row in rows_range:
        print(f"\t  {row}")
    
    if len(rows_range) != 2:
        print(f"\tFailed: expected 2 rows, got {len(rows_range)}")
        return 1
    
    days_range = [row[2] for row in rows_range]
    if 15 not in days_range or 31 not in days_range:
        print(f"\tFailed: expected day=15 and day=31")
        print(f"\tGot: {days_range}")
        return 1
    
    # 範囲検索: year=2024 AND month BETWEEN 3 AND 6
    print("\n\t範囲検索: year=2024 AND month BETWEEN 3 AND 6")
    cursor.execute(f'SELECT year, month, day FROM ha_lineairdb_test.{table_name} WHERE year = 2024 AND month BETWEEN 3 AND 6')
    rows_between = cursor.fetchall()
    for row in rows_between:
        print(f"\t  {row}")
    
    if len(rows_between) != 6:  # 3月3件 + 6月3件
        print(f"\tFailed: expected 6 rows, got {len(rows_between)}")
        return 1
    
    months = set([row[1] for row in rows_between])
    if 3 not in months or 6 not in months:
        print(f"\tFailed: expected months 3 and 6")
        print(f"\tGot: {months}")
        return 1
    
    print("\n\tPassed!")
    return 0

def test_composite_index_with_primary_key(db, cursor):
    """複合インデックス（PRIMARY KEY付きテーブル）のテスト"""
    print("COMPOSITE INDEX WITH PRIMARY KEY TEST")
    
    # ユニークなテーブル名を生成
    table_name = f"test_comp_with_pk_{int(time.time() * 1000000)}"
    
    # PRIMARY KEYと複合インデックスを持つテーブルを作成
    cursor.execute(f'''CREATE TABLE ha_lineairdb_test.{table_name} (
        id INT NOT NULL PRIMARY KEY,
        name VARCHAR(50) NOT NULL,
        age INT NOT NULL,
        department VARCHAR(50) NOT NULL,
        salary INT,
        INDEX age_dept_idx (age, department)
    ) ENGINE = LineairDB''')
    db.commit()
    
    # データを挿入
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, name, age, department, salary) VALUES (1, "alice", 25, "engineering", 5000)')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, name, age, department, salary) VALUES (2, "bob", 25, "sales", 4500)')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, name, age, department, salary) VALUES (3, "carol", 30, "engineering", 6000)')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, name, age, department, salary) VALUES (4, "dave", 25, "engineering", 5200)')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, name, age, department, salary) VALUES (5, "eve", 30, "sales", 5500)')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, name, age, department, salary) VALUES (6, "frank", 25, "marketing", 4800)')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, name, age, department, salary) VALUES (7, "grace", 30, "engineering", 6200)')
    db.commit()
    
    # 全データを確認
    print("\t全データ:")
    cursor.execute(f'SELECT id, name, age, department FROM ha_lineairdb_test.{table_name}')
    all_rows = cursor.fetchall()
    for row in all_rows:
        print(f"\t  {row}")
    
    if len(all_rows) != 7:
        print(f"\tFailed: expected 7 rows, got {len(all_rows)}")
        return 1
    
    # 完全一致検索: age=25 AND department="engineering" (alice, dave期待)
    print("\n\t複合キー完全一致: age=25 AND department='engineering'")
    cursor.execute(f'SELECT id, name, age, department FROM ha_lineairdb_test.{table_name} WHERE age = 25 AND department = "engineering"')
    rows_exact = cursor.fetchall()
    for row in rows_exact:
        print(f"\t  {row}")
    
    if len(rows_exact) != 2:
        print(f"\tFailed: expected 2 rows, got {len(rows_exact)}")
        return 1
    
    names_exact = [row[1] for row in rows_exact]
    if "alice" not in names_exact or "dave" not in names_exact:
        print(f"\tFailed: expected alice and dave")
        print(f"\tGot: {names_exact}")
        return 1
    
    # 前方一致検索: age=25 (alice, bob, dave, frank期待)
    print("\n\t複合キー前方一致: age=25")
    cursor.execute(f'SELECT id, name, age, department FROM ha_lineairdb_test.{table_name} WHERE age = 25')
    rows_prefix = cursor.fetchall()
    for row in rows_prefix:
        print(f"\t  {row}")
    
    if len(rows_prefix) != 4:
        print(f"\tFailed: expected 4 rows with age=25, got {len(rows_prefix)}")
        return 1
    
    names_prefix = [row[1] for row in rows_prefix]
    expected_names = ["alice", "bob", "dave", "frank"]
    for name in expected_names:
        if name not in names_prefix:
            print(f"\tFailed: expected {name} in results")
            print(f"\tGot: {names_prefix}")
            return 1
    
    # 範囲検索: age=30 AND department<="engineering"
    print("\n\t複合キー範囲検索: age=30 AND department<='engineering'")
    cursor.execute(f'SELECT id, name, age, department FROM ha_lineairdb_test.{table_name} WHERE age = 30 AND department <= "engineering"')
    rows_range = cursor.fetchall()
    for row in rows_range:
        print(f"\t  {row}")
    
    if len(rows_range) != 2:
        print(f"\tFailed: expected 2 rows, got {len(rows_range)}")
        return 1
    
    names_range = [row[1] for row in rows_range]
    if "carol" not in names_range or "grace" not in names_range:
        print(f"\tFailed: expected carol and grace")
        print(f"\tGot: {names_range}")
        return 1
    
    # PRIMARY KEYでの検索も確認
    print("\n\tPRIMARY KEYでの検索: id=3")
    cursor.execute(f'SELECT id, name, age, department FROM ha_lineairdb_test.{table_name} WHERE id = 3')
    rows_pk = cursor.fetchall()
    
    # デバッグ情報
    print(f"\t  返ってきた行数: {len(rows_pk)}")
    if len(rows_pk) > 0:
        for i, row in enumerate(rows_pk):
            print(f"\t  行{i}: {row}")
            print(f"\t    - id={row[0]}, name='{row[1]}', age={row[2]}, dept='{row[3]}'")
    
    if len(rows_pk) == 0:
        print(f"\t❌ Failed: No rows returned for PRIMARY KEY id=3")
        # 全データをもう一度確認
        print("\t  全データを再確認:")
        cursor.execute(f'SELECT id, name FROM ha_lineairdb_test.{table_name}')
        all_check = cursor.fetchall()
        for row in all_check:
            print(f"\t    id={row[0]}, name={row[1]}")
        return 1
    elif len(rows_pk) != 1:
        print(f"\t❌ Failed: Expected 1 row, got {len(rows_pk)}")
        return 1
    elif rows_pk[0][1] != "carol":
        print(f"\t❌ Failed: Expected name='carol', got name='{rows_pk[0][1]}'")
        return 1
    
    print(f"\t✅ PRIMARY KEY lookup succeeded")
    
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
    result |= test_composite_index_int_string(db, cursor)
    result |= test_composite_index_string_datetime(db, cursor)
    result |= test_composite_index_int_int(db, cursor)
    result |= test_composite_index_skip_middle_key(db, cursor)
    result |= test_composite_primary_key_basic(db, cursor)
    result |= test_composite_primary_key_range(db, cursor)
    result |= test_composite_primary_key_three_columns(db, cursor)
    result |= test_composite_index_with_primary_key(db, cursor)
    
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

