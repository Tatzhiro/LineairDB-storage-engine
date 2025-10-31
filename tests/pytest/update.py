import sys
import mysql.connector
from utils.reset import reset
import argparse
import time

def update_basic(db, cursor):
    """基本的なUPDATEテスト"""
    reset(db, cursor)
    print("UPDATE BASIC TEST")
    
    # データ挿入前の状態を確認
    cursor.execute('SELECT * FROM ha_lineairdb_test.items')
    print("\t[DEBUG] Before INSERT:", cursor.fetchall())
    
    cursor.execute(\
        'INSERT INTO ha_lineairdb_test.items (\
            title, content\
        ) VALUES ("carol", "ddd")'\
    )
    db.commit()
    
    # INSERT後の状態を確認
    cursor.execute('SELECT * FROM ha_lineairdb_test.items')
    rows_after_insert = cursor.fetchall()
    print("\t[DEBUG] After INSERT:", rows_after_insert)
    print("\t[DEBUG] Number of rows after INSERT:", len(rows_after_insert))
    
    # UPDATE実行
    cursor.execute('UPDATE ha_lineairdb_test.items SET content="XXX"')
    db.commit()
    
    # UPDATE後の状態を確認
    cursor.execute('SELECT * FROM ha_lineairdb_test.items')
    rows = cursor.fetchall()
    print("\t[DEBUG] After UPDATE:", rows)
    print("\t[DEBUG] Number of rows after UPDATE:", len(rows))
    
    if not rows:
        print("\tFailed: No rows returned")
        print("\t", rows)
        return 1
    
    # 期待：1行だけ存在し、contentが"XXX"であること
    if len(rows) == 1 and rows[0][1] == "XXX" and rows[0][0] == "carol":
        print("\tPassed!")
        print("\t", rows)
        return 0
    
    if len(rows) > 1:
        print("\tFailed: Multiple rows found (should be only 1)")
        print("\t", rows)
        return 1
    
    print("\tFailed")
    print("\t", rows)
    return 1


def update_secondary_index_basic(db, cursor):
    """セカンダリインデックス列の基本的なUPDATEテスト"""
    print("\nUPDATE SECONDARY INDEX BASIC TEST")
    
    table_name = f"test_update_idx_{int(time.time() * 1000000)}"
    
    # セカンダリインデックス付きテーブルを作成
    cursor.execute(f'''CREATE TABLE ha_lineairdb_test.{table_name} (
        name VARCHAR(50) NOT NULL,
        age INT NOT NULL,
        department VARCHAR(50) NOT NULL,
        INDEX age_idx (age)
    ) ENGINE = LineairDB''')
    db.commit()
    
    # テストデータを挿入
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (name, age, department) VALUES ("Alice", 25, "Sales")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (name, age, department) VALUES ("Bob", 30, "Engineering")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (name, age, department) VALUES ("Carol", 25, "Marketing")')
    db.commit()
    
    # INSERT後の全データを確認
    cursor.execute(f'SELECT name, age, department FROM ha_lineairdb_test.{table_name} ORDER BY name')
    print("\t[DEBUG] Before UPDATE - All rows:", cursor.fetchall())
    
    # age=25のレコードを確認
    cursor.execute(f'SELECT name, age FROM ha_lineairdb_test.{table_name} WHERE age=25 ORDER BY name')
    print("\t[DEBUG] Before UPDATE - Age=25:", cursor.fetchall())
    
    # セカンダリインデックス列（age）を更新
    print(f"\t[DEBUG] Executing: UPDATE {table_name} SET age=26 WHERE name='Alice'")
    cursor.execute(f'UPDATE ha_lineairdb_test.{table_name} SET age=26 WHERE name="Alice"')
    print("\t[DEBUG] UPDATE committed")

    # age=26のレコードを確認
    cursor.execute(f'SELECT name, age FROM ha_lineairdb_test.{table_name} WHERE age=26')
    rows_26 = cursor.fetchall()
    print("\t[DEBUG] After UPDATE - Age=26:", rows_26)
    
    # UPDATE後の全データを確認
    cursor.execute(f'SELECT name, age, department FROM ha_lineairdb_test.{table_name} ORDER BY name')
    all_rows = cursor.fetchall()
    print("\t[DEBUG] After UPDATE - All rows:", all_rows)
    
    # age=25のレコードを確認
    cursor.execute(f'SELECT name, age FROM ha_lineairdb_test.{table_name} WHERE age=25 ORDER BY name')
    rows_25 = cursor.fetchall()
    print("\t[DEBUG] After UPDATE - Age=25:", rows_25)
    
    # 更新確認
    if len(rows_26) != 1 or rows_26[0][0] != "Alice":
        print("\tFailed: Expected Alice with age 26")
        print("\t", rows_26)
        return 1
    
    # 古い値（25）でAliceが検索されないことを確認
    names = [row[0] for row in rows_25]
    if "Alice" in names:
        print("\tFailed: Alice should not be found with age 25")
        print("\t", rows_25)
        return 1
    
    print("\tPassed!")
    return 0


def update_secondary_index_multiple_rows(db, cursor):
    """セカンダリインデックス列の複数行UPDATEテスト"""
    print("\nUPDATE SECONDARY INDEX MULTIPLE ROWS TEST")
    
    table_name = f"test_update_multi_{int(time.time() * 1000000)}"
    
    # セカンダリインデックス付きテーブルを作成
    cursor.execute(f'''CREATE TABLE ha_lineairdb_test.{table_name} (
        name VARCHAR(50) NOT NULL,
        age INT NOT NULL,
        department VARCHAR(50) NOT NULL,
        INDEX age_idx (age),
        INDEX dept_idx (department)
    ) ENGINE = LineairDB''')
    db.commit()
    
    # テストデータを挿入（同じ部署の複数人）
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (name, age, department) VALUES ("Alice", 25, "Sales")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (name, age, department) VALUES ("Bob", 30, "Sales")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (name, age, department) VALUES ("Carol", 28, "Sales")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (name, age, department) VALUES ("Dave", 35, "Engineering")')
    db.commit()
    
    # INSERT後の全データを確認
    cursor.execute(f'SELECT name, age, department FROM ha_lineairdb_test.{table_name} ORDER BY name')
    print("\t[DEBUG] Before UPDATE:", cursor.fetchall())
    
    # Sales部署の全員の年齢を+1
    print(f"\t[DEBUG] Executing: UPDATE {table_name} SET age=age+1 WHERE department='Sales'")
    cursor.execute(f'UPDATE ha_lineairdb_test.{table_name} SET age=age+1 WHERE department="Sales"')
    db.commit()
    
    # UPDATE後の全データを確認
    cursor.execute(f'SELECT name, age, department FROM ha_lineairdb_test.{table_name} ORDER BY name')
    print("\t[DEBUG] After UPDATE:", cursor.fetchall())
    
    # 更新確認
    cursor.execute(f'SELECT name, age FROM ha_lineairdb_test.{table_name} WHERE department="Sales" ORDER BY name')
    rows = cursor.fetchall()
    expected = [("Alice", 26), ("Bob", 31), ("Carol", 29)]
    if len(rows) != 3:
        print(f"\tFailed: Expected 3 rows, got {len(rows)}")
        print("\t", rows)
        return 1
    
    for i, (name, age) in enumerate(expected):
        if rows[i][0] != name or rows[i][1] != age:
            print(f"\tFailed: Expected {expected}")
            print("\tGot:", rows)
            return 1
    
    # Engineering部署は変更されていないことを確認
    cursor.execute(f'SELECT name, age FROM ha_lineairdb_test.{table_name} WHERE department="Engineering"')
    rows = cursor.fetchall()
    if len(rows) != 1 or rows[0][0] != "Dave" or rows[0][1] != 35:
        print("\tFailed: Dave's age should remain 35")
        print("\t", rows)
        return 1
    
    print("\tPassed!")
    return 0


def update_secondary_index_to_existing_value(db, cursor):
    """セカンダリインデックス列を既存の値に更新するテスト"""
    print("\nUPDATE SECONDARY INDEX TO EXISTING VALUE TEST")
    
    table_name = f"test_update_exist_{int(time.time() * 1000000)}"
    
    # セカンダリインデックス付きテーブルを作成
    cursor.execute(f'''CREATE TABLE ha_lineairdb_test.{table_name} (
        name VARCHAR(50) NOT NULL,
        age INT NOT NULL,
        department VARCHAR(50) NOT NULL,
        INDEX age_idx (age)
    ) ENGINE = LineairDB''')
    db.commit()
    
    # テストデータを挿入
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (name, age, department) VALUES ("Alice", 25, "Sales")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (name, age, department) VALUES ("Bob", 30, "Engineering")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (name, age, department) VALUES ("Carol", 28, "Marketing")')
    db.commit()
    
    # Aliceの年齢を30に更新（Bobと同じ値）
    cursor.execute(f'UPDATE ha_lineairdb_test.{table_name} SET age=30 WHERE name="Alice"')
    db.commit()
    
    # age=30で2人（AliceとBob）が検索されることを確認
    cursor.execute(f'SELECT name FROM ha_lineairdb_test.{table_name} WHERE age=30 ORDER BY name')
    rows = cursor.fetchall()
    if len(rows) != 2:
        print(f"\tFailed: Expected 2 rows with age=30, got {len(rows)}")
        print("\t", rows)
        return 1
    
    names = [row[0] for row in rows]
    if names != ["Alice", "Bob"]:
        print(f"\tFailed: Expected ['Alice', 'Bob']")
        print("\tGot:", names)
        return 1
    
    # age=25でAliceが見つからないことを確認
    cursor.execute(f'SELECT name FROM ha_lineairdb_test.{table_name} WHERE age=25')
    rows = cursor.fetchall()
    if len(rows) != 0:
        print("\tFailed: No one should have age=25")
        print("\t", rows)
        return 1
    
    print("\tPassed!")
    return 0


def update_multiple_secondary_indexes(db, cursor):
    """複数のセカンダリインデックス列を同時に更新するテスト"""
    print("\nUPDATE MULTIPLE SECONDARY INDEXES TEST")
    
    table_name = f"test_update_multi_idx_{int(time.time() * 1000000)}"
    
    # 複数のセカンダリインデックス付きテーブルを作成
    cursor.execute(f'''CREATE TABLE ha_lineairdb_test.{table_name} (
        name VARCHAR(50) NOT NULL,
        age INT NOT NULL,
        department VARCHAR(50) NOT NULL,
        INDEX age_idx (age),
        INDEX dept_idx (department)
    ) ENGINE = LineairDB''')
    db.commit()
    
    # テストデータを挿入
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (name, age, department) VALUES ("Alice", 25, "Sales")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (name, age, department) VALUES ("Bob", 30, "Engineering")')
    db.commit()
    
    # 両方のインデックス列を同時に更新
    cursor.execute(f'UPDATE ha_lineairdb_test.{table_name} SET age=26, department="Marketing" WHERE name="Alice"')
    db.commit()
    
    # age=26で検索
    cursor.execute(f'SELECT name, department FROM ha_lineairdb_test.{table_name} WHERE age=26')
    rows = cursor.fetchall()
    if len(rows) != 1 or rows[0][0] != "Alice" or rows[0][1] != "Marketing":
        print("\tFailed: Alice should have age=26 and department=Marketing")
        print("\t", rows)
        return 1
    
    # department=Marketingで検索
    cursor.execute(f'SELECT name, age FROM ha_lineairdb_test.{table_name} WHERE department="Marketing"')
    rows = cursor.fetchall()
    if len(rows) != 1 or rows[0][0] != "Alice" or rows[0][1] != 26:
        print("\tFailed: Alice should be found in Marketing with age 26")
        print("\t", rows)
        return 1
    
    # 古い値で検索されないことを確認
    cursor.execute(f'SELECT name FROM ha_lineairdb_test.{table_name} WHERE age=25')
    rows = cursor.fetchall()
    if len(rows) != 0:
        print("\tFailed: No one should have age=25")
        print("\t", rows)
        return 1
    
    cursor.execute(f'SELECT name FROM ha_lineairdb_test.{table_name} WHERE department="Sales"')
    rows = cursor.fetchall()
    if len(rows) != 0:
        print("\tFailed: No one should be in Sales department")
        print("\t", rows)
        return 1
    
    print("\tPassed!")
    return 0


def update_secondary_index_with_transaction(db, cursor):
    """トランザクション内でのセカンダリインデックス更新テスト"""
    print("\nUPDATE SECONDARY INDEX WITH TRANSACTION TEST")
    
    table_name = f"test_update_tx_{int(time.time() * 1000000)}"
    
    # セカンダリインデックス付きテーブルを作成
    cursor.execute(f'''CREATE TABLE ha_lineairdb_test.{table_name} (
        name VARCHAR(50) NOT NULL,
        age INT NOT NULL,
        department VARCHAR(50) NOT NULL,
        INDEX age_idx (age)
    ) ENGINE = LineairDB''')
    db.commit()
    
    # テストデータを挿入
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (name, age, department) VALUES ("Alice", 25, "Sales")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (name, age, department) VALUES ("Bob", 30, "Engineering")')
    db.commit()
    
    # トランザクション内で更新
    cursor.execute(f'UPDATE ha_lineairdb_test.{table_name} SET age=26 WHERE name="Alice"')
    cursor.execute(f'UPDATE ha_lineairdb_test.{table_name} SET age=31 WHERE name="Bob"')
    db.commit()
    
    # 両方の更新が反映されていることを確認
    cursor.execute(f'SELECT name, age FROM ha_lineairdb_test.{table_name} ORDER BY name')
    rows = cursor.fetchall()
    if len(rows) != 2:
        print(f"\tFailed: Expected 2 rows, got {len(rows)}")
        print("\t", rows)
        return 1
    
    if rows[0][0] != "Alice" or rows[0][1] != 26:
        print("\tFailed: Alice should have age 26")
        print("\t", rows)
        return 1
    
    if rows[1][0] != "Bob" or rows[1][1] != 31:
        print("\tFailed: Bob should have age 31")
        print("\t", rows)
        return 1
    
    print("\tPassed!")
    return 0


def update_primary_key_basic(db, cursor):
    """PRIMARY KEYを持つテーブルの基本的なUPDATEテスト"""
    print("\nUPDATE PRIMARY KEY BASIC TEST")
    
    table_name = f"test_update_pk_{int(time.time() * 1000000)}"
    
    # PRIMARY KEY付きテーブルを作成
    cursor.execute(f'''CREATE TABLE ha_lineairdb_test.{table_name} (
        id INT NOT NULL,
        name VARCHAR(50) NOT NULL,
        age INT NOT NULL,
        PRIMARY KEY (id)
    ) ENGINE = LineairDB''')
    db.commit()
    
    # テストデータを挿入
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, name, age) VALUES (1, "Alice", 25)')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, name, age) VALUES (2, "Bob", 30)')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, name, age) VALUES (3, "Carol", 28)')
    db.commit()
    
    print("\t[DEBUG] Before UPDATE:")
    cursor.execute(f'SELECT id, name, age FROM ha_lineairdb_test.{table_name} ORDER BY id')
    print("\t", cursor.fetchall())
    
    # PRIMARY KEYを使って特定の行を更新
    cursor.execute(f'UPDATE ha_lineairdb_test.{table_name} SET name="Alice Smith", age=26 WHERE id=1')
    db.commit()
    
    print("\t[DEBUG] After UPDATE:")
    cursor.execute(f'SELECT id, name, age FROM ha_lineairdb_test.{table_name} ORDER BY id')
    rows = cursor.fetchall()
    print("\t", rows)
    
    # 更新確認
    if len(rows) != 3:
        print(f"\tFailed: Expected 3 rows, got {len(rows)}")
        return 1
    
    if rows[0] != (1, "Alice Smith", 26):
        print("\tFailed: ID 1 should be ('Alice Smith', 26)")
        print("\t", rows[0])
        return 1
    
    # 他の行が変更されていないことを確認
    if rows[1] != (2, "Bob", 30) or rows[2] != (3, "Carol", 28):
        print("\tFailed: Other rows should not be changed")
        print("\t", rows)
        return 1
    
    print("\tPassed!")
    return 0


def update_primary_key_multiple_rows(db, cursor):
    """PRIMARY KEYを持つテーブルの複数行UPDATEテスト"""
    print("\nUPDATE PRIMARY KEY MULTIPLE ROWS TEST")
    
    table_name = f"test_update_pk_multi_{int(time.time() * 1000000)}"
    
    # PRIMARY KEY付きテーブルを作成
    cursor.execute(f'''CREATE TABLE ha_lineairdb_test.{table_name} (
        id INT NOT NULL,
        name VARCHAR(50) NOT NULL,
        age INT NOT NULL,
        department VARCHAR(50) NOT NULL,
        PRIMARY KEY (id)
    ) ENGINE = LineairDB''')
    db.commit()
    
    # テストデータを挿入
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, name, age, department) VALUES (1, "Alice", 25, "Sales")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, name, age, department) VALUES (2, "Bob", 30, "Sales")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, name, age, department) VALUES (3, "Carol", 28, "Engineering")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, name, age, department) VALUES (4, "Dave", 35, "Engineering")')
    db.commit()
    
    print("\t[DEBUG] Before UPDATE:")
    cursor.execute(f'SELECT id, name, age, department FROM ha_lineairdb_test.{table_name} ORDER BY id')
    print("\t", cursor.fetchall())
    
    # 条件に合う複数行を更新
    cursor.execute(f'UPDATE ha_lineairdb_test.{table_name} SET age=age+1 WHERE department="Sales"')
    db.commit()
    
    print("\t[DEBUG] After UPDATE:")
    cursor.execute(f'SELECT id, name, age, department FROM ha_lineairdb_test.{table_name} ORDER BY id')
    rows = cursor.fetchall()
    print("\t", rows)
    
    # Sales部署の年齢が+1されていることを確認
    if rows[0][2] != 26 or rows[1][2] != 31:
        print("\tFailed: Sales department ages should be incremented")
        print("\t", rows)
        return 1
    
    # Engineering部署は変更されていないことを確認
    if rows[2][2] != 28 or rows[3][2] != 35:
        print("\tFailed: Engineering department ages should not be changed")
        print("\t", rows)
        return 1
    
    print("\tPassed!")
    return 0


def update_primary_key_with_secondary_index(db, cursor):
    """PRIMARY KEYとセカンダリインデックスを持つテーブルのUPDATEテスト"""
    print("\nUPDATE PRIMARY KEY WITH SECONDARY INDEX TEST")
    
    table_name = f"test_update_pk_idx_{int(time.time() * 1000000)}"
    
    # PRIMARY KEYとセカンダリインデックス付きテーブルを作成
    cursor.execute(f'''CREATE TABLE ha_lineairdb_test.{table_name} (
        id INT NOT NULL,
        name VARCHAR(50) NOT NULL,
        age INT NOT NULL,
        department VARCHAR(50) NOT NULL,
        PRIMARY KEY (id),
        INDEX age_idx (age),
        INDEX dept_idx (department)
    ) ENGINE = LineairDB''')
    db.commit()
    
    # テストデータを挿入
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, name, age, department) VALUES (1, "Alice", 25, "Sales")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, name, age, department) VALUES (2, "Bob", 30, "Engineering")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, name, age, department) VALUES (3, "Carol", 25, "Marketing")')
    db.commit()
    
    print("\t[DEBUG] Before UPDATE:")
    cursor.execute(f'SELECT id, name, age, department FROM ha_lineairdb_test.{table_name} ORDER BY id')
    print("\t", cursor.fetchall())
    
    # PRIMARY KEYで指定してセカンダリインデックス列を更新
    cursor.execute(f'UPDATE ha_lineairdb_test.{table_name} SET age=26, department="HR" WHERE id=1')
    db.commit()
    
    print("\t[DEBUG] After UPDATE:")
    cursor.execute(f'SELECT id, name, age, department FROM ha_lineairdb_test.{table_name} ORDER BY id')
    print("\t", cursor.fetchall())
    
    # PRIMARY KEYで検索して更新を確認
    cursor.execute(f'SELECT name, age, department FROM ha_lineairdb_test.{table_name} WHERE id=1')
    rows = cursor.fetchall()
    if len(rows) != 1 or rows[0] != ("Alice", 26, "HR"):
        print("\tFailed: ID 1 should have age=26 and department=HR")
        print("\t", rows)
        return 1
    
    # セカンダリインデックスで検索
    cursor.execute(f'SELECT id, name FROM ha_lineairdb_test.{table_name} WHERE age=26')
    rows = cursor.fetchall()
    if len(rows) != 1 or rows[0] != (1, "Alice"):
        print("\tFailed: age=26 should find Alice")
        print("\t", rows)
        return 1
    
    cursor.execute(f'SELECT id, name FROM ha_lineairdb_test.{table_name} WHERE department="HR"')
    rows = cursor.fetchall()
    if len(rows) != 1 or rows[0] != (1, "Alice"):
        print("\tFailed: department=HR should find Alice")
        print("\t", rows)
        return 1
    
    # 古い値で検索されないことを確認
    cursor.execute(f'SELECT id FROM ha_lineairdb_test.{table_name} WHERE age=25 ORDER BY id')
    rows = cursor.fetchall()
    if len(rows) != 1 or rows[0][0] != 3:  # Carolのみ
        print("\tFailed: age=25 should only find Carol")
        print("\t", rows)
        return 1
    
    cursor.execute(f'SELECT id FROM ha_lineairdb_test.{table_name} WHERE department="Sales"')
    rows = cursor.fetchall()
    if len(rows) != 0:
        print("\tFailed: No one should be in Sales department")
        print("\t", rows)
        return 1
    
    print("\tPassed!")
    return 0


def update_composite_primary_key(db, cursor):
    """複合PRIMARY KEYを持つテーブルのUPDATEテスト"""
    print("\nUPDATE COMPOSITE PRIMARY KEY TEST")
    
    table_name = f"test_update_comp_pk_{int(time.time() * 1000000)}"
    
    # 複合PRIMARY KEY付きテーブルを作成
    cursor.execute(f'''CREATE TABLE ha_lineairdb_test.{table_name} (
        dept_id INT NOT NULL,
        emp_id INT NOT NULL,
        name VARCHAR(50) NOT NULL,
        salary INT NOT NULL,
        PRIMARY KEY (dept_id, emp_id)
    ) ENGINE = LineairDB''')
    db.commit()
    
    # テストデータを挿入
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (dept_id, emp_id, name, salary) VALUES (1, 1, "Alice", 50000)')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (dept_id, emp_id, name, salary) VALUES (1, 2, "Bob", 55000)')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (dept_id, emp_id, name, salary) VALUES (2, 1, "Carol", 60000)')
    db.commit()
    
    print("\t[DEBUG] Before UPDATE:")
    cursor.execute(f'SELECT dept_id, emp_id, name, salary FROM ha_lineairdb_test.{table_name} ORDER BY dept_id, emp_id')
    print("\t", cursor.fetchall())
    
    # 複合PRIMARY KEYで特定の行を更新
    cursor.execute(f'UPDATE ha_lineairdb_test.{table_name} SET name="Alice Smith", salary=52000 WHERE dept_id=1 AND emp_id=1')
    db.commit()
    
    print("\t[DEBUG] After UPDATE:")
    cursor.execute(f'SELECT dept_id, emp_id, name, salary FROM ha_lineairdb_test.{table_name} ORDER BY dept_id, emp_id')
    rows = cursor.fetchall()
    print("\t", rows)
    
    # 更新確認
    if rows[0] != (1, 1, "Alice Smith", 52000):
        print("\tFailed: (1, 1) should be updated")
        print("\t", rows[0])
        return 1
    
    # 他の行が変更されていないことを確認
    if rows[1] != (1, 2, "Bob", 55000) or rows[2] != (2, 1, "Carol", 60000):
        print("\tFailed: Other rows should not be changed")
        print("\t", rows)
        return 1
    
    print("\tPassed!")
    return 0

 
def main():
    # test
    db=mysql.connector.connect(host="localhost", user=args.user, password=args.password)
    cursor=db.cursor()
    
    failed = 0
    
    # 基本的なUPDATEテスト
    #if update_basic(db, cursor) != 0:
        #failed += 1
    
    # セカンダリインデックス関連のUPDATEテスト
    if update_secondary_index_basic(db, cursor) != 0:
        failed += 1
    
    if update_secondary_index_multiple_rows(db, cursor) != 0:
        failed += 1
    
    if update_secondary_index_to_existing_value(db, cursor) != 0:
        failed += 1
    
    if update_multiple_secondary_indexes(db, cursor) != 0:
        failed += 1
    
    if update_secondary_index_with_transaction(db, cursor) != 0:
        failed += 1
    
    # PRIMARY KEY関連のUPDATEテスト
    if update_primary_key_basic(db, cursor) != 0:
        failed += 1
    
    if update_primary_key_multiple_rows(db, cursor) != 0:
        failed += 1
    
    if update_primary_key_with_secondary_index(db, cursor) != 0:
        failed += 1
    
    if update_composite_primary_key(db, cursor) != 0:
        failed += 1
    
    if failed > 0:
        print(f"\n{failed} test(s) failed")
        sys.exit(1)
    else:
        print("\nAll tests passed!")
        sys.exit(0)


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