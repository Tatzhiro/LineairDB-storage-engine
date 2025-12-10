import sys
import mysql.connector
import argparse
import time

def test_write_operation(db, cursor):
    """Test a basic INSERT operation."""
    print("WRITE OPERATION TEST")
    
    table_name = f"test_write_{int(time.time() * 1000000)}"
    
    cursor.execute(f'''CREATE TABLE ha_lineairdb_test.{table_name} (
        title VARCHAR(50) NOT NULL,
        content TEXT,
        INDEX title_idx (title)
    ) ENGINE = LineairDB''')
    db.commit()
    
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (title, content) VALUES ("alice", "test data 1")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (title, content) VALUES ("bob", "test data 2")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (title, content) VALUES ("carol", "test data 3")')
    db.commit()
    
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
    """Test a basic SELECT operation."""
    print("READ OPERATION TEST")
    
    table_name = f"test_read_{int(time.time() * 1000000)}"
    
    cursor.execute(f'''CREATE TABLE ha_lineairdb_test.{table_name} (
        title VARCHAR(50) NOT NULL,
        content TEXT,
        INDEX title_idx (title)
    ) ENGINE = LineairDB''')
    db.commit()
    
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (title, content) VALUES ("alice", "data1")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (title, content) VALUES ("bob", "data2")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (title, content) VALUES ("carol", "data3")')
    db.commit()
    
    cursor.execute(f'SELECT title, content FROM ha_lineairdb_test.{table_name} WHERE title = "bob"')
    rows = cursor.fetchall()
    if len(rows) != 1 or rows[0][0] != "bob" or rows[0][1] != "data2":
        print("\tFailed: expected ('bob', 'data2')")
        print("\tGot:", rows)
        return 1
    
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
    """Verify multiple rows share the same secondary-index key."""
    print("SECONDARY INDEX MULTIPLE VALUES TEST")
    
    # Generate unique table name
    table_name = f"test_multi_{int(time.time() * 1000000)}"
    
    # Create table with secondary index on age
    cursor.execute(f'''CREATE TABLE ha_lineairdb_test.{table_name} (
        name VARCHAR(50) NOT NULL,
        age INT NOT NULL,
        department VARCHAR(50),
        INDEX age_idx (age)
    ) ENGINE = LineairDB''')
    db.commit()
    
    # Insert rows with duplicate age values
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (name, age, department) VALUES ("alice", 25, "engineering")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (name, age, department) VALUES ("bob", 30, "sales")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (name, age, department) VALUES ("carol", 25, "marketing")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (name, age, department) VALUES ("dave", 25, "engineering")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (name, age, department) VALUES ("john", 25, "student")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (name, age, department) VALUES ("eve", 30, "hr")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (name, age, department) VALUES ("gariman", 30, "programmer")')
    db.commit()
    
    # Expect 4 rows with age=25
    cursor.execute(f'SELECT name, age, department FROM ha_lineairdb_test.{table_name} WHERE age = 25')
    rows_25 = cursor.fetchall()
    
    if len(rows_25) != 4:
        print(f"\tFailed: expected 4 rows with age=25, got {len(rows_25)}")
        return 1
    
    names_25 = [row[0] for row in rows_25]
    if "alice" not in names_25 or "carol" not in names_25 or "dave" not in names_25:
        print(f"\tFailed: expected alice, carol, and dave, got {names_25}")
        return 1
    
    # Expect 3 rows with age=30
    cursor.execute(f'SELECT name, age, department FROM ha_lineairdb_test.{table_name} WHERE age = 30')
    rows_30 = cursor.fetchall()
    
    if len(rows_30) != 3:
        print(f"\tFailed: expected 3 rows with age=30, got {len(rows_30)}")
        return 1
    
    names_30 = [row[0] for row in rows_30]
    if "bob" not in names_30 or "eve" not in names_30:
        print(f"\tFailed: expected bob and eve, got {names_30}")
        return 1
    
    # Expect no rows with age=99
    cursor.execute(f'SELECT name, age, department FROM ha_lineairdb_test.{table_name} WHERE age = 99')
    rows_99 = cursor.fetchall()
    if len(rows_99) != 0:
        print(f"\tFailed: expected 0 rows with age=99, got {len(rows_99)}")
        return 1
    
    print("\tPassed!")
    return 0

def test_secondary_index_range_query(db, cursor):
    """Test range queries using a secondary index."""
    print("SECONDARY INDEX RANGE QUERY TEST")
    
    table_name = f"test_range_{int(time.time() * 1000000)}"
    
    cursor.execute(f'''CREATE TABLE ha_lineairdb_test.{table_name} (
        name VARCHAR(50) NOT NULL,
        age INT NOT NULL,
        department VARCHAR(50),
        INDEX age_idx (age)
    ) ENGINE = LineairDB''')
    db.commit()
    
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
    
    # Verify row count
    cursor.execute(f'SELECT name, age, department FROM ha_lineairdb_test.{table_name}')
    all_rows = cursor.fetchall()
    
    if len(all_rows) != 10:
        print(f"\tFailed: expected 10 rows, got {len(all_rows)}")
        return 1
    
    # Lookup age = 25 (expect carol, john)
    cursor.execute(f'SELECT name, age, department FROM ha_lineairdb_test.{table_name} WHERE age = 25')
    rows_25 = cursor.fetchall()
    
    if len(rows_25) != 2:
        print(f"\tFailed: expected 2 rows with age=25, got {len(rows_25)}")
        return 1
    
    names_25 = [row[0] for row in rows_25]
    if "carol" not in names_25 or "john" not in names_25:
        print(f"\tFailed: expected carol and john, got {names_25}")
        return 1
    
    # Lookup age = 30 (expect bob)
    cursor.execute(f'SELECT name, age, department FROM ha_lineairdb_test.{table_name} WHERE age = 30')
    rows_30 = cursor.fetchall()
    
    if len(rows_30) != 1:
        print(f"\tFailed: expected 1 row with age=30, got {len(rows_30)}")
        return 1
    
    if rows_30[0][0] != "bob":
        print(f"\tFailed: expected bob, got {rows_30[0][0]}")
        return 1
    
    # Range query: age < 30 (expect 6 rows)
    cursor.execute(f'SELECT name, age, department FROM ha_lineairdb_test.{table_name} WHERE age < 30')
    rows_lt_30 = cursor.fetchall()
    
    if len(rows_lt_30) != 6:
        print(f"\tFailed: expected 6 rows with age<30, got {len(rows_lt_30)}")
        return 1
    
    names_lt_30 = [row[0] for row in rows_lt_30]
    expected_names = ["alice", "lisa", "carol", "john", "dave", "haru"]
    for name in expected_names:
        if name not in names_lt_30:
            print(f"\tFailed: expected {name} in age<30 results, got {names_lt_30}")
            return 1
    
    # Range query: age <= 30 (expect 7 rows)
    cursor.execute(f'SELECT name, age, department FROM ha_lineairdb_test.{table_name} WHERE age <= 30')
    rows_lte_30 = cursor.fetchall()
    
    if len(rows_lte_30) != 7:
        print(f"\tFailed: expected 7 rows with age<=30, got {len(rows_lte_30)}")
        return 1
    
    # Range query: age > 30 (expect 3 rows)
    cursor.execute(f'SELECT name, age, department FROM ha_lineairdb_test.{table_name} WHERE age > 30')
    rows_gt_30 = cursor.fetchall()
    
    if len(rows_gt_30) != 3:
        print(f"\tFailed: expected 3 rows with age>30, got {len(rows_gt_30)}")
        return 1
    
    names_gt_30 = [row[0] for row in rows_gt_30]
    expected_names_gt = ["eve", "gariman", "ken"]
    for name in expected_names_gt:
        if name not in names_gt_30:
            print(f"\tFailed: expected {name} in age>30 results, got {names_gt_30}")
            return 1
    
    # Range query: age >= 30 (expect 4 rows)
    cursor.execute(f'SELECT name, age, department FROM ha_lineairdb_test.{table_name} WHERE age >= 30')
    rows_gte_30 = cursor.fetchall()
    
    if len(rows_gte_30) != 4:
        print(f"\tFailed: expected 4 rows with age>=30, got {len(rows_gte_30)}")
        return 1
    
    # Range query: age BETWEEN 25 AND 30 (expect 5 rows)
    cursor.execute(f'SELECT name, age, department FROM ha_lineairdb_test.{table_name} WHERE age BETWEEN 25 AND 30')
    rows_between = cursor.fetchall()
    
    if len(rows_between) != 5:
        print(f"\tFailed: expected 5 rows with age BETWEEN 25 AND 30, got {len(rows_between)}")
        return 1
    
    names_between = [row[0] for row in rows_between]
    expected_names_between = ["carol", "john", "dave", "haru", "bob"]
    for name in expected_names_between:
        if name not in names_between:
            print(f"\tFailed: expected {name} in BETWEEN results, got {names_between}")
            return 1
    
    print("\tPassed!")
    return 0

def test_string_range_query(db, cursor):
    """Test range queries on a string-based secondary index."""
    print("STRING RANGE QUERY TEST")
    
    table_name = f"test_str_{int(time.time() * 1000000)}"
    
    cursor.execute(f'''CREATE TABLE ha_lineairdb_test.{table_name} (
        id INT NOT NULL,
        code VARCHAR(5) NOT NULL,
        name VARCHAR(20),
        INDEX code_idx (code)
    ) ENGINE = LineairDB''')
    db.commit()
    
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, code, name) VALUES (1, "A1", "alpha")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, code, name) VALUES (2, "B2", "beta")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, code, name) VALUES (3, "C3", "gamma")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, code, name) VALUES (4, "D4", "delta")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, code, name) VALUES (5, "E5", "epsilon")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, code, name) VALUES (6, "AA", "test1")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, code, name) VALUES (7, "BB", "test2")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, code, name) VALUES (8, "CC", "test3")')
    db.commit()
    
    cursor.execute(f'SELECT id, code, name FROM ha_lineairdb_test.{table_name}')
    all_rows = cursor.fetchall()
    
    if len(all_rows) != 8:
        print(f"\tFailed: expected 8 rows, got {len(all_rows)}")
        return 1
    
    # Range query: code < 'C3'
    cursor.execute(f'SELECT id, code, name FROM ha_lineairdb_test.{table_name} WHERE code < "C3"')
    rows_lt = cursor.fetchall()
    
    if len(rows_lt) != 4:
        print(f"\tFailed: expected 4 rows with code < 'C3', got {len(rows_lt)}")
        return 1
    
    codes_lt = [row[1] for row in rows_lt]
    expected_codes = ["A1", "AA", "B2", "BB"]
    for code in expected_codes:
        if code not in codes_lt:
            print(f"\tFailed: expected {code} in code < 'C3' results, got {codes_lt}")
            return 1
    
    # Range query: code >= 'C3'
    cursor.execute(f'SELECT id, code, name FROM ha_lineairdb_test.{table_name} WHERE code >= "C3"')
    rows_gte = cursor.fetchall()
    
    if len(rows_gte) != 4:
        print(f"\tFailed: expected 4 rows with code >= 'C3', got {len(rows_gte)}")
        return 1
    
    codes_gte = [row[1] for row in rows_gte]
    expected_codes_gte = ["C3", "CC", "D4", "E5"]
    for code in expected_codes_gte:
        if code not in codes_gte:
            print(f"\tFailed: expected {code} in code >= 'C3' results, got {codes_gte}")
            return 1
    
    # Range query: code BETWEEN 'B2' AND 'D4'
    cursor.execute(f'SELECT id, code, name FROM ha_lineairdb_test.{table_name} WHERE code BETWEEN "B2" AND "D4"')
    rows_between = cursor.fetchall()
    
    if len(rows_between) != 5:
        print(f"\tFailed: expected 5 rows with code BETWEEN 'B2' AND 'D4', got {len(rows_between)}")
        return 1
    
    codes_between = [row[1] for row in rows_between]
    expected_codes_between = ["B2", "BB", "C3", "CC", "D4"]
    for code in expected_codes_between:
        if code not in codes_between:
            print(f"\tFailed: expected {code} in BETWEEN results, got {codes_between}")
            return 1
    
    print("\tPassed!")
    return 0

def test_datetime_range_query(db, cursor):
    """Test range queries on a datetime-based secondary index."""
    print("DATETIME RANGE QUERY TEST")
    
    table_name = f"test_dt_{int(time.time() * 1000000)}"
    
    cursor.execute(f'''CREATE TABLE ha_lineairdb_test.{table_name} (
        id INT NOT NULL,
        user VARCHAR(10),
        reg_date DATETIME NOT NULL,
        INDEX date_idx (reg_date)
    ) ENGINE = LineairDB''')
    db.commit()
    
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, user, reg_date) VALUES (1, "alice", "2024-01-15 10:00:00")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, user, reg_date) VALUES (2, "bob", "2024-03-20 14:30:00")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, user, reg_date) VALUES (3, "carol", "2024-06-10 09:15:00")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, user, reg_date) VALUES (4, "dave", "2024-06-25 16:45:00")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, user, reg_date) VALUES (5, "eve", "2024-09-05 11:20:00")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, user, reg_date) VALUES (6, "frank", "2024-12-01 08:00:00")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, user, reg_date) VALUES (7, "grace", "2024-12-15 13:30:00")')
    db.commit()
    
    cursor.execute(f'SELECT id, user, reg_date FROM ha_lineairdb_test.{table_name}')
    all_rows = cursor.fetchall()
    
    if len(all_rows) != 7:
        print(f"\tFailed: expected 7 rows, got {len(all_rows)}")
        return 1
    
    # Range query: reg_date < '2024-06-01'
    cursor.execute(f'SELECT id, user, reg_date FROM ha_lineairdb_test.{table_name} WHERE reg_date < "2024-06-01"')
    rows_lt = cursor.fetchall()
    
    if len(rows_lt) != 2:
        print(f"\tFailed: expected 2 rows with reg_date < '2024-06-01', got {len(rows_lt)}")
        return 1
    
    users_lt = [row[1] for row in rows_lt]
    if "alice" not in users_lt or "bob" not in users_lt:
        print(f"\tFailed: expected alice and bob, got {users_lt}")
        return 1
    
    # Range query: reg_date >= '2024-09-01'
    cursor.execute(f'SELECT id, user, reg_date FROM ha_lineairdb_test.{table_name} WHERE reg_date >= "2024-09-01"')
    rows_gte = cursor.fetchall()
    
    if len(rows_gte) != 3:
        print(f"\tFailed: expected 3 rows with reg_date >= '2024-09-01', got {len(rows_gte)}")
        return 1
    
    users_gte = [row[1] for row in rows_gte]
    expected_users = ["eve", "frank", "grace"]
    for user in expected_users:
        if user not in users_gte:
            print(f"\tFailed: expected {user} in results, got {users_gte}")
            return 1
    
    # Range query: reg_date BETWEEN '2024-06-01' AND '2024-09-30'
    cursor.execute(f'SELECT id, user, reg_date FROM ha_lineairdb_test.{table_name} WHERE reg_date BETWEEN "2024-06-01" AND "2024-09-30"')
    rows_between = cursor.fetchall()
    
    if len(rows_between) != 3:
        print(f"\tFailed: expected 3 rows with reg_date BETWEEN '2024-06-01' AND '2024-09-30', got {len(rows_between)}")
        return 1
    
    users_between = [row[1] for row in rows_between]
    expected_users_between = ["carol", "dave", "eve"]
    for user in expected_users_between:
        if user not in users_between:
            print(f"\tFailed: expected {user} in BETWEEN results, got {users_between}")
            return 1
    
    # Range query: reg_date > '2024-06-10 12:00:00'
    cursor.execute(f'SELECT id, user, reg_date FROM ha_lineairdb_test.{table_name} WHERE reg_date > "2024-06-10 12:00:00"')
    rows_time = cursor.fetchall()
    
    if len(rows_time) != 4:
        print(f"\tFailed: expected 4 rows with reg_date > '2024-06-10 12:00:00', got {len(rows_time)}")
        return 1
    
    users_time = [row[1] for row in rows_time]
    expected_users_time = ["dave", "eve", "frank", "grace"]
    for user in expected_users_time:
        if user not in users_time:
            print(f"\tFailed: expected {user} in results, got {users_time}")
            return 1
    
    print("\tPassed!")
    return 0

def test_composite_index_int_string(db, cursor):
    """Test a composite secondary index on INT + STRING columns."""
    print("COMPOSITE INDEX (INT + STRING) TEST")
    
    table_name = f"test_comp_int_str_{int(time.time() * 1000000)}"
    
    cursor.execute(f'''CREATE TABLE ha_lineairdb_test.{table_name} (
        name VARCHAR(50) NOT NULL,
        age INT NOT NULL,
        department VARCHAR(50) NOT NULL,
        salary INT,
        INDEX age_dept_idx (age, department)
    ) ENGINE = LineairDB''')
    db.commit()
    
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (name, age, department, salary) VALUES ("alice", 25, "engineering", 5000)')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (name, age, department, salary) VALUES ("bob", 25, "sales", 4500)')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (name, age, department, salary) VALUES ("carol", 30, "engineering", 6000)')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (name, age, department, salary) VALUES ("dave", 25, "engineering", 5200)')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (name, age, department, salary) VALUES ("eve", 30, "sales", 5500)')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (name, age, department, salary) VALUES ("frank", 25, "marketing", 4800)')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (name, age, department, salary) VALUES ("grace", 30, "engineering", 6200)')
    db.commit()
    
    cursor.execute(f'SELECT name, age, department, salary FROM ha_lineairdb_test.{table_name}')
    all_rows = cursor.fetchall()
    
    if len(all_rows) != 7:
        print(f"\tFailed: expected 7 rows, got {len(all_rows)}")
        return 1
    
    # Exact match: age=25 AND department='engineering'
    cursor.execute(f'SELECT name, age, department, salary FROM ha_lineairdb_test.{table_name} WHERE age = 25 AND department = "engineering"')
    rows_exact = cursor.fetchall()
    
    if len(rows_exact) != 2:
        print(f"\tFailed: expected 2 rows, got {len(rows_exact)}")
        return 1
    
    names_exact = [row[0] for row in rows_exact]
    if "alice" not in names_exact or "dave" not in names_exact:
        print(f"\tFailed: expected alice and dave, got {names_exact}")
        return 1
    
    # Prefix match: age=25
    cursor.execute(f'SELECT name, age, department FROM ha_lineairdb_test.{table_name} WHERE age = 25')
    rows_prefix = cursor.fetchall()
    
    if len(rows_prefix) != 4:
        print(f"\tFailed: expected 4 rows with age=25, got {len(rows_prefix)}")
        return 1
    
    names_prefix = [row[0] for row in rows_prefix]
    expected_names = ["alice", "bob", "dave", "frank"]
    for name in expected_names:
        if name not in names_prefix:
            print(f"\tFailed: expected {name} in results, got {names_prefix}")
            return 1
    
    # Range query: age=30 AND department <= 'engineering'
    cursor.execute(f'SELECT name, age, department FROM ha_lineairdb_test.{table_name} WHERE age = 30 AND department <= "engineering"')
    rows_range = cursor.fetchall()
    
    if len(rows_range) != 2:
        print(f"\tFailed: expected 2 rows, got {len(rows_range)}")
        return 1
    
    names_range = [row[0] for row in rows_range]
    if "carol" not in names_range or "grace" not in names_range:
        print(f"\tFailed: expected carol and grace, got {names_range}")
        return 1
    
    print("\tPassed!")
    return 0

def test_composite_index_string_datetime(db, cursor):
    """Test a composite secondary index on STRING + DATETIME columns."""
    print("COMPOSITE INDEX (STRING + DATETIME) TEST")
    
    table_name = f"test_comp_str_dt_{int(time.time() * 1000000)}"
    
    cursor.execute(f'''CREATE TABLE ha_lineairdb_test.{table_name} (
        id INT NOT NULL,
        status VARCHAR(20) NOT NULL,
        created_at DATETIME NOT NULL,
        description VARCHAR(50),
        INDEX status_date_idx (status, created_at)
    ) ENGINE = LineairDB''')
    db.commit()
    
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, status, created_at, description) VALUES (1, "active", "2024-01-15 10:00:00", "task1")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, status, created_at, description) VALUES (2, "active", "2024-03-20 14:30:00", "task2")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, status, created_at, description) VALUES (3, "pending", "2024-02-10 09:15:00", "task3")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, status, created_at, description) VALUES (4, "active", "2024-06-25 16:45:00", "task4")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, status, created_at, description) VALUES (5, "completed", "2024-05-05 11:20:00", "task5")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, status, created_at, description) VALUES (6, "pending", "2024-07-01 08:00:00", "task6")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, status, created_at, description) VALUES (7, "active", "2024-02-15 13:30:00", "task7")')
    db.commit()
    
    cursor.execute(f'SELECT id, status, created_at, description FROM ha_lineairdb_test.{table_name}')
    all_rows = cursor.fetchall()
    
    if len(all_rows) != 7:
        print(f"\tFailed: expected 7 rows, got {len(all_rows)}")
        return 1
    
    # Exact match: status='active' AND created_at='2024-03-20 14:30:00'
    cursor.execute(f'SELECT id, status, created_at FROM ha_lineairdb_test.{table_name} WHERE status = "active" AND created_at = "2024-03-20 14:30:00"')
    rows_exact = cursor.fetchall()
    
    if len(rows_exact) != 1:
        print(f"\tFailed: expected 1 row, got {len(rows_exact)}")
        return 1
    
    if rows_exact[0][0] != 2:
        print(f"\tFailed: expected id=2, got {rows_exact[0][0]}")
        return 1
    
    # Prefix match: status='active'
    cursor.execute(f'SELECT id, status, created_at FROM ha_lineairdb_test.{table_name} WHERE status = "active"')
    rows_prefix = cursor.fetchall()
    
    if len(rows_prefix) != 4:
        print(f"\tFailed: expected 4 rows with status='active', got {len(rows_prefix)}")
        return 1
    
    ids_prefix = [row[0] for row in rows_prefix]
    expected_ids = [1, 2, 4, 7]
    for id_val in expected_ids:
        if id_val not in ids_prefix:
            print(f"\tFailed: expected id={id_val} in results, got {ids_prefix}")
            return 1
    
    # Range query: status='active' AND created_at < '2024-03-01'
    cursor.execute(f'SELECT id, status, created_at FROM ha_lineairdb_test.{table_name} WHERE status = "active" AND created_at < "2024-03-01"')
    rows_range = cursor.fetchall()
    
    if len(rows_range) != 2:
        print(f"\tFailed: expected 2 rows, got {len(rows_range)}")
        return 1
    
    ids_range = [row[0] for row in rows_range]
    if 1 not in ids_range or 7 not in ids_range:
        print(f"\tFailed: expected id=1 and id=7, got {ids_range}")
        return 1
    
    print("\tPassed!")
    return 0

def test_composite_index_int_int(db, cursor):
    """Test a composite secondary index on INT + INT columns."""
    print("COMPOSITE INDEX (INT + INT) TEST")
    
    table_name = f"test_comp_int_int_{int(time.time() * 1000000)}"
    
    cursor.execute(f'''CREATE TABLE ha_lineairdb_test.{table_name} (
        id INT NOT NULL,
        year INT NOT NULL,
        month INT NOT NULL,
        sales INT,
        INDEX year_month_idx (year, month)
    ) ENGINE = LineairDB''')
    db.commit()
    
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, year, month, sales) VALUES (1, 2023, 1, 1000)')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, year, month, sales) VALUES (2, 2023, 6, 1500)')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, year, month, sales) VALUES (3, 2023, 12, 2000)')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, year, month, sales) VALUES (4, 2024, 1, 1800)')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, year, month, sales) VALUES (5, 2024, 3, 2200)')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, year, month, sales) VALUES (6, 2024, 6, 2500)')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, year, month, sales) VALUES (7, 2024, 12, 3000)')
    db.commit()
    
    cursor.execute(f'SELECT id, year, month, sales FROM ha_lineairdb_test.{table_name}')
    all_rows = cursor.fetchall()
    
    if len(all_rows) != 7:
        print(f"\tFailed: expected 7 rows, got {len(all_rows)}")
        return 1
    
    # Exact match: year=2024 AND month=6
    cursor.execute(f'SELECT id, year, month, sales FROM ha_lineairdb_test.{table_name} WHERE year = 2024 AND month = 6')
    rows_exact = cursor.fetchall()
    
    if len(rows_exact) != 1:
        print(f"\tFailed: expected 1 row, got {len(rows_exact)}")
        return 1
    
    if rows_exact[0][0] != 6:
        print(f"\tFailed: expected id=6, got {rows_exact[0][0]}")
        return 1
    
    # Prefix match: year=2024
    cursor.execute(f'SELECT id, year, month FROM ha_lineairdb_test.{table_name} WHERE year = 2024')
    rows_prefix = cursor.fetchall()
    
    if len(rows_prefix) != 4:
        print(f"\tFailed: expected 4 rows with year=2024, got {len(rows_prefix)}")
        return 1
    
    ids_prefix = [row[0] for row in rows_prefix]
    expected_ids = [4, 5, 6, 7]
    for id_val in expected_ids:
        if id_val not in ids_prefix:
            print(f"\tFailed: expected id={id_val} in results, got {ids_prefix}")
            return 1
    
    # Range query: year=2024 AND month>=6
    cursor.execute(f'SELECT id, year, month FROM ha_lineairdb_test.{table_name} WHERE year = 2024 AND month >= 6')
    rows_range = cursor.fetchall()
    
    if len(rows_range) != 2:
        print(f"\tFailed: expected 2 rows, got {len(rows_range)}")
        return 1
    
    ids_range = [row[0] for row in rows_range]
    if 6 not in ids_range or 7 not in ids_range:
        print(f"\tFailed: expected id=6 and id=7, got {ids_range}")
        return 1
    
    # Range query: year=2023 AND month BETWEEN 6 AND 12
    cursor.execute(f'SELECT id, year, month FROM ha_lineairdb_test.{table_name} WHERE year = 2023 AND month BETWEEN 6 AND 12')
    rows_between = cursor.fetchall()
    
    if len(rows_between) != 2:
        print(f"\tFailed: expected 2 rows, got {len(rows_between)}")
        return 1
    
    ids_between = [row[0] for row in rows_between]
    if 2 not in ids_between or 3 not in ids_between:
        print(f"\tFailed: expected id=2 and id=3, got {ids_between}")
        return 1
    
    print("\tPassed!")
    return 0

def test_composite_index_skip_middle_key(db, cursor):
    """Test composite index behavior when the middle key is omitted."""
    print("COMPOSITE INDEX SKIP MIDDLE KEY TEST")

    table_name = f"test_skip_middle_{int(time.time() * 1000000)}"

    cursor.execute(f'''CREATE TABLE ha_lineairdb_test.{table_name} (
        id INT NOT NULL,
        category VARCHAR(20) NOT NULL,
        status VARCHAR(20) NOT NULL,
        priority INT NOT NULL,
        description VARCHAR(50),
        INDEX cat_stat_pri_idx (category, status, priority)
    ) ENGINE = LineairDB''')
    db.commit()

    rows_to_insert = [
        (1, "bug", "open", 1, "critical bug"),
        (2, "bug", "open", 3, "minor bug"),
        (3, "bug", "closed", 1, "fixed bug"),
        (4, "bug", "closed", 3, "wontfix"),
        (5, "feature", "open", 1, "new feature"),
        (6, "feature", "open", 2, "enhancement"),
        (7, "feature", "closed", 1, "implemented"),
        (8, "bug", "open", 2, "normal bug"),
    ]

    insert_sql = (
        f'INSERT INTO ha_lineairdb_test.{table_name} '
        f'(id, category, status, priority, description) VALUES (%s, %s, %s, %s, %s)'
    )
    for row in rows_to_insert:
        cursor.execute(insert_sql, row)
    db.commit()

    cursor.execute(f'SELECT COUNT(*) FROM ha_lineairdb_test.{table_name}')
    if cursor.fetchone()[0] != 8:
        print("\tFailed: expected 8 rows after setup")
        return 1

    cursor.execute(
        f'SELECT id FROM ha_lineairdb_test.{table_name} '
        f'WHERE category = "bug" AND status = "open" AND priority = 1 ORDER BY id'
    )
    ids_full = [row[0] for row in cursor.fetchall()]
    if ids_full != [1]:
        print(f"\tFailed: expected id=1 for exact match, got {ids_full}")
        return 1

    cursor.execute(
        f'SELECT id FROM ha_lineairdb_test.{table_name} '
        f'WHERE category = "bug" ORDER BY id'
    )
    ids_category = [row[0] for row in cursor.fetchall()]
    if ids_category != [1, 2, 3, 4, 8]:
        print(f"\tFailed: expected ids [1, 2, 3, 4, 8] for category prefix, got {ids_category}")
        return 1

    cursor.execute(
        f'SELECT id FROM ha_lineairdb_test.{table_name} '
        f'WHERE category = "bug" AND status = "open" ORDER BY id'
    )
    ids_category_status = [row[0] for row in cursor.fetchall()]
    if ids_category_status != [1, 2, 8]:
        print(f"\tFailed: expected ids [1, 2, 8] for category+status prefix, got {ids_category_status}")
        return 1

    cursor.execute(
        f'SELECT id FROM ha_lineairdb_test.{table_name} '
        f'WHERE category = "bug" AND priority = 1 ORDER BY id'
    )
    ids_category_priority = [row[0] for row in cursor.fetchall()]
    if ids_category_priority != [1, 3]:
        print(f"\tFailed: expected ids [1, 3] for category+priority filter, got {ids_category_priority}")
        return 1

    cursor.execute(
        f'SELECT id FROM ha_lineairdb_test.{table_name} '
        f'WHERE category = "feature" AND priority >= 2 ORDER BY id'
    )
    ids_feature_range = [row[0] for row in cursor.fetchall()]
    if ids_feature_range != [6]:
        print(f"\tFailed: expected id [6] for feature priority range, got {ids_feature_range}")
        return 1

    print("\tPassed!")
    return 0

def test_composite_primary_key_basic(db, cursor):
    """Test basic lookups on a composite primary key."""
    print("COMPOSITE PRIMARY KEY BASIC TEST")
    
    table_name = f"test_comp_pk_basic_{int(time.time() * 1000000)}"
    
    cursor.execute(f'''CREATE TABLE ha_lineairdb_test.{table_name} (
        year INT NOT NULL,
        month INT NOT NULL,
        sales INT,
        region VARCHAR(20),
        PRIMARY KEY (year, month)
    ) ENGINE = LineairDB''')
    db.commit()
    
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
    
    cursor.execute(f'SELECT year, month, sales, region FROM ha_lineairdb_test.{table_name}')
    all_rows = cursor.fetchall()
    
    if len(all_rows) != 9:
        print(f"\tFailed: expected 9 rows, got {len(all_rows)}")
        return 1
    
    # Exact match: year=2024 AND month=6
    cursor.execute(f'SELECT year, month, sales, region FROM ha_lineairdb_test.{table_name} WHERE year = 2024 AND month = 6')
    rows_exact = cursor.fetchall()
    if len(rows_exact) != 1:
        print(f"\tFailed: expected 1 row, got {len(rows_exact)}")
        return 1
    
    if rows_exact[0][2] != 2500 or rows_exact[0][3] != "Tokyo":
        print(f"\tFailed: expected sales=2500 and region='Tokyo', got sales={rows_exact[0][2]}, region={rows_exact[0][3]}")
        return 1
    
    # Prefix match: year=2023
    cursor.execute(f'SELECT year, month, sales FROM ha_lineairdb_test.{table_name} WHERE year = 2023')
    rows_prefix = cursor.fetchall()
    
    if len(rows_prefix) != 4:
        print(f"\tFailed: expected 4 rows with year=2023, got {len(rows_prefix)}")
        return 1
    
    months_2023 = [row[1] for row in rows_prefix]
    expected_months = [1, 3, 6, 12]
    for month in expected_months:
        if month not in months_2023:
            print(f"\tFailed: expected month={month} in results, got {months_2023}")
            return 1
    
    print("\tPassed!")
    return 0

def test_composite_primary_key_range(db, cursor):
    """Test range queries on a composite primary key."""
    print("COMPOSITE PRIMARY KEY RANGE TEST")
    
    table_name = f"test_comp_pk_range_{int(time.time() * 1000000)}"
    
    cursor.execute(f'''CREATE TABLE ha_lineairdb_test.{table_name} (
        category VARCHAR(20) NOT NULL,
        item_id INT NOT NULL,
        item_name VARCHAR(50),
        price INT,
        PRIMARY KEY (category, item_id)
    ) ENGINE = LineairDB''')
    db.commit()
    
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
    
    cursor.execute(f'SELECT category, item_id, item_name, price FROM ha_lineairdb_test.{table_name}')
    all_rows = cursor.fetchall()
    
    if len(all_rows) != 10:
        print(f"\tFailed: expected 10 rows, got {len(all_rows)}")
        return 1
    
    # Range query: category='book' AND item_id BETWEEN 5 AND 12
    cursor.execute(f'SELECT category, item_id, item_name FROM ha_lineairdb_test.{table_name} WHERE category = "book" AND item_id BETWEEN 5 AND 12')
    rows_range = cursor.fetchall()
    
    if len(rows_range) != 2:
        print(f"\tFailed: expected 2 rows, got {len(rows_range)}")
        return 1
    
    item_ids = [row[1] for row in rows_range]
    if 5 not in item_ids or 10 not in item_ids:
        print(f"\tFailed: expected item_id=5 and item_id=10, got {item_ids}")
        return 1
    
    # Range query: category='electronics' AND item_id < 10
    cursor.execute(f'SELECT category, item_id, item_name FROM ha_lineairdb_test.{table_name} WHERE category = "electronics" AND item_id < 10')
    rows_lt = cursor.fetchall()
    
    if len(rows_lt) != 2:
        print(f"\tFailed: expected 2 rows, got {len(rows_lt)}")
        return 1
    
    item_ids_lt = [row[1] for row in rows_lt]
    if 2 not in item_ids_lt or 8 not in item_ids_lt:
        print(f"\tFailed: expected item_id=2 and item_id=8, got {item_ids_lt}")
        return 1
    
    # Range query: category='food' AND item_id > 5
    cursor.execute(f'SELECT category, item_id, item_name FROM ha_lineairdb_test.{table_name} WHERE category = "food" AND item_id > 5')
    rows_gt = cursor.fetchall()
    
    if len(rows_gt) != 2:
        print(f"\tFailed: expected 2 rows, got {len(rows_gt)}")
        return 1
    
    item_ids_gt = [row[1] for row in rows_gt]
    if 7 not in item_ids_gt or 11 not in item_ids_gt:
        print(f"\tFailed: expected item_id=7 and item_id=11, got {item_ids_gt}")
        return 1
    
    # Range on first primary-key column: category >= 'electronics'
    cursor.execute(f'SELECT category, item_id, item_name FROM ha_lineairdb_test.{table_name} WHERE category >= "electronics"')
    rows_cat = cursor.fetchall()
    
    if len(rows_cat) != 6:
        print(f"\tFailed: expected 6 rows, got {len(rows_cat)}")
        return 1
    
    categories = set([row[0] for row in rows_cat])
    if "electronics" not in categories or "food" not in categories:
        print(f"\tFailed: expected electronics and food categories, got {categories}")
        return 1
    
    print("\tPassed!")
    return 0

def test_composite_primary_key_three_columns(db, cursor):
    """Test a three-column composite primary key."""
    print("COMPOSITE PRIMARY KEY (3 COLUMNS) TEST")

    table_name = f"test_comp_pk_3col_{int(time.time() * 1000000)}"

    cursor.execute(f'''CREATE TABLE ha_lineairdb_test.{table_name} (
        year INT NOT NULL,
        month INT NOT NULL,
        day INT NOT NULL,
        temperature FLOAT,
        weather VARCHAR(20),
        PRIMARY KEY (year, month, day)
    ) ENGINE = LineairDB''')
    db.commit()

    rows_to_insert = [
        (2024, 1, 1, 5.5, "sunny"),
        (2024, 1, 15, 3.2, "cloudy"),
        (2024, 1, 31, 6.8, "rainy"),
        (2024, 3, 1, 12.5, "sunny"),
        (2024, 3, 15, 15.2, "sunny"),
        (2024, 3, 31, 18.5, "cloudy"),
        (2024, 6, 1, 22.5, "sunny"),
        (2024, 6, 15, 25.8, "sunny"),
        (2024, 6, 30, 28.2, "hot"),
    ]

    insert_sql = (
        f'INSERT INTO ha_lineairdb_test.{table_name} '
        f'(year, month, day, temperature, weather) VALUES (%s, %s, %s, %s, %s)'
    )
    for row in rows_to_insert:
        cursor.execute(insert_sql, row)
    db.commit()

    cursor.execute(f'SELECT COUNT(*) FROM ha_lineairdb_test.{table_name}')
    if cursor.fetchone()[0] != 9:
        print("\tFailed: expected 9 rows after setup")
        return 1

    cursor.execute(
        f'SELECT temperature FROM ha_lineairdb_test.{table_name} '
        f'WHERE year = 2024 AND month = 3 AND day = 15'
    )
    temp_row = cursor.fetchall()
    if len(temp_row) != 1 or abs(temp_row[0][0] - 15.2) > 1e-6:
        print(f"\tFailed: expected temperature 15.2 for 2024-03-15, got {temp_row}")
        return 1

    cursor.execute(
        f'SELECT COUNT(*) FROM ha_lineairdb_test.{table_name} '
        f'WHERE year = 2024'
    )
    if cursor.fetchone()[0] != 9:
        print("\tFailed: expected 9 rows for year=2024")
        return 1

    cursor.execute(
        f'SELECT day FROM ha_lineairdb_test.{table_name} '
        f'WHERE year = 2024 AND month = 1 ORDER BY day'
    )
    days_january = [row[0] for row in cursor.fetchall()]
    if days_january != [1, 15, 31]:
        print(f"\tFailed: expected days [1, 15, 31] for January, got {days_january}")
        return 1

    cursor.execute(
        f'SELECT day FROM ha_lineairdb_test.{table_name} '
        f'WHERE year = 2024 AND month = 3 AND day >= 15 ORDER BY day'
    )
    days_range = [row[0] for row in cursor.fetchall()]
    if days_range != [15, 31]:
        print(f"\tFailed: expected days [15, 31] for March range, got {days_range}")
        return 1

    cursor.execute(
        f'SELECT month FROM ha_lineairdb_test.{table_name} '
        f'WHERE year = 2024 AND month BETWEEN 3 AND 6 ORDER BY month, day'
    )
    months_in_range = [row[0] for row in cursor.fetchall()]
    if len(months_in_range) != 6 or set(months_in_range) != {3, 6}:
        print(f"\tFailed: expected 6 rows covering months 3 and 6, got {months_in_range}")
        return 1

    print("\tPassed!")
    return 0

def test_composite_index_with_primary_key(db, cursor):
    """Test composite secondary indexes on a table with a primary key."""
    print("COMPOSITE INDEX WITH PRIMARY KEY TEST")

    table_name = f"test_comp_with_pk_{int(time.time() * 1000000)}"

    cursor.execute(f'''CREATE TABLE ha_lineairdb_test.{table_name} (
        id INT NOT NULL PRIMARY KEY,
        name VARCHAR(50) NOT NULL,
        age INT NOT NULL,
        department VARCHAR(50) NOT NULL,
        salary INT,
        INDEX age_dept_idx (age, department)
    ) ENGINE = LineairDB''')
    db.commit()

    rows_to_insert = [
        (1, "alice", 25, "engineering", 5000),
        (2, "bob", 25, "sales", 4500),
        (3, "carol", 30, "engineering", 6000),
        (4, "dave", 25, "engineering", 5200),
        (5, "eve", 30, "sales", 5500),
        (6, "frank", 25, "marketing", 4800),
        (7, "grace", 30, "engineering", 6200),
    ]

    insert_sql = (
        f'INSERT INTO ha_lineairdb_test.{table_name} '
        f'(id, name, age, department, salary) VALUES (%s, %s, %s, %s, %s)'
    )
    for row in rows_to_insert:
        cursor.execute(insert_sql, row)
    db.commit()

    cursor.execute(f'SELECT COUNT(*) FROM ha_lineairdb_test.{table_name}')
    if cursor.fetchone()[0] != 7:
        print("\tFailed: expected 7 rows after setup")
        return 1

    cursor.execute(
        f'SELECT name FROM ha_lineairdb_test.{table_name} '
        f'WHERE age = 25 AND department = "engineering" ORDER BY name'
    )
    names_exact = [row[0] for row in cursor.fetchall()]
    if names_exact != ["alice", "dave"]:
        print(f"\tFailed: expected ['alice', 'dave'] for exact match, got {names_exact}")
        return 1

    cursor.execute(
        f'SELECT name FROM ha_lineairdb_test.{table_name} '
        f'WHERE age = 25 ORDER BY name'
    )
    names_age_25 = [row[0] for row in cursor.fetchall()]
    if names_age_25 != ["alice", "bob", "dave", "frank"]:
        print(f"\tFailed: expected ['alice', 'bob', 'dave', 'frank'] for age=25, got {names_age_25}")
        return 1

    cursor.execute(
        f'SELECT name FROM ha_lineairdb_test.{table_name} '
        f'WHERE age = 30 AND department <= "engineering" ORDER BY name'
    )
    names_range = [row[0] for row in cursor.fetchall()]
    if names_range != ["carol", "grace"]:
        print(f"\tFailed: expected ['carol', 'grace'] for age=30 range, got {names_range}")
        return 1

    cursor.execute(
        f'SELECT id, name, age, department FROM ha_lineairdb_test.{table_name} '
        f'WHERE id = 3'
    )
    pk_row = cursor.fetchall()
    if len(pk_row) != 1 or pk_row[0][1] != "carol":
        print(f"\tFailed: expected PRIMARY KEY lookup to return carol, got {pk_row}")
        return 1

    print("\tPassed!")
    return 0

def test_exclusive_range_boundary(db, cursor):
    """Test exclusive range queries (< and >) to ensure end_range->flag is handled correctly."""
    print("EXCLUSIVE RANGE BOUNDARY TEST")
    
    table_name = f"test_excl_range_{int(time.time() * 1000000)}"
    
    cursor.execute(f'''CREATE TABLE ha_lineairdb_test.{table_name} (
        id INT NOT NULL,
        value INT NOT NULL,
        INDEX value_idx (value)
    ) ENGINE = LineairDB''')
    db.commit()
    
    # Insert values 1, 2, 3, 4, 5
    for i in range(1, 6):
        cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, value) VALUES ({i}, {i})')
    db.commit()
    
    # Test: value < 3 should return values 1, 2 (NOT 3)
    cursor.execute(f'SELECT value FROM ha_lineairdb_test.{table_name} WHERE value < 3 ORDER BY value')
    rows_lt = cursor.fetchall()
    values_lt = [row[0] for row in rows_lt]
    
    if values_lt != [1, 2]:
        print(f"\tFailed: value < 3 should return [1, 2], got {values_lt}")
        if 3 in values_lt:
            print("\t*** BUG: end_range->flag (HA_READ_BEFORE_KEY) is not being honored! ***")
        return 1
    
    # Test: value > 3 should return values 4, 5 (NOT 3)
    cursor.execute(f'SELECT value FROM ha_lineairdb_test.{table_name} WHERE value > 3 ORDER BY value')
    rows_gt = cursor.fetchall()
    values_gt = [row[0] for row in rows_gt]
    
    if values_gt != [4, 5]:
        print(f"\tFailed: value > 3 should return [4, 5], got {values_gt}")
        if 3 in values_gt:
            print("\t*** BUG: find_flag (HA_READ_AFTER_KEY) for start key is not working! ***")
        return 1
    
    # Test: value <= 3 should return values 1, 2, 3
    cursor.execute(f'SELECT value FROM ha_lineairdb_test.{table_name} WHERE value <= 3 ORDER BY value')
    rows_lte = cursor.fetchall()
    values_lte = [row[0] for row in rows_lte]
    
    if values_lte != [1, 2, 3]:
        print(f"\tFailed: value <= 3 should return [1, 2, 3], got {values_lte}")
        return 1
    
    # Test: value >= 3 should return values 3, 4, 5
    cursor.execute(f'SELECT value FROM ha_lineairdb_test.{table_name} WHERE value >= 3 ORDER BY value')
    rows_gte = cursor.fetchall()
    values_gte = [row[0] for row in rows_gte]
    
    if values_gte != [3, 4, 5]:
        print(f"\tFailed: value >= 3 should return [3, 4, 5], got {values_gte}")
        return 1
    
    # Test combined: 2 < value < 4 should return only 3
    cursor.execute(f'SELECT value FROM ha_lineairdb_test.{table_name} WHERE value > 2 AND value < 4 ORDER BY value')
    rows_between = cursor.fetchall()
    values_between = [row[0] for row in rows_between]
    
    if values_between != [3]:
        print(f"\tFailed: 2 < value < 4 should return [3], got {values_between}")
        return 1
    
    print("\tPassed!")
    return 0

def test_composite_index_exclusive_range(db, cursor):
    """Test exclusive range (< and >) on composite secondary index."""
    print("COMPOSITE INDEX EXCLUSIVE RANGE TEST")
    
    table_name = f"test_comp_excl_{int(time.time() * 1000000)}"
    
    cursor.execute(f'''CREATE TABLE ha_lineairdb_test.{table_name} (
        id INT NOT NULL,
        category VARCHAR(20) NOT NULL,
        priority INT NOT NULL,
        INDEX cat_pri_idx (category, priority)
    ) ENGINE = LineairDB''')
    db.commit()
    
    # Insert test data: category='bug' with priorities 1, 3, 5, 7, 9
    test_data = [
        (1, "bug", 1),
        (2, "bug", 3),
        (3, "bug", 5),
        (4, "bug", 7),
        (5, "bug", 9),
    ]
    for id_val, cat, pri in test_data:
        cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} VALUES ({id_val}, "{cat}", {pri})')
    db.commit()
    
    # Test: category='bug' AND priority < 5 should return 1, 3 (NOT 5)
    cursor.execute(f'SELECT priority FROM ha_lineairdb_test.{table_name} WHERE category = "bug" AND priority < 5 ORDER BY priority')
    rows = cursor.fetchall()
    priorities = [row[0] for row in rows]
    
    if priorities != [1, 3]:
        print(f"\tFailed: category='bug' AND priority < 5 should return [1, 3], got {priorities}")
        if 5 in priorities:
            print("\t*** BUG: priority=5 should be excluded (< is exclusive)! ***")
        return 1
    
    # Test: category='bug' AND priority > 5 should return 7, 9 (NOT 5)
    cursor.execute(f'SELECT priority FROM ha_lineairdb_test.{table_name} WHERE category = "bug" AND priority > 5 ORDER BY priority')
    rows = cursor.fetchall()
    priorities = [row[0] for row in rows]
    
    if priorities != [7, 9]:
        print(f"\tFailed: category='bug' AND priority > 5 should return [7, 9], got {priorities}")
        if 5 in priorities:
            print("\t*** BUG: priority=5 should be excluded (> is exclusive)! ***")
        return 1
    
    # Test: category='bug' AND priority <= 5 should return 1, 3, 5
    cursor.execute(f'SELECT priority FROM ha_lineairdb_test.{table_name} WHERE category = "bug" AND priority <= 5 ORDER BY priority')
    rows = cursor.fetchall()
    priorities = [row[0] for row in rows]
    
    if priorities != [1, 3, 5]:
        print(f"\tFailed: category='bug' AND priority <= 5 should return [1, 3, 5], got {priorities}")
        return 1
    
    # Test: category='bug' AND priority >= 5 should return 5, 7, 9
    cursor.execute(f'SELECT priority FROM ha_lineairdb_test.{table_name} WHERE category = "bug" AND priority >= 5 ORDER BY priority')
    rows = cursor.fetchall()
    priorities = [row[0] for row in rows]
    
    if priorities != [5, 7, 9]:
        print(f"\tFailed: category='bug' AND priority >= 5 should return [5, 7, 9], got {priorities}")
        return 1
    
    # Test: category='bug' AND 3 < priority < 7 should return only 5
    cursor.execute(f'SELECT priority FROM ha_lineairdb_test.{table_name} WHERE category = "bug" AND priority > 3 AND priority < 7 ORDER BY priority')
    rows = cursor.fetchall()
    priorities = [row[0] for row in rows]
    
    if priorities != [5]:
        print(f"\tFailed: 3 < priority < 7 should return [5], got {priorities}")
        return 1
    
    print("\tPassed!")
    return 0

def test_composite_index_string_collision(db, cursor):
    """Ensure composite index differentiates ('ab','c') and ('a','bc')."""
    print("COMPOSITE INDEX STRING COLLISION TEST")

    table_name = f"test_comp_str_collision_{int(time.time() * 1000000)}"

    cursor.execute(f'''CREATE TABLE ha_lineairdb_test.{table_name} (
        id INT NOT NULL,
        key_part1 VARCHAR(10) NOT NULL,
        key_part2 VARCHAR(10) NOT NULL,
        payload VARCHAR(50),
        INDEX key_idx (key_part1, key_part2)
    ) ENGINE = LineairDB''')
    db.commit()

    rows_to_insert = [
        (1, "ab", "c", "value_ab_c"),
        (2, "a", "bc", "value_a_bc"),
    ]

    insert_sql = (
        f'INSERT INTO ha_lineairdb_test.{table_name} '
        f'(id, key_part1, key_part2, payload) VALUES (%s, %s, %s, %s)'
    )
    for row in rows_to_insert:
        cursor.execute(insert_sql, row)
    db.commit()

    cursor.execute(
        f'SELECT COUNT(*) FROM ha_lineairdb_test.{table_name}')
    if cursor.fetchone()[0] != 2:
        print("\tFailed: expected 2 rows after setup")
        return 1

    cursor.execute(
        f'SELECT payload FROM ha_lineairdb_test.{table_name} '
        f'WHERE key_part1 = "ab" AND key_part2 = "c"')
    rows_ab_c = cursor.fetchall()
    if len(rows_ab_c) != 1 or rows_ab_c[0][0] != "value_ab_c":
        print("\tFailed: expected single match for ('ab','c') with payload 'value_ab_c'")
        print(f"\tGot: {rows_ab_c}")
        return 1

    cursor.execute(
        f'SELECT payload FROM ha_lineairdb_test.{table_name} '
        f'WHERE key_part1 = "a" AND key_part2 = "bc"')
    rows_a_bc = cursor.fetchall()
    if len(rows_a_bc) != 1 or rows_a_bc[0][0] != "value_a_bc":
        print("\tFailed: expected single match for ('a','bc') with payload 'value_a_bc'")
        print(f"\tGot: {rows_a_bc}")
        return 1

    cursor.execute(
        f'SELECT COUNT(*) FROM ha_lineairdb_test.{table_name} '
        f'WHERE key_part1 = "ab" AND key_part2 = "bc"')
    count_wrong = cursor.fetchone()[0]
    if count_wrong != 0:
        print("\tFailed: unexpected match for ('ab','bc')")
        return 1

    cursor.execute(
        f'SELECT COUNT(*) FROM ha_lineairdb_test.{table_name} '
        f'WHERE key_part1 = "a" AND key_part2 = "c"')
    count_wrong2 = cursor.fetchone()[0]
    if count_wrong2 != 0:
        print("\tFailed: unexpected match for ('a','c')")
        return 1

    print("\tPassed!")
    return 0

def main():
    # Connect to the database
    db = mysql.connector.connect(host="localhost", user=args.user, password=args.password)
    cursor = db.cursor()
    
    # Ensure the test database exists
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
    result |= test_exclusive_range_boundary(db, cursor)
    result |= test_composite_index_exclusive_range(db, cursor)
    result |= test_composite_index_string_collision(db, cursor)
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

