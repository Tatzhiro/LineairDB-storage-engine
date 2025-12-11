import sys
import mysql.connector
from utils.connection import get_connection
from utils.reset import reset
import argparse
import time

def update_basic(db, cursor):
    reset(db, cursor)
    print("UPDATE BASIC TEST")
    
    cursor.execute('SELECT * FROM ha_lineairdb_test.items')
    print("\t[DEBUG] Before INSERT:", cursor.fetchall())
    
    cursor.execute(\
        'INSERT INTO ha_lineairdb_test.items (\
            title, content\
        ) VALUES ("carol", "ddd")'\
    )
    db.commit()
    
    cursor.execute('SELECT * FROM ha_lineairdb_test.items')
    rows_after_insert = cursor.fetchall()
    print("\t[DEBUG] After INSERT:", rows_after_insert)
    print("\t[DEBUG] Number of rows after INSERT:", len(rows_after_insert))
    
    cursor.execute('UPDATE ha_lineairdb_test.items SET content="XXX"')
    db.commit()
    
    cursor.execute('SELECT * FROM ha_lineairdb_test.items')
    rows = cursor.fetchall()
    print("\t[DEBUG] After UPDATE:", rows)
    print("\t[DEBUG] Number of rows after UPDATE:", len(rows))
    
    if not rows:
        print("\tFailed: No rows returned")
        print("\t", rows)
        return 1
    
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
    print("\nUPDATE SECONDARY INDEX BASIC TEST")
    
    table_name = f"test_update_idx_{int(time.time() * 100000)}"
    cursor.execute(f'''CREATE TABLE ha_lineairdb_test.{table_name} (
        name VARCHAR(50) NOT NULL,
        age INT NOT NULL,
        department VARCHAR(50) NOT NULL,
        INDEX age_idx (age)
    ) ENGINE = LineairDB''')
    db.commit()
    
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (name, age, department) VALUES ("Alice", 25, "Sales")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (name, age, department) VALUES ("Bob", 30, "Engineering")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (name, age, department) VALUES ("Carol", 25, "Marketing")')
    db.commit()
    
    cursor.execute(f'SELECT name, age, department FROM ha_lineairdb_test.{table_name} ORDER BY name')
    print("\t[DEBUG] Before UPDATE - All rows:", cursor.fetchall())
    
    cursor.execute(f'SELECT name, age FROM ha_lineairdb_test.{table_name} WHERE age=25 ORDER BY name')
    print("\t[DEBUG] Before UPDATE - Age=25:", cursor.fetchall())
    
    print(f"\t[DEBUG] Executing: UPDATE {table_name} SET age=26 WHERE name='Alice'")
    cursor.execute(f'UPDATE ha_lineairdb_test.{table_name} SET age=26 WHERE name="Alice"')
    print("\t[DEBUG] UPDATE committed")

    db.commit()

    cursor.execute(f'SELECT name, age FROM ha_lineairdb_test.{table_name} WHERE age=26')
    rows_26 = cursor.fetchall()
    print("\t[DEBUG] After UPDATE - Age=26:", rows_26)
    
    cursor.execute(f'SELECT name, age, department FROM ha_lineairdb_test.{table_name} ORDER BY name')
    all_rows = cursor.fetchall()
    print("\t[DEBUG] After UPDATE - All rows:", all_rows)
    
    cursor.execute(f'SELECT name, age FROM ha_lineairdb_test.{table_name} WHERE age=25 ORDER BY name')
    rows_25 = cursor.fetchall()
    print("\t[DEBUG] After UPDATE - Age=25:", rows_25)
    
    if len(rows_26) != 1 or rows_26[0][0] != "Alice":
        print("\tFailed: Expected Alice with age 26")
        print("\t", rows_26)
        return 1
    
    names = [row[0] for row in rows_25]
    if "Alice" in names:
        print("\tFailed: Alice should not be found with age 25")
        print("\t", rows_25)
        return 1
    
    print("\tPassed!")
    return 0


def update_secondary_index_multiple_rows(db, cursor):
    print("\nUPDATE SECONDARY INDEX MULTIPLE ROWS TEST")
    
    table_name = f"test_update_multi_{int(time.time() * 1000000)}"
    
    cursor.execute(f'''CREATE TABLE ha_lineairdb_test.{table_name} (
        name VARCHAR(50) NOT NULL,
        age INT NOT NULL,
        department VARCHAR(50) NOT NULL,
        INDEX age_idx (age),
        INDEX dept_idx (department)
    ) ENGINE = LineairDB''')
    db.commit()
    
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (name, age, department) VALUES ("Alice", 25, "Sales")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (name, age, department) VALUES ("Bob", 30, "Sales")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (name, age, department) VALUES ("Carol", 28, "Sales")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (name, age, department) VALUES ("Dave", 35, "Engineering")')
    db.commit()
    
    cursor.execute(f'SELECT name, age, department FROM ha_lineairdb_test.{table_name} ORDER BY name')
    print("\t[DEBUG] Before UPDATE:", cursor.fetchall())
    print(f"\t[DEBUG] Executing: UPDATE {table_name} SET age=age+1 WHERE department='Sales'")
    cursor.execute(f'UPDATE ha_lineairdb_test.{table_name} SET age=age+1 WHERE department="Sales"')
    db.commit()
    
    cursor.execute(f'SELECT name, age, department FROM ha_lineairdb_test.{table_name} ORDER BY name')
    print("\t[DEBUG] After UPDATE:", cursor.fetchall())
    
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
    
    cursor.execute(f'SELECT name, age FROM ha_lineairdb_test.{table_name} WHERE department="Engineering"')
    rows = cursor.fetchall()
    if len(rows) != 1 or rows[0][0] != "Dave" or rows[0][1] != 35:
        print("\tFailed: Dave's age should remain 35")
        print("\t", rows)
        return 1
    
    print("\tPassed!")
    return 0


def update_secondary_index_to_existing_value(db, cursor):
    print("\nUPDATE SECONDARY INDEX TO EXISTING VALUE TEST")
    
    table_name = f"test_update_exist_{int(time.time() * 1000000)}"
    
    cursor.execute(f'''CREATE TABLE ha_lineairdb_test.{table_name} (
        name VARCHAR(50) NOT NULL,
        age INT NOT NULL,
        department VARCHAR(50) NOT NULL,
        INDEX age_idx (age)
    ) ENGINE = LineairDB''')
    db.commit()
    
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (name, age, department) VALUES ("Alice", 25, "Sales")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (name, age, department) VALUES ("Bob", 30, "Engineering")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (name, age, department) VALUES ("Carol", 28, "Marketing")')
    db.commit()
    
    cursor.execute(f'UPDATE ha_lineairdb_test.{table_name} SET age=30 WHERE name="Alice"')
    db.commit()
    
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

    cursor.execute(f'SELECT name FROM ha_lineairdb_test.{table_name} WHERE age=25')
    rows = cursor.fetchall()
    if len(rows) != 0:
        print("\tFailed: No one should have age=25")
        print("\t", rows)
        return 1
    
    print("\tPassed!")
    return 0


def update_multiple_secondary_indexes(db, cursor):
    print("\nUPDATE MULTIPLE SECONDARY INDEXES TEST")
    
    table_name = f"test_update_multi_idx_{int(time.time() * 1000000)}"
    
    cursor.execute(f'''CREATE TABLE ha_lineairdb_test.{table_name} (
        name VARCHAR(50) NOT NULL,
        age INT NOT NULL,
        department VARCHAR(50) NOT NULL,
        INDEX age_idx (age),
        INDEX dept_idx (department)
    ) ENGINE = LineairDB''')
    db.commit()
    
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (name, age, department) VALUES ("Alice", 25, "Sales")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (name, age, department) VALUES ("Bob", 30, "Engineering")')
    db.commit()
    
    cursor.execute(f'UPDATE ha_lineairdb_test.{table_name} SET age=26, department="Marketing" WHERE name="Alice"')
    db.commit()
    
    cursor.execute(f'SELECT name, department FROM ha_lineairdb_test.{table_name} WHERE age=26')
    rows = cursor.fetchall()
    if len(rows) != 1 or rows[0][0] != "Alice" or rows[0][1] != "Marketing":
        print("\tFailed: Alice should have age=26 and department=Marketing")
        print("\t", rows)
        return 1
    
    cursor.execute(f'SELECT name, age FROM ha_lineairdb_test.{table_name} WHERE department="Marketing"')
    rows = cursor.fetchall()
    if len(rows) != 1 or rows[0][0] != "Alice" or rows[0][1] != 26:
        print("\tFailed: Alice should be found in Marketing with age 26")
        print("\t", rows)
        return 1
    
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
    print("\nUPDATE SECONDARY INDEX WITH TRANSACTION TEST")
    
    table_name = f"test_update_tx_{int(time.time() * 1000000)}"
    
    cursor.execute(f'''CREATE TABLE ha_lineairdb_test.{table_name} (
        name VARCHAR(50) NOT NULL,
        age INT NOT NULL,
        department VARCHAR(50) NOT NULL,
        INDEX age_idx (age)
    ) ENGINE = LineairDB''')
    db.commit()
    
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (name, age, department) VALUES ("Alice", 25, "Sales")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (name, age, department) VALUES ("Bob", 30, "Engineering")')
    db.commit()
    
    cursor.execute(f'UPDATE ha_lineairdb_test.{table_name} SET age=26 WHERE name="Alice"')
    cursor.execute(f'UPDATE ha_lineairdb_test.{table_name} SET age=31 WHERE name="Bob"')
    db.commit()
    
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
    print("\nUPDATE PRIMARY KEY BASIC TEST")
    
    table_name = f"test_update_pk_{int(time.time() * 1000000)}"
    
    cursor.execute(f'''CREATE TABLE ha_lineairdb_test.{table_name} (
        id INT NOT NULL,
        name VARCHAR(50) NOT NULL,
        age INT NOT NULL,
        PRIMARY KEY (id)
    ) ENGINE = LineairDB''')
    db.commit()
    
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, name, age) VALUES (1, "Alice", 25)')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, name, age) VALUES (2, "Bob", 30)')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, name, age) VALUES (3, "Carol", 28)')
    db.commit()
    
    print("\t[DEBUG] Before UPDATE:")
    cursor.execute(f'SELECT id, name, age FROM ha_lineairdb_test.{table_name} ORDER BY id')
    print("\t", cursor.fetchall())
    
    cursor.execute(f'UPDATE ha_lineairdb_test.{table_name} SET name="Alice Smith", age=26 WHERE id=1')
    db.commit()
    
    print("\t[DEBUG] After UPDATE:")
    cursor.execute(f'SELECT id, name, age FROM ha_lineairdb_test.{table_name} ORDER BY id')
    rows = cursor.fetchall()
    print("\t", rows)
    
    if len(rows) != 3:
        print(f"\tFailed: Expected 3 rows, got {len(rows)}")
        return 1
    
    if rows[0] != (1, "Alice Smith", 26):
        print("\tFailed: ID 1 should be ('Alice Smith', 26)")
        print("\t", rows[0])
        return 1
    
    if rows[1] != (2, "Bob", 30) or rows[2] != (3, "Carol", 28):
        print("\tFailed: Other rows should not be changed")
        print("\t", rows)
        return 1
    
    print("\tPassed!")
    return 0


def update_primary_key_multiple_rows(db, cursor):
    print("\nUPDATE PRIMARY KEY MULTIPLE ROWS TEST")
    
    table_name = f"test_update_pk_multi_{int(time.time() * 1000000)}"
    
    cursor.execute(f'''CREATE TABLE ha_lineairdb_test.{table_name} (
        id INT NOT NULL,
        name VARCHAR(50) NOT NULL,
        age INT NOT NULL,
        department VARCHAR(50) NOT NULL,
        PRIMARY KEY (id)
    ) ENGINE = LineairDB''')
    db.commit()
    
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, name, age, department) VALUES (1, "Alice", 25, "Sales")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, name, age, department) VALUES (2, "Bob", 30, "Sales")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, name, age, department) VALUES (3, "Carol", 28, "Engineering")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, name, age, department) VALUES (4, "Dave", 35, "Engineering")')
    db.commit()
    
    print("\t[DEBUG] Before UPDATE:")
    cursor.execute(f'SELECT id, name, age, department FROM ha_lineairdb_test.{table_name} ORDER BY id')
    print("\t", cursor.fetchall())
    
    cursor.execute(f'UPDATE ha_lineairdb_test.{table_name} SET age=age+1 WHERE department="Sales"')
    db.commit()
    
    print("\t[DEBUG] After UPDATE:")
    cursor.execute(f'SELECT id, name, age, department FROM ha_lineairdb_test.{table_name} ORDER BY id')
    rows = cursor.fetchall()
    print("\t", rows)

    if rows[0][2] != 26 or rows[1][2] != 31:
        print("\tFailed: Sales department ages should be incremented")
        print("\t", rows)
        return 1
    
    if rows[2][2] != 28 or rows[3][2] != 35:
        print("\tFailed: Engineering department ages should not be changed")
        print("\t", rows)
        return 1
    
    print("\tPassed!")
    return 0


def update_primary_key_with_secondary_index(db, cursor):
    print("\nUPDATE PRIMARY KEY WITH SECONDARY INDEX TEST")
    
    table_name = f"test_update_pk_idx_{int(time.time() * 1000000)}"
    
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
    
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, name, age, department) VALUES (1, "Alice", 25, "Sales")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, name, age, department) VALUES (2, "Bob", 30, "Engineering")')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (id, name, age, department) VALUES (3, "Carol", 25, "Marketing")')
    db.commit()
    
    print("\t[DEBUG] Before UPDATE:")
    cursor.execute(f'SELECT id, name, age, department FROM ha_lineairdb_test.{table_name} ORDER BY id')
    print("\t", cursor.fetchall())
    
    cursor.execute(f'UPDATE ha_lineairdb_test.{table_name} SET age=26, department="HR" WHERE id=1')
    db.commit()
    
    print("\t[DEBUG] After UPDATE:")
    cursor.execute(f'SELECT id, name, age, department FROM ha_lineairdb_test.{table_name} ORDER BY id')
    print("\t", cursor.fetchall())
    
    cursor.execute(f'SELECT name, age, department FROM ha_lineairdb_test.{table_name} WHERE id=1')
    rows = cursor.fetchall()
    if len(rows) != 1 or rows[0] != ("Alice", 26, "HR"):
        print("\tFailed: ID 1 should have age=26 and department=HR")
        print("\t", rows)
        return 1
    
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
    
    cursor.execute(f'SELECT id FROM ha_lineairdb_test.{table_name} WHERE age=25 ORDER BY id')
    rows = cursor.fetchall()
    if len(rows) != 1 or rows[0][0] != 3:
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
    print("\nUPDATE COMPOSITE PRIMARY KEY TEST")
    
    table_name = f"test_update_comp_pk_{int(time.time() * 1000000)}"
    
    cursor.execute(f'''CREATE TABLE ha_lineairdb_test.{table_name} (
        dept_id INT NOT NULL,
        emp_id INT NOT NULL,
        name VARCHAR(50) NOT NULL,
        salary INT NOT NULL,
        PRIMARY KEY (dept_id, emp_id)
    ) ENGINE = LineairDB''')
    db.commit()
    
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (dept_id, emp_id, name, salary) VALUES (1, 1, "Alice", 50000)')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (dept_id, emp_id, name, salary) VALUES (1, 2, "Bob", 55000)')
    cursor.execute(f'INSERT INTO ha_lineairdb_test.{table_name} (dept_id, emp_id, name, salary) VALUES (2, 1, "Carol", 60000)')
    db.commit()
    
    print("\t[DEBUG] Before UPDATE:")
    cursor.execute(f'SELECT dept_id, emp_id, name, salary FROM ha_lineairdb_test.{table_name} ORDER BY dept_id, emp_id')
    print("\t", cursor.fetchall())
    
    cursor.execute(f'UPDATE ha_lineairdb_test.{table_name} SET name="Alice Smith", salary=52000 WHERE dept_id=1 AND emp_id=1')
    db.commit()
    
    print("\t[DEBUG] After UPDATE:")
    cursor.execute(f'SELECT dept_id, emp_id, name, salary FROM ha_lineairdb_test.{table_name} ORDER BY dept_id, emp_id')
    rows = cursor.fetchall()
    print("\t", rows)
    
    if rows[0] != (1, 1, "Alice Smith", 52000):
        print("\tFailed: (1, 1) should be updated")
        print("\t", rows[0])
        return 1

    if rows[1] != (1, 2, "Bob", 55000) or rows[2] != (2, 1, "Carol", 60000):
        print("\tFailed: Other rows should not be changed")
        print("\t", rows)
        return 1
    
    print("\tPassed!")
    return 0

 
def main(): 
    db=get_connection(user=args.user, password=args.password)
    cursor=db.cursor()
    
    failed = 0
    
    if update_basic(db, cursor) != 0:
        failed += 1
    
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