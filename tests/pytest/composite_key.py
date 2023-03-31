import sys
import mysql.connector
import argparse

def reset (db, cursor) :
    cursor.execute('DROP DATABASE IF EXISTS ha_lineairdb_test')
    cursor.execute('CREATE DATABASE ha_lineairdb_test')
    cursor.execute('CREATE TABLE ha_lineairdb_test.items (\
        first_name VARCHAR(50) NOT NULL,\
        middle_name VARCHAR(50) NOT NULL,\
        last_name VARCHAR(50) NOT NULL,\
        age int NOT NULL,\
        content TEXT,\
        PRIMARY KEY(age, middle_name, last_name, first_name)  \
    )ENGINE = LineairDB')
    db.commit()

def composite_key (db, cursor) :
    print("COMPOSITE KEY TEST")
    try:
      reset(db, cursor)
    except Exception as e:
      print(e)
      return 1
    cursor.execute(\
        'INSERT INTO ha_lineairdb_test.items (\
            first_name, middle_name, last_name, age, content\
        ) VALUES ("alice", "avril", "ada", 5, "alice meets bob")'\
    )
    db.commit()
    cursor.execute('SELECT * FROM ha_lineairdb_test.items')
    rows = cursor.fetchall()
    if not rows :
        print("\tCheck 1 Failed")
        print("\t", rows)
        return 1
    print("\tCheck 1 Passed")
    db.commit()
    cursor.execute(\
        'INSERT INTO ha_lineairdb_test.items (\
            first_name, middle_name, last_name, age, content\
        ) VALUES ("alice", "ann", "adalace", 3, "new comer")'\
    )
    db.commit()
    cursor.execute('SELECT * FROM ha_lineairdb_test.items WHERE age = 5')
    rows = cursor.fetchall()
    if not rows :
        print("\tCheck 2 Failed")
        print("\t", rows)
        return 1
    db.commit()
    cursor.execute('SELECT * FROM ha_lineairdb_test.items WHERE first_name = "alice"')
    rows = cursor.fetchall()
    if len(rows) != 2 :
        print("\tCheck 3 Failed")
        print("\t", rows)
        return 1
    db.commit()
    print("\tPassed!")
    print("\t", rows)
    return 0



def main():
    # test
    db=mysql.connector.connect(host="localhost", user=args.user, password=args.password)
    cursor=db.cursor()
    
    sys.exit(composite_key(db, cursor))


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