import sys
import mysql.connector

def reset () :
    cursor.execute('DROP DATABASE IF EXISTS ha_lineairdb_test')
    cursor.execute('CREATE DATABASE ha_lineairdb_test')
    cursor.execute('CREATE TABLE ha_lineairdb_test.items (\
        title VARCHAR(50) NOT NULL,\
        content TEXT,\
        content2 TEXT,\
        content3 TEXT,\
        content4 TEXT,\
        content5 TEXT,\
        content6 TEXT,\
        content7 TEXT,\
        content8 TEXT,\
        content9 TEXT,\
        INDEX title_idx (title)\
    )ENGINE = LineairDB')
    db.commit()

def delete () :
    reset()
    print("DELETE TEST")
    cursor.execute(\
        'INSERT INTO ha_lineairdb_test.items (\
            title, content\
        ) VALUES ("carol", "carol meets dave")'\
    )
    cursor.execute('DELETE FROM ha_lineairdb_test.items')
    db.commit()

    cursor.execute('SELECT * FROM ha_lineairdb_test.items')
    rows = cursor.fetchall()
    if rows :
        print("\tFailed 1")
        print("\t", rows)
        return 1

    cursor.execute(\
        'INSERT INTO ha_lineairdb_test.items (\
            title, content\
        ) VALUES ("carol", "carol meets dave")'\
    )
    cursor.execute('DELETE FROM ha_lineairdb_test.items WHERE title = "carol"')
    db.commit()

    cursor.execute('SELECT * FROM ha_lineairdb_test.items')
    rows = cursor.fetchall()
    if rows :
        print("\tFailed 2")
        print("\t", rows)
        return 1
    print("\tPassed!")
    return 0
 
# test
db=mysql.connector.connect(host="localhost", user="root")
cursor=db.cursor()
 
sys.exit(delete())