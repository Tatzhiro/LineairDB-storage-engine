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

def insert () :
    reset()
    print("INSERT TEST")
    cursor.execute(\
        'INSERT INTO ha_lineairdb_test.items (\
            title, content\
        ) VALUES ("alice", "alice meets bob")'\
    )
    cursor.execute(\
        'INSERT INTO ha_lineairdb_test.items (\
            title, content\
        ) VALUES ("bob", "bob meets carol")'\
    )
    db.commit()
    cursor.execute('SELECT title FROM ha_lineairdb_test.items')
    rows = cursor.fetchall()
    if not rows: 
        print("\tFailed: list empty")
        return 1
    elif rows[0][0] == "alice" and rows[1][0] == "bob":
        print("\tPassed!")
        return 0
    else : 
        print("\tFailed")
        return 1

 
# test
db=mysql.connector.connect(host="localhost", user="root")
cursor=db.cursor()
 
sys.exit(insert())