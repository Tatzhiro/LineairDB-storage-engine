import sys
import mysql.connector

def reset () :
    cursor.execute('DROP DATABASE IF EXISTS ha_lineairdb_test')
    cursor.execute('CREATE DATABASE ha_lineairdb_test')
    cursor.execute('CREATE TABLE ha_lineairdb_test.items (\
        title VARCHAR(50) NOT NULL,\
        content TEXT,\
        INDEX title_idx (title)\
    )ENGINE = LineairDB')
    db.commit()

def update () :
    reset()
    print("UPDATE TEST")
    cursor.execute(\
        'INSERT INTO ha_lineairdb_test.items (\
            title, content\
        ) VALUES ("carol", "carol meets dave")'\
    )
    cursor.execute('UPDATE ha_lineairdb_test.items SET content="XXX"')

    db.commit()

    cursor.execute('SELECT * FROM ha_lineairdb_test.items')
    rows = cursor.fetchall()
    if not rows :
        print("\tFailed")
        print("\t", rows)
        return 1
    if rows[0][1] == "XXX" and rows[0][0] == "carol":
        print("\tPassed!")
        print("\t", rows)
        return 0
    print("\tFailed")
    print("\t", rows)
    return 1

 
# test
db=mysql.connector.connect(host="localhost", user="root")
cursor=db.cursor()
 
sys.exit(update())