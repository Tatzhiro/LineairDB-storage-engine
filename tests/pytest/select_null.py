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

def selectNull () :
    reset()
    print("NULL SELECT TEST")
    cursor.execute(\
        'INSERT INTO ha_lineairdb_test.items (\
            title, content9\
        ) VALUES ("carol", "")'\
    )
    db.commit()

    cursor.execute('SELECT * FROM ha_lineairdb_test.items')
    rows = cursor.fetchall()
    if not rows :
        print("\tFailed")
        print("\t", rows)
        return 1
    for i in rows[0] :
        if i != "carol" and i != None:
            print("\tFailed")
            print("\t", rows)
            return 1
    print("\tPassed!")
    if (rows[0][9] == None) :
        print("\tWANTFIX: content9 should not be NULL")
        print("\t", rows)
    return 0

 
# test
db=mysql.connector.connect(host="localhost", user="root")
cursor=db.cursor()
 
sys.exit(selectNull())