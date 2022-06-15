from time import sleep
import mysql.connector

def INSERT (prm) :
    cursor.execute('INSERT INTO ha_lineairdb_test.items (title, content) VALUES (%s, %s)', prm)

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
    # sleep(0.1)
    cursor.execute('SELECT title FROM ha_lineairdb_test.items')
    rows = cursor.fetchall()
    if not rows: 
        print("\tFailed: list empty")
    elif rows[0][0] == "alice" and rows[1][0] == "bob":
        print("\tPassed!")
    else : 
        print("\tFailed")


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
    # sleep(0.1)

    cursor.execute('SELECT * FROM ha_lineairdb_test.items')
    rows = cursor.fetchall()
    if rows :
        print("\tFailed 1")
        print("\t", rows)
        return

    cursor.execute(\
        'INSERT INTO ha_lineairdb_test.items (\
            title, content\
        ) VALUES ("carol", "carol meets dave")'\
    )
    cursor.execute('DELETE FROM ha_lineairdb_test.items WHERE title = "carol"')
    db.commit()
    # sleep(0.1)

    cursor.execute('SELECT * FROM ha_lineairdb_test.items')
    rows = cursor.fetchall()
    if rows :
        print("\tFailed 2")
        print("\t", rows)
        return
    print("\tPassed!")

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
        return
    for i in rows[0] :
        if i != "carol" and i != None:
            print("\tFailed")
            print("\t", rows)
            return
    print("\tPassed!")
    if (rows[0][9] == None) :
        print("\tWANTFIX: content9 should not be NULL")
        print("\t", rows)

 
# test
db=mysql.connector.connect(host="localhost", user="root")
cursor=db.cursor()
 
insert()
delete()
selectNull()