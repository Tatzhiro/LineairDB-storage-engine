from time import sleep
import mysql.connector
from reset import reset

def INSERT (prm) :
    cursor.execute('INSERT INTO ha_lineairdb_test.items (title, content) VALUES (%s, %s)', prm)

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

def update () :
    reset()
    print("BLOB UPDATE TEST")
    cursor.execute(\
        'INSERT INTO ha_lineairdb_test.items (\
            title, content\
        ) VALUES ("carol", "ddd")'\
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
 
insert()
delete()
selectNull()
update()