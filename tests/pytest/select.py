import sys
import mysql.connector
from reset import reset

def select () :
    reset()
    print("SELECT TEST")
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

    cursor.execute('SELECT * FROM ha_lineairdb_test.items')
    rows = cursor.fetchall()
    if not rows :
        print("\tFailed")
        print("\t", rows)
        return 1

    cursor.execute('SELECT title, content FROM ha_lineairdb_test.items;')
    rows = cursor.fetchall()

    if len(rows[0]) != 2 :
        print("\tFailed")
        print("\t", rows)
        return 1
    print("\tPassed!")
    print("\t", rows)
    return 0

 
# test
db=mysql.connector.connect(host="localhost", user="root")
cursor=db.cursor()
 
sys.exit(select())