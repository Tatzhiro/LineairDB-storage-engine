import sys
import mysql.connector
from reset import reset

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