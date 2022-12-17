import sys
import mysql.connector
from reset import reset

def update () :
    reset()
    print("UPDATE TEST")
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
 
sys.exit(update())