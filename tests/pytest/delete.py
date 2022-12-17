import sys
import mysql.connector
from reset import reset

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