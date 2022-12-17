import sys
import mysql.connector
from reset import reset

def where () :
    reset()
    print("SELECT WHERE TEST")
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

    cursor.execute('SELECT * FROM ha_lineairdb_test.items WHERE content is NULL')
    rows = cursor.fetchall()
    if rows :
        print("\tCheck 1 Failed")
        print("\t", rows)
        return 1

    cursor.execute('SELECT * FROM ha_lineairdb_test.items WHERE content = "alice meets bob"')
    rows = cursor.fetchall()

    if rows[0][0] != "alice" :
        print("\tCheck 2 Failed")
        print("\t", rows)
        return 1

    cursor.execute('SELECT * FROM ha_lineairdb_test.items WHERE title = "alice"')
    rows = cursor.fetchall()
    if rows[0][0] != "alice" :
        print("\tCheck 3 Failed")
        print("\t", rows)
        return 1
        
    print("\tPassed!")
    print("\t", rows)
    return 0

 
# test
db=mysql.connector.connect(host="localhost", user="root")
cursor=db.cursor()
 
sys.exit(where())