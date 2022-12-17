import sys
import mysql.connector
from reset import reset

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
        print("\tCheck 1 Failed")
        print("\t", rows)
        return 1
    for i in rows[0] :
        if i != "carol" and i != None and i != "":
            print("\tCheck 2 Failed")
            print("\t", rows)
            return 1
    if (rows[0][9] == None) :
        print("\tCheck 3 Failed")
        print("\t", rows)
    print("\tPassed!")
    return 0

 
# test
db=mysql.connector.connect(host="localhost", user="root")
cursor=db.cursor()
 
sys.exit(selectNull())