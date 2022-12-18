import mysql.connector

def reset (db, cursor) :
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