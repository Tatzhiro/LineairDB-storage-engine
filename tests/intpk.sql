DROP DATABASE IF EXISTS ha_lineairdb_test;
CREATE DATABASE ha_lineairdb_test;
CREATE TABLE ha_lineairdb_test.items (
    content TEXT,
    content2 TEXT,
    content3 TEXT,
    id INT NOT NULL,
    content4 TEXT,
    content5 TEXT,
    content6 TEXT,
    content7 TEXT,
    content8 TEXT,
    content9 TEXT,
    INDEX id_idx (id)
) ENGINE = LineairDB;
SELECT * FROM ha_lineairdb_test.items WHERE id = 1;
