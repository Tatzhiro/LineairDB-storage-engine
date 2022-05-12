DROP DATABASE IF EXISTS ha_lineairdb_test;
CREATE DATABASE ha_lineairdb_test;
CREATE TABLE ha_lineairdb_test.items (
    title VARCHAR(50) NOT NULL,
    content TEXT,
    content2 TEXT,
    content3 TEXT,
    content4 TEXT,
    content5 TEXT,
    content6 TEXT,
    content7 TEXT,
    content8 TEXT,
    content9 TEXT,
    INDEX title_idx (title)
) ENGINE = LineairDB;
INSERT INTO ha_lineairdb_test.items (
    title, content10
) VALUES ("alice", "alice meets bob");
SELECT * FROM ha_lineairdb_test.items;