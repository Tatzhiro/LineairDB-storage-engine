DROP DATABASE IF EXISTS ha_lineairdb_test;
CREATE DATABASE ha_lineairdb_test;

CREATE TABLE ha_lineairdb_test.items (
    title VARCHAR(50) NOT NULL,
    content TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX title_idx (title)
) ENGINE = LineairDB;

INSERT INTO ha_lineairdb_test.items (
    title, content
) VALUES ("alice", "alice meets bob");

INSERT INTO ha_lineairdb_test.items (
    title, content
) VALUES ("bob", "bob meets carol");

CREATE TABLE ha_lineairdb_test.items_w_pk (
    id INT AUTO_INCREMENT NOT NULL PRIMARY KEY, 
    title VARCHAR(50),
    content TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE = LineairDB;

CREATE TABLE ha_lineairdb_test.items_w_two_index (
    id INT AUTO_INCREMENT NOT NULL,
    title VARCHAR(50),
    content TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX id_idx (id),
    INDEX created_at_idx (created_at)
) ENGINE = LineairDB;
