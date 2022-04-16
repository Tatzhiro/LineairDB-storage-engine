DROP DATABASE IF EXISTS ha_lineairdb_test;
CREATE DATABASE ha_lineairdb_test;

CREATE TABLE ha_lineairdb_test.items (
    id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
    title VARCHAR(50),
    content TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE = LineairDB;
-- ) ENGINE = InnoDB;

INSERT INTO ha_lineairdb_test.items (
    title, content
) VALUES ("alice", "alice meets bob");

INSERT INTO ha_lineairdb_test.items (
    title, content
) VALUES ("bob", "bob meets carol");
