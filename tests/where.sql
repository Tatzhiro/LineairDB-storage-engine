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

INSERT INTO ha_lineairdb_test.items (
    title
) VALUES ("carol");

SELECT * FROM ha_lineairdb_test.items WHERE content is NULL;
SELECT * FROM ha_lineairdb_test.items WHERE content = "alice meets bob";
SELECT * FROM ha_lineairdb_test.items WHERE title = "alice";