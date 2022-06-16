INSERT INTO ha_lineairdb_test.items (
    title, content
) VALUES ("carol", "carol meets dave");
DELETE FROM ha_lineairdb_test.items;


DELETE FROM ha_lineairdb_test.items WHERE title = "carol";