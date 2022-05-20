-- Expected to pass the following tests
-- Since they accesses the field "content", which is not the primary-key of this table.
SELECT * FROM ha_lineairdb_test.items WHERE content is NULL;
SELECT * FROM ha_lineairdb_test.items WHERE content = "alice meets bob";

-- Expected to pass if this storage engine supports the primary-key ("title") index search.
SELECT * FROM ha_lineairdb_test.items WHERE title = "alice";
