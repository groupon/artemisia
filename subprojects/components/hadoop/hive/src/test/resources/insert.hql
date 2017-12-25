DELETE FROM database.test_table_1;
INSERT INTO OVERWRITE database.test_table_1
SELECT * FROM database.test_table_2;

