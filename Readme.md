Complie with 
	"javac -d bin src\*.java"
	
cd to bin to execute with
	"java DavisBasePrompt"
	
Queries:
--------
	Show Meta Tables:
		SHOW TABLES;
		
	Create table, the first column should be called rowid of type int and is by default considered as NOT NULL.
	- Not mentioning NULL makes the column not null-able, and similarly just mentioning NULL will make it null-able.
		CREATE TABLE table_name ( row_id INT, column_name2 data_type2 [NULL],
									column_name3 data_type3 [NULL], ...);
	
	Insert to table: all the column names should be mentioned.
		INSERT INTO table_name [column_list] VALUES value_list;
	
	Select from table with where clause:
		SELECT [col_names] FROM table_name [WHERE condition];
	
	Update a record with where clause:
		UPDATE table_name SET column_name = value [WHERE condition];
	
	Delete a record from table:
		DELETE FROM table_name [WHERE condition];
	
	Drop a table that is created:
		DROP TABLE table_name;