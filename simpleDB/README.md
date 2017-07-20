SimpleDB is a simple database. SimpleDB has the following primary feature:

- Support basic database insert, select, delete, filter and join operation. 
- Support multiple-table operations.
- Implement a simple query optimization schema
- Support transaction, ensure ACID properties.

More bottom details

- Slice data files into pages.
- Implement a buffer pool to manage the pages.

Usage sample

1. (under terminal)`ant dist`
2. `java -jar dist/simpledb.jar parser data/dblp_data/dblp_simpledb.schema` load schema and data, get into database simulation terminal
3. `select p.title from papers p where p.title like 'selectivity';`execucate normal sql statements.