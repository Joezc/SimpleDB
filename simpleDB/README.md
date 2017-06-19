# CS186
## TODO
- 不同层次之间异常的捕捉和处理
    - IntegerAggregator -> Dbiterator
- In HeapFile.insertTuple, did not restrict the size of the whole file.



## **Life of a query in SimpleDB**

**Step 1: simpledb.Parser.main() and simpledb.Parser.start()**

`simpledb.Parser.main()` is the entry point for the SimpleDB system. It calls `simpledb.Parser.start()`. The latter performs three main actions:

- It populates the SimpleDB catalog from the catalog text file provided by the user as argument (`Database.getCatalog().loadSchema(argv[0]);`).
- For each table defined in the system catalog, it computes statistics over the data in the table by calling: `TableStats.computeStatistics()`, which then does: `TableStats s = new TableStats(tableid, IOCOSTPERPAGE)`;
- It processes the statements submitted by the user (`processNextStatement(new ByteArrayInputStream(statementBytes));`)

**Step 2: simpledb.Parser.processNextStatement()**

This method takes two key actions:

- First, it gets a physical plan for the query by invoking `handleQueryStatement((ZQuery)s);`
- Then it executes the query by calling `query.execute();`

**Step 3: simpledb.Parser.handleQueryStatement()**

- ​