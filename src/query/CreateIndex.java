package query;

import global.Minibase;
import global.RID;
import global.SearchKey;
import heap.HeapFile;
import heap.HeapScan;
import parser.ParseException;
import parser.AST_CreateIndex;
import relop.Schema;
import relop.Tuple;
import index.HashIndex;

/**
 * Execution plan for creating indexes.
 */
class CreateIndex implements Plan {

  protected String fileName;

  protected String ixTable;

  protected String ixColumn;

  protected Schema schema;

  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if index already exists or table/column invalid
   */
  public CreateIndex(AST_CreateIndex tree) throws QueryException {

    fileName = tree.getFileName();
    ixTable = tree.getIxTable();
    ixCol = tree.getIxColumn();

    QueryCheck.fileNotExists(fileName);
    schema = Minibase.SystemCatalog.getSchema(fileName);

    QueryCheck.tableExists(ixTable);
    QueryCheck.columnExists(schema, ixCol);
    }

  } // public CreateIndex(AST_CreateIndex tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {

    HashIndex index = new HashIndex(fileName);
    HeapFile heap = new HeapFile(ixTable);
    HeapScan scan = new HeapScan(heap);

    RID rid = new RID();
    while(scan.hasNext()) {
      Tuple tup = new Tuple(schema, scan.getNext(rid));
      scan.insertEntry(new SearchKey(tup.getField(ixCol)), rid);
    }
    scan.close();

    Minibase.SystemCatalog.createIndex(index);

    // print the output message
    System.out.println("Index created");

  } // public void execute()

} // class CreateIndex implements Plan
