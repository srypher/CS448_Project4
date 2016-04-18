package query;

import parser.AST_Delete;
import relop.FileScan;
import heap.HeapFile;
import relop.Tuple;
import relop.Schema;
import relop.Predicate;
import global.RID;

/**
 * Execution plan for deleting tuples.
 */
class Delete implements Plan {
  
  protected String fileName;
  protected Predicate[][] predicates;
  protected Schema schema;
  protected HeapFile hf;
  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if table doesn't exist or predicates are invalid
   */
  public Delete(AST_Delete tree) throws QueryException {
    //get stuff from tree
    this.fileName = tree.getFileName();
    this.predicates = tree.getPredicates();

    //check query
    this.schema = QueryCheck.tableExists(this.fileName);
    QueryCheck.predicates(this.schema, this.predicates);
    hf = new HeapFile(this.fileName);
  } // public Delete(AST_Delete tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {
    int rowsAffected = 0;
    Tuple tuple;
    boolean pass = false;
    RID rid;
    
    FileScan fs = new FileScan(schema, hf);

    //search through the filescan
    while (fs.hasNext()) {
      tuple = fs.getNext();

      for (Predicate[] pred1 : predicates) {
        for (Predicate pred2 : pred1) {
	  pass = pred2.evaluate(tuple);
	  if (pass) break;
	}
	if (!pass) break;
      }
      if (pass) {
        rowsAffected++;
	//delete this record based on its RID
	hf.deleteRecord(fs.getLastRID());
      }

    }

    fs.close();
    // print the output message
    System.out.printf("%d rows deleted.\n", rowsAffected);

  } // public void execute()

} // class Delete implements Plan
