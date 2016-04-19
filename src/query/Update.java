package query;

import parser.AST_Update;
import relop.Schema;
import global.RID;
import relop.FileScan;
import relop.Tuple;
import relop.Predicate;
import heap.HeapFile;
import global.AttrOperator;
import global.AttrType;


/**
 * Execution plan for updating tuples.
 */
class Update implements Plan {

  protected String fileName;
  protected String[] columns;
  protected Object[] values;
  protected Predicate[][] predicates;
  protected Schema schema;
  protected int[] fldnos;

  protected FileScan fs;
  protected HeapFile heap;

  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if invalid column names, values, or pedicates
   */
  public Update(AST_Update tree) throws QueryException {
    //get what we need from tree
    this.fileName = tree.getFileName();
    this.columns = tree.getColumns();
    this.values = tree.getValues();
    this.predicates = tree.getPredicates();

    //check everything
    this.schema = QueryCheck.tableExists(this.fileName);
    QueryCheck.predicates(this.schema, this.predicates);
    this.fldnos = QueryCheck.updateFields(this.schema, this.columns);
    QueryCheck.updateValues(this.schema, this.fldnos, this.values);

    //open filescan (for updating)
    this.heap = new HeapFile(this.fileName);

    if (predicates.length < 1) {
      predicates = new Predicate[1][1];
      predicates[0][0] = new Predicate(AttrOperator.EQ, AttrType.INTEGER, 1, AttrType.INTEGER, 1);
    }
    
  } // public Update(AST_Update tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {
    int rowsAffected = 0; 
    Tuple tuple;
    boolean pass = false;;
    RID rid;
    String toPrint = "";
    
    fs = new FileScan(schema, heap);
    
    //update tuples
    while (fs.hasNext()) {
      tuple = fs.getNext();
      
      /*System.out.printf("checking: ");
      toPrint = "";
      for (int i = 0; i < schema.getCount(); i++) {
	toPrint = toPrint + schema.fieldName(i) + ":" + tuple.getField(i) + ", ";
      }
      toPrint = toPrint.substring(0, toPrint.length() - 2);
      System.out.println(toPrint);*/ //this block is for testing

      //check if tuple fits criteria
      for (Predicate[] pred1 : predicates) {
        for (Predicate pred2 : pred1) {
          pass = pred2.evaluate(tuple);
	  if (pass) break; // if true, we've passed the OR
	}
	if (!pass) break; // if false, we've failed the AND
      }
      if (pass) {
        rowsAffected++;
        //modify the tuple we got
	for (int i = 0; i < fldnos.length; i++) {
          tuple.setField(fldnos[i], values[i]);
	}
	rid = fs.getLastRID();
        //put it back
	heap.updateRecord(rid, tuple.getData());
      }
    }
    fs.close();
    // print the output message
    System.out.printf("%d rows affected.\n", rowsAffected);

  } // public void execute()

} // class Update implements Plan
