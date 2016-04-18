package query;

import parser.AST_Insert;
import global.Minibase;
import heap.HeapFile;
import parser.ParseException;
import relop.Schema;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.IOException;
import relop.Tuple;

/**
 * Execution plan for inserting tuples.
 */
class Insert implements Plan {

  //the name of the table to insert into
  protected String fileName;
  //the values to insert
  protected Object[] values;
  //the schema of the table we're inserting into
  protected Schema schema;

  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if table doesn't exists or values are invalid
   */
  public Insert(AST_Insert tree) throws QueryException {
    this.fileName = tree.getFileName();
    this.values = tree.getValues();
    
    //check if table exists and get schema
    this.schema = QueryCheck.tableExists(this.fileName);

    //check if values match the schema
    QueryCheck.insertValues(this.schema, this.values);

  } // public Insert(AST_Insert tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {
    HeapFile hf = new HeapFile(fileName);
    Tuple toInsert = new Tuple(this.schema);
    
    //construct tuple
    for (int i = 0; i < schema.getCount(); i++) {
      toInsert.setField(i, values[i]);
    }
    
    hf.insertRecord(toInsert.getData());

    // print the output message
    String toPrint = "";
    
    for (int i = 0; i < schema.getCount(); i++) {
      toPrint = toPrint + schema.fieldName(i) + ":" + toInsert.getField(i).toString() +", ";
    }
    toPrint = toPrint.substring(0, toPrint.length() - 2);
    System.out.println("Inserted " + toPrint);

  } // public void execute()

} // class Insert implements Plan
