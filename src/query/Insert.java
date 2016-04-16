package query;

import parser.AST_Insert;
import global.Minibase;
import heap.HeapFile;
import parser.ParseException;
import relop.Schema;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.IOException;


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
   
    //streams for converting Object to Byte[]

    
    try {
      ByteArrayOutputStream bout = new ByteArrayOutputStream();
      ObjectOutputStream oout = new ObjectOutputStream(bout);
    
      for (Object value : values) {
        oout.writeObject(value);
      }

      hf.insertRecord(bout.toByteArray());
    } catch (IOException e) {
      System.out.println("Error with output streams in Insert");
    }

    // print the output message
    System.out.println("1 rows affected.");

  } // public void execute()

} // class Insert implements Plan
