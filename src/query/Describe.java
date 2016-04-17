package query;

import parser.AST_Describe;
import relop.Schema;
import global.AttrType;
import java.util.ArrayList;
/**
 * Execution plan for describing tables.
 */
class Describe implements Plan {

  //name of table
  protected String fileName;
  //schema of table
  protected Schema schema;
  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if table doesn't exist
   */
  public Describe(AST_Describe tree) throws QueryException {
    this.fileName = tree.getFileName();
    this.schema = QueryCheck.tableExists(this.fileName);
  } // public Describe(AST_Describe tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {
    int fieldCount = schema.getCount();
    ArrayList<String> types = new ArrayList<String>(fieldCount); 
    ArrayList<String> names = new ArrayList<String>(fieldCount);

    for (int i = 0; i < fieldCount; i++) {
      types.add(AttrType.toString(schema.fieldType(i)));
      names.add(schema.fieldName(i));
    }
    // print the output message
    for (int i = 0; i < fieldCount; i++) {
      System.out.printf("(%s) %s | ", types.get(i), names.get(i));
    }
    System.out.println();
    System.out.println("---------------------------------------------------");

  } // public void execute()

} // class Describe implements Plan
