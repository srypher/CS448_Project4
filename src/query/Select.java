package query;

import parser.AST_Select;

/**
 * Execution plan for selecting tuples.
 */
class Select implements Plan {

  String[] tables;
  String[] columns;
  Predicate[][] predicates;
  SortKey[] orders;
  FileScan[] scans;

  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if validation fails
   */
  public Select(AST_Select tree) throws QueryException {
    tables = tree.getTables();
    columns = tree.getColumns();
    predicates = tree.getPredicates();
    orders = tree.getOrders();
    scans = new FileScan[tables.length];

    for(int i = 0; i < tables.length; i++) {
      QueryCheck.tableExists(tables[i]);
    }
    for(int i = 0; i < scans.length; i++) {
      scans[i] = new FileScan(); //can't remember filescan constructor and the documentation link is broken ATM
    }

  } // public Select(AST_Select tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {

    // print the output message
    System.out.println("0 rows affected. (Not implemented)");

  } // public void execute()

} // class Select implements Plan
