package query;

import parser.AST_Select;
import relop.Predicate;
import relop.FileScan;
import global.SortKey;
import heap.HeapFile;
import relop.Schema;
import relop.SimpleJoin;
import relop.Projection;
import relop.Selection;
import global.AttrOperator;
import global.AttrType;


/**
 * Execution plan for selecting tuples.
 */
class Select implements Plan {

  protected String[] tables;
  protected String[] columns;
  protected Predicate[][] predicates;
  protected SortKey[] orders;//shouldnt be needed
  protected FileScan[] scans;
  protected HeapFile[] heaps;
  protected Schema[] schema;

  protected AST_Select tree;
  protected Selection selects[];
  protected SimpleJoin tableJoins[];
  protected Projection plan;

  protected boolean isExplain;
  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if validation fails
   */
  public Select(AST_Select tree) throws QueryException {
    this.tree = tree;
    this.isExplain = tree.isExplain;
    tables = tree.getTables();
    columns = tree.getColumns();
    predicates = tree.getPredicates();
    orders = tree.getOrders();
    
    schema = new Schema[tables.length];
    heaps = new HeapFile[tables.length];
    scans = new FileScan[tables.length];

    for(int i = 0; i < tables.length; i++) {
      try {
        schema[i] = QueryCheck.tableExists(tables[i]);
      } catch (QueryException e) { //remove scans that we'e already made
        for (int j = 0; j < i; j++) scans[j].close();
	throw e; //continue throwing exception
      }
      heaps[i] = new HeapFile(tables[i]);
      scans[i] = new FileScan(schema[i], heaps[i]);
    }
    //naive
    Schema bigSchema = new Schema(1);
    if (predicates.length > 0) selects = new Selection[predicates.length];
    else selects = new Selection[1];
    Predicate alwaysTrue[] = { new Predicate(AttrOperator.EQ, AttrType.INTEGER, 1, AttrType.INTEGER, 1) };


    if (heaps.length > 1) {
      //chain joins into into big table
      tableJoins = new SimpleJoin[heaps.length - 1];
      
      tableJoins[0] = new SimpleJoin(scans[0], scans[1]);
      bigSchema = Schema.join(schema[0], schema[1]);
      for (int i = 2; i < heaps.length; i++) {
        tableJoins[i - 1] = new SimpleJoin(tableJoins[i - 2], scans[i]);
	bigSchema = Schema.join(bigSchema, schema[i]);
      }
      
      try {
      //querycheck our huge table
        for (String column : columns) {
          QueryCheck.columnExists(bigSchema, column);
        }
        QueryCheck.predicates(bigSchema, predicates);
      } catch (QueryException e) { //close what we need
        for (SimpleJoin tableJoin : tableJoins) tableJoin.close();
	for (FileScan scan : scans) scan.close();
	throw e;
      }
      //make first selection
      selects[0] = new Selection(tableJoins[tableJoins.length - 1], (predicates.length < 1) ? alwaysTrue : predicates[0]);

    } else if (heaps.length > 0) {
      tableJoins = null;
      //work with only one table;
      bigSchema = schema[0];
      //querycheck
      
      try {
        for (String column : columns) {
          QueryCheck.columnExists(bigSchema, column);
        }
        QueryCheck.predicates(bigSchema, predicates);
      } catch (QueryException e) { //close what we need
        for (FileScan scan : scans) scan.close();
	throw e;
      }

      //make first selection
      selects[0] = new Selection(scans[0], (predicates.length < 1) ? alwaysTrue : predicates[0]);
    } else {
      throw new QueryException("No files to select from!");
    }
    
    //chain the rest of the selections (the "AND"s)
    for (int i = 1; i < predicates.length; i++) {
      selects[i] = new Selection(selects[i - 1], predicates[i]);
    }

    //project answer
    
    Integer[] fldnos = new Integer[columns.length];
    if (columns.length == 0) fldnos = new Integer[bigSchema.getCount()];

    for (int i = 0; i < fldnos.length; i++) {
      if (columns.length == 0) fldnos[i] = i;
      else fldnos[i] = bigSchema.fieldNumber(columns[i]);
      
      /*if (fldnos[i] == -1) {
        for (Selection select : selects) select.close();
        if (tableJoins != null) for (SimpleJoin tableJoin : tableJoins) tableJoin.close();
	for (FileScan scan : scans) scan.close();
	throw new QueryException("Column doesn't exist!");
      }*/
    }
    plan = new Projection(selects[selects.length - 1], fldnos);


  } // public Select(AST_Select tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {
    // print the output message
    if (isExplain) {
      plan.explain(0);
      //explain doesnt close anything......
      for (FileScan scan : scans) scan.close();
      if ( tableJoins != null ) for (SimpleJoin tableJoin : tableJoins) tableJoin.close();
      for (Selection select : selects) select.close();
      plan.close();
    } else plan.execute();
  } // public void execute()

} // class Select implements Plan
