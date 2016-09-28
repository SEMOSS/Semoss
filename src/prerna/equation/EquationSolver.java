package prerna.equation;

/**
 * Equation Solver
 * @author Jason Adleberg: jadleberg@deloitte.com, Rishi Luthar: rluthar@deloitte.com
 * @version 1.0
 * @date 1/6/2016
 *  
 * This object is used to run an equation on an ITableDataFrame. It takes in two arguments -- 
 * an ITableDataFrame, and an equation written as a String.
 * 
 * The equation can contain any of the table header names as variables. If the equation contains
 * the name for a variable which is not defined, it will throw a ParseException.
 * 
 * Once the object is constructed, calling .crunch() will compute the new column and add it to the BTree
 * behind the scenes. A javascript call is then made to refresh the BTree as it appears in the graph view.
 * 
 * ----------------------------------------------------------------------
 * 
 * This version also contains a mechanism to iterate vertically. This functionality is implemented in the constructor.
 * If the equation contains the phrase 'aggregate([String groupBy], [String variable])', this will be parsed out first.
 * 
 * For instance, if the user types in 'aggregate(Genre, median(Revenue))', the new column's value for each row will be
 * the median Revenue value for that row's Genre. 
 * 
 */

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Set;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.BTreeDataFrame;
import prerna.ds.ExactStringMatcher;
import prerna.util.ArrayUtilityMethods;

public class EquationSolver {
	
	// ITableDataFrame information
	private ITableDataFrame tree;
	private HashSet<String> headerSet;
	
	// parser object 
	org.nfunk.jep.JEP myParser;
	
	// expression string 
	private String expString;
	
	// information necessary for aggregation to work
	private boolean vertical;							// is aggregate on?
	private Hashtable<String, Double> aggregateTable;   // intermediate table to hold condensed info
	private int aggregrateColumnIndex;
	private int groupByColumnIndex;
	private String aggregateString;
	
	// term used to connote vertical operation
	private static String AGGREGATE = "aggregate(";
	
	
	/**
	 * Constructor for our EquationSolver object.
	 * 
	 * If the equation contains an "aggregation" event, then there is specific logic used to handle that event as well.
	 * 
	 * @param mainTree - the tree used for operations
	 * @param tempExpString - expression to be parsed
	 */
	@SuppressWarnings("unchecked")
	public EquationSolver(ITableDataFrame mainTree, String tempExpString) throws ParseException {
		if (mainTree.getData().size() == 0)             { throw new ParseException("Table is empty.", 0); }
		if (tempExpString.isEmpty())    				{ throw new ParseException("Formula string is Empty.", 0); }
		if (mainTree.getColumnHeaders().length == 0)    { throw new ParseException("Couldn't find header names.", 0); }
		
		// copy over data
		tree = mainTree;
		expString = tempExpString;
		headerSet = new HashSet<String>(Arrays.asList(mainTree.getColumnHeaders()));
				
		// create parser object
		myParser = new org.nfunk.jep.JEP();
		myParser.addStandardFunctions(); // sin, tan, etc
		myParser.addStandardConstants(); // pi, e, etc
		myParser.setImplicitMul(true);   // let a(b) = a*b
		myParser.addFunction("median", new Median());
		myParser.addFunction("if", new If());
		myParser.addFunction("mean", new Mean());
		myParser.addFunction("mode", new Mode());
		myParser.addFunction("sum", new Sum());
		myParser.addFunction("average", new Mean());
		myParser.addFunction("count", new Count());
		myParser.setAllowUndeclared(true);
		
		tree.setColumnsToSkip(null);
			
		// handle aggregation
		if (expString.contains(AGGREGATE)) {
			Hashtable<String, String> parsedStringInfo = parseAggregate(expString);
			solveAggregate(parsedStringInfo);
		}
		
		// if no aggregation, check for potential errors of undefined variables.
		else {
			myParser.parseExpression(expString);
					
			// check all parsed variables are column headers
			Set<String> parsedVariables = myParser.getSymbolTable().keySet();
			for (String var : parsedVariables) {
				if (var != "e" && var != "pi" && !headerSet.contains(var)) {
					String parseError = "Couldn't find variable "+var+" in the column headers";
					throw new ParseException(parseError, 0);
				}
				else {
					myParser.addVariable(var, 1);
				}
			}
			
			// check for other errors
			if (myParser.getErrorInfo() != null) { 
				throw new ParseException(myParser.getErrorInfo(), -1);
			}
		}
	}
	
	 /**
	 * This code is specific to aggregate functions.
	 * 
	 * This sets a bunch of variables specific for aggregating things, if 
	 * "aggregate" is contained in the expression string.
	 * 
	 * @param aggregate - the String phrase to be parsed
	 * @return a dictionary of aggregate-specific items 
	 * 
	 */
	private Hashtable<String, String> parseAggregate(String aggregate) {
		
		Hashtable<String, String> parsedStringInfo = new Hashtable<String, String>();
		vertical = true;
		aggregateString = "";
		
		// get the actual string
		int aggregateStringIndex = expString.indexOf(AGGREGATE) + AGGREGATE.length();
		int parenthesisCounter = 1;
		for (int i = aggregateStringIndex; i < expString.length(); i++) {
			if (expString.charAt(i) == '(') {parenthesisCounter++;}
			if (expString.charAt(i) == ')') {parenthesisCounter--;}
			if (parenthesisCounter == 0) {
				aggregateString = expString.substring(aggregateStringIndex-10, i+1);
				break;
			}
		}
		
		// this is the beginning of some string parsing stuff. 
		String groupByVariable    = aggregateString.substring(aggregateString.indexOf('(')+1, aggregateString.indexOf(',')); 	 	// "Genre"
		String operationToPerform = aggregateString.substring(aggregateString.indexOf(',')+1, aggregateString.lastIndexOf(')')+1);  // median(Title__RevenueDomestic)
		String operationVariable = operationToPerform.substring(operationToPerform.indexOf('(')+1,operationToPerform.indexOf(')'));	// Title__RevenueDomestic
		String operationFunction = operationToPerform.substring(0, operationToPerform.indexOf('('));							    // median
		
		// which column in our table is the operationVariable?
		aggregrateColumnIndex = 0;
		for (String s: tree.getColumnHeaders()) {
			if (operationVariable.equals(s)) { break; }
			aggregrateColumnIndex++;
		}
		
		// which column in our table is the operationVariable?
		groupByColumnIndex = 0;
		for (String s: tree.getColumnHeaders()) {
			if (groupByVariable.equals(s)) { break; }
			groupByColumnIndex++;
		}
		
		parsedStringInfo.put("groupByVariable", groupByVariable);
		parsedStringInfo.put("operationToPerform", operationToPerform);
		parsedStringInfo.put("operationVariable", operationVariable);
		parsedStringInfo.put("operationFunction", operationFunction);
		
		return parsedStringInfo;
	}
	
	/**
	 * Returns the column headers that are common between the original table and joining table.
	 * These headers will be the basis for joining.
	 * 
	 * @param origHeaders - the headers of the original table
	 * @param varHeaders - the set of variables being used by the parser
	 * @return set of headers to be used for joining
	 */
	private String[] getJoinTableLevelNames(String[] origHeaders, Set<String> varHeaders) {
		// for vertical aggregation, we need access to all headers
		if (vertical) { return origHeaders; }
		
		ArrayList<String> newHeaders = new ArrayList<String>();
		for(int i = 0; i < origHeaders.length; i++) {
			if(varHeaders.contains(origHeaders[i])) {
				newHeaders.add(origHeaders[i]);
			}
		}
		
		// this will happen if we set our new column to be a constant.
		if (newHeaders.size() == 0) {
			newHeaders.add(origHeaders[0]);
		}
		
		return newHeaders.toArray(new String[0]);
	}
	
	/**
	 * Returns a list of headers not necessary for joining purposes.
	 * 
	 * @param origHeaders - the headers of the original table
	 * @param joinHeaders - the headers to be used for joining table
	 * @return set of headers to be not used for joining
	 */
	private List<String> getSkipColumns(String[] origHeaders, String[] joinHeaders) {
		List<String> skipColumns = new ArrayList<String>();
		for(String header : origHeaders) {
			if(!ArrayUtilityMethods.arrayContainsValue(joinHeaders, header)) {
				skipColumns.add(header);
			}
		}

		return skipColumns;
	}
	
	/**
	 * Handles the aggregation component to equation solving by creating an intermediate table
	 * to store aggregation variables.
	 * 
	 * @param parsedStringInfo - a dictionary of terms necessary for computing the aggregation variables.
	 */
	private void solveAggregate(Hashtable<String, String> parsedStringInfo) {

//		// get all instances of groupByVariable [e.g. Genre]
//		Object[] uniqueInstances = tree.getColumn(parsedStringInfo.get("groupByVariable"));	    // ["Comedy", "Comedy", "Drama" ... ]
//		Set<Object> uniqueValuesInColumn = new HashSet<Object>(Arrays.asList(uniqueInstances)); // ["Comedy", "Drama" ...]
//		Hashtable<String, Double> intermediateTable = new Hashtable<String, Double>();
//				
//		// get operation to perform on each groupByVariable [e.g. median(Revenue) for "Comedy"]
//		for (Object uniqueValue: uniqueValuesInColumn) {
////			List<Object[]> subSet = tree.getData(parsedStringInfo.get("groupByVariable"), uniqueValue);
//			Double[] values = new Double[subSet.size()];
//			
//			int i = 0;
//			for (Object[] row: subSet) {
//				values[i] = (double) row[aggregrateColumnIndex];
//				i++;
//			}
//						
//			// get a list of all values
//			String tempString = parsedStringInfo.get("operationFunction") + "(" + Arrays.toString(values) + ")";
//			tempString = tempString.replace("[","");
//			tempString = tempString.replace("]","");
//			
//			// use JEP to compute object
//			myParser.parseExpression(tempString);
//			double calculation = myParser.getValue();	
//			
//			// save this single value into intermediate hashtable.
//			intermediateTable.put(uniqueValue.toString(), calculation);
//		}
//		
//		// print and save
//		System.out.println(parsedStringInfo.get("operationToPerform")+": "+intermediateTable);
//		aggregateTable = intermediateTable;
//		
	}
	
	/**
	 * This method actually computes the new column and updates the BTree in the background.
	 * 
	 * If aggregate is on, we will need to reference the intermediate table we created earlier to 
	 * fetch the values we so tediously calculated.
	 * 
	 * It returns a string dictating the state of the success.
	 * 
	 * @param newHeaderName - new name for the column
	 * @return String dictating success of method.
	 */
	public String crunch(String newHeaderName) {
		
		if (headerSet.contains(newHeaderName)) { return "Column name already exists"; }
		
		// create new BTreeDataFrame with the joining headers and the new header
		@SuppressWarnings("unchecked")
		String[] joinTableNames = getJoinTableLevelNames(tree.getColumnHeaders(), myParser.getSymbolTable().keySet());
		String[] newTableHeaders = new String[joinTableNames.length+1];
		for (int i = 0; i < joinTableNames.length; i++) { newTableHeaders[i] = joinTableNames[i]; }
		newTableHeaders[newTableHeaders.length-1] = newHeaderName;
		BTreeDataFrame newTable = new BTreeDataFrame(newTableHeaders);
		
		//Set columns to skip in Table
		if (!vertical) {
			tree.setColumnsToSkip(getSkipColumns(tree.getColumnHeaders(), joinTableNames));
		}
		
		String[] headerNames = tree.getColumnHeaders();
		
		for (Object[] row: tree.getData()) {
			String tempString = "";
			if (vertical) {
				// for each row, replace the aggregate part with a single numeric value
				Object value = row[groupByColumnIndex];
				double replaceValue = aggregateTable.get(value);
				tempString = expString.replace(aggregateString, Double.toString(replaceValue));
			}
			
			Object[] newRow = new Object[row.length + 1];
			for(int i = 0; i < row.length; i++) {
				Object cell = row[i];
				if (cell instanceof Number) {
					myParser.addVariable(headerNames[i], ((Number)cell).doubleValue());
				} else {
					myParser.addVariable(headerNames[i], 1);
				}
				newRow[i] = cell;
			}
			
			//parse expression, get value, add new row to table
			if (vertical) {	myParser.parseExpression(tempString); }
			else { myParser.parseExpression(expString); }
			double value = myParser.getValue();
			newRow[newRow.length-1] = value;
			newTable.addRow(newRow);
		}
		
		tree.setColumnsToSkip(null);

		//join new table onto old table
		if(joinTableNames.length > 1) {
			((BTreeDataFrame)tree).join(newTable, joinTableNames, joinTableNames, 1.0);
		} else {
			tree.join(newTable, joinTableNames[0], joinTableNames[0], 1.0, new ExactStringMatcher());
		}
		
		return "success";
	}
	
	/****/
	public static void main(String[] args) { }

}