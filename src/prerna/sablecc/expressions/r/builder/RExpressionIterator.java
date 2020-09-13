package prerna.sablecc.expressions.r.builder;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.ds.r.RDataTable;
import prerna.sablecc.expressions.IExpressionSelector;
import prerna.sablecc.expressions.sql.H2SqlExpressionIterator;
import prerna.util.Utility;

public class RExpressionIterator implements Iterator<Object[]> {

	private static final Logger LOGGER = LogManager.getLogger(H2SqlExpressionIterator.class.getName());

	private RDataTable frame;
	private RExpressionBuilder builder;
	
	private String dataTableName;

	private int rowIndex;
	private int numRows;
	private int numCols;
	
	private Map<String, String> headerTypes;
	private String[] headers;

	// This will hold the full sql expression to execute
	private String rScript;

	private List<Object[]> rBulkData;
	
	public RExpressionIterator(RExpressionBuilder builder) {
		this.builder = builder;
		this.frame = builder.getFrame();
		this.numCols = builder.numSelectors();
		// need to get a random name for the script
		this.dataTableName = "DT_" + Utility.getRandomString(6);

		generateExpression();
		runExpression();

		LOGGER.info("GENERATED R EXPRESSION SCRIPT : " + this.rScript);
	}
	
	//	@Override
	public void generateExpression() {
		this.rScript = this.builder.toString();
		this.headers = this.builder.getSelectorNames().toArray(new String[]{});
	}

//	@Override
	public void runExpression() {
		if(this.rScript == null) {
			generateExpression();
		}
		frame.executeRScript(this.dataTableName + " <- " + rScript);
		this.numRows = frame.getNumRows(this.dataTableName);
		int limit = this.builder.getLimit();
		int offset = this.builder.getOffset();
		
		// gotta do some error handling so that the values are not too large
		if(offset > numRows) {
			offset = numRows;
		}
		int endIndex = offset + limit;
		if(endIndex > numRows) {
			endIndex = numRows;
		}
		if(limit > 0) {
			if(offset > 0) {
				frame.executeRScript(this.dataTableName + "<-" + this.dataTableName + "[" + offset + ":" + (endIndex) + ",]");
			} else {
				frame.executeRScript(this.dataTableName + "<-" + this.dataTableName + "[1:" + (endIndex) + ",]");
			}
		} else if(offset > 0) {
			frame.executeRScript(this.dataTableName + "<-" + this.dataTableName + "[" + offset + ":nrow(" + this.dataTableName + "),]");
		}
		IExpressionSelector sortBy = this.builder.getSortSelector();
		if(sortBy != null) {
			frame.executeRScript(this.dataTableName + "<-" + this.dataTableName + sortBy.toString());
		}
		
		this.rBulkData = frame.getBulkDataRow(this.dataTableName, this.headers);
		this.numRows = rBulkData.size();
		this.rowIndex = 0;
		getHeaderTypes();
	}

	@Override
	public boolean hasNext() {
		if(rowIndex < numRows) {
			return true;
		} else {
			// clean up variables
			this.frame.executeRScript("rm(" + this.dataTableName + "); gc();");
			return false;
		}
	}

	@Override
	public Object[] next() {
		// grab the rowIndex from the data table
		Object[] retArray = this.rBulkData.get(rowIndex);
		// update the row index
		this.rowIndex++;

		return retArray;
	}

//	@Override
	public void close() {
		this.frame.executeRScript("rm(" + dataTableName + ")");
	}
	
	private void getHeaderTypes() {
		headerTypes = new HashMap<String, String>();

		String[] types = this.frame.getColumnTypes(this.dataTableName);
		String[] names = this.frame.getColumnNames(this.dataTableName);

		int i = 0;
		for(; i < types.length; i++) {
			String type = types[i];
			String name = names[i].toUpperCase();

			if(type.equalsIgnoreCase("character") || type.equalsIgnoreCase("factor")) {
				headerTypes.put(name, "STRING");
			} else if(type.equalsIgnoreCase("numeric") | type.equalsIgnoreCase("integer")){
				headerTypes.put(name, "NUMBER");
			} else {
				// ughhh... just assume everything else is date for now..
				headerTypes.put(name, "DATE");
			}
		}
	}
	
	public List<Map<String, Object>> getHeaderInformation(List<String> vizTypes, List<String> vizFormula) {
		List<Map<String, Object>> returnMap = new Vector<Map<String, Object>>();
		
		List<IExpressionSelector> selectors = builder.getSelectors();
		for(int i = 0; i < numCols; i++) {
			IExpressionSelector selector = selectors.get(i);

			// map to store the info
			Map<String, Object> headMap = new HashMap<String, Object>();

			// the name of the column is set by its expression
			String origHeader = selector.toString();
			// the name of the column is set by its expression
			String header = selector.getName();
						
			headMap.put("uri", header);
			headMap.put("varKey", header);
			headMap.put("type", headerTypes.get(header.toUpperCase()));
			headMap.put("vizType", vizTypes.get(i).replace("=", ""));
			
			// TODO push this on the selector to provide its type
			
			// based on type, fill in the information
			if(selector instanceof RColumnSelector || selector instanceof RConstantSelector) {
				// we don't have a derivation
				// just a normal column
				// put in an empty map and you are done
				headMap.put("operation", new HashMap<String, Object>());
				
			} else {
				// if not a column or a constant
				// it is some kind of expression
				
				// if its an expression and there is a group
				// then the group must have been applied to this value
				HashMap<String, Object> operationMap = new HashMap<String, Object>();
				List<String> groupBys = builder.getGroupByColumns();
				if(groupBys != null && !groupBys.isEmpty()) {
					operationMap.put("groupBy", groupBys);
				}

				// get the columns used
				List<String> colsUsed = selector.getTableColumns();
				operationMap.put("calculatedBy", colsUsed);

				if(selector instanceof RMathSelector) {
					operationMap.put("math", ((RMathSelector) selector).getPkqlMath() );
				}

				// add the formula if it is not just a simple column
				operationMap.put("formula", vizFormula.get(i));
				
				// add to main map
				headMap.put("operation", operationMap);
			}

			returnMap.add(headMap);
		}

		return returnMap;
	}
	
}
