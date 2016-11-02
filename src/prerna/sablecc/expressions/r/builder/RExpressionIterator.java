package prerna.sablecc.expressions.r.builder;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.rosuda.REngine.REXP;
import org.rosuda.REngine.REXPMismatchException;

import prerna.ds.R.RDataTable;
import prerna.sablecc.expressions.IExpressionSelector;
import prerna.sablecc.expressions.sql.H2SqlExpressionIterator;
import prerna.util.Utility;

public class RExpressionIterator implements Iterator<Object[]> {

	private static final Logger LOGGER = LogManager.getLogger(H2SqlExpressionIterator.class.getName());

	private RDataTable frame;
	private RExpressionBuilder builder;
	
	private String dataTableName;

	private int rowIndex = 1;
	private int numRows;
	private int numCols;
	
	private Map<String, String> headerTypes;
	private List<String> headers;
	private String headerString;

	// This will hold the full sql expression to execute
	private String rScript;

	public RExpressionIterator(RExpressionBuilder builder) {
		this.builder = builder;
		this.frame = builder.getFrame();
		this.numCols = builder.numSelectors();
		// need to get a random name for the script
		this.dataTableName = "DT_" + Utility.getRandomString(6);

		generateExpression();
		
		LOGGER.info("GENERATED R EXPRESSION SCRIPT : " + this.rScript);
	}
	
	//	@Override
	public void generateExpression() {
		this.rScript = this.builder.toString();
		this.headers = this.builder.getSelectorNames();
	}

//	@Override
	public void runExpression() {
		if(this.rScript == null) {
			generateExpression();
		}
		frame.executeRScript(this.dataTableName + "<-" + rScript);
		REXP result = frame.executeRScript("nrow(" + this.dataTableName + ")");
		try {
			this.numRows = result.asInteger();
		} catch (REXPMismatchException e) {
			e.printStackTrace();
		}
		this.rowIndex = 1;
		getHeaderTypes();
		getHeadersString();
	}

	private void getHeadersString() {
		StringBuilder builder = new StringBuilder("c(");
		int i = 0;
		builder.append("\"").append(this.headers.get(0)).append("\"");
		for(i = 1; i < this.headers.size(); i++) {
			builder.append(" , \"").append(this.headers.get(i)).append("\"");
		}
		builder.append(")");
		this.headerString = builder.toString();
	}

	@Override
	public boolean hasNext() {
		if(rowIndex < numRows) {
			return true;
		} else {
			return false;
		}
	}

	@Override
	public Object[] next() {
		// grab the rowIndex from the data table
		REXP result = this.frame.executeRScript(this.dataTableName + "[" + rowIndex + "," + headerString + " , with=FALSE ]");
		
		Map<String, Object> data = null;
		try {
			// grab as a generic list object
			data = (Map<String, Object>) result.asNativeJavaObject();
		} catch (REXPMismatchException e) {
			e.printStackTrace();
		}
		
		// iterate through the list and fill into an object array to return
		Object[] retArray = new Object[this.headers.size()];
		for(int colIndex = 0; colIndex < numCols; colIndex++) {
			Object val = data.get(this.headers.get(colIndex));
			if(val instanceof Object[]) {
				retArray[colIndex] = ((Object[]) val)[0];
			} else if(val instanceof double[]) {
				retArray[colIndex] = ((double[]) val)[0];
			} else if( val instanceof int[]) {
				retArray[colIndex] = ((int[]) val)[0];
			} else {
				retArray[colIndex] = val;
			}
		}
		
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
		
		String[] types = null;
		String[] names = null;
		REXP typesR = this.frame.executeRScript("sapply(" + dataTableName + " , class)");
		REXP namesR = this.frame.executeRScript("names(" + dataTableName + ")");
		try {
			types = typesR.asStrings();
			names = namesR.asStrings();
			
			int i = 0;
			for(; i < types.length; i++) {
				String type = types[i];
				String name = names[i];
				
				if(type.equalsIgnoreCase("character") || type.equalsIgnoreCase("factor")) {
					headerTypes.put(name, "STRING");
				} else if(type.equalsIgnoreCase("numeric") | type.equalsIgnoreCase("integer")){
					headerTypes.put(name, "NUMBER");
				} else {
					// ughhh... just assume everything else is date for now..
					headerTypes.put(name, "DATE");
				}
			}
		} catch (REXPMismatchException e) {
			e.printStackTrace();
		}
	}
	
	public List<Map<String, Object>> getHeaderInformation(Vector<String> vizTypes, Vector<String> vizFormula) {
		List<Map<String, Object>> returnMap = new Vector<Map<String, Object>>();
		
		List<IExpressionSelector> selectors = builder.getSelectors();
		for(int i = 0; i < numCols; i++) {
			IExpressionSelector selector = selectors.get(i);

			// map to store the info
			Map<String, Object> headMap = new HashMap<String, Object>();

			// the name of the column is set by its expression
			String header = selector.toString();
			
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
