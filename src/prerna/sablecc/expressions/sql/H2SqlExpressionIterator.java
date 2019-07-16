package prerna.sablecc.expressions.sql;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.ds.rdbms.h2.H2Frame;
import prerna.sablecc.expressions.IExpressionSelector;
import prerna.sablecc.expressions.sql.builder.SqlColumnSelector;
import prerna.sablecc.expressions.sql.builder.SqlConstantSelector;
import prerna.sablecc.expressions.sql.builder.SqlExpressionBuilder;
import prerna.sablecc.expressions.sql.builder.SqlMathSelector;
import prerna.util.Utility;

public class H2SqlExpressionIterator implements Iterator<Object[]> {

	private static final Logger LOGGER = LogManager.getLogger(H2SqlExpressionIterator.class.getName());

	private H2Frame frame;
	private SqlExpressionBuilder builder;
	private ResultSet rs;

	private int numCols;
	private Map<String, String> headerTypes;

	// This will hold the full sql expression to execute
	private String sqlScript;

	public H2SqlExpressionIterator(SqlExpressionBuilder builder) {
		this.builder = builder;
		this.frame = builder.getFrame();
		this.numCols = builder.numSelectors();
		this.sqlScript = builder.toString();
		LOGGER.info("GENERATED SQL EXPRESSION SCRIPT : " + this.sqlScript);
	}
	
//	@Override
	public void generateExpression() {
		this.sqlScript = this.builder.toString();
	}

//	@Override
	public void runExpression() {
		if(this.sqlScript == null) {
			generateExpression();
		}
		rs = frame.execQuery(sqlScript);
		getHeaderTypes();
	}

	@Override
	public boolean hasNext() {
		if(rs == null) {
			runExpression();
		}
		boolean hasNext = false;
		try {
			hasNext = rs.next();
			if(!hasNext) {
				close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return hasNext;
	}

	@Override
	public Object[] next() {
		if(rs == null) {
			runExpression();
		}
		Object[] values = new Object[numCols];
		try {
			for(int i = 0; i < numCols; i++) {
				values[i] = rs.getObject(i+1);
			}
		} catch(SQLException e) {
			e.printStackTrace();
		}
		return values;
	}

//	@Override
	public void close() {
		if(this.rs != null) {
			try {
				this.rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	private void getHeaderTypes() {
//		if(rs == null) {
//			runExpression();
//		}
		this.headerTypes = new HashMap<String, String>();
		try {
			ResultSetMetaData rsmd = this.rs.getMetaData();
			for(int i = 1; i <= numCols; i++) {
				String name = rsmd.getColumnName(i).toUpperCase();
				String type = Utility.getCleanDataType(rsmd.getColumnTypeName(i));
				if(type.equals("DOUBLE")) {
					type = "NUMBER";
				}
				// note, this will put everything upper case
				headerTypes.put(name, type);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public List<Map<String, Object>> getHeaderInformation(List<String> vizTypes, List<String> vizFormula) {
		// need to run this in order to get the types
		if(rs == null) {
			runExpression();
		}
		List<Map<String, Object>> returnMap = new Vector<Map<String, Object>>();
		
		List<IExpressionSelector> selectors = builder.getSelectors();
		for(int i = 0; i < numCols; i++) {
			IExpressionSelector selector = selectors.get(i);

			// map to store the info
			Map<String, Object> headMap = new HashMap<String, Object>();

			// the name of the column is set by its expression
			String header = selector.getName();
			
			headMap.put("uri", header);
			headMap.put("varKey", header);
			headMap.put("type", headerTypes.get(selector.toString().toUpperCase()));
			headMap.put("vizType", vizTypes.get(i).replace("=", ""));
			
			// TODO push this on the selector to provide its type
			
			// based on type, fill in the information
			if(selector instanceof SqlColumnSelector || selector instanceof SqlConstantSelector) {
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
				List<IExpressionSelector> groupBys = builder.getGroupBySelectors();
				if(groupBys != null && !groupBys.isEmpty()) {
					List<String> groupByCleaned = new Vector<String>();
					for(IExpressionSelector gSelector : groupBys) {
						groupByCleaned.add(gSelector.getName());
					}
					operationMap.put("groupBy", groupByCleaned);
				}

				// get the columns used
				List<String> colsUsed = selector.getTableColumns();
				// this is super annoying...
				// TODO: need to come back to the interface and figure out how to return 
				// the names we want to show and not the view table names
//				if(frame.isJoined()) {
//					List<String> cleanColsUsed = new Vector<String>();
//					String tname = frame.getTableName();
//					for(String colUsed : colsUsed) {
//						// this is because we know the column is the original table it came from
//						// concat with underscore concat with the column name in the frame
//						cleanColsUsed.add(colUsed.replace(tname + "_", ""));
//					} 
//					operationMap.put("calculatedBy", cleanColsUsed);
//				} else {
					operationMap.put("calculatedBy", colsUsed);
//				}
				

				if(selector instanceof SqlMathSelector) {
					operationMap.put("math", ((SqlMathSelector) selector).getPkqlMath() );
				}
				if (headerTypes.containsKey(selector.getName().toUpperCase())) {
					headMap.put("type", headerTypes.get(selector.getName().toUpperCase()));
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
