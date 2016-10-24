package prerna.sablecc.expressions.sql;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.H2.H2Frame;
import prerna.sablecc.expressions.AbstractExpressionIterator;

public class H2SqlExpressionIterator extends AbstractExpressionIterator {

	private static final Logger LOGGER = LogManager.getLogger(H2SqlExpressionIterator.class.getName());

	private H2Frame frame;
	private ResultSet rs;

	// This will hold the full sql expression to execute
	private String sqlScript;

	public H2SqlExpressionIterator() {
		
	}
	
	// iterator to wrap the result set from an expression
	// so we do not need to hold additional information
	// in memory and can grab each row as needed
	public H2SqlExpressionIterator(H2Frame frame, String sqlExpression, String newColumnName, String[] joinCols, String[] groupColumns) {
		this.frame = frame;
		this.expression = sqlExpression;
		this.newColumnName = newColumnName;
		this.joinCols = joinCols;
		this.groupColumns = groupColumns;

		setHeaders();
//		this.sqlScript = generateSqlScript(sqlExpression, newColumnName, joinCols, frame.getViewTableName(), frame.getSqlFilter(), groupColumns);
		LOGGER.info("GENERATED SQL EXPRESSION SCRIPT : " + this.sqlScript);
	}
	
	private void setHeaders() {
		Set<String> totalSelectors = new LinkedHashSet<>();
		
		if(joinCols != null) {
			for(int i = 0; i < joinCols.length; i++) {
				totalSelectors.add(joinCols[i]);
			}
		}
		if(groupColumns != null) {
			for(int i = 0; i < groupColumns.length; i++) {
				totalSelectors.add(groupColumns[i]);
			}
		}
		
		this.numCols = totalSelectors.size()+1;
		this.headers = new String[numCols];
		this.headers[0] = newColumnName;
		int counter = 1;
		for(String selector : totalSelectors) {
			headers[counter] = selector;
			counter++;
		}
		
	}
	
	@Override
	public void generateExpression() {
		String[] translatedJoinCols = null;
		if(joinCols != null) {
			translatedJoinCols = new String[joinCols.length];
			for(int i = 0; i < joinCols.length; i++) {
				translatedJoinCols[i] = frame.getTableColumnName(joinCols[i]);
			}
		}
		
		String[] translatedGroupColumns = null;
		if(groupColumns != null) {
			translatedGroupColumns = new String[groupColumns.length];
			for(int i = 0; i < groupColumns.length; i++) {
				translatedGroupColumns[i] = frame.getTableColumnName(groupColumns[i]);
			}
		}
		this.sqlScript = generateSqlScript(this.expression, this.newColumnName, translatedJoinCols, frame.getViewTableName(), frame.getSqlFilter(), translatedGroupColumns);
	}

	@Override
	public void runExpression() {
		if(this.sqlScript == null) {
			generateExpression();
		}
		rs = frame.execQuery(sqlScript);
	}

	/**
	 * Generate the appropriate sql script for execution
	 * @param sqlExpression			The expression to process
	 * @param newCol				The alias to assign the expression
	 * @param joinCols				The join columns to also return
	 * @param tableName				The table name to execute on
	 * @return
	 */
	private String generateSqlScript(String sqlExpression, String newCol, String[] joinCols, String tableName, String filters, String[] groupColumns) {
		// this will generate the script
		// but also keep track of the columns 
		// can't use the rsmd since it will always return
		// in upper case :(

		StringBuilder builder = new StringBuilder("SELECT DISTINCT ");
		builder.append("(").append(sqlExpression).append(") AS ").append(newCol);
		// due to tracking of selectors, we need this set to keep track of order
		Set<String> totalSelectors = new LinkedHashSet<String>();
		if(joinCols != null) {
			for(int i = 0; i < joinCols.length; i++) {
				totalSelectors.add(joinCols[i]);
			}
		}
		if(groupColumns != null) {
			for(int i = 0; i < groupColumns.length; i++) {
				totalSelectors.add(groupColumns[i]);
			}
		}
		Iterator<String> returnHeaders = totalSelectors.iterator();
		while(returnHeaders.hasNext()) {
			builder.append(" , ").append(returnHeaders.next());
		}

		builder.append(" FROM ").append(tableName);
		if(filters != null && !filters.isEmpty()) {
			builder.append(" ").append(filters);
		}
		if(groupColumns != null && groupColumns.length > 0) {
			StringBuilder groupBuilder = new StringBuilder();
			for(String groupBy : groupColumns) {
				if(groupBuilder.length() == 0) {
					groupBuilder.append(groupBy);
				} else {
					groupBuilder.append(" , ").append(groupBy);
				}
			}
			builder.append(" GROUP BY ").append(groupBuilder.toString());
		}

		this.numCols = totalSelectors.size()+1;
//		this.headers = new String[numCols];
//		this.headers[0] = newCol;
//		int counter = 1;
//		for(String selector : totalSelectors) {
//			headers[counter] = selector;
//			counter++;
//		}

		setHeaders();
		return builder.toString();
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
				rs.close();
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

	@Override
	public void close() {
		if(this.rs != null) {
			try {
				rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	@Override
	public void setFrame(ITableDataFrame frame) {
		this.frame = (H2Frame) frame;
	}
	
}
