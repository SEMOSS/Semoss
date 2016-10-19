package prerna.sablecc;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import prerna.ds.H2.H2Frame;
import prerna.engine.api.IExpressionIterator;

public class H2SqlExpressionIterator implements IExpressionIterator {

	private ResultSet rs;
	
	private int numCols = 0;
	private String[] columnsToGet;
	private String[] joinCols;
	
	private String sqlExpression;
	private String aliasForScript;
	
	// iterator to wrap the result set from an expression
	// so we do not need to hold additional information
	// in memory and can grab each row as needed
	public H2SqlExpressionIterator(H2Frame frame, String sqlExpression, String newCol, String[] joinCols) {
		this.sqlExpression = sqlExpression;
		this.joinCols = joinCols;
		this.aliasForScript = newCol;
		
		String sqlScript = generateSqlScript(sqlExpression, newCol, joinCols, frame.getTableName(), frame.getSqlFilter());
		rs = frame.execQuery(sqlScript);
		processMetadata();
	}
	
	/**
	 * Generate the appropriate sql script for execution
	 * @param sqlExpression			The expression to process
	 * @param newCol				The alias to assign the expression
	 * @param joinCols				The join columns to also return
	 * @param tableName				The table name to execute on
	 * @return
	 */
	private String generateSqlScript(String sqlExpression, String newCol, String[] joinCols, String tableName, String filters) {
		StringBuilder builder = new StringBuilder("SELECT DISTINCT ");
		builder.append("(").append(sqlExpression).append(") AS ").append(newCol);
		for(int i = 0; i < joinCols.length; i++) {
			builder.append(" , ").append(joinCols[i]);
		}
		builder.append(" FROM ").append(tableName);
		if(filters != null && !filters.isEmpty()) {
			builder.append(" ").append(filters);
		}
		
		return builder.toString();
	}

	private void processMetadata() {
		ResultSetMetaData rsmd;
		try {
			rsmd = rs.getMetaData();
			this.numCols = rsmd.getColumnCount();
			columnsToGet = new String[numCols];
			for(int i = 0; i < numCols; i++) {
				columnsToGet[i] = rsmd.getColumnName(i+1);
			}			
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public String getAliasForScript() {
		return this.aliasForScript;
	}
	
	@Override
	public boolean hasNext() {
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
	public String[] getHeaders() {
		return this.columnsToGet;
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
	public String[] getJoinColumns() {
		return this.joinCols;
	}

	@Override
	public String toString() {
		return this.sqlExpression;
	}

}
