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

		this.sqlScript = generateSqlScript(this.frame, this.expression, this.newColumnName, joinCols, groupColumns);
		LOGGER.info("GENERATED SQL EXPRESSION SCRIPT : " + this.sqlScript);
	}
	
	@Override
	public void generateExpression() {
		this.sqlScript = generateSqlScript(this.frame, this.expression, this.newColumnName, joinCols, groupColumns);
	}

	private String generateSqlScript(H2Frame frame2, String sqlExpression, String newColumnName, String[] joinCols, String[] groupColumns) {
		// this will generate the script
		// but also keep track of the columns 
		// can't use the rsmd since it will always return
		// in upper case :(

		boolean hasReturn = false;
		StringBuilder builder = new StringBuilder("SELECT DISTINCT ");
		if(sqlExpression != null && !sqlExpression.isEmpty()) { 
			if(newColumnName != null && !newColumnName.isEmpty()) {
				builder.append("(").append(sqlExpression).append(") AS ").append(newColumnName);
			} else {
				builder.append(sqlExpression);
			}
			hasReturn = true;
		}
		// due to tracking of selectors, we need this set to keep track of order
		Set<String> totalSelectors = new LinkedHashSet<String>();
		if(joinCols != null) {
			for(int i = 0; i < joinCols.length; i++) {
				totalSelectors.add(frame.getTableColumnName(joinCols[i]));
			}
		}
		if(groupColumns != null) {
			for(int i = 0; i < groupColumns.length; i++) {
				totalSelectors.add(frame.getTableColumnName(groupColumns[i]));
			}
		}
		Iterator<String> returnHeaders = totalSelectors.iterator();
		if(!hasReturn) {
			if(returnHeaders.hasNext()) {
				builder.append(returnHeaders.next());
			}
		}
		while(returnHeaders.hasNext()) {
			builder.append(" , ").append(returnHeaders.next());
		}
		
		if(frame.isJoined()) {
			builder.append(" FROM ").append(frame.getViewTableName());
		} else {
			builder.append(" FROM ").append(frame.getTableName());
		}
		
		String filters = frame.getSqlFilter();
		if(filters != null && !filters.isEmpty()) {
			builder.append(" ").append(filters);
		}
		
		if(groupColumns != null && groupColumns.length > 0) {
			StringBuilder groupBuilder = new StringBuilder();
			for(String groupBy : groupColumns) {
				if(groupBuilder.length() == 0) {
					groupBuilder.append(frame.getTableColumnName(groupBy));
				} else {
					groupBuilder.append(" , ").append(frame.getTableColumnName(groupBy));
				}
			}
			builder.append(" GROUP BY ").append(groupBuilder.toString());
		}

		this.numCols = totalSelectors.size()+1;
		this.headers = new String[numCols];
		
		// if there is an alias, use it
		// otherwise, just use the expression
		if(newColumnName != null) {
			this.headers[0] = newColumnName;
		} else {
			this.headers[0] = sqlExpression;
		}
		int counter = 1;
		for(String selector : totalSelectors) {
			headers[counter] = selector;
			counter++;
		}

		return builder.toString();
	}

	@Override
	public void runExpression() {
		if(this.sqlScript == null) {
			generateExpression();
		}
		rs = frame.execQuery(sqlScript);
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
