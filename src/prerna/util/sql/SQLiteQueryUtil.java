package prerna.util.sql;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.sqlite.Function;

import prerna.algorithm.api.SemossDataType;
import prerna.sablecc2.om.Join;

public class SQLiteQueryUtil extends AnsiSqlQueryUtil {

	SQLiteQueryUtil() {
		super();
	}
	
	SQLiteQueryUtil(String connectionUrl, String username, String password) {
		super(connectionUrl, username, password);
	}
	
	SQLiteQueryUtil(RdbmsTypeEnum dbType, String hostname, String port, String schema, String username, String password) {
		super(dbType, hostname, port, schema, username, password);
	}
	
	@Override
	public void enhanceConnection(Connection con) {
		try {
			try {
				Function.create(con, "REGEXP", new Function() {
					@Override
					protected void xFunc() throws SQLException {
						String value = value_text(0);
				        String expression = value_text(1);
				        String caseInsensitive = value_text(2);
				        if (value == null) {
				            value = "";
				        }

				        Pattern pattern = null;
				        if(caseInsensitive != null && caseInsensitive.equals("i")) {
				        	pattern = Pattern.compile(expression, Pattern.CASE_INSENSITIVE);
				        } else {
				        	pattern = Pattern.compile(expression);
				        }
				        result(pattern.matcher(value).find() ? 1 : 0);
					}
				});
			} catch (SQLException e) {
				e.printStackTrace();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public String getRegexLikeFunctionSyntax() {
		return "REGEXP";
	}
	
	/////////////////////////////////////////////////////////////////////////////////////
	
	/**
	 * Modification from default as SQLite doesn't do CREATE TABLE AS ( )
	 * And doesn't require the additional ( )
	 */
	public String createNewTableFromJoiningTables(
			String returnTableName, 
			String leftTableName, 
			Map<String, SemossDataType> leftTableTypes,
			String rightTableName, 
			Map<String, SemossDataType> rightTableTypes, 
			List<Join> joins,
			Map<String, String> leftTableAlias,
			Map<String, String> rightTableAlias) 
	{
		final String LEFT_TABLE_ALIAS = "A";
		final String RIGHT_TABLE_ALIAS = "B";
		
		// 1) get the join portion of the sql syntax
		
		// keep a list of the right table join cols
		// so we know not to include them in the new table
		Set<String> rightTableJoinCols = new HashSet<String>();
		
		StringBuilder joinString = new StringBuilder();
		int numJoins = joins.size();
		for(int jIdx = 0; jIdx < numJoins; jIdx++) {
			Join j = joins.get(jIdx);
			String leftTableJoinCol = j.getSelector();
			if(leftTableJoinCol.contains("__")) {
				leftTableJoinCol = leftTableJoinCol.split("__")[1];
			}
			String rightTableJoinCol = j.getQualifier();
			if(rightTableJoinCol.contains("__")) {
				rightTableJoinCol = rightTableJoinCol.split("__")[1];
			}
			
			// keep track of join columns on the right table
			rightTableJoinCols.add(rightTableJoinCol.toUpperCase());
			
			String joinType = j.getJoinType();
			String joinSql = null;
			if(joinType.equalsIgnoreCase("inner.join")) {
				joinSql = "INNER JOIN";
			} else if(joinType.equalsIgnoreCase("left.outer.join")) {
				joinSql = "LEFT OUTER JOIN";
			} else if(joinType.equalsIgnoreCase("right.outer.join")) {
				joinSql = "RIGHT OUTER JOIN";
			} else if(joinType.equalsIgnoreCase("outer.join")) {
				joinSql = "FULL OUTER JOIN";
			} else {
				joinSql = "INNER JOIN";
			}
			
			if(jIdx != 0) {
				joinString.append(" AND ");
			} else {
				joinString.append(joinSql).append(" ").append(rightTableName)
							.append(" AS ").append(RIGHT_TABLE_ALIAS)
							.append(" ON (");
			}
			
			// need to make sure the data types are good to go
			SemossDataType leftColType = leftTableTypes.get(leftTableName + "__" + leftTableJoinCol);
			// the right column types are not tablename__colname...
			SemossDataType rightColType = rightTableTypes.get(rightTableJoinCol);
			
			if(leftColType == rightColType) {
				joinString.append(" ")
						.append(LEFT_TABLE_ALIAS).append(".").append(leftTableJoinCol)
						.append(" = ")
						.append(RIGHT_TABLE_ALIAS).append(".").append(rightTableJoinCol);
			} else {
				if( (leftColType == SemossDataType.INT || leftColType == SemossDataType.DOUBLE)  && rightColType == SemossDataType.STRING) {
					// one is a number
					// other is a string
					// convert the string to a number
					joinString.append(" ")
						.append(LEFT_TABLE_ALIAS).append(".").append(leftTableJoinCol)
						.append(" = CAST(")
						.append(RIGHT_TABLE_ALIAS).append(".").append(rightTableJoinCol)
						.append(" AS DOUBLE)");
				} else if( (rightColType == SemossDataType.INT || rightColType == SemossDataType.DOUBLE ) && leftColType == SemossDataType.STRING) {
					// one is a number
					// other is a string
					// convert the string to a number
					joinString.append(" CAST(")
						.append(LEFT_TABLE_ALIAS).append(".").append(leftTableJoinCol)
						.append(" AS DOUBLE) =")
						.append(RIGHT_TABLE_ALIAS).append(".").append(rightTableJoinCol);
				} else {
					// not sure... just make everything a string
					joinString.append(" CAST(")
					.append(LEFT_TABLE_ALIAS).append(".").append(leftTableJoinCol)
					.append(" AS VARCHAR(800)) = CAST(")
					.append(RIGHT_TABLE_ALIAS).append(".").append(rightTableJoinCol)
					.append(" AS VARCHAR(800))");
				}
			}
		}
		joinString.append(")");
		
		// 2) get the create table and the selector portions
		Set<String> leftTableHeaders = leftTableTypes.keySet();
		Set<String> rightTableHeaders = rightTableTypes.keySet();
		StringBuilder sql = new StringBuilder();
		sql.append("CREATE TABLE ").append(returnTableName).append(" AS SELECT ");
		
		// select all the columns from the left side
		int counter = 0;
		int size = leftTableHeaders.size();
		for(String leftTableCol : leftTableHeaders) {
			if(leftTableCol.contains("__")) {
				leftTableCol = leftTableCol.split("__")[1];
			}
			sql.append(LEFT_TABLE_ALIAS).append(".").append(leftTableCol);
			// add the alias if there
			if(leftTableAlias.containsKey(leftTableCol)) {
				sql.append(" AS ").append(leftTableAlias.get(leftTableCol));
			}
			if(counter + 1 < size) {
				sql.append(", ");
			}
			counter++;
		}
		
		// select the columns from the right side which are not part of the join!!!
		for(String rightTableCol : rightTableHeaders) {
			if(rightTableCol.contains("__")) {
				rightTableCol = rightTableCol.split("__")[1];
			}
			if(rightTableJoinCols.contains(rightTableCol.toUpperCase())) {
				counter++;
				continue;
			}
			sql.append(", ").append(RIGHT_TABLE_ALIAS).append(".").append(rightTableCol);
			// add the alias if there
			if(rightTableAlias.containsKey(rightTableCol)) {
				sql.append(" AS ").append(rightTableAlias.get(rightTableCol));
			}
			counter++;
		}
		
		// 3) combine everything
		
		sql.append(" FROM ").append(leftTableName).append(" AS ").append(LEFT_TABLE_ALIAS).append(" ")
				.append(joinString.toString());

		return sql.toString();
	}
	
	/////////////////////////////////////////////////////////////////////////////////////
	
	@Override
	public boolean allowArrayDatatype() {
		return false;
	}

	@Override
	public boolean allowRedefineColumn() {
		return false;
	}
	
	@Override
	public boolean allowDropColumn() {
		return false;
	}
	
	@Override
	public boolean allowIfExistsModifyColumnSyntax() {
		return false;
	}
	
	@Override
	public boolean allowMultiAddColumn() {
		return false;
	}
	
	/////////////////////////////////////////////////////////////////////////////////////
	
	@Override
	public String dropIndex(String indexName, String tableName) {
		return "DROP INDEX " + indexName;
	}
	
	@Override
	public String dropIndexIfExists(String indexName, String tableName) {
		// sqlite allows this for some reason...
		return "DROP INDEX IF EXISTS " + indexName;
	}
	
	/////////////////////////////////////////////////////////////////////////////////////

	/*
	 * Query database scripts
	 */
		
	@Override
	public String tableExistsQuery(String tableName, String schema) {
		// do not need to use the schema
		return "SELECT NAME, TYPE FROM SQLITE_MASTER WHERE TYPE='table' AND NAME='" + tableName + "';";
	}
	
	@Override
	public String getAllColumnDetails(String tableName, String schema) {
		// do not need to use the schema
		return "SELECT NAME, TYPE FROM PRAGMA_TABLE_INFO('" + tableName + "');";
	}
	
	@Override
	public String columnDetailsQuery(String tableName, String columnName, String schema) {
		// do not need to use the schema
		// the column name appears to always be stored in lower case...
		return "SELECT NAME, TYPE FROM PRAGMA_TABLE_INFO('" + tableName + "') WHERE NAME='" + columnName.toLowerCase() + "';";
	}
	
	@Override
	public String getIndexList(String schema) {
		// do not need to use the schema
		return "SELECT DISTINCT NAME, TBL_NAME FROM SQLITE_MASTER WHERE TYPE='index';";
	}

	@Override
	public String getIndexDetails(String indexName, String tableName, String schema) {
		//TODO: MAHER COME BACK TO GETTING THIS COLUMN
		//TODO: MAHER COME BACK TO GETTING THIS COLUMN
		//TODO: MAHER COME BACK TO GETTING THIS COLUMN
		//TODO: MAHER COME BACK TO GETTING THIS COLUMN
		return "SELECT DISTINCT TBL_NAME, null AS COLUMN FROM SQLITE_MASTER WHERE TYPE='index' AND NAME='" + indexName + "' AND TBL_NAME='" + tableName + "';";
	}
	
	@Override
	public String allIndexForTableQuery(String tableName, String schema) {
		//TODO: MAHER COME BACK TO GETTING THIS COLUMN
		//TODO: MAHER COME BACK TO GETTING THIS COLUMN
		//TODO: MAHER COME BACK TO GETTING THIS COLUMN
		//TODO: MAHER COME BACK TO GETTING THIS COLUMN

		// do not need to use the schema
		// sadly, sqlite does not provide the columns for the index
		return "SELECT NAME, null AS COLUMN FROM SQLITE_MASTER WHERE TYPE='index' AND TBL_NAME='" + tableName + "';";
	}
}
