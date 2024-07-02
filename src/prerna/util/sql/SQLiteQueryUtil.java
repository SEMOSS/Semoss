package prerna.util.sql;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sqlite.Function;
import org.sqlite.SQLiteConfig;
import org.sqlite.SQLiteConfig.Pragma;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.api.SemossDataType;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.impl.CaseInsensitiveProperties;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.interpreters.sql.SQLiteSqlInterpreter;
import prerna.sablecc2.om.Join;
import prerna.util.Constants;

public class SQLiteQueryUtil extends AnsiSqlQueryUtil {
	
	private static final Logger classLogger = LogManager.getLogger(SQLiteQueryUtil.class);

	SQLiteQueryUtil() {
		super();
		setDbType(RdbmsTypeEnum.SQLITE);
	}
	
	SQLiteQueryUtil(String connectionUrl, String username, String password) {
		super(connectionUrl, username, password);
		setDbType(RdbmsTypeEnum.SQLITE);
	}
	
	@Override
	public IQueryInterpreter getInterpreter(IDatabaseEngine engine) {
		return new SQLiteSqlInterpreter(engine);
	}

	@Override
	public IQueryInterpreter getInterpreter(ITableDataFrame frame) {
		return new SQLiteSqlInterpreter(frame);
	}

	@Override
	public String setConnectionDetailsfromMap(Map<String, Object> configMap) throws RuntimeException {
		if(configMap == null || configMap.isEmpty()){
			throw new RuntimeException("Configuration map is null or empty");
		}
		
		this.connectionUrl = (String) configMap.get(AbstractSqlQueryUtil.CONNECTION_URL);
		
		this.hostname = (String) configMap.get(AbstractSqlQueryUtil.HOSTNAME);
		if((this.connectionUrl == null || this.connectionUrl.isEmpty()) && 
				(hostname == null || hostname.isEmpty())
			) {
			throw new RuntimeException("Must pass in a hostname");
		}
		//... just in case
		if(this.hostname != null) {
			this.hostname = this.hostname.replace(".mv.db", "");
		}

		this.additionalProps = (String) configMap.get(AbstractSqlQueryUtil.ADDITIONAL);

		// do we need to make the connection url?
		if(this.connectionUrl == null || this.connectionUrl.isEmpty()) {
			this.connectionUrl = this.dbType.getUrlPrefix()+":"+this.hostname;
			
			if(this.additionalProps != null && !this.additionalProps.isEmpty()) {
				if(!this.additionalProps.startsWith(";") && !this.additionalProps.startsWith("&")) {
					this.connectionUrl += ";" + this.additionalProps;
				} else {
					this.connectionUrl += this.additionalProps;
				}
			}
		}
		
		return this.connectionUrl;
	}

	@Override
	public String setConnectionDetailsFromSMSS(CaseInsensitiveProperties prop) throws RuntimeException {
		if(prop == null || prop.isEmpty()){
			throw new RuntimeException("Properties object is null or empty");
		}
		
		this.connectionUrl = (String) prop.get(AbstractSqlQueryUtil.CONNECTION_URL);
		
		this.hostname = (String) prop.get(AbstractSqlQueryUtil.HOSTNAME);
		if((this.connectionUrl == null || this.connectionUrl.isEmpty()) && 
				(hostname == null || hostname.isEmpty())
			) {
			throw new RuntimeException("Must pass in a hostname");
		}
		//... just in case
		if(this.hostname != null) {
			this.hostname = this.hostname.replace(".mv.db", "");
		}
		
		this.additionalProps = (String) prop.get(AbstractSqlQueryUtil.ADDITIONAL);

		// do we need to make the connection url?
		if(this.connectionUrl == null || this.connectionUrl.isEmpty()) {
			this.connectionUrl = this.dbType.getUrlPrefix()+":"+this.hostname;
			
			if(this.additionalProps != null && !this.additionalProps.isEmpty()) {
				if(!this.additionalProps.startsWith(";") && !this.additionalProps.startsWith("&")) {
					this.connectionUrl += ";" + this.additionalProps;
				} else {
					this.connectionUrl += this.additionalProps;
				}
			}
		}
		
		return this.connectionUrl;
	}

	@Override
	public String buildConnectionString() {
		if(this.connectionUrl != null && !this.connectionUrl.isEmpty()) {
			return this.connectionUrl;
		}
		
		if(this.hostname == null || this.hostname.isEmpty()) {
			throw new RuntimeException("Must pass in a hostname");
		}
		
		this.connectionUrl = this.dbType.getUrlPrefix()+":"+this.hostname;
		
		if(this.additionalProps != null && !this.additionalProps.isEmpty()) {
			if(!this.additionalProps.startsWith(";") && !this.additionalProps.startsWith("&")) {
				this.connectionUrl += ";" + this.additionalProps;
			} else {
				this.connectionUrl += this.additionalProps;
			}
		}
		
		return this.connectionUrl;
	}
	
	@Override
	public void enhanceConnection(Connection con) {
		SQLiteConfig sqLiteConfig = new SQLiteConfig();
		Properties properties = sqLiteConfig.toProperties();
		properties.setProperty(Pragma.DATE_STRING_FORMAT.pragmaName, "yyyy-MM-dd HH:mm:ss");
		
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
				classLogger.error(Constants.STACKTRACE, e);
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
	}
	
	@Override
	public String getRegexLikeFunctionSyntax() {
		return "REGEXP";
	}
	
	@Override
	public String getDateFormatFunctionSyntax() {
		return "STRFTIME";
	}
	
	@Override
	public String getCurrentDate() {
		return "DATE('now')";
	}
	
	@Override
	public String getCurrentTimestamp() {
		return "DATETIME('now')";
	}
	
	@Override
	public String getDateAddFunctionSyntax(String timeUnit, int value, String dateToModify) {
		return "DATE(" + dateToModify + ", '" + value + " " + timeUnit + "')";
	}
	
	@Override
	public String getDateDiffFunctionSyntax(String timeUnit, String dateTimeField1, String dateTimeField2) {
		if(timeUnit.equalsIgnoreCase("day")) {
			return "(JulianDay("+dateTimeField1+") - JulianDay("+dateTimeField2+"))";
		} else if(timeUnit.equalsIgnoreCase("year")) {
			return "(date("+dateTimeField1+")-date("+dateTimeField2+"))";
		}
		
		double divider = 1;
		double multiplier = 1;
		if(timeUnit.equalsIgnoreCase("hour")) {
			multiplier = 24;
		} else if(timeUnit.equalsIgnoreCase("minute")) {
			multiplier = 24*60;
		} else if(timeUnit.equalsIgnoreCase("second")) {
			multiplier = 24*60*60;
		} else if(timeUnit.equals("weeks")) {
			divider = 7;
		} else if(timeUnit.equalsIgnoreCase("month")) {
			divider = 365/12;
		}
		
		if(divider > 1) {
			return "(JulianDay("+dateTimeField1+") - JulianDay("+dateTimeField2+"))/" + divider;
		} else {
			return "(JulianDay("+dateTimeField1+") - JulianDay("+dateTimeField2+"))*" + multiplier;
		}
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
			String leftTableJoinCol = j.getLColumn();
			if(leftTableJoinCol.contains("__")) {
				leftTableJoinCol = leftTableJoinCol.split("__")[1];
			}
			String rightTableJoinCol = j.getRColumn();
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
				if(leftColType == SemossDataType.DOUBLE && rightColType == SemossDataType.INT) {
					// left is double
					// right is int 
					// need to cast the right hand side
					joinString.append(" ")
						.append(LEFT_TABLE_ALIAS).append(".").append(leftTableJoinCol)
						.append(" = CAST(")
						.append(RIGHT_TABLE_ALIAS).append(".").append(rightTableJoinCol)
						.append(" AS DOUBLE)");
				} else if(leftColType == SemossDataType.INT && rightColType == SemossDataType.DOUBLE) {
					// left is int
					// right is double
					// need to cast the left hand side
					joinString.append(" ")
						.append("CAST(").append(LEFT_TABLE_ALIAS).append(".").append(leftTableJoinCol)
						.append(" AS DOUBLE) = ")
						.append(RIGHT_TABLE_ALIAS).append(".").append(rightTableJoinCol);
				} else if( (leftColType == SemossDataType.INT || leftColType == SemossDataType.DOUBLE)  && rightColType == SemossDataType.STRING) {
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
	public boolean allowBlobJavaObject() {
		return false;
	}
	
	@Override
	public void handleInsertionOfBlob(Connection conn, PreparedStatement statement, String object, int index) throws SQLException {
		if(object == null) {
			statement.setNull(index, java.sql.Types.BLOB);
		} else {
			statement.setString(index, object);
		}
	}
	
	@Override
	public String handleBlobRetrieval(ResultSet result, String key) throws SQLException, IOException {
		return result.getString(key);
	}
	
	@Override
	public String handleBlobRetrieval(ResultSet result, int index) throws SQLException, IOException {
		return result.getString(index);
	}
	
	@Override
	public boolean allowClobJavaObject() {
		return false;
	}

	@Override
	public boolean allowRedefineColumn() {
		return false;
	}
	
	@Override
	public boolean allowDropColumn() {
		return true;
	}
	
	@Override
	public boolean allowIfExistsModifyColumnSyntax() {
		return false;
	}
	
	@Override
	public boolean allowMultiAddColumn() {
		return false;
	}
	
	@Override
	public boolean allowMultiDropColumn() {
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
	public String tableExistsQuery(String tableName, String database, String schema) {
		// do not need to use the schema
		return "SELECT NAME, TYPE FROM SQLITE_MASTER WHERE TYPE='table' AND NAME='" + tableName + "';";
	}
	
	@Override
	public String getAllColumnDetails(String tableName, String database, String schema) {
		// do not need to use the schema
		return "SELECT NAME, TYPE FROM PRAGMA_TABLE_INFO('" + tableName + "');";
	}
	
	@Override
	public String columnDetailsQuery(String tableName, String columnName, String database, String schema) {
		// do not need to use the schema
		// the column name appears to always be stored in lower case...
		return "SELECT NAME, TYPE FROM PRAGMA_TABLE_INFO('" + tableName + "') WHERE NAME='" + columnName.toLowerCase() + "';";
	}
	
	@Override
	public String getIndexList(String database, String schema) {
		// do not need to use the schema
		return "SELECT DISTINCT NAME, TBL_NAME FROM SQLITE_MASTER WHERE TYPE='index';";
	}

	@Override
	public String getIndexDetails(String indexName, String tableName, String database, String schema) {
		//TODO: MAHER COME BACK TO GETTING THIS COLUMN
		//TODO: MAHER COME BACK TO GETTING THIS COLUMN
		//TODO: MAHER COME BACK TO GETTING THIS COLUMN
		//TODO: MAHER COME BACK TO GETTING THIS COLUMN
		return "SELECT DISTINCT TBL_NAME, null AS COLUMN FROM SQLITE_MASTER WHERE TYPE='index' AND NAME='" + indexName + "' AND TBL_NAME='" + tableName + "';";
	}
	
	@Override
	public String allIndexForTableQuery(String tableName, String database, String schema) {
		//TODO: MAHER COME BACK TO GETTING THIS COLUMN
		//TODO: MAHER COME BACK TO GETTING THIS COLUMN
		//TODO: MAHER COME BACK TO GETTING THIS COLUMN
		//TODO: MAHER COME BACK TO GETTING THIS COLUMN

		// do not need to use the schema
		// sadly, sqlite does not provide the columns for the index
		return "SELECT NAME, null AS COLUMN FROM SQLITE_MASTER WHERE TYPE='index' AND TBL_NAME='" + tableName + "';";
	}
	
	@Override
	public String modColumnName(String tableName, String curColName, String newColName) {
		if(isSelectorKeyword(tableName)) {
			tableName = getEscapeKeyword(tableName);
		}
		if(isSelectorKeyword(curColName)) {
			curColName = getEscapeKeyword(curColName);
		}
		if(isSelectorKeyword(newColName)) {
			newColName = getEscapeKeyword(newColName);
		}
		return "ALTER TABLE " + tableName + " RENAME COLUMN " + curColName + " TO " + newColName;
	}
}
