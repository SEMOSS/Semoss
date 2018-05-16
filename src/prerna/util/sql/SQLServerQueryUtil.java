package prerna.util.sql;

import java.util.List;

import prerna.util.Constants;
import prerna.util.DIHelper;

public class SQLServerQueryUtil extends SQLQueryUtil {	
	public static final String DATABASE_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
	private String connectionBase = "jdbc:sqlserver://localhost:"+DIHelper.getInstance().getProperty(Constants.SQL_Server_PORT); // 127.0.0.1 or localhost using SQL Server authentication, default port number is 1433 or use jdbc:sqlserver://127.0.0.1:1433;databaseName=TestDB;user=root;Password=root

	public SQLServerQueryUtil(){
		setDialect();
		connectionBase = "jdbc:sqlserver://localhost";
		super.setDefaultDbUserName("root");
		super.setDefaultDbPassword("root");
	}
	
	public SQLServerQueryUtil(String hostname, String port, String schema, String username, String password) {
		setDialect(schema);
		connectionBase = connectionBase + ";databaseName=SCHEMA";
		connectionBase = connectionBase.replace("localhost", hostname).replace("SCHEMA", schema);
		
		connectionBase = (port != null && !port.isEmpty()) ? connectionBase.replace(":"+DIHelper.getInstance().getProperty(Constants.SQL_Server_PORT), ":" + port) : connectionBase.replace(":"+DIHelper.getInstance().getProperty(Constants.SQL_Server_PORT), "");
		super.setDefaultDbUserName(username);
		super.setDefaultDbPassword(password);
	}
	
	public SQLServerQueryUtil(String connectionURL, String username, String password) {
		setDialect();
		connectionBase = connectionURL;
		super.setDefaultDbUserName(username);
		super.setDefaultDbPassword(password);
	}
	
	private void setDialect() {
		super.setDialectAllTables(" SELECT * FROM INFORMATION_SCHEMA.TABLES ");//
		super.setDialectAllColumns(" SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ");
		//super.setDialectAllColumns(" SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ");//
		super.setResultAllTablesTableName("TABLE_NAME");//
		super.setResultAllColumnsColumnName("COLUMN_NAME");
		super.setResultAllColumnsColumnType("DATA_TYPE");
		super.setDialectAllIndexesInDB("SELECT DISTINCT NAME FROM SYS.INDEXES ORDER BY NAME");//
		super.setDialectOuterJoinLeft(" LEFT OUTER JOIN ");
		super.setDialectOuterJoinRight(" RIGHT OUTER JOIN ");
	}
	
	private void setDialect(String schema) {
		setDialect();
		super.setDialectAllTables("SELECT table_name FROM [" + schema + "].information_schema.tables");
		super.setDialectAllColumns(" SELECT COLUMN_NAME, DATA_TYPE FROM [" + schema + "].INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ");
	}

	@Override
	public SQLQueryUtil.DB_TYPE getDatabaseType(){
		return SQLQueryUtil.DB_TYPE.SQL_SERVER;
	}
	public String getDialectAllColumns(String tableName){
		return super.getDialectAllColumns() + "'" + tableName + "'" ;
	}
	@Override
	public String getDialectAllIndexesInDB(String schema){
		return super.getDialectAllIndexesInDB(); //dont plop schema into here
	}

	//jdbc:sqlserver://127.0.0.1:1433;databaseName=dbname;user=username;Password=password;selectMethod=cursor
	@Override
	public String getConnectionURL(String baseFolder,String dbname){
		return (connectionBase.contains(";databaseName=") ? connectionBase : (connectionBase + ";databaseName=SCHEMA")).replace("SCHEMA", dbname);		
	}
	
	@Override
	public String getTempConnectionURL(){
		return connectionBase;
	}

	@Override
	public String getDatabaseDriverClassName(){
		return DATABASE_DRIVER;
	}

	@Override
	public String getDialectIndexInfo(String indexName, String dbName){
		return super.getDialectIndexInfo() + "'" + indexName+ "'"; 
	}

	public String getDialectAlterTableName(String fromName, String toName){
		return "sp_rename '" + fromName + "', '" + toName + "'"; //"sp_rename '" + fromName', '" + toName + "'"		
	}

	//For SQL server, TOP is used instead of LIMIT (last parameter)
	public String getDialectDistinctInnerJoinQuery(String selectors, String froms, String joins, int top){		
		String query = super.dialectSelect + super.dialectDistinct;
		if(top!=-1){
			query += "TOP " + top + " ";
		}
		query += selectors + "  FROM  " + froms + joins;		
		return query;
	}

	//"full outer join"
	//For SQL server, TOP is used instead of LIMIT (last parameter)
	public String getDialectFullOuterJoinQuery(boolean distinct, String selectors, List<String> rightJoinsArr, 
			List<String> leftJoinsArr, List<String> joinsArr, String filters, int top, String groupBy){

		String rightOuterJoins = "";
		String leftOuterJoins = "";
		String joins = "";

		if(rightJoinsArr.size() == leftJoinsArr.size() && (rightJoinsArr.size()!=0 && (rightJoinsArr.size()-1) <= joinsArr.size())){
			System.out.println("getDialectDistinctFullOuterJoinQuery: can continue");
		} else {
			System.out.println("getDialectDistinctFullOuterJoinQuery: cant continue");
		}

		for(int i = 0; i < rightJoinsArr.size(); i++){
			rightOuterJoins += rightJoinsArr.get(i);
			leftOuterJoins += leftJoinsArr.get(i);
			if(i!=0){
				rightOuterJoins += " ON " + joinsArr.get(i-1);
				leftOuterJoins += " ON " + joinsArr.get(i-1);
			}
		}
		// if the joinsArray still has more
		if(rightJoinsArr.size()!=0 && ((rightJoinsArr.size()-1) < joinsArr.size())){
			for(int i = rightJoinsArr.size()-1; i < joinsArr.size(); i++){
				if(joins.length()>0) joins += " AND ";
				joins += joinsArr.get(i);
			}
		}

		String queryJoinsAndFilters = "";
		if(joins.length() > 0 && filters.length() > 0)
			queryJoinsAndFilters = joins + " AND " + filters;
		else if(joins.length() > 0){
			queryJoinsAndFilters = joins;
		}
		else if(filters.length() > 0){
			queryJoinsAndFilters = filters;
		}
		if(queryJoinsAndFilters.length() > 0)
			queryJoinsAndFilters = " WHERE " + queryJoinsAndFilters;

		String selectStr = super.dialectSelect;
		if(distinct) selectStr+= super.dialectDistinct;

		if(top!=-1){
			selectStr += " TOP " + top + " ";
		}

		String query = selectStr + selectors + "  FROM  " + leftOuterJoins + queryJoinsAndFilters;
		if(groupBy !=null && groupBy.length()>0){
			query += " GROUP BY " + groupBy;
		}

		query += " UNION ";
		query += selectStr + selectors + "  FROM  " + rightOuterJoins + queryJoinsAndFilters;

		if(groupBy !=null && groupBy.length()>0){
			query += " GROUP BY " + groupBy;
		}
		return query;

	}

	@Override
	public String addLimitToQuery(String query, int limit) {
		if(query.contains("DISTINCT ")) {
			return query.replace("SELECT DISTINCT ", "SELECT DISTINCT TOP " + limit + " ");
		} else {
			return query.replace("SELECT ", "SELECT TOP " + limit + " ");
		}
	}
}
