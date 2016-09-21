package prerna.util.sql;

import java.util.ArrayList;
import java.util.List;

public class SQLServerQueryUtil extends SQLQueryUtil {	
	public static final String DATABASE_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
	private String connectionBase = "jdbc:sqlserver://HOST:PORT;databaseName=SCHEMA"; // localhost using SQL Server authentication, default port number is 1433 or use jdbc:sqlserver://127.0.0.1:1433;databaseName=TestDB;user=root;Password=root

	public SQLServerQueryUtil(){
		setDialect();
		super.setDefaultDbUserName("root");//
		super.setDefaultDbPassword("password");//
	}
	
	public SQLServerQueryUtil(String hostname, String port, String schema, String username, String password) {
		setDialect(schema);
		connectionBase = connectionBase.replace("HOST", hostname).replace("SCHEMA", schema);
		if(port != null && !port.isEmpty()) {
			connectionBase = connectionBase.replace(":PORT", ":" + port);
		} else {
			connectionBase = connectionBase.replace(":PORT", "");
		}
		super.setDefaultDbUserName(username);//
		super.setDefaultDbPassword(password);//
	}
	
	public SQLServerQueryUtil(String connectionURL, String username, String password) {
		setDialect();
		connectionBase = connectionURL;
		super.setDefaultDbUserName(username);
		super.setDefaultDbPassword(password);
	}
	
	private void setDialect() {
		super.setDialectAllTables(" SELECT * FROM INFORMATION_SCHEMA.TABLES ");//
		super.setDialectAllColumns(" SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ");//
		super.setResultAllTablesTableName("TABLE_NAME");//
		super.setResultAllColumnsColumnName("COLUMN_NAME");
		super.setResultAllColumnsColumnType("DATA_TYPE");
		super.setDialectAllIndexesInDB("SELECT DISTINCT NAME FROM SYS.INDEXES ORDER BY NAME");//
		super.setDialectOuterJoinLeft(" LEFT OUTER JOIN ");
		super.setDialectOuterJoinRight(" RIGHT OUTER JOIN ");
	}
	
	private void setDialect(String schema) {
		setDialect();
		super.setDialectAllTables("SELECT table_name FROM " + schema + ".information_schema.tables where table_type='BASE TABLE'"); //TODO: check if we want only base tables
		super.setDialectAllColumns(" SELECT COLUMN_NAME, DATA_TYPE FROM " + schema + ".INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ");
	}

	@Override
	public SQLQueryUtil.DB_TYPE getDatabaseType(){
		return SQLQueryUtil.DB_TYPE.SQL_Server;
	}
	public String getDialectAllColumns(String tableName){
		return super.getDialectAllColumns() + "'" + tableName + "'" ;
	}
	@Override
	public String getDialectAllIndexesInDB(String schema){
		return super.getDialectAllIndexesInDB(); //dont plop schema into here
	}
	//SQL Server - USE_OUTER_JOINS_NO
	@Override
	public String getDefaultOuterJoins(){
		return SQLQueryUtil.USE_OUTER_JOINS_TRUE;
	}

	//jdbc:sqlserver://localhost:1433;databaseName=dbname;user=username;Password=password;selectMethod=cursor
	@Override
	public String getConnectionURL(String baseFolder,String dbname){
		return connectionBase;
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
		//String query = super.dialectSelect + super.dialectDistinct;
		if(top!=-1){
			selectStr += " TOP " + top + " ";
		}

		String query = selectStr + selectors + "  FROM  " + leftOuterJoins + queryJoinsAndFilters; //+ " LIMIT " + limit ;
		if(groupBy !=null && groupBy.length()>0){
			query += " GROUP BY " + groupBy;
		}

		query += " UNION ";
		query += selectStr + selectors + "  FROM  " + rightOuterJoins + queryJoinsAndFilters; //+ " LIMIT " + limit ;

		if(groupBy !=null && groupBy.length()>0){
			query += " GROUP BY " + groupBy;
		}
		return query;

	}

//	@Override
	public String getDialectFullOuterJoinQuery1(boolean distinct,
			String selectors, ArrayList<String> rightJoinsArr,
			ArrayList<String> leftJoinsArr, ArrayList<String> joinsArr,
			String filters, int limit, String groupBy) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getEngineNameFromConnectionURL(String connectionURL) {
		String splitConnectionURL[] = connectionURL.split("=");
		String engineName[] = splitConnectionURL[1].split(";");
		return engineName[0];
	}
	
	
	/*@Override
	public String getDialectCreateDatabase(String engineName){
		return "CREATE DATABASE " + engineName +  " COLLATE Latin1_General_BIN2 "; 
		//originally I wanted to use Latin1_General_BIN2 which would allow case sensitive search, binary fastest for sort/ordering, ordering will sort upper case then lower case
		//BUT that meant that when querying tables we had to use the exact case select * from Title is not the same as select * from TITLE in sql server with this collation.
		//other collations considered also gave the same issue with table and column name sensitivity --Collate Latin1_General_CS_AS; --COLLATE Latin1_General_100_CS_AS_SC;
	}*/
}
