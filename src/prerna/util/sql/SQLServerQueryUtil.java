package prerna.util.sql;

import prerna.util.Constants;
import prerna.util.DIHelper;

public class SQLServerQueryUtil extends SQLQueryUtil {	
	
	public static final String DATABASE_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
	private String connectionBase = "jdbc:sqlserver://localhost:"+DIHelper.getInstance().getProperty(Constants.SQL_Server_PORT); // 127.0.0.1 or localhost using SQL Server authentication, default port number is 1433 or use jdbc:sqlserver://127.0.0.1:1433;databaseName=TestDB;user=root;Password=root

	public SQLServerQueryUtil(){
		connectionBase = "jdbc:sqlserver://localhost";
		super.setDefaultDbUserName("root");
		super.setDefaultDbPassword("root");
		super.setDialectAllIndexesInDB("SELECT DISTINCT NAME FROM SYS.INDEXES ORDER BY NAME");//
	}
	
	public SQLServerQueryUtil(String hostname, String port, String schema, String username, String password) {
		connectionBase = connectionBase + ";databaseName=SCHEMA";
		connectionBase = connectionBase.replace("localhost", hostname).replace("SCHEMA", schema);
		
		connectionBase = (port != null && !port.isEmpty()) ? connectionBase.replace(":"+DIHelper.getInstance().getProperty(Constants.SQL_Server_PORT), ":" + port) : connectionBase.replace(":"+DIHelper.getInstance().getProperty(Constants.SQL_Server_PORT), "");
		super.setDefaultDbUserName(username);
		super.setDefaultDbPassword(password);
		super.setDialectAllIndexesInDB("SELECT DISTINCT NAME FROM SYS.INDEXES ORDER BY NAME");//
	}
	
	public SQLServerQueryUtil(String connectionURL, String username, String password) {
		connectionBase = connectionURL;
		super.setDefaultDbUserName(username);
		super.setDefaultDbPassword(password);
		super.setDialectAllIndexesInDB("SELECT DISTINCT NAME FROM SYS.INDEXES ORDER BY NAME");//
	}
	
	@Override
	public SQLQueryUtil.DB_TYPE getDatabaseType(){
		return SQLQueryUtil.DB_TYPE.SQL_SERVER;
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
	public String getDatabaseDriverClassName(){
		return DATABASE_DRIVER;
	}

	@Override
	public String getDialectIndexInfo(String indexName, String dbName){
		return super.getDialectIndexInfo() + "'" + indexName+ "'"; 
	}

	@Override
	public String getDialectAlterTableName(String fromName, String toName){
		return "sp_rename '" + fromName + "', '" + toName + "'"; //"sp_rename '" + fromName', '" + toName + "'"		
	}
}
