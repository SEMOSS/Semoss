package prerna.sablecc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import prerna.ds.TinkerMetaHelper;
import prerna.rdf.engine.wrappers.RawRDBMSSelectWrapper;
import prerna.sablecc.meta.IPkqlMetadata;

public class RemoteRdbmsQueryApiReactor extends AbstractReactor {

	public static final String QUERY_KEY = "QUERY";
	public static final String DB_DRIVER_KEY = "dbDriver";
	public static final String CONNECTION_STRING_KEY = "connectionString";
	public static final String USERNAME_KEY = "userName";
	public static final String PASSWORD_KEY = "password";

	//TODO: need this and connect to existing constants to be in a shared location
	private static final String MYSQL = "MySQL";
	private static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
	private static final String ORACLE = "Oracle";
	private static final String ORACLE_DRIVER = "oracle.jdbc.driver.OracleDriver";
	private static final String SQLSERVER = "SQL_Server";
	private static final String SQLSERVER_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";

	public RemoteRdbmsQueryApiReactor() {
		String [] thisReacts = {PKQLEnum.MAP_OBJ};
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.REMOTE_RDBMS_QUERY_API;
	}

	@Override
	public Iterator process() {
		// grab the connection info
		Map<Object, Object> connectionInformation = (Map<Object, Object>) myStore.get(PKQLEnum.MAP_OBJ);
		
		// get the driver and load it
		String driver = (String) connectionInformation.get(DB_DRIVER_KEY);
		if(driver == null) {
			throw new IllegalArgumentException("Need to define a proper driver");
		}

		try {
			if(driver.equalsIgnoreCase(MYSQL)) {
				Class.forName(MYSQL_DRIVER);
			} else if(driver.equalsIgnoreCase(ORACLE)) {
				Class.forName(ORACLE_DRIVER);
			} else if(driver.equalsIgnoreCase(SQLSERVER)) {
				Class.forName(SQLSERVER_DRIVER);
			} else {
				throw new IllegalArgumentException("Invalid driver");
			}
		} catch(ClassNotFoundException e) {
			e.printStackTrace();
			throw new IllegalArgumentException("Unable to find driver for engine type");
		}
		
		// get the connection url
		// should contain all the necessary information
		String connectionUrl = (String) connectionInformation.get(CONNECTION_STRING_KEY);
		if(connectionUrl == null) {
			throw new IllegalArgumentException("Need to define a connection string");
		}

		// get the username
		String userName = (String) connectionInformation.get(USERNAME_KEY);
		if(userName == null) {
			throw new IllegalArgumentException("Need to define a username for the connection string");
		}

		// get the password
		String password = (String) connectionInformation.get(PASSWORD_KEY);
		if(password == null) {
			throw new IllegalArgumentException("Need to define a passwrod for the connection string");
		}

		// get the query
		String query = (String) myStore.get(QUERY_KEY);
		if(query == null) {
			throw new IllegalArgumentException("Need to define a query to execute");
		}

		// create the iterator
		Connection con;
		try {
			con = DriverManager.getConnection(connectionUrl, userName, password);
		} catch (SQLException e) {
			e.printStackTrace();
			throw new IllegalArgumentException(e.getMessage());
		}
		
		RawRDBMSSelectWrapper it = new RawRDBMSSelectWrapper();
		try {
			it.setCloseConenctionAfterExecution(true);
			it.directExecutionViaConnection(con, query);
		} catch (Exception e) {
			e.printStackTrace();
			throw new IllegalArgumentException(e.getMessage());
		}
		// determine the edge hash
		// create a prim key
		String[] headers = it.getDisplayVariables();
		Map<String, Set<String>> edgeHash = TinkerMetaHelper.createPrimKeyEdgeHash(headers);
		this.put("EDGE_HASH", edgeHash);

		try {
			ResultSetMetaData meta = it.getMetaData();
			int numCols = meta.getColumnCount();

			Map<String, String> dataTypeMap = new HashMap<String, String>();
			Map<String, String> logicalToValueMap = new HashMap<String, String>();
			for(int index = 0; index < numCols; index++) {
				dataTypeMap.put(meta.getColumnLabel(index+1), meta.getColumnTypeName(index+1));
				logicalToValueMap.put(meta.getColumnLabel(index+1), meta.getColumnLabel(index+1));
			}

			this.put("DATA_TYPE_MAP", dataTypeMap);
			this.put("LOGICAL_TO_VALUE", logicalToValueMap);
		} catch (SQLException e) {
			e.printStackTrace();
			throw new IllegalArgumentException("ERROR WITH EXECUTION OF SQL QUERY");
		}

		this.put((String) getValue(PKQLEnum.RAW_API), it);
		this.put("RESPONSE", "success");
		this.put("STATUS", PKQLRunner.STATUS.SUCCESS);

		return null;
	}

	@Override
	public IPkqlMetadata getPkqlMetadata() {
		// TODO Auto-generated method stub
		return null;
	}
}