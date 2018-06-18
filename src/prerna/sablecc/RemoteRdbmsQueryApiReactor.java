package prerna.sablecc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import prerna.rdf.engine.wrappers.RawRDBMSSelectWrapper;
import prerna.sablecc.meta.IPkqlMetadata;

public class RemoteRdbmsQueryApiReactor extends AbstractReactor {

	public static final String QUERY_KEY = "QUERY";
	public static final String DB_DRIVER_KEY = "dbDriver";
	public static final String CONNECTION_STRING_KEY = "connectionString";
	public static final String USERNAME_KEY = "userName";
	public static final String PASSWORD_KEY = "password";

	//TODO: need this and connect to existing constants to be in a shared location
	private static final String ASTER = "ASTER_DB";
	private static final String ASTER_DRIVER = "com.asterdata.ncluster.jdbc.core.NClusterJDBCDriver";
	private static final String CASSANDRA = "CASSANDRA";
	private static final String CASSANDRA_DRIVER = "com.github.adejanovski.cassandra.jdbc.CassandraDriver";
	private static final String DB2 = "DB2";
	private static final String DB2_DRIVER = "com.ibm.db2.jcc.DB2Driver";
	private static final String DERBY = "DERBY";
	private static final String DERBY_DRIVER = "org.apache.derby.jdbc.EmbeddedDriver";
	private static final String H2 = "H2_DB";
	private static final String H2_DRIVER = "org.h2.Driver";
	private static final String IMPALA = "IMPALA";
	private static final String IMPALA_DRIVER = "com.cloudera.impala.jdbc3.Driver";
	private static final String MARIADB = "MARIA_DB";
	private static final String MARIADB_DRIVER = "org.mariadb.jdbc.Driver";
	private static final String MYSQL = "MYSQL";
	private static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
	private static final String ORACLE = "ORACLE";
	private static final String ORACLE_DRIVER = "oracle.jdbc.driver.OracleDriver";
	private static final String PHOENIX = "PHOENIX";
	private static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
	private static final String POSTGRES = "POSTGRES";
	private static final String POSTGRES_DRIVER = "org.postgresql.Driver";
	private static final String SAP_HANA = "SAP_HANA";
	private static final String SAP_HANA_DRIVER = "com.sap.db.jdbc.Driver";
	private static final String SQLSERVER = "SQL_SERVER";
	private static final String SQLSERVER_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
	private static final String TERADATA = "TERADATA";
	private static final String TERADATA_DRIVER = "com.teradata.jdbc.TeraDriver";

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
			if(driver.equalsIgnoreCase(ASTER)) {
				Class.forName(ASTER_DRIVER);
			} else if(driver.equalsIgnoreCase(CASSANDRA)) {
				Class.forName(CASSANDRA_DRIVER);
			} else if(driver.equalsIgnoreCase(DB2)) {
				Class.forName(DB2_DRIVER);
			} else if(driver.equalsIgnoreCase(DERBY)) {
				Class.forName(DERBY_DRIVER);
			} else if(driver.equalsIgnoreCase(H2)) {
				Class.forName(H2_DRIVER);
			} else if(driver.equalsIgnoreCase(IMPALA)) {
				Class.forName(IMPALA_DRIVER);
			} else if(driver.equalsIgnoreCase(MARIADB)) {
				Class.forName(MARIADB_DRIVER);
			} else if(driver.equalsIgnoreCase(MYSQL)) {
				Class.forName(MYSQL_DRIVER);
			} else if(driver.equalsIgnoreCase(ORACLE)) {
				Class.forName(ORACLE_DRIVER);
			} else if(driver.equalsIgnoreCase(PHOENIX)) {
				Class.forName(PHOENIX_DRIVER);
			} else if(driver.equalsIgnoreCase(POSTGRES)) {
				Class.forName(POSTGRES_DRIVER);
			} else if(driver.equalsIgnoreCase(SAP_HANA)) {
				Class.forName(SAP_HANA_DRIVER);
			} else if(driver.equalsIgnoreCase(SQLSERVER)) {
				Class.forName(SQLSERVER_DRIVER);
			} else if(driver.equalsIgnoreCase(TERADATA)) {
				Class.forName(TERADATA_DRIVER);
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
//		if(userName == null) {
//			throw new IllegalArgumentException("Need to define a username for the connection string");
//		}

		// get the password
		String password = (String) connectionInformation.get(PASSWORD_KEY);
//		if(password == null) {
//			throw new IllegalArgumentException("Need to define a passwrod for the connection string");
//		}

		// get the query
		String query = (String) myStore.get(QUERY_KEY);
		if(query == null) {
			throw new IllegalArgumentException("Need to define a query to execute");
		}

		// create the iterator
		Connection con;
		try {
			if(userName == null || password == null) {
				con = DriverManager.getConnection(connectionUrl);
			} else {
				con = DriverManager.getConnection(connectionUrl, userName, password);
			}
		} catch (SQLException e) {
			e.printStackTrace();
			throw new IllegalArgumentException(e.getMessage());
		}
		
		RawRDBMSSelectWrapper it = new RawRDBMSSelectWrapper();
		try {
			it.setCloseConenctionAfterExecution(true);
			it.directExecutionViaConnection(con, query, true);
		} catch (Exception e) {
			e.printStackTrace();
			throw new IllegalArgumentException(e.getMessage());
		}
		// determine the edge hash
		// create a prim key
		String[] headers = it.getHeaders();
//		Map<String, Set<String>> edgeHash = TinkerMetaHelper.createPrimKeyEdgeHash(headers);
//		this.put("EDGE_HASH", edgeHash);

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