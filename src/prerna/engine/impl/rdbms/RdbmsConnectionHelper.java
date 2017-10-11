package prerna.engine.impl.rdbms;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class RdbmsConnectionHelper {
	
	public static final String ASTER = "ASTER_DB";
	public static final String ASTER_DRIVER = "com.asterdata.ncluster.jdbc.core.NClusterJDBCDriver";
	
	public static final String CASSANDRA = "CASSANDRA";
	public static final String CASSANDRA_DRIVER = "com.github.adejanovski.cassandra.jdbc.CassandraDriver";
	
	public static final String DB2 = "DB2";
	public static final String DB2_DRIVER = "com.ibm.db2.jcc.DB2Driver";
	
	public static final String DERBY = "DERBY";
	public static final String DERBY_DRIVER = "org.apache.derby.jdbc.EmbeddedDriver";
	
	public static final String H2 = "H2_DB";
	public static final String H2_DRIVER = "org.h2.Driver";
	
	public static final String IMPALA = "IMPALA";
	public static final String IMPALA_DRIVER = "com.cloudera.impala.jdbc4.Driver";
	
	public static final String MARIADB = "MARIA_DB";
	public static final String MARIADB_DRIVER = "org.mariadb.jdbc.Driver";
	
	public static final String MYSQL = "MYSQL";
	public static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
	
	public static final String ORACLE = "ORACLE";
	public static final String ORACLE_DRIVER = "oracle.jdbc.driver.OracleDriver";
	
	public static final String PHOENIX = "PHOENIX";
	public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
	
	public static final String POSTGRES = "POSTGRES";
	public static final String POSTGRES_DRIVER = "org.postgresql.Driver";
	
	public static final String SAP_HANA = "SAP_HANA";
	public static final String SAP_HANA_DRIVER = "com.sap.db.jdbc.Driver";
	
	public static final String SQLSERVER = "SQL_SERVER";
	public static final String SQLSERVER_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
	
	public static final String TERADATA = "TERADATA";
	public static final String TERADATA_DRIVER = "com.teradata.jdbc.TeraDriver";

	private RdbmsConnectionHelper() {
		
	}
	
	/**
	 * Method to get a connection to an existing RDBMS engine
	 * If the username or password are null, we will assume the information is already provided within the connectionUrl
	 * @param connectionUrl
	 * @param userName
	 * @param password
	 * @param driver
	 * @return
	 * @throws SQLException 
	 */
	public static Connection getConnection(String connectionUrl, String userName, String password, String driver) throws SQLException {
		driver = driver.toUpperCase();
		try {
			if (driver.equalsIgnoreCase(ASTER)) {
				Class.forName(ASTER_DRIVER);
			} else if (driver.equalsIgnoreCase(CASSANDRA)) {
				Class.forName(CASSANDRA_DRIVER);
			} else if (driver.equalsIgnoreCase(DB2)) {
				Class.forName(DB2_DRIVER);
			} else if (driver.equalsIgnoreCase(DERBY)) {
				Class.forName(DERBY_DRIVER);
			} else if (driver.equalsIgnoreCase(H2)) {
				Class.forName(H2_DRIVER);
			} else if (driver.equalsIgnoreCase(IMPALA)) {
				Class.forName(IMPALA_DRIVER);
			} else if (driver.equalsIgnoreCase(MARIADB)) {
				Class.forName(MARIADB_DRIVER);
			} else if (driver.equalsIgnoreCase(MYSQL)) {
				Class.forName(MYSQL_DRIVER);
			} else if (driver.equalsIgnoreCase(ORACLE)) {
				Class.forName(ORACLE_DRIVER);
			} else if (driver.equalsIgnoreCase(PHOENIX)) {
				Class.forName(PHOENIX_DRIVER);
			} else if (driver.equalsIgnoreCase(POSTGRES)) {
				Class.forName(POSTGRES_DRIVER);
			} else if (driver.equalsIgnoreCase(SAP_HANA)) {
				Class.forName(SAP_HANA_DRIVER);
			} else if (driver.equalsIgnoreCase(SQLSERVER)) {
				Class.forName(SQLSERVER_DRIVER);
			} else if (driver.equalsIgnoreCase(TERADATA)) {
				Class.forName(TERADATA_DRIVER);
			} else {
				throw new SQLException("Invalid driver");
			}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			throw new SQLException("Unable to find driver for engine type");
		}
		
		// create the iterator
		Connection conn;
		try {
			if (userName == null || password == null) {
				conn = DriverManager.getConnection(connectionUrl);
			} else {
				conn = DriverManager.getConnection(connectionUrl, userName, password);
			}
		} catch (SQLException e) {
			e.printStackTrace();
			throw new SQLException(e.getMessage());
		}
		
		return conn;
	}
	
	/**
	 * Try to construct the connection URL based on inputs
	 * @param driver
	 * @param host
	 * @param port
	 * @param userName
	 * @param password
	 * @param schema
	 * @param additonalProperties
	 * @return
	 * @throws SQLException 
	 */
	public static Connection buildConnection(String driver, String host, String port, String userName, String password, String schema, String additonalProperties) throws SQLException {
		String connectionUrl = "";
		driver = driver.toUpperCase();
		try {
			if (driver.equalsIgnoreCase(ASTER)) {
				Class.forName(ASTER_DRIVER);
				connectionUrl = "jdbc:ncluster://HOST:PORT/SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
				if (port != null && !port.isEmpty()) {
					connectionUrl = connectionUrl.replace(":PORT", ":" + port);
				} else {
					connectionUrl = connectionUrl.replace(":PORT", "");
				}
			} 
			
			else if (driver.equalsIgnoreCase(CASSANDRA)) {
				Class.forName(CASSANDRA_DRIVER);
				connectionUrl = "jdbc:cassandra://HOST:PORT/SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
				if (port != null && !port.isEmpty()) {
					connectionUrl = connectionUrl.replace(":PORT", ":" + port);
				} else {
					connectionUrl = connectionUrl.replace(":PORT", "");
				}
			} 
			
			else if (driver.equalsIgnoreCase(DB2)) {
				Class.forName(DB2_DRIVER);
				connectionUrl = "jdbc:db2://HOST:PORT/SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
				if (port != null && !port.isEmpty()) {
					connectionUrl = connectionUrl.replace(":PORT", ":" + port);
				} else {
					connectionUrl = connectionUrl.replace(":PORT", "");
				}
			} 
			
			else if (driver.equalsIgnoreCase(DERBY)) {
				Class.forName(DERBY_DRIVER);
				connectionUrl = "jdbc:derby://HOST:PORT/SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
				if (port != null && !port.isEmpty()) {
					connectionUrl = connectionUrl.replace(":PORT", ":" + port);
				} else {
					connectionUrl = connectionUrl.replace(":PORT", "");
				}
			}
			
			else if (driver.equalsIgnoreCase(H2)) {
				Class.forName(H2_DRIVER);
				connectionUrl = "jdbc:h2:tcp://HOST:PORT/SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
				if (port != null && !port.isEmpty()) {
					connectionUrl = connectionUrl.replace(":PORT", ":" + port);
				} else {
					connectionUrl = connectionUrl.replace(":PORT", "");
				}
			} 
			
			else if (driver.equalsIgnoreCase(IMPALA)) {
				Class.forName(IMPALA_DRIVER);
				connectionUrl = "jdbc:impala://HOST:PORT/SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
				if (port != null && !port.isEmpty()) {
					connectionUrl = connectionUrl.replace(":PORT", ":" + port);
				} else {
					connectionUrl = connectionUrl.replace(":PORT", "");
				}
			} 
			
			else if (driver.equalsIgnoreCase(MARIADB)) {
				Class.forName(MARIADB_DRIVER);
				connectionUrl = "jdbc:mariadb://HOST:PORT/SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
				if (port != null && !port.isEmpty()) {
					connectionUrl = connectionUrl.replace(":PORT", ":" + port);
				} else {
					connectionUrl = connectionUrl.replace(":PORT", "");
				}
			} 
			
			else if (driver.equalsIgnoreCase(MYSQL)) {
				Class.forName(MYSQL_DRIVER);
				connectionUrl = "jdbc:mysql://HOST:PORT/SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
				if (port != null && !port.isEmpty()) {
					connectionUrl = connectionUrl.replace(":PORT", ":" + port);
				} else {
					connectionUrl = connectionUrl.replace(":PORT", "");
				}
			} 
			
			else if (driver.equalsIgnoreCase(ORACLE)) {
				Class.forName(ORACLE_DRIVER);
				connectionUrl = "jdbc:oracle:thin:@HOST:PORT:SERVICE".replace("HOST", host).replace("SERVICE", schema);
				if (port != null && !port.isEmpty()) {
					connectionUrl = connectionUrl.replace(":PORT", ":" + port);
				} else {
					connectionUrl = connectionUrl.replace(":PORT", "");
				}
			}
			
			else if (driver.equalsIgnoreCase(PHOENIX)) {
				Class.forName(PHOENIX_DRIVER);
				connectionUrl = "jdbc:phoenix:HOST:PORT/SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
				if (port != null && !port.isEmpty()) {
					connectionUrl = connectionUrl.replace(":PORT", ":" + port);
				} else {
					connectionUrl = connectionUrl.replace(":PORT", "");
				}
			} 
			
			else if (driver.equalsIgnoreCase(POSTGRES)) {
				Class.forName(POSTGRES_DRIVER);
				connectionUrl = "jdbc:postgresql://HOST:PORT/SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
				if (port != null && !port.isEmpty()) {
					connectionUrl = connectionUrl.replace(":PORT", ":" + port);
				} else {
					connectionUrl = connectionUrl.replace(":PORT", "");
				}
			} 
			
			else if (driver.equalsIgnoreCase(SAP_HANA)) {
				Class.forName(SAP_HANA_DRIVER);
				connectionUrl = "jdbc:sap://HOST:PORT/SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
				if (port != null && !port.isEmpty()) {
					connectionUrl = connectionUrl.replace(":PORT", ":" + port);
				} else {
					connectionUrl = connectionUrl.replace(":PORT", "");
				}
			} 
			
			else if (driver.equalsIgnoreCase(SQLSERVER)) {
				Class.forName(SQLSERVER_DRIVER);
				connectionUrl = "jdbc:sqlserver://HOST:PORT;databaseName=SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
				if (port != null && !port.isEmpty()) {
					connectionUrl = connectionUrl.replace(":PORT", ":" + port);
				} else {
					connectionUrl = connectionUrl.replace(":PORT", "");
				}
			} 
			
			else if (driver.equalsIgnoreCase(TERADATA)) {
				Class.forName(TERADATA_DRIVER);
				connectionUrl = "jdbc:teradata://HOST:PORT/SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
				if (port != null && !port.isEmpty()) {
					connectionUrl = connectionUrl.replace(":PORT", ":" + port);
				} else {
					connectionUrl = connectionUrl.replace(":PORT", "");
				}
			} 
			
			else {
				throw new SQLException("Invalid driver");
			}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			throw new SQLException("Unable to find driver for engine type");
		}
		
		// add additional properties that are considered optional
		if(additonalProperties != null && !additonalProperties.isEmpty()) {
			if(!additonalProperties.startsWith(";")) {
				connectionUrl += ";" + additonalProperties;
			} else {
				connectionUrl += additonalProperties;
			}
		}
		
		Connection conn;
		try {
			if (userName == null || password == null) {
				conn = DriverManager.getConnection(connectionUrl);
			} else {
				conn = DriverManager.getConnection(connectionUrl, userName, password);
			}
		} catch (SQLException e) {
			e.printStackTrace();
			throw new SQLException(e.getMessage());
		}
		
		return conn;
	}
}
