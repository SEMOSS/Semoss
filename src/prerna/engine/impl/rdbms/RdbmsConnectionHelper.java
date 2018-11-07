package prerna.engine.impl.rdbms;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.commons.dbcp2.BasicDataSource;

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
	
	public static final String REDSHIFT = "REDSHIFT";
	public static final String REDSHIFT_DRIVER = "com.amazon.redshift.jdbc.Driver";

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

	public static final String TIBCO = "TIBCO";
	public static final String TIBCO_DRIVER = "cs.jdbc.driver.CompositeDriver";
	
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
		String driverType = driver.toUpperCase();
		try {
			if (driverType.equalsIgnoreCase(ASTER)) {
				Class.forName(ASTER_DRIVER);
			} else if (driverType.equalsIgnoreCase(CASSANDRA)) {
				Class.forName(CASSANDRA_DRIVER);
			} else if (driverType.equalsIgnoreCase(DB2)) {
				Class.forName(DB2_DRIVER);
			} else if (driverType.equalsIgnoreCase(DERBY)) {
				Class.forName(DERBY_DRIVER);
			} else if (driverType.equalsIgnoreCase(H2)) {
				Class.forName(H2_DRIVER);
			} else if (driverType.equalsIgnoreCase(IMPALA)) {
				Class.forName(IMPALA_DRIVER);
			} else if (driverType.equalsIgnoreCase(MARIADB)) {
				Class.forName(MARIADB_DRIVER);
			} else if (driverType.equalsIgnoreCase(MYSQL)) {
				Class.forName(MYSQL_DRIVER);
			} else if (driverType.equalsIgnoreCase(ORACLE)) {
				Class.forName(ORACLE_DRIVER);
			} else if (driverType.equalsIgnoreCase(PHOENIX)) {
				Class.forName(PHOENIX_DRIVER);
			} else if (driverType.equalsIgnoreCase(POSTGRES)) {
				Class.forName(POSTGRES_DRIVER);
			} else if (driverType.equalsIgnoreCase(SAP_HANA)) {
				Class.forName(SAP_HANA_DRIVER);
			} else if (driverType.equalsIgnoreCase(SQLSERVER)) {
				Class.forName(SQLSERVER_DRIVER);
			} else if (driverType.equalsIgnoreCase(TERADATA)) {
				Class.forName(TERADATA_DRIVER);
			} else if (driverType.equalsIgnoreCase(REDSHIFT)) {
				Class.forName(REDSHIFT_DRIVER);
			} else if (driverType.equalsIgnoreCase(TIBCO)) {
				Class.forName(TIBCO_DRIVER);
			} 
			else {
				Class.forName(driver);
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
	 * Return the connection url string
	 * @param driverType
	 * @param host
	 * @param port
	 * @param schema
	 * @param additonalProperties
	 * @return
	 * @throws SQLException
	 */
	public static String getConnectionUrl(String driverType, String host, String port, String schema, String additonalProperties) throws SQLException {
		String connectionUrl = "";
		driverType = driverType.toUpperCase();
		if (driverType.equalsIgnoreCase(ASTER)) {
			connectionUrl = "jdbc:ncluster://HOST:PORT/SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
		}
		else if (driverType.equalsIgnoreCase(CASSANDRA)) {
			connectionUrl = "jdbc:cassandra://HOST:PORT/SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
		}
		else if (driverType.equalsIgnoreCase(DB2)) {
			connectionUrl = "jdbc:db2://HOST:PORT/SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
		}
		else if (driverType.equalsIgnoreCase(DERBY)) {
			connectionUrl = "jdbc:derby://HOST:PORT/SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
		}
		else if (driverType.equalsIgnoreCase(H2)) {
			connectionUrl = "jdbc:h2:tcp://HOST:PORT/SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
		}
		else if (driverType.equalsIgnoreCase(IMPALA)) {
			connectionUrl = "jdbc:impala://HOST:PORT/SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
		}
		else if (driverType.equalsIgnoreCase(MARIADB)) {
			connectionUrl = "jdbc:mariadb://HOST:PORT/SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
		}
		else if (driverType.equalsIgnoreCase(MYSQL)) {
			connectionUrl = "jdbc:mysql://HOST:PORT/SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
		}
		else if (driverType.equalsIgnoreCase(ORACLE)) {
			connectionUrl = "jdbc:oracle:thin:@HOST:PORT:SERVICE".replace("HOST", host).replace("SERVICE", schema);
		}
		else if (driverType.equalsIgnoreCase(PHOENIX)) {
			connectionUrl = "jdbc:phoenix:HOST:PORT/SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
		}
		else if (driverType.equalsIgnoreCase(POSTGRES)) {
			connectionUrl = "jdbc:postgresql://HOST:PORT/SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
		}
		else if (driverType.equalsIgnoreCase(SAP_HANA)) {
			connectionUrl = "jdbc:sap://HOST:PORT/?currentSchema=SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
		}
		else if (driverType.equalsIgnoreCase(SQLSERVER)) {
			connectionUrl = "jdbc:sqlserver://HOST:PORT;databaseName=SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
		}
		else if (driverType.equalsIgnoreCase(TERADATA)) {
			connectionUrl = "jdbc:teradata://HOST:PORT/SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
		}
		else if (driverType.equalsIgnoreCase(REDSHIFT)) {
			connectionUrl = "jdbc:redshift://HOST:PORT/SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
		} 
		else if (driverType.equalsIgnoreCase(TIBCO)) {
			connectionUrl = "jdbc:compositesw:dbapi@HOST:PORT?SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
		}
		
		else {
			throw new SQLException("Invalid driver");
		}
		
		// replace the PORT if defined
		// else it should use the default
		if (port != null && !port.isEmpty()) {
			connectionUrl = connectionUrl.replace(":PORT", ":" + port);
		} else {
			connectionUrl = connectionUrl.replace(":PORT", "");
		}
		
		// add additional properties that are considered optional
		if(additonalProperties != null && !additonalProperties.isEmpty()) {
			if(!additonalProperties.startsWith(";") || !additonalProperties.startsWith("&")) {
				connectionUrl += ";" + additonalProperties;
			} else {
				connectionUrl += additonalProperties;
			}
		}
		return connectionUrl;
	}
	
	/**
	 * Get the driver for a specific db type
	 * @param driver
	 * @return
	 */
	public static String getDriver(String driver) {
		String driverType = driver.toUpperCase();
		if (driverType.equalsIgnoreCase(ASTER)) {
			return ASTER_DRIVER;
		} else if (driverType.equalsIgnoreCase(CASSANDRA)) {
			return CASSANDRA_DRIVER;
		} else if (driverType.equalsIgnoreCase(DB2)) {
			return DB2_DRIVER;
		} else if (driverType.equalsIgnoreCase(DERBY)) {
			return DERBY_DRIVER;
		} else if (driverType.equalsIgnoreCase(H2)) {
			return H2_DRIVER;
		} else if (driverType.equalsIgnoreCase(IMPALA)) {
			return IMPALA_DRIVER;
		} else if (driverType.equalsIgnoreCase(MARIADB)) {
			return MARIADB_DRIVER;
		} else if (driverType.equalsIgnoreCase(MYSQL)) {
			return MYSQL_DRIVER;
		} else if (driverType.equalsIgnoreCase(ORACLE)) {
			return ORACLE_DRIVER;
		} else if (driverType.equalsIgnoreCase(PHOENIX)) {
			return PHOENIX_DRIVER;
		} else if (driverType.equalsIgnoreCase(POSTGRES)) {
			return POSTGRES_DRIVER;
		} else if (driverType.equalsIgnoreCase(SAP_HANA)) {
			return SAP_HANA_DRIVER;
		} else if (driverType.equalsIgnoreCase(SQLSERVER)) {
			return SQLSERVER_DRIVER;
		} else if (driverType.equalsIgnoreCase(TERADATA)) {
			return TERADATA_DRIVER;
		} else if (driverType.equalsIgnoreCase(REDSHIFT)) {
			return REDSHIFT_DRIVER;
		} else if (driverType.equalsIgnoreCase(TIBCO)) {
			return TIBCO_DRIVER;
		} else {
			// assume the input is good
			return driver;
		}
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
		String connectionUrl = getConnectionUrl(driver, host, port, schema, additonalProperties);
		return getConnection(connectionUrl, userName, password, driver);
	}
	
	/**
	 * 
	 * @param driver
	 * @param connectURI
	 * @param userName
	 * @param password
	 * @return
	 * @throws SQLException
	 */
	public static BasicDataSource getDataSourceFromPool(String driver, String connectURI, String userName, String password) throws SQLException {
		BasicDataSource ds = new BasicDataSource();
		ds.setDriverClassName(driver);
		ds.setUrl(connectURI);
		ds.setUsername(userName);
		ds.setPassword(password);
		ds.setDefaultAutoCommit(false);
		return ds;
	}
}
