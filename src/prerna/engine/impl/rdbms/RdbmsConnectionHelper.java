package prerna.engine.impl.rdbms;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.dbcp2.BasicDataSource;

import prerna.util.sql.RdbmsTypeEnum;

public class RdbmsConnectionHelper {
	
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
			String predictedDriver = RdbmsTypeEnum.getDriverFromString(driverType);
			if(predictedDriver != null) {
				Class.forName(predictedDriver);
			} else {
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
		RdbmsTypeEnum rdbmsType = RdbmsTypeEnum.getEnumFromString(driverType);
		if(rdbmsType == null) {
			throw new SQLException("Invalid driver");
		}
		
		if (rdbmsType == RdbmsTypeEnum.ASTER) {
			connectionUrl = "jdbc:ncluster://HOST:PORT/SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
		}
		else if (rdbmsType == RdbmsTypeEnum.CASSANDRA) {
			connectionUrl = "jdbc:cassandra://HOST:PORT/SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
		}
		else if (rdbmsType == RdbmsTypeEnum.DB2) {
			connectionUrl = "jdbc:db2://HOST:PORT/SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
		}
		else if (rdbmsType == RdbmsTypeEnum.DERBY) {
			connectionUrl = "jdbc:derby://HOST:PORT/SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
		}
		else if (rdbmsType == RdbmsTypeEnum.H2_DB) {
			connectionUrl = "jdbc:h2:tcp://HOST:PORT/SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
		}
		else if (rdbmsType == RdbmsTypeEnum.IMPALA) {
			connectionUrl = "jdbc:impala://HOST:PORT/SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
		}
		else if (rdbmsType == RdbmsTypeEnum.MARIADB) {
			connectionUrl = "jdbc:mariadb://HOST:PORT/SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
		}
		else if (rdbmsType == RdbmsTypeEnum.MYSQL) {
			connectionUrl = "jdbc:mysql://HOST:PORT/SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
		}
		else if (rdbmsType == RdbmsTypeEnum.ORACLE) {
			connectionUrl = "jdbc:oracle:thin:@HOST:PORT:SERVICE".replace("HOST", host).replace("SERVICE", schema);
		}
		else if (rdbmsType == RdbmsTypeEnum.PHOENIX) {
			connectionUrl = "jdbc:phoenix:HOST:PORT/SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
		}
		else if (rdbmsType == RdbmsTypeEnum.POSTGRES) {
			connectionUrl = "jdbc:postgresql://HOST:PORT/SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
		}
		else if (rdbmsType == RdbmsTypeEnum.SAP_HANA) {
			connectionUrl = "jdbc:sap://HOST:PORT/?currentSchema=SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
		}
		else if (rdbmsType == RdbmsTypeEnum.SQLSERVER) {
			connectionUrl = "jdbc:sqlserver://HOST:PORT;databaseName=SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
		}
		else if (rdbmsType == RdbmsTypeEnum.TERADATA) {
			connectionUrl = "jdbc:teradata://HOST:PORT/SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
		}
		else if (rdbmsType == RdbmsTypeEnum.REDSHIFT) {
			connectionUrl = "jdbc:redshift://HOST:PORT/SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
		} 
		else if (rdbmsType == RdbmsTypeEnum.TIBCO) {
			connectionUrl = "jdbc:compositesw:dbapi@HOST:PORT?SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
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
			if(!additonalProperties.startsWith(";") && !additonalProperties.startsWith("&")) {
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
		String predictedDriver = RdbmsTypeEnum.getDriverFromString(driverType);
		if(predictedDriver != null) {
			return predictedDriver;
		} else {
			// assume input is already a jdbc driver path
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
	
	/**
	 * Try to predict the current schema for a given database connection
	 * @param meta
	 * @param con
	 * @return
	 */
	public static String getSchema(DatabaseMetaData meta, Connection con) {
		String schema = null;
		// THIS IS BECAUSE ONLY JAVA 7 REQUIRES
		// THIS METHOD OT BE IMPLEMENTED ON THE
		// DRIVERS
		try {
			if(meta.getJDBCMajorVersion() >= 7) {
				schema = con.getSchema();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

		if(schema != null) {
			return schema;
		}
		
		String driverName = null;
		try {
			driverName = meta.getDriverName().toLowerCase();
		} catch (SQLException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		
		// in oracle
		// the datbase/schema/user are all considered the same thing
		// so here, if we want to filter
		// we use the user name
		if(driverName.contains("oracle")) {
			try {
				schema = meta.getUserName();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		} else if(driverName.contains("postgres")) {
			try {
				String url = meta.getURL();
				if(url.contains("?currentSchema=")) {
					Pattern p = Pattern.compile("currentSchema=[a-zA-Z0-9_]*");
					Matcher m = p.matcher(url);
					if(m.find()) {
						schema = m.group(0);
						schema = schema.replace("currentSchema=", "");
					}
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
			
		}
		
		return schema;
	}
	
}
