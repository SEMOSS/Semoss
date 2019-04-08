package prerna.engine.impl.rdbms;

import java.io.File;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
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
			File f = new File(host);
			if(f.exists()) {
				host = host.replace(".mv.db", "");
				// there is no port for files
				connectionUrl = "jdbc:h2:nio:HOST/SCHEMA".replace("HOST", host);
			} else {
				connectionUrl = "jdbc:h2:tcp://HOST:PORT/SCHEMA".replace("HOST", host);
			}
			// schema may be empty
			if(schema == null || schema.isEmpty()) {
				connectionUrl = connectionUrl.replace("/SCHEMA", "");
			} else {
				connectionUrl = connectionUrl.replace("SCHEMA", schema);
			}
		} else if( rdbmsType == RdbmsTypeEnum.SQLITE) {
			host = host.replace(".mv.db", "");
			// there is no port for files
			// sqlite doesn't really support schemas
			connectionUrl = "jdbc:sqlite:HOST".replace("HOST", host);
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
		else if (rdbmsType == RdbmsTypeEnum.SNOWFLAKE) {
			connectionUrl = "jdbc:snowflake://HOST:PORT/?db=SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
		}
		else if (rdbmsType == RdbmsTypeEnum.SQLSERVER) {
			connectionUrl = "jdbc:sqlserver://HOST:PORT;database=SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
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
	public static Connection buildConnection(String connectionUrl, String userName, String password, String driver) throws SQLException {
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
	public static String getSchema(DatabaseMetaData meta, Connection con, String connectionUrl) {
		String schema = null;
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
		}
		
		if(schema != null) {
			return schema;
		}
		
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

		String url = null;
		try {
			url = meta.getURL();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		schema = predictSchemaFromUrl(url);
		if(schema != null) {
			return schema;
		}
		schema = predictSchemaFromUrl(connectionUrl);
		
		return schema;
	}
	
	/**
	 * Get tables result set for the given connection, metadata parser, and
	 * catalog / schema filters. Must return a result set containing table_name
	 * and table_type (with values 'TABLE' or 'VIEW').
	 * 
	 * @param con
	 * @param meta
	 * @param catalogFilter
	 * @param schemaFilter
	 * @param driver
	 * @return
	 * @throws SQLException
	 */
	public static ResultSet getTables(Connection con, DatabaseMetaData meta, String catalogFilter, String schemaFilter, RdbmsTypeEnum driver) throws SQLException {					
		ResultSet tablesRs;
		if (driver == RdbmsTypeEnum.ORACLE) {
			String query = "SELECT TABLE_NAME AS \"table_name\", 'TABLE' AS \"table_type\"" + 
					"FROM ALL_TABLES WHERE TABLESPACE_NAME = 'USERS'" +
					"UNION SELECT VIEW_NAME AS \"table_name\", 'VIEW' AS \"table_type\" " + 
					"FROM ALL_VIEWS WHERE ORIGIN_CON_ID > 1";
			tablesRs = con.createStatement().executeQuery(query);
		} else {
			tablesRs = meta.getTables(catalogFilter, schemaFilter, null, new String[] { "TABLE", "VIEW" });
		}
		return tablesRs;
	}
	
	/**
	 * Get columns result set for the given metadata parser, table or view name,
	 * and catalog / schema filters. Must return a result set containing
	 * column_name and type_name.
	 * 
	 * @param meta
	 * @param tableOrView
	 * @param catalogFilter
	 * @param schemaFilter
	 * @param driver
	 * @return
	 * @throws SQLException
	 */
	public static ResultSet getColumns(DatabaseMetaData meta, String tableOrView, String catalogFilter, String schemaFilter, RdbmsTypeEnum driver) throws SQLException {
		ResultSet columnsRs;
		if (driver == RdbmsTypeEnum.ORACLE) {
			columnsRs = meta.getColumns(catalogFilter, null, tableOrView, null);
		} else {
			columnsRs = meta.getColumns(catalogFilter, schemaFilter, tableOrView, null);
		}
		return columnsRs;
	}
	
	private static String predictSchemaFromUrl(String url) {
		String schema = null;
		
		if(url.contains("?currentSchema=")) {
			Pattern p = Pattern.compile("currentSchema=[a-zA-Z0-9_]*");
			Matcher m = p.matcher(url);
			if(m.find()) {
				schema = m.group(0);
				schema = schema.replace("currentSchema=", "");
				return schema;
			}
		}
		
		if(url.contains("?schema=")) {
			Pattern p = Pattern.compile("schema=[a-zA-Z0-9_]*");
			Matcher m = p.matcher(url);
			if(m.find()) {
				schema = m.group(0);
				schema = schema.replace("schema=", "");
				return schema;
			}
		}
		
		return schema;
	}
	
}
