package prerna.engine.impl.rdbms;

import java.io.File;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.zaxxer.hikari.HikariDataSource;

import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.sql.RdbmsTypeEnum;

public class RdbmsConnectionHelper {

	private static final Logger logger = LogManager.getLogger(RdbmsConnectionHelper.class);

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
			logger.error(Constants.STACKTRACE, e);
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
			logger.error(Constants.STACKTRACE, e);
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
		RdbmsTypeEnum rdbmsType = RdbmsTypeEnum.getEnumFromString(driverType);
		if(rdbmsType == null) {
			throw new SQLException("Invalid driver");
		}
		String connectionUrl = rdbmsType.getUrlPrefix();

		if (rdbmsType == RdbmsTypeEnum.ASTER) {
			connectionUrl += "://HOST:PORT/SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
		} else if (rdbmsType == RdbmsTypeEnum.ATHENA) {
			connectionUrl += "://";
		}
		else if (rdbmsType == RdbmsTypeEnum.CASSANDRA) {
			connectionUrl += "://HOST:PORT/SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
		}
		else if (rdbmsType == RdbmsTypeEnum.CLICKHOUSE) {
			connectionUrl += "://HOST:PORT/SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
		}
		else if (rdbmsType == RdbmsTypeEnum.DB2) {
			connectionUrl += "://HOST:PORT/SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
		}
		else if (rdbmsType == RdbmsTypeEnum.DERBY) {
			connectionUrl += "://HOST:PORT/SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
		}
		else if (rdbmsType == RdbmsTypeEnum.H2_DB) {
			File f = new File(Utility.normalizePath(host));
			if(f.exists()) {
				host = host.replace(".mv.db", "");
				// there is no port for files
				connectionUrl += ":nio:HOST/SCHEMA".replace("HOST", host);
			} else {
				connectionUrl += ":tcp://HOST:PORT/SCHEMA".replace("HOST", host);
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
			connectionUrl += ":HOST".replace("HOST", host);
		}
		else if (rdbmsType == RdbmsTypeEnum.HIVE) {
			connectionUrl += "://HOST:PORT/SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
		}
		else if (rdbmsType == RdbmsTypeEnum.IMPALA) {
			connectionUrl += "://HOST:PORT/SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
		}
		else if (rdbmsType == RdbmsTypeEnum.REDSHIFT) {
			connectionUrl += "://HOST:PORT/SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
		}
		else if (rdbmsType == RdbmsTypeEnum.MARIADB) {
			connectionUrl += "://HOST:PORT/SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
		}
		else if (rdbmsType == RdbmsTypeEnum.MYSQL) {
			connectionUrl += "://HOST:PORT/SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
		}
		else if (rdbmsType == RdbmsTypeEnum.ORACLE) {
			connectionUrl += ":@HOST:PORT:SERVICE".replace("HOST", host).replace("SERVICE", schema);
		}
		else if (rdbmsType == RdbmsTypeEnum.PHOENIX) {
			connectionUrl += ":HOST:PORT/SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
		}
		else if (rdbmsType == RdbmsTypeEnum.POSTGRES) {
			connectionUrl += "://HOST:PORT/SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
		}
		else if (rdbmsType == RdbmsTypeEnum.SAP_HANA) {
			connectionUrl += "://HOST:PORT/?currentSchema=SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
		}
		else if (rdbmsType == RdbmsTypeEnum.SPARK) {
			connectionUrl += "://HOST:PORT/SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
		}
		else if (rdbmsType == RdbmsTypeEnum.SNOWFLAKE) {
			connectionUrl += "://HOST:PORT/?db=SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
		}
		else if (rdbmsType == RdbmsTypeEnum.SYNAPSE) {
			connectionUrl += "://HOST:PORT;databaseName=SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
		}
		else if (rdbmsType == RdbmsTypeEnum.SQL_SERVER) {
			connectionUrl += "://HOST:PORT;databaseName=SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
		}
		else if (rdbmsType == RdbmsTypeEnum.TERADATA) {
			connectionUrl += "://HOST/DATABASE=SCHEMA,:PORT".replace("HOST", host).replace("SCHEMA", schema);
		}		
		else if (rdbmsType == RdbmsTypeEnum.TIBCO) {
			connectionUrl += "@HOST:PORT?SCHEMA".replace("HOST", host).replace("SCHEMA", schema);
		}

		// replace the PORT if defined
		// else it should use the default
		if (port != null && !port.isEmpty()) {
			if (rdbmsType == RdbmsTypeEnum.TERADATA){
				connectionUrl = connectionUrl.replace(":PORT", "DBS_PORT=" + port);
			} else {
				connectionUrl = connectionUrl.replace(":PORT", ":" + port);
			}
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
	public static HikariDataSource getDataSourceFromPool(String driver, String connectURI, String userName, String password) throws SQLException {
		HikariDataSource ds = new HikariDataSource();
		ds.setDriverClassName(driver);
		ds.setJdbcUrl(connectURI);
		ds.setUsername(userName);
		ds.setPassword(password);
		return ds;
	}

	/**
	 * Try to predict the current schema for a given database connection
	 * @param meta
	 * @param con
	 * @return
	 */
	public static String getSchema(DatabaseMetaData meta, Connection con, String connectionUrl, RdbmsTypeEnum rdbmsType) {
		String schema = null;
		String driverName = null;
		try {
			driverName = meta.getDriverName();
			if(driverName != null) {
				driverName = driverName.toLowerCase();
			}
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		}

		// in oracle
		// the datbase/schema/user are all considered the same thing
		// so here, if we want to filter
		// we use the user name
		if((driverName != null && driverName.contains("oracle")) || (rdbmsType != null && rdbmsType == RdbmsTypeEnum.ORACLE)) {
			try {
				schema = meta.getUserName();
			} catch (SQLException e) {
				logger.error(Constants.STACKTRACE, e);
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
			logger.error(Constants.STACKTRACE, e);
		}
		
		//hive doesn't support getURL
		if(rdbmsType != RdbmsTypeEnum.HIVE){
			String url = null;
			try {
				url = meta.getURL();
			} catch (SQLException e) {
				logger.error(Constants.STACKTRACE, e);
			}
			
	
			schema = predictSchemaFromUrl(url);
			if(schema != null) {
				return schema;
			}
		}
		schema = predictSchemaFromUrl(connectionUrl);
		if(schema != null) {
			return schema;
		}
		
		// add logic for when schema is called database
		if((driverName != null && driverName.contains("teradata")) || (rdbmsType != null && rdbmsType == RdbmsTypeEnum.TERADATA)) {
			schema = predictSchemaAsDatbaseFromUrl(connectionUrl);
		}
		if(schema != null) {
			return schema;
		}
		
		String truncatedUrl = connectionUrl;
		if(rdbmsType != null) {
			truncatedUrl = connectionUrl.substring(rdbmsType.getUrlPrefix().length());
		}

		if(schema == null) {
			// try schema...
			ResultSet schemaRs = null;
			try {
				schemaRs = meta.getSchemas();
				while(schemaRs.next()) {
					String tableSchema = schemaRs.getString(1);
					if(truncatedUrl.contains(tableSchema)) {
						schema = tableSchema;
						break;
					}
				}
			} catch (SQLException e) {
				logger.error(Constants.STACKTRACE, e);
			} finally {
				if(schemaRs != null) {
					try {
						schemaRs.close();
					} catch (SQLException e) {
						logger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}

		// try catalog...
		if(schema == null) {
			ResultSet catalogRs = null;
			try {
				catalogRs = meta.getCatalogs();
				while(catalogRs.next()) {
					String tableSchema = catalogRs.getString(1);
					if(truncatedUrl.contains(tableSchema)) {
						schema = tableSchema;
						break;
					}
				}
			} catch (SQLException e) {
				logger.error(Constants.STACKTRACE, e);
			} finally {
				if(catalogRs != null) {
					try {
						catalogRs.close();
					} catch (SQLException e) {
						logger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}

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
	public static ResultSet getTables(Connection con, Statement stmt, DatabaseMetaData meta, String catalogFilter, String schemaFilter, RdbmsTypeEnum driver) throws SQLException {					
		ResultSet tablesRs;
		if (driver == RdbmsTypeEnum.ORACLE) {
			String query = "SELECT TABLE_NAME AS \"table_name\", 'TABLE' AS \"table_type\", '" + meta.getUserName() + "' AS \"table_schem\" FROM ALL_TABLES WHERE "
					+ "OWNER NOT IN ('SYS', 'SYSTEM', 'WMSYS', 'XDB', 'CTXSYS', 'LBASYS', 'MDSYS', 'OLAPSYS','ORDSYS','LBACSYS', 'GSMADMIN_INTERNAL', 'ORDDATA')"
					+ " OR TABLESPACE_NAME NOT IN ('SYSAUX', 'SYSTEM')"
					+ " UNION SELECT VIEW_NAME AS \"table_name\", 'VIEW' AS \"table_type\", '" + meta.getUserName() +"' AS \"table_schem\" FROM ALL_VIEWS WHERE"
					+ " OWNER NOT IN ('SYS', 'SYSTEM', 'WMSYS', 'XDB', 'CTXSYS', 'LBASYS', 'MDSYS', 'OLAPSYS','ORDSYS','LBACSYS', 'GSMADMIN_INTERNAL', 'ORDDATA')";
			tablesRs = stmt.executeQuery(query);
		} else if (driver == RdbmsTypeEnum.ATHENA || driver == RdbmsTypeEnum.REDSHIFT){
			tablesRs = meta.getTables(catalogFilter, schemaFilter, null, new String[] { "TABLE", "EXTERNAL TABLE", "EXTERNAL_TABLE", "VIEW" });
		} 
//		else if (driver == RdbmsTypeEnum.SQL_SERVER) {
//			// do not pass in the schema...
//			tablesRs = meta.getTables(catalogFilter, schemaFilter, null, new String[] { "TABLE", "VIEW"});
//		} 
		else if(driver == RdbmsTypeEnum.MYSQL){
			// these take the schema as a proper regex search
			tablesRs = meta.getTables(catalogFilter, "^" + schemaFilter + "$", null, new String[] { "TABLE", "VIEW" });
		} else if (driver == RdbmsTypeEnum.CASSANDRA){
			if(catalogFilter.isEmpty()) {
				tablesRs = meta.getTables("cassandra", schemaFilter, null, new String[] { "TABLE", "VIEW" });
			} else {
				tablesRs = meta.getTables(catalogFilter, schemaFilter, null, new String[] { "TABLE", "VIEW" });
			}
		}
		else {
			// these do not take in the schema as a proper regex search
			// i know POSTGRES is an example
			tablesRs = meta.getTables(catalogFilter, schemaFilter, null, new String[] { "TABLE", "VIEW" });
		}
		return tablesRs;
	}

	/**
	 * Get the keys to grab from the result set from calling {@link #getTables}
	 * First key is table name, second key is table type, the third is the table schema
	 * @param driver
	 * @return
	 */
	public static String[] getTableKeys(RdbmsTypeEnum driver) {
		String[] arr = new String[4];
		if(driver == RdbmsTypeEnum.SNOWFLAKE || 
				driver == RdbmsTypeEnum.CLICKHOUSE || 
				driver == RdbmsTypeEnum.ATHENA || 
				driver == RdbmsTypeEnum.CASSANDRA ||
				driver == RdbmsTypeEnum.OPEN_SEARCH
				) {
			arr[0] = "TABLE_NAME";
			arr[1] = "TABLE_TYPE";
			arr[2] = "TABLE_SCHEM";
			arr[3] = "TABLE_CAT";
		} else {
			arr[0] = "table_name";
			arr[1] = "table_type";
			arr[2] = "table_schem";
			arr[3] = "table_cat";
		}
		return arr;
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
		if (driver == RdbmsTypeEnum.ORACLE) { // || driver == RdbmsTypeEnum.SQL_SERVER) {
			// do not pass in schema
			columnsRs = meta.getColumns(catalogFilter, null, tableOrView, null);
		} else if(driver == RdbmsTypeEnum.SNOWFLAKE) {
			if(schemaFilter != null) {
				schemaFilter = schemaFilter.replace("_", "\\_");
			}
			if(tableOrView != null) {
				tableOrView = tableOrView.replace("_", "\\_");
			}
			columnsRs = meta.getColumns(catalogFilter, schemaFilter, tableOrView, null);
		} else if (driver == RdbmsTypeEnum.CASSANDRA){
			if(catalogFilter.isEmpty()) {
				catalogFilter="cassandra";
			}
			columnsRs = meta.getColumns(catalogFilter, schemaFilter, tableOrView, null);	
		}
		else {
			columnsRs = meta.getColumns(catalogFilter, schemaFilter, tableOrView, null);
		}
		return columnsRs;
	}

	/**
	 * Get the keys to grab from the result set from calling {@link #getColumns}
	 * First key is column name, second key is column type
	 * @param driver
	 * @return
	 */
	public static String[] getColumnKeys(RdbmsTypeEnum driver) {
		String[] arr = new String[2];
		if(driver == RdbmsTypeEnum.SNOWFLAKE || 
				driver == RdbmsTypeEnum.CLICKHOUSE || 
				driver == RdbmsTypeEnum.CASSANDRA ||
				driver == RdbmsTypeEnum.OPEN_SEARCH
				) {
			arr[0] = "COLUMN_NAME";
			arr[1] = "TYPE_NAME";
		} else {
			arr[0] = "column_name";
			arr[1] = "type_name";
		}
		return arr;
	}

	private static String predictSchemaFromUrl(String url) {
		String schema = null;
		if(url == null) {
			return schema;
		}
		
		if(url.contains("?currentSchema=")) {
			Pattern p = Pattern.compile("currentSchema=[a-zA-Z0-9_]*");
			Matcher m = p.matcher(url);
			if(m.find()) {
				schema = m.group(0);
				schema = schema.replace("currentSchema=", "");
				return schema;
			}
		}

		if(url.contains(";currentSchema=")) {
			Pattern p = Pattern.compile("currentSchema=[a-zA-Z0-9_]*");
			Matcher m = p.matcher(url);
			if(m.find()) {
				schema = m.group(0);
				schema = schema.replace("currentSchema=", "");
				return schema;
			}
		}
		
		if(url.contains("&currentSchema=")) {
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

		if(url.contains(";schema=")) {
			Pattern p = Pattern.compile("schema=[a-zA-Z0-9_]*");
			Matcher m = p.matcher(url);
			if(m.find()) {
				schema = m.group(0);
				schema = schema.replace("schema=", "");
				return schema;
			}
		}
		
		if(url.contains("&schema=")) {
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
	
	private static String predictSchemaAsDatbaseFromUrl(String url) {
		String schema = null;

		if(url.contains("/DATABASE=")) {
			Pattern p = Pattern.compile("DATABASE=[a-zA-Z0-9_]*");
			Matcher m = p.matcher(url);
			if(m.find()) {
				schema = m.group(0);
				schema = schema.replace("DATABASE=", "");
				return schema;
			}
		}

		return schema;
	}


}
