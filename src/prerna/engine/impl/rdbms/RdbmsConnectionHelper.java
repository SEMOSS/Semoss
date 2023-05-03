package prerna.engine.impl.rdbms;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.zaxxer.hikari.HikariDataSource;

import prerna.util.Constants;
import prerna.util.sql.RdbmsTypeEnum;

public class RdbmsConnectionHelper {

	private static final Logger logger = LogManager.getLogger(RdbmsConnectionHelper.class);

	private RdbmsConnectionHelper() {

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
