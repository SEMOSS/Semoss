/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.util.sql;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.api.SemossDataType;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.CaseInsensitiveProperties;
import prerna.engine.impl.owl.WriteOWLEngine;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.interpreters.sql.SqlInterpreter;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.Join;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.Utility;

public abstract class AbstractSqlQueryUtil {
	
	// special key when not required
	public static final String NO_KEY_REQUIRED = "NO_KEY_REQUIRED";
	
	// inputs for connection string builder
	public static final String CONNECTION_URL = Constants.CONNECTION_URL;
	public static final String DRIVER_NAME = "dbDriver";

	public static final String HOSTNAME = "hostname";
	public static final String PORT = "port";
	public static final String DATABASE = "database";
	public static final String CATALOG = "catalog";
	public static final String SCHEMA = "schema";
	public static final String USERNAME = Constants.USERNAME;
	public static final String PASSWORD = Constants.PASSWORD;
	public static final String ADDITIONAL = "additional";
	
	// relatively specific inputs
	// athena
	public static final String SERVICE = "service";
	public static final String REGION = "region";
	public static final String ACCESS_KEY = "accessKey";
	public static final String SECRET_KEY = "secretKey";
	public static final String OUTPUT = "output";
	// bigquery
	public static final String PROJECT_ID = "projectId";
	public static final String OAUTH_TYPE = "oauthType";
	public static final String OAUTH_SERVICE_ACCT_EMAIL = "oauthServiceAcctEmail";
	public static final String OAUTH_PRIVATE_KEY_PATH = "oauthPvtKeyPath";
	public static final String OAUTH_ACCESS_TOKEN = "oauthAccessToken";
	public static final String OAUTH_REFRESH_TOKEN = "oauthRefreshToken";
	public static final String OAUTH_CLIENT_ID = "oauthClientId";
	public static final String OAUTH_CLIENT_SECRET = "oauthClientSecret";
	public static final String DEFAULT_DATASET = "defaultDataSet";
	// snowflake
	public static final String WAREHOUSE = "warehouse";
	public static final String ROLE = "role";
	// elasticsearch
	public static final String HTTP_TYPE = "httpType";
	// databricks
	public static final String HTTP_PATH = "httpPath";
	public static final String UID = "UID";
	public static final String PWD = "PWD";
	// semoss
	public static final String PROTOCOL = "protocol";
	public static final String ENDPOINT = "endpoint";
	public static final String SUB_URL = "sub_url";
	public static final String PROJECT = "project";
	public static final String INSIGHT = "insight";

	// h2 force file for creating embedded file
	public static final String FORCE_FILE = "forceFile";
	
	private static final Logger classLogger = LogManager.getLogger(AbstractSqlQueryUtil.class);

	protected RdbmsTypeEnum dbType = null;
	// there are 2 different ways of providing the inputs
	// properties - primarily for grabbing from SMSS files
	// map - primarily for getting input details from FE / JSON
	protected Properties properites;
	protected Map<String, Object> conDetails;

	protected String connectionUrl;
	protected String username;
	protected String password;
	
	// these should be replaced and use the properties / conDetails
	protected String hostname;
	protected String port;
	protected String database;
	protected String schema;
	protected String additionalProps;
	
	// reserved words
	protected List<String> reservedWords = null;
	// type conversions
	protected Map<String, String> typeConversionMap = new HashMap<>();

	AbstractSqlQueryUtil() {
		initTypeConverstionMap();
	}
	
	AbstractSqlQueryUtil(String connectionURL, String username, String password) {
		this.connectionUrl = connectionURL;
		this.username = username;
		this.password = password;
		initTypeConverstionMap();
	}

	/**
	 * Set the connection details from a map
	 * @param configMap
	 * @return
	 * @throws SQLException 
	 */
	public abstract String setConnectionDetailsfromMap(Map<String, Object> configMap) throws RuntimeException;
	
	/**
	 * Set the connection details from a properties file (SMSS file)
	 * @param prop
	 * @return
	 */
	public abstract String setConnectionDetailsFromSMSS(CaseInsensitiveProperties prop) throws RuntimeException;
	
	/**
	 * Build the connection string after the connection details have been set
	 * @return
	 */
	public abstract String buildConnectionString();
	
	/**
	 * Method to get a connection to an existing RDBMS engine
	 * @param driverEnum
	 * @param connectionUrl
	 * @param connectionDetails
	 * @return
	 * @throws SQLException 
	 */
	public static Connection makeConnection(AbstractSqlQueryUtil util, String connectionUrl, Map<String, Object> connectionDetails) throws SQLException {
		return AbstractSqlQueryUtil.makeConnection(util.getDbType(), 
				connectionUrl, 
				(String) connectionDetails.get(util.getConnectionUserKey()), 
				(String) connectionDetails.get(util.getConnectionPasswordKey()));
	}
	
	/**
	 * Method to get a connection to an existing RDBMS engine
	 * @param driverEnum
	 * @param connectionUrl
	 * @param connectionDetails
	 * @return
	 * @throws SQLException 
	 */
	public static Connection makeConnection(AbstractSqlQueryUtil util, String connectionUrl, CaseInsensitiveProperties prop) throws SQLException {
		return AbstractSqlQueryUtil.makeConnection(util.getDbType(), 
				connectionUrl, 
				(String) prop.get(util.getConnectionUserKey()), 
				(String) prop.get(util.getConnectionPasswordKey()));
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
	public static Connection makeConnection(RdbmsTypeEnum type, String connectionUrl, String userName, String password) throws SQLException {
		try {
			Class.forName(type.getDriver());
		} catch (ClassNotFoundException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SQLException("Unable to find class: " + type.getDriver());
		}

		// create the iterator
		Connection conn;
		try {
			if (userName == null && password == null) {
				conn = DriverManager.getConnection(connectionUrl);
			} else {
				conn = DriverManager.getConnection(connectionUrl, userName, password);
			}
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SQLException(e.getMessage());
		}

		return conn;
	}
	
	/**
	 * Use this when we need to make any modifications to the connection object for
	 * proper usage Example ::: Adding user defined functions for RDBMS types that
	 * allow it
	 * 
	 * @param con
	 */
	public abstract void enhanceConnection(Connection con);

	/**
	 * Initialize the type conversion map to account for sql discrepancies in type
	 * names
	 */
	public abstract void initTypeConverstionMap();

	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////

	/*
	 * All connection details the setters and getters
	 */

	public RdbmsTypeEnum getDbType() {
		return dbType;
	}

	void setDbType(RdbmsTypeEnum dbType) {
		this.dbType = dbType;
	}

	public String getDriver() {
		return this.dbType.getDriver();
	}

	public String getHostname() {
		return hostname;
	}
	
	public void setHostname(String hostname) {
		this.hostname = hostname;
	}

	public String getPort() {
		return port;
	}

	public void setPort(String port) {
		this.port = port;
	}
	
	public String getDatabase() {
		return database;
	}
	
	public void setDatabase(String database) {
		this.database = database;
	}
	
	public String getSchema() {
		return schema;
	}

	public void setSchema(String schema) {
		this.schema = schema;
	}
	
	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}
	
	public String getPassword() {
		return password;
	}
	
	public void setPassword(String password) {
		this.password = password;
	}

	public String getAdditionalProps() {
		return additionalProps;
	}
	
	public void setAdditionalProps(String additionalProps) {
		this.additionalProps = additionalProps;
	}
	
	public String getConnectionUrl() {
		return connectionUrl;
	}
	
	public void setConnectionUrl(String connectionUrl) {
		this.connectionUrl = connectionUrl;
	}

	public String getConnectionUserKey() {
		return AbstractSqlQueryUtil.USERNAME;
	}
	
	public String getConnectionPasswordKey() {
		return AbstractSqlQueryUtil.PASSWORD;
	}

	public IQueryInterpreter getInterpreter(IDatabaseEngine engine) {
		return new SqlInterpreter(engine);
	}

	public IQueryInterpreter getInterpreter(ITableDataFrame frame) {
		return new SqlInterpreter(frame);
	}
	
	public Map<String, String> getTypeConversionMap() {
		return Collections.unmodifiableMap(typeConversionMap);
	}
	
	public String getDatabaseMetadataCatalogFilter() {
		return null;
	}
	
	public String getDatabaseMetadataSchemaFilter() {
		return null;
	}

	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////

	/**
	 * Set the list of reserved words
	 * 
	 * @param reservedWords
	 */
	public void setReservedWords(List<String> reservedWords) {
		this.reservedWords = reservedWords;
	}

	/**
	 * Check if the selector is in fact a reserved word
	 * 
	 * @param selector
	 * @return
	 */
	public boolean isSelectorKeyword(String selector) {
		return this.reservedWords != null && this.reservedWords.contains(selector.toUpperCase());
	}

	/**
	 * Get the escaped keyword Default is to wrap the selector in quotes
	 * 
	 * @param selector
	 * @return
	 */
	public String getEscapeKeyword(String selector) {
		return "\"" + selector + "\"";
	}

	/**
	 * Get any modification required to an alias
	 * @param alias
	 * @return
	 */
	public String escapeReferencedAlias(String alias) {
		return alias;
	}
	
	/**
	 * Determine if the subquery column name needs to be aliased to be recognized
	 * @param columnReturnedFromSubquery
	 * @return
	 */
	public String escapeSubqueryColumnName(String columnReturnedFromSubquery) {
		return columnReturnedFromSubquery;
	}

	/**
	 * Escape sql statement literals
	 * 
	 * @param s
	 * @return
	 */
	public static String escapeForSQLStatement(String s) {
		if (s == null) {
			return s;
		}
		return s.replace("'", "''");
	}

	/**
	 * Escape regex searching
	 * 
	 * @param s
	 * @return
	 */
	public static String escapeRegexCharacters(String s) {
		s = s.trim();
		s = s.replace("(", "\\(");
		s = s.replace(")", "\\)");
		return s;
	}
	
	/**
	 * 
	 * @param connection
	 * @param blobInput
	 * @return
	 */
	public static Blob stringToBlob(Connection connection, String blobInput) {
		Blob blob = null;

		try {
			blob = connection.createBlob();
			blob.setBytes(1, blobInput.getBytes());
		} catch (SQLException se) {
			classLogger.error("Failed to convert string to blob...");
			classLogger.error(Constants.STACKTRACE, se);
		}

		return blob;
	}
	
	/**
	 * 
	 * @param blob
	 * @return
	 */
	public static String flushBlobToString(Blob blob) throws SQLException, IOException {
		if(blob == null) {
			return null;
		}
		StringBuffer strOut = new StringBuffer();
		String aux;

		InputStream is = null;
		InputStreamReader isr = null;
		BufferedReader br = null;

		try {
			is = blob.getBinaryStream();
			isr = new InputStreamReader(is);
			br = new BufferedReader(isr);
			while ((aux=br.readLine())!=null) {
				strOut.append(aux);
			}
		} finally {
			if(is != null) {
				try {
					is.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
			if(isr != null) {
				try {
					isr.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
			if(br != null) {
				try {
					br.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		return strOut.toString();
	}

	/**
	 * Flush clob to string
	 * 
	 * @param inputClob
	 * @return
	 */
	public static String flushClobToString(java.sql.Clob inputClob) {
		Reader inputstream = null;
		if (inputClob != null) {
			try {
				inputstream = inputClob.getCharacterStream();
				return IOUtils.toString(inputstream);
			} catch (SQLException sqe) {
				classLogger.error(Constants.STACKTRACE, sqe);
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		}
		return null;
	}
	
	/**
	 * 
	 * @param sourceClob
	 * @param targetClob
	 * @throws SQLException
	 * @throws IOException
	 */
	public static void transferClob(Clob sourceClob, Clob targetClob) throws SQLException, IOException {
		InputStream source = sourceClob.getAsciiStream();
		OutputStream target = targetClob.setAsciiStream(1);
		
		byte[] buf = new byte[8192];
		int length;
		while ((length = source.read(buf)) > 0) {
			target.write(buf, 0, length);
		}
	}
	
	/**
	 * Clean the table name so it is valid for SQL
	 * @param tableName
	 * @return
	 */
	public static String cleanTableName(String tableName) {
		tableName = Utility.makeAlphaNumeric(tableName);
		if(tableName.isEmpty()) {
			throw new IllegalArgumentException("After removing unallowed special characters, the table name is empty");
		}
		if(Character.isDigit(tableName.charAt(0))) {
			tableName = "_" + tableName;
		}
		return tableName;
	}

	/////////////////////////////////////////////////////////////////////////////////////

	/*
	 * Methods to clean the sql type
	 */

	/**
	 * Clean the types to account for sql naming differences
	 * 
	 * @param type
	 * @return
	 */
	public String cleanType(String type) {
		if (type == null) {
			type = "VARCHAR(800)";
		}
		type = type.toUpperCase();
		if (typeConversionMap.containsKey(type)) {
			type = typeConversionMap.get(type);
		} else {
			if (typeConversionMap.containsValue(type)) {
				return type;
			}
			type = "VARCHAR(800)";
		}
		return type;
	}

	/**
	 * Clean the types to account for sql naming differences
	 * 
	 * @param types
	 * @return
	 */
	public String[] cleanTypes(String[] types) {
		String[] cleanTypes = new String[types.length];
		for (int i = 0; i < types.length; i++) {
			cleanTypes[i] = cleanType(types[i]);
		}

		return cleanTypes;
	}

	/////////////////////////////////////////////////////////////////////////////////////

	/*
	 * This section is so I can properly convert the intended function names
	 */

	/**
	 * Get the sql function string
	 * 
	 * @param inputFunction
	 * @return
	 */

	// there are all the specific functions
	// the {@link #getSqlFunctionSyntax(String) getSqlFunctionSyntax}
	// only needs to be implemented in the AnsiSqlQueryUtil
	// where it loops through everything and the specifics can be
	// implemented in the query util implementations

	public abstract String getSqlFunctionSyntax(String inputFunction);

	public abstract String getMinFunctionSyntax();

	public abstract String getMaxFunctionSyntax();

	public abstract String getAvgFunctionSyntax();

	public abstract String getMedianFunctionSyntax();

	public abstract String getSumFunctionSyntax();

	public abstract String getStdevFunctionSyntax();

	public abstract String getCountFunctionSyntax();

	public abstract String getConcatFunctionSyntax();

	public abstract String getGroupConcatFunctionSyntax();
	
	public abstract String getSubstringFunctionSyntax();
	
	public abstract String getDateFormatFunctionSyntax();
	
	public abstract String getCastFunctionSyntax();

	public abstract String getLowerFunctionSyntax();

	public abstract String getCoalesceFunctionSyntax();

	public abstract String getRegexLikeFunctionSyntax();

	public abstract String getMonthNameFunctionSyntax();
	
	public abstract String getDayNameFunctionSyntax();
	
	public abstract String getQuarterFunctionSyntax();
	
	public abstract String getWeekFunctionSyntax();
	
	public abstract String getYearFunctionSyntax();
	
	// TODO: NEED TO BUILD OUT MORE FUNCTIONS THIS WAY TO ACCOUNT 
	// FUNCTION SYNTAX REQUIREMENTS BASED ON THE SQL TYPE
	public abstract String processGroupByFunction(String selector, String separator, boolean distinct);
	
	// TODO: this might potentially be replaced from the above
	// once we implement all the various functions
	public abstract void appendDefaultFunctionOptions(QueryFunctionSelector fun);

	// date functions - require more complex inputs
	public abstract String getCurrentDate();
	
	public abstract String getCurrentTimestamp();
	
	public abstract String getDateAddFunctionSyntax(String timeUnit, int value, String  dateTimeField);
	
	public abstract String getDateDiffFunctionSyntax(String timeUnit, String dateTimeField1, String dateTimeField2);
	
	/////////////////////////////////////////////////////////////////////////////////////

	/*
	 * This section is intended for modifications to select queries to pull data
	 */

	
	/**
	 * Retrieve the first row of a query
	 * 
	 * @param query
	 * @param limit
	 * @param offset
	 * @return
	 */
	public abstract StringBuilder getFirstRow(StringBuilder query);
	
	/**
	 * Add the limit and offset to a query
	 * 
	 * @param query
	 * @param limit
	 * @param offset
	 * @return
	 */
	public abstract StringBuilder addLimitOffsetToQuery(StringBuilder query, long limit, long offset);

	/**
	 * Add the limit and offset to a query
	 * 
	 * @param query
	 * @param limit
	 * @param offset
	 * @return
	 */
	public abstract StringBuffer addLimitOffsetToQuery(StringBuffer query, long limit, long offset);
	
	/**
	 * Remove duplicates that exist from an existing table by creating a new temp
	 * intermediary table
	 * 
	 * @param tableName
	 * @param fullColumnNameList
	 * @return
	 */
	public abstract String removeDuplicatesFromTable(String tableName, String fullColumnNameList);

	/**
	 * Create an insert prepared statement
	 * 
	 * @param tableName
	 * @param columns
	 */
	public abstract String createInsertPreparedStatementString(String tableName, String[] columns);

	/**
	 * Create an update prepared statement
	 * 
	 * @param tableName
	 * @param columnsToUpdate
	 * @param whereColumns
	 * @return
	 */
	public abstract String createUpdatePreparedStatementString(String tableName, String[] columnsToUpdate,
			String[] whereColumns);

	/**
	 * Append a regex filter search on a column
	 * @param qs
	 * @param columnQs
	 * @param searchTerm
	 */
	public abstract void appendSearchRegexFilter(AbstractQueryStruct qs, String columnQs, String searchTerm);
	
	/**
	 * Create the syntax to merge 2 tables together
	 * @param returnTableName	The return table name
	 * @param leftTableName		The left table
	 * @param leftTableTypes	The {header -> type} of the left table
	 * @param rightTableName	The right table name
	 * @param rightTableTypes	The {header -> type} of the right table
	 * @param joins				The joins between the right and left table
	 * @param leftTableAlias	The {header -> alias} of the left table
	 * @param rightTableAlias	The {header -> alias} of the right table
	 * @param rightJoinFlag		Flag if we are doing a right join to switch the ordering of the tables
	 * @return
	 */
	public abstract String createNewTableFromJoiningTables(String returnTableName, String leftTableName,
			Map<String, SemossDataType> leftTableTypes, String rightTableName,
			Map<String, SemossDataType> rightTableTypes, List<Join> joins, Map<String, String> leftTableAlias,
			Map<String, String> rightTableAlias, boolean rightJoinFlag);
	
	/**
	 * Similar to {@link #createNewTableFromJoiningTables()} but only returns the select portion without 
	 * the "CREATE TABLE AS " syntax
	 * @param leftTableName
	 * @param leftTableTypes
	 * @param rightTableName
	 * @param rightTableTypes
	 * @param joins
	 * @param leftTableAlias
	 * @param rightTableAlias
	 * @param rightJoinFlag
	 * @return
	 */
	public abstract String selectFromJoiningTables(String leftTableName,
			Map<String, SemossDataType> leftTableTypes, String rightTableName,
			Map<String, SemossDataType> rightTableTypes, List<Join> joins, Map<String, String> leftTableAlias,
			Map<String, String> rightTableAlias, boolean rightJoinFlag);

	/////////////////////////////////////////////////////////////////////////////////////

	/**
	 * Does the RDBMS type support array data types
	 * 
	 * @return
	 */
	public abstract boolean allowArrayDatatype();

	/**
	 * Does the RDBMS type support boolean types
	 * @return
	 */
	public abstract boolean allowBooleanDataType();
	
	/**
	 * Get the date time data type used by the RDBMS
	 * @return
	 */
	public abstract String getDateWithTimeDataType();
	
	/**
	 * Does the RDBMS type support blob data type
	 * @return
	 */
	public abstract boolean allowBlobDataType();
	
	/**
	 * Does the RDBMS type support blob java object storage
	 * i.e. - connection.createBlob();
	 * @return
	 */
	public abstract boolean allowBlobJavaObject();
	
	/**
	 * 
	 * @param conn
	 * @param statement
	 * @param object
	 * @param index
	 * @throws SQLException
	 * @throws UnsupportedEncodingException 
	 */
	public abstract void handleInsertionOfBlob(Connection conn, PreparedStatement statement, String object, int index) throws SQLException, UnsupportedEncodingException;
	
	/**
	 * 
	 * @param result
	 * @param key
	 * @return
	 * @throws IOException 
	 * @throws SQLException 
	 */
	public abstract String handleBlobRetrieval(ResultSet result, String key) throws SQLException, IOException;
	
	/**
	 * 
	 * @param result
	 * @param index
	 * @return
	 * @throws IOException 
	 * @throws SQLException 
	 */
	public abstract String handleBlobRetrieval(ResultSet result, int index) throws SQLException, IOException;
	
	/**
	 * 
	 * @param conn
	 * @param statement
	 * @param object
	 * @param index
	 * @param gson
	 * @throws SQLException
	 * @throws UnsupportedEncodingException
	 */
	public abstract void handleInsertionOfClob(Connection conn, PreparedStatement statement, Object object, int index, Gson gson) throws SQLException, UnsupportedEncodingException;
	
	/**
	 * Get the RDBMS type name for blob type (BLOB is ANSI)
	 * @return
	 */
	public abstract String getBlobDataTypeName();
	
	/**
	 * Get the RDBMS type equivalent for clob type (CLOB is ANSI)
	 * @return
	 */
	public abstract String getClobDataTypeName();
	
	/**
	 * Get the RDBMS type equivalent for varchar() type
	 * @return
	 */
	public abstract String getVarcharDataTypeName();
	
	/**
	 * Get the RDBMS type equivalent for boolean type
	 * @return
	 */
	public abstract String getBooleanDataTypeName();

	/**
	 * Get the RDBMS type equivalent for int type
	 * @return
	 */
	public abstract String getIntegerDataTypeName();
	
	/**
	 * Get the RDBMS type equivalent for double type
	 * @return
	 */
	public abstract String getDoubleDataTypeName();
	
	/**
	 * Get the RDBMS type equivalent for image data type
	 * @return
	 */
	public abstract String getImageDataTypeName();
	
	/**
	 * Does the RDBMS type support clob java object storage
	 * i.e. - connection.createClob();
	 * @return
	 */
	public abstract boolean allowClobJavaObject();

	/**
	 * Does the engine allow you to add a column to an existing table
	 * 
	 * @return
	 */
	public abstract boolean allowAddColumn();

	/**
	 * Does the engine allow you to add multiple columns in a single statement
	 * 
	 * @return
	 */
	public abstract boolean allowMultiAddColumn();

	/**
	 * Does the engine allow you to rename a column in an existing table
	 * 
	 * @return
	 */
	public abstract boolean allowRedefineColumn();

	/**
	 * Does the engine allow you to drop a column in an existing table
	 * 
	 * @return
	 */
	public abstract boolean allowDropColumn();

	/**
	 * Does the engine allow you to drop multiple columns in a single statement
	 * 
	 * @return
	 */
	public abstract boolean allowMultiDropColumn();
	
	/**
	 * Does the engine allow "CREATE TABLE IF NOT EXISTS " syntax
	 * 
	 * @return
	 */
	public abstract boolean allowsIfExistsTableSyntax();

	/**
	 * Does the engine allow "CREATE INDEX IF NOT EXISTS " syntax
	 * 
	 * @return
	 */
	public abstract boolean allowIfExistsIndexSyntax();

	/**
	 * Does the engine allow "ALTER TABLE xxx ADD COLUMN IF NOT EXISTS" and "ALTER
	 * TABLE xxx DROP COLUMN IF EXISTS" syntax
	 * 
	 * @return
	 */
	public abstract boolean allowIfExistsModifyColumnSyntax();
	
	/**
	 * Does the engine allow " ADD CONSTRAINT IF NOT EXISTS " syntax
	 * 
	 * @return
	 */
	public abstract boolean allowIfExistsAddConstraint();

	/**
	 * Is the savepoint auto released? 
	 * If true, then dont need to release savepoint / method might not be implemented and throw error
	 * @return
	 */
	public abstract boolean savePointAutoRelease();
	
	/////////////////////////////////////////////////////////////////////////

	/*
	 * Create table scripts
	 */

	/**
	 * Create a new table with passed in columns + types + default values
	 * 
	 * @param tableName
	 * @param colNames
	 * @param types
	 * @return
	 */
	public abstract String createTable(String tableName, String[] colNames, String[] types);

	/**
	 * Create a new table with passed in columns + types + default values
	 * 
	 * @param tableName
	 * @param colToTypeMap
	 * @return
	 */
	public abstract String createTable(String tableName, Map<String, String> colToTypeMap);
	
	/**
	 * Create a new table with passed in columns + types + default values
	 * 
	 * @param tableName
	 * @param colNames
	 * @param types
	 * @param defaultValues
	 * @return
	 */
	public abstract String createTableWithDefaults(String tableName, String[] colNames, String[] types, Object[] defaultValues);

	/**
	 * Create a new table with custom constraints
	 * 
	 * @param tableName
	 * @param colNames
	 * @param types
	 * @param customConstraints
	 * @return
	 */
	public abstract String createTableWithCustomConstraints(String tableName, String[] colNames, String[] types, Object[] customConstraints);

	/**
	 * Create a new table if it does not exist with passed in columns + types +
	 * default values
	 * 
	 * @param tableName
	 * @param colNames
	 * @param types
	 * @return
	 */
	public abstract String createTableIfNotExists(String tableName, String[] colNames, String[] types);

	/**
	 * Create a new table if it does not exist with passed in columns + types +
	 * default values
	 * 
	 * @param tableName
	 * @param colNames
	 * @param types
	 * @param defaultValues
	 * @return
	 */
	public abstract String createTableIfNotExistsWithDefaults(String tableName, String[] colNames, String[] types, Object[] defaultValues);

	/**
	 * Create a new table if it does not exist with custom constraints
	 * 
	 * @param tableName
	 * @param colNames
	 * @param types
	 * @param customConstraints
	 * @return
	 */
	public abstract String createTableIfNotExistsWithCustomConstraints(String tableName, String[] colNames, String[] types, Object[] customConstraints);
	
	/*
	 * Drop table scripts
	 */

	/**
	 * Drop a table
	 * 
	 * @param tableName
	 * @return
	 */
	public abstract String dropTable(String tableName);

	/**
	 * Drop a table if it exists
	 * 
	 * @param tableName
	 * @return
	 */
	public abstract String dropTableIfExists(String tableName);

	/*
	 * Alter table scripts
	 */

	/**
	 * Rename a table
	 * 
	 * @param tableName
	 * @param newName
	 * @return
	 */
	public abstract String alterTableName(String tableName, String newTableName);

	/**
	 * Add a new column to an existing table
	 * 
	 * @param tableName
	 * @param newColumn
	 * @param newColType
	 * @return
	 */
	public abstract String alterTableAddColumn(String tableName, String newColumn, String newColType);

	/**
	 * Add a new column to an existing table with default value
	 * 
	 * @param tableName
	 * @param newColumn
	 * @param newColType
	 * @param defaultValue
	 * @return
	 */
	public abstract String alterTableAddColumnWithDefault(String tableName, String newColumn, String newColType, Object defualtValue);

	/**
	 * Add a new column to an existing table if the column does not exist
	 * 
	 * @param tableName
	 * @param newColumn
	 * @param newColType
	 * @return
	 */
	public abstract String alterTableAddColumnIfNotExists(String tableName, String newColumn, String newColType);

	/**
	 * Add a new column to an existing table if the column does not exist with
	 * default value
	 * 
	 * @param tableName
	 * @param newColumn
	 * @param newColType
	 * @param defaultValue
	 * @return
	 */
	public abstract String alterTableAddColumnIfNotExistsWithDefault(String tableName, String newColumn,
			String newColType, Object defualtValue);

	/**
	 * Add new columns to an existing table
	 * 
	 * @param tableName
	 * @param newColumns
	 * @param newColTypes
	 * @return
	 */
	public abstract String alterTableAddColumns(String tableName, String[] newColumns, String[] newColTypes);

	/**
	 * Add new columns to an existing table
	 * 
	 * @param tableName
	 * @param newColToTypeMap
	 * @return
	 */
	public abstract String alterTableAddColumns(String tableName, Map<String, String> newColToTypeMap);
	
	/**
	 * Add new columns to an existing table with default value
	 * 
	 * @param tableName
	 * @param newColumns
	 * @param newColTypes
	 * @param defaultValue
	 * @return
	 */
	public abstract String alterTableAddColumnsWithDefaults(String tableName, String[] newColumns, String[] newColTypes, Object[] defaultValues);

	/**
	 * Drop a column from an existing table
	 * 
	 * @param tableName
	 * @param columnName
	 * @return
	 */
	public abstract String alterTableDropColumn(String tableName, String columnName);

	/**
	 * Drop a column from an existing table if it exists
	 * 
	 * @param tableName
	 * @param columnName
	 * @return
	 */
	public abstract String alterTableDropColumnIfExists(String tableName, String columnName);

	/**
	 * Drop columns from an existing table
	 * 
	 * @param tableName
	 * @param columnNames
	 * @return
	 */
	public abstract String alterTableDropColumns(String tableName, Collection<String> columnNames);

	/**
	 * Modify a column definition
	 * 
	 * @param tableName
	 * @param columnName
	 * @param dataType
	 * @return
	 */
	public abstract String modColumnType(String tableName, String columnName, String dataType);

	/**
	 * Modify a column definition with default value
	 * 
	 * @param tableName
	 * @param columnName
	 * @param dataType
	 * @param defaultValue
	 * @return
	 */
	public abstract String modColumnTypeWithDefault(String tableName, String columnName, String dataType, Object defualtValue);

	/**
	 * Modify a column definition if it exists
	 * 
	 * @param tableName
	 * @param columnName
	 * @param dataType
	 * @return
	 */
	public abstract String modColumnTypeIfExists(String tableName, String columnName, String dataType);

	/**
	 * Modify a column definition with a default value if it exists
	 * 
	 * @param tableName
	 * @param columnName
	 * @param dataType
	 * @param defaultValue
	 * @return
	 */
	public abstract String modColumnTypeIfExistsWithDefault(String tableName, String columnName, String dataType, Object defualtValue);

	/**
	 * Modify a column to not allow nulls
	 * @param tableName
	 * @param columnName
	 * @param dataType
	 * @return
	 */
	public abstract String modColumnNotNull(String tableName, String columnName, String dataType);
	
	/**
	 * Modify a column name in a table
	 * @param tableName
	 * @param curColName
	 * @param newColName
	 * @return
	 */
	public abstract String modColumnName(String tableName, String curColName, String newColName);
	
	/*
	 * Index
	 */

	/**
	 * Create an index on a table for a given column
	 * 
	 * @param indexName
	 * @param tableName
	 * @param column
	 * @return
	 */
	public abstract String createIndex(String indexName, String tableName, String columnName);

	/**
	 * Create an index on a table with a set of columns
	 * 
	 * @param indexName
	 * @param tableName
	 * @param columns
	 * @return
	 */
	public abstract String createIndex(String indexName, String tableName, Collection<String> columns);

	/**
	 * Create an index on a table for a given column
	 * 
	 * @param indexName
	 * @param tableName
	 * @param column
	 * @return
	 */
	public abstract String createIndexIfNotExists(String indexName, String tableName, String columnName);

	/**
	 * Create an index on a table with a set of columns
	 * 
	 * @param indexName
	 * @param tableName
	 * @param columns
	 * @return
	 */
	public abstract String createIndexIfNotExists(String indexName, String tableName, Collection<String> columns);

	/**
	 * Drop an existing index
	 * 
	 * @param indexName
	 * @param tableName
	 * @return
	 */
	public abstract String dropIndex(String indexName, String tableName);

	/**
	 * Drop an index if it exists
	 * 
	 * @param indexName
	 * @param tableName
	 * @return
	 */
	public abstract String dropIndexIfExists(String indexName, String tableName);

	/**
	 * Insert a row into a table
	 * 
	 * @param tableName
	 * @param columnNames
	 * @param types
	 * @param values
	 * @return
	 */
	public abstract String insertIntoTable(String tableName, String[] columnNames, String[] types, Object[] values);

	/**
	 * Drop all rows from a table
	 * 
	 * @param tableName
	 * @return
	 */
	public abstract String deleteAllRowsFromTable(String tableName);

	/**
	 * Quick syntax to copy a table into another table
	 * 
	 * @param newTableName
	 * @param oldTableName
	 * @return
	 */
	public abstract String copyTable(String newTableName, String oldTableName);
	
	/////////////////////////////////////////////////////////////////////////////////////

	/*
	 * Query database scripts
	 */

	/**
	 * Query to execute if has next, the table exists 
	 * The database and schema input is optional and only required by certain engines
	 * 
	 * @param tableName
	 * @param database
	 * @param schema
	 * @return
	 */
	public abstract String tableExistsQuery(String tableName, String database, String schema);

	/**
	 * Query to execute if has next, the table constraint exists
	 * The database and schema input is optional and only required by certain engines
	 * 
	 * @param constraintName
	 * @param tableName
	 * @param database
	 * @param schema
	 * @return
	 */
	public abstract String tableConstraintExistsQuery(String constraintName, String tableName, String database, String schema);
	
	/**
	 * Query to execute if has next, the referential constraint exists
	 * The database and schema input is optional and only required by certain engines
	 * 
	 * @param constraintName
	 * @param database
	 * @param schema
	 * @return
	 */
	public abstract String referentialConstraintExistsQuery(String constraintName, String database, String schema);
	
	/**
	 * Query to get the list of column names for a table 
	 * The schema input is optional and only required by certain engines
	 * Returns the column name and column type
	 *
	 * @param tableName
	 * @param database
	 * @param schema
	 * @return
	 */
	public abstract String getAllColumnDetails(String tableName, String database, String schema);

	/**
	 * Query to execute to get the column details 
	 * Can also imply if the query returns that the column exists
	 * 
	 * @param tableName
	 * @param columnName
	 * @param database
	 * @param schema
	 * @return
	 */
	public abstract String columnDetailsQuery(String tableName, String columnName, String database, String schema);

	/**
	 * Query to get a list of all the indexes in the schema Since indexes are not
	 * unique across tables, this must return (index based) 1) INDEX_NAME 2)
	 * TABLE_NAME The schema input is optional and only required by certain engines
	 * 
	 * @param database
	 * @param schema
	 * @return
	 */
	public abstract String getIndexList(String database, String schema);

	/**
	 * Query to get the index details Must return data in the following order (index
	 * based) 1) TABLE_NAME 2) COLUMN_NAME The schema input is optional and only
	 * required by certain engines
	 * 
	 * @param indexName
	 * @param tableName
	 * @param database
	 * @param schema
	 * @return
	 */
	public abstract String getIndexDetails(String indexName, String tableName, String database, String schema);

	/**
	 * Query to get all the indexes on a given table Must return the data in the
	 * following order (index based) 1) INDEX NAME 2) COLUMN_NAME The schema input
	 * is optional and only required by certain engines
	 * 
	 * @param tableName
	 * @param database
	 * @param schema
	 * @return
	 */
	public abstract String allIndexForTableQuery(String tableName, String database, String schema);

	/////////////////////////////////////////////////////////////////////////////////////

	/*
	 * Utility methods
	 */

	/**
	 * Test on the connection if a table exists Assumption that the conn and sql
	 * util are of same type
	 * 
	 * @param conn
	 * @param tableName
	 * @param database
	 * @param schema
	 * @return
	 */
	public boolean tableExists(Connection conn, String tableName, String database, String schema) {
		String query = this.tableExistsQuery(tableName, database, schema);
		Statement stmt = null;
		ResultSet rs = null;
		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery(query);

			return rs.next();
		} catch (SQLException e) {
			return false;
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}

	/**
	 * Test on the connection if a table exists Assumption that the conn and sql
	 * util are of same type
	 * 
	 * @param engine
	 * @param tableName
	 * @param database
	 * @param schema
	 * @return
	 */
	public boolean tableExists(IDatabaseEngine engine, String tableName, String database, String schema) {
		String query = this.tableExistsQuery(tableName, database, schema);
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(engine, query);
			if (wrapper.hasNext()) {
				return true;
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if (wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}

		return false;
	}

	/**
	 * Helper method to see if an index exists based on Query Utility class
	 * 
	 * @param engine
	 * @param indexName
	 * @param tableName
	 * @param database
	 * @param schema
	 * @return
	 */
	public boolean indexExists(IDatabaseEngine engine, String indexName, String tableName, String database, String schema) {
		String indexCheckQ = this.getIndexDetails(indexName, tableName, database, schema);
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(engine, indexCheckQ);
			if (wrapper.hasNext()) {
				return true;
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if (wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}

		return false;
	}
	
	/**
	 * Test on the connection if a constraint exists
	 * 
	 * @param conn
	 * @param constraintName
	 * @param tableName
	 * @param database
	 * @param schema
	 * @return
	 */
	public boolean tableConstraintExists(Connection conn, String constraintName, String tableName, String database, String schema) {
		String query = this.tableConstraintExistsQuery(constraintName, tableName, database, schema);
		Statement stmt = null;
		ResultSet rs = null;
		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery(query);

			return rs.next();
		} catch (SQLException e) {
			return false;
		} finally {
			ConnectionUtils.closeAllConnections(null, stmt, rs);
		}
	}
	
	/**
	 * Test on the engine if a constraint exists
	 * 
	 * @param engine
	 * @param constraintName
	 * @param tableName
	 * @param database
	 * @param schema
	 * @return
	 */
	public boolean tableConstraintExists(IDatabaseEngine engine, String constraintName, String tableName, String database, String schema) {
		String query = this.tableConstraintExistsQuery(constraintName, tableName, database, schema);
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(engine, query);
			if (wrapper.hasNext()) {
				return true;
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if (wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}

		return false;
	}

	/**
	 * Test on the connection if a constraint exists
	 * 
	 * @param conn
	 * @param constraintName
	 * @param database
	 * @param schema
	 * @return
	 */
	public boolean referentialConstraintExists(Connection conn, String constraintName, String database, String schema) {
		String query = this.referentialConstraintExistsQuery(constraintName, database, schema);
		Statement stmt = null;
		ResultSet rs = null;
		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery(query);

			return rs.next();
		} catch (SQLException e) {
			return false;
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}
	
	/**
	 * Test on the engine if a constraint exists
	 * 
	 * @param engine
	 * @param constraintName
	 * @param database
	 * @param schema
	 * @return
	 */
	public boolean referentialConstraintExists(IDatabaseEngine engine, String constraintName, String database, String schema) {
		String query = this.referentialConstraintExistsQuery(constraintName, database, schema);
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(engine, query);
			if (wrapper.hasNext()) {
				return true;
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if (wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}

		return false;
	}
	
	/**
	 * Get all the table columns Will return them all upper cased
	 * 
	 * @param conn
	 * @param tableName
	 * @param database
	 * @param schema
	 * @return
	 */
	public List<String> getTableColumns(Connection conn, String tableName, String database, String schema) {
		List<String> tableColumns = new ArrayList<>();
		String query = this.getAllColumnDetails(tableName, database, schema);
		Statement stmt = null;
		ResultSet rs = null;
		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery(query);
			while (rs.next()) {
				tableColumns.add(rs.getString(1).toUpperCase());
			}
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}

		return tableColumns;
	}
	
	/**
	 * Get the details for a specific column
	 * @param conn
	 * @param tableName
	 * @param columnName
	 * @param database
	 * @param schema
	 * @return
	 */
	public String[] getColumnDetails(Connection conn, String tableName, String columnName, String database, String schema) {
		String query = this.columnDetailsQuery(tableName, columnName, database, schema);
		Statement stmt = null;
		ResultSet rs = null;
		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery(query);
			while (rs.next()) {
				return new String[] { rs.getString(1).toUpperCase(), rs.getString(2) };
			}
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}

		return null;
	}
	
	public static DatabaseUpdateMetadata performDatabaseAdditions(IRDBMSEngine rdbmsDb, Map<String, Map<String, String>> updates, Logger logger) throws InterruptedException {
		DatabaseUpdateMetadata meta = new DatabaseUpdateMetadata();
		
		Set<String> tableExists = new HashSet<>();

		AbstractSqlQueryUtil queryUtil = rdbmsDb.getQueryUtil();
		Map<String, String> typeConversionMap = queryUtil.getTypeConversionMap();
		
		try {
			Connection conn = rdbmsDb.getConnection();
			String database = rdbmsDb.getDatabase();
			String schema = rdbmsDb.getSchema();
			
			// first run a validation on the input
			for(String tableName : updates.keySet()) {
				logger.info("Validating table " + tableName);
				if(queryUtil.tableExists(rdbmsDb, tableName, database, schema)) {
					logger.info("Validating columns for " + tableName);
					// we are altering - make sure everything is valid
					
					List<String> currentColumns = queryUtil.getTableColumns(conn, tableName, database, schema);
					Set<String> currentColumnsLower = currentColumns.stream().map(s -> s.toLowerCase()).collect(Collectors.toSet());
					
					Set<String> newColumns = updates.get(tableName).keySet();
					Set<String> newColumnsLower = newColumns.stream().map(s -> s.toLowerCase()).collect(Collectors.toSet());
					
					Set<String> overlap = newColumnsLower.stream().filter(s -> currentColumnsLower.contains(s)).collect(Collectors.toSet());
					if(!overlap.isEmpty()) {
						throw new IllegalArgumentException("The following column names already exist: " + overlap);
					}

					tableExists.add(tableName);
				}
			}
		} catch(SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error validating the input. Detailed message = " + e.getMessage());
		}
		
		// create an owler to track the meta modifications
		WriteOWLEngine owlEngine = rdbmsDb.getOWLEngineFactory().getWriteOWL();
		meta.setOwlEngine(owlEngine);
		
		StringBuilder errorMessages = new StringBuilder();
		// now do the operations
		for(String tableName : updates.keySet()) {

			Map<String, String> finalColumnUpdates = new HashMap<>(updates.size());

			Map<String, String> columnUpdates = updates.get(tableName);
			for(String column : columnUpdates.keySet()) {

				String columnType = columnUpdates.get(column).toUpperCase();
				if(typeConversionMap.containsKey(columnType)) {
					columnType = typeConversionMap.get(columnType);
				}

				finalColumnUpdates.put(column, columnType);
			}

			String query = null;
			if(tableExists.contains(tableName)) {
				logger.info("Altering table " + tableName);
				query = queryUtil.alterTableAddColumns(tableName, finalColumnUpdates);
			} else {
				logger.info("Creating table " + tableName);
				query = queryUtil.createTable(tableName, finalColumnUpdates);
			}
			try {
				rdbmsDb.insertData(query);
				
				// add to the owl
				logger.info("Updating metadata for table " + tableName);
				owlEngine.addConcept(tableName, null, null);
				for(String column : finalColumnUpdates.keySet()) {
					String columnType = finalColumnUpdates.get(column);
					owlEngine.addProp(tableName, column, columnType);
				}
				
				// store the metadata
				meta.addSuccessfulUpdate(tableName);
			} catch(Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
				errorMessages.append("Error executing query = '" + query +"' with detailed error = " + e.getMessage() + ". ");
				meta.addFailedUpdates(tableName);
			}
		}
		
		meta.setCombinedErrors(errorMessages.toString());
		
		return meta;
	}

	public static DatabaseUpdateMetadata performDatabaseDeletions(IRDBMSEngine rdbmsDb, Map<String, List<String>> updates, Logger logger) throws InterruptedException {
		DatabaseUpdateMetadata meta = new DatabaseUpdateMetadata();
		Set<String> tableDeletes = new HashSet<>();
		AbstractSqlQueryUtil queryUtil = rdbmsDb.getQueryUtil();
		
		// validate that the tables and columns provided exist, and tag tables for removal if all or no columns given
		try {
			Connection conn = rdbmsDb.getConnection();
			String database = rdbmsDb.getDatabase();
			String schema = rdbmsDb.getSchema();
			
			for(String tableName : updates.keySet()) {
				logger.info("Validating table " + tableName);
				if(queryUtil.tableExists(rdbmsDb, tableName, database, schema)) {
					logger.info("Validating columns for " + tableName);
					
					List<String> currentColumns = queryUtil.getTableColumns(conn, tableName, database, schema);
					Set<String> currentColumnsLower = currentColumns.stream().map(s -> s.toLowerCase()).collect(Collectors.toSet());
					
					Set<String> givenColumnsLower = updates.get(tableName).stream().map(s -> s.toLowerCase()).collect(Collectors.toSet());
					
					if(givenColumnsLower.isEmpty()) {
						tableDeletes.add(tableName);
					} else {
						Set<String> gap = givenColumnsLower.stream().filter(s -> !currentColumnsLower.contains(s)).collect(Collectors.toSet());
						if(!gap.isEmpty()) {
							throw new IllegalArgumentException("The following column names do not exist in table " + tableName + ": " + gap);
						}
						if(givenColumnsLower.size() == currentColumnsLower.size()) {
							tableDeletes.add(tableName);
						}
					}
				} else {
					throw new IllegalArgumentException("The following table does not exist:" + tableName);
				}
			}
		} catch(SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error validating the input. Detailed message = " + e.getMessage());
		}
		
		// create an owler to track the meta modifications
		WriteOWLEngine owlEngine = rdbmsDb.getOWLEngineFactory().getWriteOWL();
		meta.setOwlEngine(owlEngine);
		
		StringBuilder errorMessages = new StringBuilder();
		// now do the operations
		for(String tableName : updates.keySet()) {
			boolean deleteTable = tableDeletes.contains(tableName);
			String query = null;
			try {
				if(deleteTable) {
					logger.info("Dropping table " + tableName);
					query = queryUtil.dropTable(tableName);
					rdbmsDb.insertData(query);
				} else {
					logger.info("Removing columns from table " + tableName);
					// prefer using multi-drop if supported
					if(queryUtil.allowMultiDropColumn()) {
						query = queryUtil.alterTableDropColumns(tableName, updates.get(tableName));
						rdbmsDb.insertData(query);
					} else {
						for(String columnName : updates.get(tableName)) {
							query = queryUtil.alterTableDropColumn(tableName, columnName);
							rdbmsDb.insertData(query);
						}
					}
				}
				
				// update the owl
				logger.info("Updating metadata for table " + tableName);
				if(deleteTable) {
					owlEngine.removeConcept(tableName);
				} else {
					for(String column : updates.get(tableName)) {
						owlEngine.removeProp(tableName, column);
					}
				}
				// store the metadata
				meta.addSuccessfulUpdate(tableName);
			} catch(Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
				errorMessages.append("Error executing query = '" + query +"' with detailed error = " + e.getMessage() + ". ");
				meta.addFailedUpdates(tableName);
			}
		}
		
		meta.setCombinedErrors(errorMessages.toString());
		return meta;
	}
	
	/**
	 * 
	 * @param value
	 * @return
	 */
	public static int getConnectionTypeValueFromString(String value) {
		if(value == null) {
			return -1;
		}
		
		if(value.equalsIgnoreCase(Constants.TRANSACTION_NONE)) {
			return Connection.TRANSACTION_NONE;
		} else if(value.equalsIgnoreCase(Constants.TRANSACTION_READ_UNCOMMITTED)) {
			return Connection.TRANSACTION_READ_UNCOMMITTED;
		} else if(value.equalsIgnoreCase(Constants.TRANSACTION_READ_COMMITTED)) {
			return Connection.TRANSACTION_READ_COMMITTED;
		} else if(value.equalsIgnoreCase(Constants.TRANSACTION_REPEATABLE_READ)) {
			return Connection.TRANSACTION_REPEATABLE_READ;
		} else if(value.equalsIgnoreCase(Constants.TRANSACTION_SERIALIZABLE)) {
			return Connection.TRANSACTION_SERIALIZABLE;
		}
		
		return -1;
	}

	/////////////////////////////////////////////////////////////////////////////////////

	/*
	 * These are older methods
	 * Need to come back and see where to 
	 * utilize these/clean up
	 */

	public String getDialectSelectRowCountFrom(String tableName, String whereClause) {
		String query = "SELECT COUNT(*) as ROW_COUNT FROM " + tableName;
		if (whereClause.length() > 0) {
			query += " WHERE " + whereClause;
		}
		return query;
	}

	public String getDialectMergeStatement(String tableKey, String insertIntoClause, List<String> columnList,
			HashMap<String, String> whereValues, String fkVal, String whereClause) {
		ArrayList<String> subqueries = new ArrayList<>();
		String query = "INSERT INTO " + tableKey + " (" + insertIntoClause + ") SELECT DISTINCT ";
		for (String column : columnList) {
			String tempColumnName = column + "TEMP";
			String subquery = "(SELECT DISTINCT " + column + " FROM " + tableKey + " WHERE " + whereClause;
			String tempquery = subquery + " union select null where not exists" + subquery + ")) AS " + tempColumnName;
			subqueries.add(tempquery);
			query += tempColumnName + "." + column + " AS " + column + ",";
		}
		for (String whereKey : whereValues.keySet()) {
			query += whereValues.get(whereKey) + " AS " + whereKey + ", ";
		}
		query += fkVal + " FROM " + tableKey;
		for (int i = 0; i < subqueries.size(); i++) {
			query += ", " + subqueries.get(i);
		}
		return query;
	}

	public String hashColumn(String tableName, String[] columns){
		throw new UnsupportedOperationException();
	}

	///////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////

//	public static void main(String[] args) throws Exception {
//		TestUtilityMethods.loadAll("C:\\workspace2\\Semoss_Dev\\RDF_Map.prop");
//
//		RDBMSNativeEngine security = (RDBMSNativeEngine) Utility.getEngine("security");
//		AbstractSqlQueryUtil util = security.getQueryUtil();
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(security,
//				"SELECT * FROM PRAGMA_TABLE_INFO('USER') WHERE NAME='email'");
//		while (wrapper.hasNext()) {
//			logger.debug(wrapper.next());
//		}
//	}


}