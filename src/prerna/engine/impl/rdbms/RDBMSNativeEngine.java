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
package prerna.engine.impl.rdbms;

import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.h2.tools.DeleteDbFiles;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;

import com.zaxxer.hikari.HikariDataSource;

import prerna.engine.api.IEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.AbstractEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.rdf.util.AbstractQueryParser;
import prerna.rdf.util.SQLQueryParser;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.PersistentHash;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RDBMSUtility;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SqlQueryUtilFactory;

public class RDBMSNativeEngine extends AbstractEngine implements IRDBMSEngine {

	private static final Logger logger = LogManager.getLogger(RDBMSNativeEngine.class);

	public static final String STATEMENT_OBJECT = "STATEMENT_OBJECT";
	public static final String RESULTSET_OBJECT = "RESULTSET_OBJECT";
	public static final String CONNECTION_OBJECT = "CONNECTION_OBJECT";
	public static final String ENGINE_CONNECTION_OBJECT = "ENGINE_CONNECTION_OBJECT";
	public static final String DATASOURCE_POOLING_OBJECT = "DATASOURCE_POOLING_OBJECT";

	public static final String USE_FILE = "USE_FILE";
	public static final String DATA_FILE = "DATA_FILE";

	private boolean engineConnected = false;
	private boolean datasourceConnected = false;
	private RdbmsTypeEnum dbType;
	private HikariDataSource dataSource = null;
	private Connection engineConn = null;
	private boolean useConnectionPooling = false;
	private boolean autoCommit = false;

	public PersistentHash conceptIdHash = null;

	private RdbmsConnectionBuilder connBuilder;
	private String userName = null;
	private String password = null;
	private String driver = null;
	private String connectionURL = null;
	private String database = null;
	private String schema = null;
	// parameterized in SMSS
	// fetch size -1 which will 
	private int fetchSize = -1;
	private int poolMinSize = 1;
	private int poolMaxSize = 16;
	private int queryTimeout = -1;
	
	private long leakDetectionThresholdMilliseconds = 30_000;
	private long idelTimeout = 60_000;
	
	private AbstractSqlQueryUtil queryUtil = null;

	private String fileDB = null;
	private String createString = null;

	@Override
	public void openDB(String propFile)
	{
		if(propFile == null && this.prop == null){
			if(dataSource!= null){
				try{
					this.engineConn = getConnection();
					this.engineConnected = true;
					this.autoCommit = this.engineConn.getAutoCommit();
				} catch (Exception e){
					logger.error("error RDBMS opening database", e);
				}
			} else {
				logger.info("using engine connection");
			}
		} else {
			// will mostly be sent the connection string and I will connect here
			// I need to see if the connection pool has been initiated
			// if not initiate the connection pool
			if(this.prop == null) {
				this.prop = Utility.loadProperties(propFile);
				// if this is not a temp then open the super
				if(!this.prop.containsKey("TEMP")) { 
					// not temp, in which case, this engine has a insights rdbms and an owl
					// so call super to open them and set them in the engine
					super.openDB(propFile);
				}
			}

			// grab the values from the prop file 
			String dbTypeString = prop.getProperty(Constants.RDBMS_TYPE);
			if(dbTypeString == null) {
				dbTypeString = prop.getProperty(AbstractSqlQueryUtil.DRIVER_NAME);
				if(dbTypeString == null) {
					dbTypeString = prop.getProperty(AbstractSqlQueryUtil.DRIVER_NAME.toUpperCase());
				}
			}
			this.driver = prop.getProperty(Constants.DRIVER);
			// get the dbType from the input or from the driver itself
			this.dbType = (dbTypeString != null) ? RdbmsTypeEnum.getEnumFromString(dbTypeString) : RdbmsTypeEnum.getEnumFromDriver(this.driver);
			if(this.dbType == null) {
				this.dbType = RdbmsTypeEnum.H2_DB;
			}
			// make the query util first
			// since this will help with getting the correct keys for the connection
			this.queryUtil = SqlQueryUtilFactory.initialize(this.dbType);

			// get the database so we have it - can be used for filtering tables/columns
			this.database = prop.getProperty(AbstractSqlQueryUtil.DATABASE.toUpperCase());
			if(this.database == null) {
				this.database = prop.getProperty(AbstractSqlQueryUtil.DATABASE);
			}
			
			// get the schema so we have it - can be used for filtering tables/columns
			this.schema = prop.getProperty(AbstractSqlQueryUtil.SCHEMA.toUpperCase());
			if(this.schema == null) {
				this.schema = prop.getProperty(AbstractSqlQueryUtil.SCHEMA);
			}
			
			// grab the username/password
			// keys can be username/password
			// but some will have it as accessKey/secretKey
			// so accounting for that here
			this.userName = prop.getProperty(queryUtil.getConnectionUserKey());
			if(propFile != null) {
				this.password = decryptPass(propFile, false);
			} 
			if(this.password == null) {
				this.password = prop.containsKey(queryUtil.getConnectionPasswordKey()) ? prop.getProperty(queryUtil.getConnectionPasswordKey()) : "";
			}

			// grab the connection url
			this.connectionURL = prop.getProperty(Constants.CONNECTION_URL);
			if(this.dbType == RdbmsTypeEnum.H2_DB || this.dbType == RdbmsTypeEnum.SQLITE) {
				this.connectionURL = RDBMSUtility.fillParameterizedFileConnectionUrl(this.connectionURL, this.engineId, this.engineName);
			}
			
			// make a check to see if it is asking to use file
			boolean useFile = false;
			if(prop.containsKey(USE_FILE)) {
				useFile = Boolean.valueOf(prop.getProperty(USE_FILE));
			}

			// see if connection pooling
			this.useConnectionPooling = Boolean.valueOf(prop.getProperty(Constants.USE_CONNECTION_POOLING));

			// fetch size
			if(prop.getProperty(Constants.FETCH_SIZE) != null) {
				String strFetchSize = prop.getProperty(Constants.FETCH_SIZE);
				try {
					this.fetchSize = Integer.parseInt(strFetchSize);
				} catch(Exception e) {
					System.out.println("Error occured trying to parse and get the fetch size");
					logger.error(Constants.STACKTRACE, e);
				}
			}
			// connection timeout
			if(prop.getProperty(Constants.CONNECTION_QUERY_TIMEOUT) != null) {
				String queryTimeoutStr = prop.getProperty(Constants.CONNECTION_QUERY_TIMEOUT);
				try {
					this.queryTimeout = Integer.parseInt(queryTimeoutStr);
				} catch(Exception e) {
					System.out.println("Error occured trying to parse and get the query timeout");
					logger.error(Constants.STACKTRACE, e);
				}
			}
			// leak detection threshold
			if(prop.getProperty(Constants.LEAK_DETECTION_THRESHOLD_MILLISECONDS) != null) {
				String leakDetectionStr = prop.getProperty(Constants.LEAK_DETECTION_THRESHOLD_MILLISECONDS);
				try {
					this.leakDetectionThresholdMilliseconds = Long.parseLong(leakDetectionStr);
				} catch(Exception e) {
					System.out.println("Error occured trying to parse and get the leak detection threshold");
					logger.error(Constants.STACKTRACE, e);
				}
			}
			// idle timeout
			if(prop.getProperty(Constants.IDLE_TIMEOUT) != null) {
				String idleTimeoutStr = prop.getProperty(Constants.IDLE_TIMEOUT);
				try {
					this.idelTimeout = Long.parseLong(idleTimeoutStr);
				} catch(Exception e) {
					System.out.println("Error occured trying to parse and get the idle timeout");
					logger.error(Constants.STACKTRACE, e);
				}
			}
			// pool min size
			if(prop.getProperty(Constants.POOL_MIN_SIZE) != null) {
				String strMinPoolSize = prop.getProperty(Constants.POOL_MIN_SIZE);
				try {
					this.poolMinSize = Integer.parseInt(strMinPoolSize);
				} catch(Exception e) {
					System.out.println("Error occured trying to parse and get the min pool size");
					logger.error(Constants.STACKTRACE, e);
				}
			}
			// pool max size
			if(prop.getProperty(Constants.POOL_MAX_SIZE) != null) {
				String strMaxPoolSize = prop.getProperty(Constants.POOL_MAX_SIZE);
				try {
					this.poolMaxSize = Integer.parseInt(strMaxPoolSize);
				} catch(Exception e) {
					System.out.println("Error occured trying to parse and get the max pool size");
					logger.error(Constants.STACKTRACE, e);
				}
			}

			this.connBuilder = null;
			if(useFile) {
				connBuilder = new RdbmsConnectionBuilder(RdbmsConnectionBuilder.CONN_TYPE.BUILD_FROM_FILE);

				// determine the location of the file relative to where SEMOSS is installed
				this.fileDB = SmssUtilities.getDataFile(prop).getAbsolutePath();
				// set the file location
				connBuilder.setFileLocation(this.fileDB);

				// set the types
				Vector<String> concepts = this.getConcepts();
				String [] conceptsArray = concepts.toArray(new String[concepts.size()]);
				Map <String,String> conceptAndType = this.getDataTypes(conceptsArray);
				for(int conceptIndex = 0;conceptIndex < conceptsArray.length;conceptIndex++) {
					List<String> propList = getPropertyUris4PhysicalUri(conceptsArray[conceptIndex]);
					String [] propArray = propList.toArray(new String[propList.size()]);
					Map<String, String> typeMap = getDataTypes(propArray);
					conceptAndType.putAll(typeMap);
				}
				connBuilder.setColumnToTypesMap(conceptAndType);

				// also update the connection url
				Hashtable<String, String> paramHash = new Hashtable<String, String>();
				String dbName = this.fileDB.replace(".csv", "").replace(".tsv", "");
				paramHash.put("database", dbName);
				this.connectionURL = Utility.fillParam2(connectionURL, paramHash);

			} else if(useConnectionPooling) {
				connBuilder = new RdbmsConnectionBuilder(RdbmsConnectionBuilder.CONN_TYPE.CONNECTION_POOL);
			} else {
				connBuilder = new RdbmsConnectionBuilder(RdbmsConnectionBuilder.CONN_TYPE.DIRECT_CONN_URL);
			}

			connBuilder.setConnectionUrl(this.connectionURL);
			connBuilder.setUserName(this.userName);
			connBuilder.setPassword(this.password);
			connBuilder.setDriver(this.driver);

			init(connBuilder);

			try {
				// update the query utility values
				this.queryUtil.setConnectionUrl(this.connectionURL);
				this.queryUtil.setUsername(this.userName);
				this.queryUtil.setPassword(this.password);

				// build the connection
				this.engineConn = connBuilder.build();
				if(useConnectionPooling) {
					this.dataSource = connBuilder.getDataSource();
					setDataSourceProperties(this.dataSource);
					this.datasourceConnected = true;
					this.autoCommit = this.engineConn.getAutoCommit();
					this.queryUtil.enhanceConnection(this.engineConn);
					this.engineConn.close();
				} else {
					this.autoCommit = this.engineConn.getAutoCommit();
					this.queryUtil.enhanceConnection(this.engineConn);
				}
				this.engineConnected = true;
			} catch (SQLException e) {
				logger.error(Constants.STACKTRACE, e);
			}
		}
	}	

	/**
	 * This is for when there are other engines that extend
	 * the base RDBMSNativeEngine that need to do additional processing
	 * before a connection can be made
	 * @param connBuilder
	 */
	protected void init(RdbmsConnectionBuilder connBuilder) {
		// default does nothing
		init(connBuilder, false);
	}

	/**
	 * This is for when there are other engines that extend
	 * the base RDBMSNativeEngine that need to do additional processing
	 * before a connection can be made
	 * @param connBuilder
	 * @param force force the init again
	 */
	protected void init(RdbmsConnectionBuilder connBuilder, boolean force) {
		// default does nothing
	}

	protected void setDataSourceProperties(HikariDataSource dataSource) {
		dataSource.setMinimumIdle(this.poolMinSize);
		dataSource.setMaximumPoolSize(this.poolMaxSize);
		dataSource.setLeakDetectionThreshold(this.leakDetectionThresholdMilliseconds);
		dataSource.setIdleTimeout(this.idelTimeout);
//		dataSource.setConnectionTimeout(connectionTimeoutMs);
	}

	@Override
	public AbstractSqlQueryUtil getQueryUtil() {
		return this.queryUtil;
	}

	@Override
	public String getSchema() {
		// if not set in the prop file
		// try to grab from the connection details
		if(this.schema == null) {
			DatabaseMetaData meta = getConnectionMetadata();
			if(meta != null) {
				Connection conn = null;
				try {
					conn = getConnection();
					this.schema = RdbmsConnectionHelper.getSchema(meta, conn, this.connectionURL, this.dbType);
				} catch(SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				} finally {
					if(this.datasourceConnected && conn != null) {
						try {
							conn.close();
						} catch (SQLException e) {
							logger.error(Constants.STACKTRACE, e);
						}
						try {
							meta.getConnection().close();
						} catch (SQLException e) {
							logger.error(Constants.STACKTRACE, e);
							e.printStackTrace();
						}
					}
				}
			}
		}
		return this.schema;
	}

	private void makeConnection(String driver, String userName, String password, String connectionUrl, String createString) {
		Statement stmt = null;
		try {
			Class.forName(driver);
			this.connectionURL = connectionUrl;
			this.userName = userName;
			this.password = password;
			this.engineConn = DriverManager.getConnection(connectionUrl, userName, password);
			if(createString != null) {
				stmt = this.engineConn.createStatement();
				stmt.execute(createString);
			}
			this.engineConnected = true;
			this.autoCommit = this.engineConn.getAutoCommit();
			this.queryUtil = SqlQueryUtilFactory.initialize(RdbmsTypeEnum.getEnumFromDriver(driver), this.connectionURL, this.userName, this.password);
		} catch (ClassNotFoundException e) {
			logger.error(Constants.STACKTRACE, e);
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

	// you cant change owl right now
	public void reloadFile() {
		try {
			this.engineConn.close();
			makeConnection(this.driver, this.userName, this.password, this.connectionURL, this.createString);
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		}
	}

	@Override
	public HikariDataSource getDataSource() {
		return this.dataSource;
	}

	@Override
	public Connection getConnection() throws SQLException {
		if(this.dataSource != null) {
			// re-establish bad connections
			if(this.dataSource.isClosed()) {
				try {
					init(connBuilder, true);
					this.engineConn = connBuilder.build();
					if(useConnectionPooling) {
						this.dataSource = connBuilder.getDataSource();
						setDataSourceProperties(this.dataSource);
						this.datasourceConnected = true;
					}
					this.engineConnected = true;
					this.autoCommit = this.engineConn.getAutoCommit();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}

			// return/generate a connection object
			return dataSource.getConnection();
		}

		// re-establish bad connections
		if(!isConnected() || this.engineConn.isClosed() || !this.engineConn.isValid(1)) {
			init(connBuilder, true);
			this.engineConn = connBuilder.build();
			if(useConnectionPooling) {
				this.dataSource = connBuilder.getDataSource();
				setDataSourceProperties(this.dataSource);
				this.datasourceConnected = true;
			}
			this.engineConnected = true;
			this.autoCommit = this.engineConn.getAutoCommit();
		}

		return this.engineConn;
	}

	@Override
	public void insertData(String query) throws SQLException {
		Connection conn = getConnection(); 
		if(query.startsWith("CREATE") && !(query.startsWith("CREATE DATABASE"))){
			try(Statement statement = conn.createStatement()){
				statement.executeUpdate(query);
			} catch(SQLException e){
				logger.error(Constants.STACKTRACE, e);
				throw e;
			}
		} else {
			try(PreparedStatement statement = conn.prepareStatement(query)){
				statement.execute();
			} catch(SQLException e){
				logger.error(Constants.STACKTRACE, e);
				throw e;
			}
		}
		// if datasource, give back the conn to the pool
		if(this.datasourceConnected) {
			// you have to commit on the connection itself
			if(!autoCommit) {
				conn.commit();
			}
			conn.close();
		}
	}

	private void closeConnections(Connection conn, ResultSet rs, Statement stmt){
		if(isConnected()){
			conn = null;
		}
		ConnectionUtils.closeAllConnections(conn, stmt, null);
	}

	@Override
	public ENGINE_TYPE getEngineType() {
		return IEngine.ENGINE_TYPE.RDBMS;
	}

	@Override
	public Vector<Object> getEntityOfType(String type)
	{
		String table; // table in RDBMS
		String column; // column of table in RDBMS
		String query;

		// ugh... for legacy stuff, we do not have the table name on the property
		// so we need to do the check that the type is not "contains"
		if(type.contains("http://semoss.org/ontologies") && !Utility.getClassName(type).equals("Contains")){
			// we are dealing with the physical uri which is in the form ...Concept/Column/Table
			query = "SELECT DISTINCT " + Utility.getClassName(type) + " FROM " + Utility.getInstanceName(type);
		}
		else if(type.contains("http://semoss.org/ontologies/Relation/Contains")){// this is such a mess... 
			String xmlQuery = "SELECT ?concept WHERE { ?concept rdfs:subClassOf <http://semoss.org/ontologies/Concept>. ?concept <http://www.w3.org/2002/07/owl#DatatypeProperty> <"+type+">}";
			TupleQueryResult ret = (TupleQueryResult) this.execOntoSelectQuery(xmlQuery);
			String conceptURI = null;
			try {
				if(ret.hasNext()){
					BindingSet row = ret.next();
					conceptURI = row.getBinding("concept").getValue().toString();
				}
			} catch (QueryEvaluationException e) {
				logger.error(Constants.STACKTRACE, e);
			}
			query = "SELECT DISTINCT " + Utility.getInstanceName(type) + " FROM " + Utility.getInstanceName(conceptURI);
		}
		else if(type.contains(":")) {
			int tableStartIndex = type.indexOf("-") + 1;
			int columnStartIndex = type.indexOf(":") + 1;
			table = type.substring(tableStartIndex, columnStartIndex - 1);
			column = type.substring(columnStartIndex);
			query = "SELECT DISTINCT " + column + " FROM " + table;
		} else {
			query = "SELECT DISTINCT " + type + " FROM " + type;
		}
		Connection conn = null;
		ResultSet rs = null;
		Statement stmt = null;
		try {
			conn = getConnection();
			stmt = conn.createStatement();
			rs = getResults(stmt, query);
			Vector<Object> columnsFromResult = getColumnsFromResultSet(1, rs);
			return columnsFromResult;
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			closeConnections(conn,rs,stmt);
		}
		return null;

	}

	public Vector<Object> getCleanSelect(String query){
		Connection conn = null;
		ResultSet rs = null;
		Statement stmt = null;
		try {
			conn = getConnection();
			stmt = conn.createStatement();
			rs = getResults(stmt, query);
			Vector<Object> columnsFromResult = getColumnsFromResultSet(1, rs);
			return columnsFromResult;
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			closeConnections(conn,rs,stmt);	
		}
		return null;
	}

	public Map<String, Object> execQuery(String query) throws SQLException
	{
		Map<String, Object> map = new HashMap<>();
		boolean hasError = false;
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		try {
			conn = getConnection();
			stmt = conn.createStatement();
			if(this.queryTimeout > 0) {
				stmt.setQueryTimeout(queryTimeout);
			}
			rs = null;
			//System.out.println("PRINT QUERY: "+ query);
			rs = stmt.executeQuery(query);
			if(this.fetchSize > 0) {
				try {
					rs.setFetchSize(this.fetchSize);
				} catch(Exception e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		} catch(SQLTimeoutException e) {
			hasError = true;
			if(this.queryTimeout > 0) {
				throw new SQLTimeoutException("Query execution cancelled - processing time took longer than set limit of " 
						+ this.queryTimeout + " seconds");
			}
			int statementTimeout = -1;
			try {
				if(stmt != null) {
					statementTimeout = stmt.getQueryTimeout();
				}
			} catch(Exception e2) {
				// ignore
			}
			if(statementTimeout > 0) {
				throw new SQLTimeoutException("Query execution cancelled - processing time took longer than set limit of " 
						+ statementTimeout + " seconds");
			}
			throw e;
		} catch(SQLException e) {
			hasError = true;
			throw e;
		} finally {
			if(hasError) {
				if(this.dataSource != null) {
					ConnectionUtils.closeAllConnections(conn, stmt, rs);
				} else {
					ConnectionUtils.closeAllConnections(null, stmt, rs);
				}
			}
		}
		//normally would use instance.getClass() but when we retrieve the 
		//references from the object we can't guarantee that they will not be null
		//this makes it cleaner and less error prone.
		map.put(RDBMSNativeEngine.RESULTSET_OBJECT, rs);
		if(isConnected()){
			map.put(RDBMSNativeEngine.CONNECTION_OBJECT, null);
			map.put(RDBMSNativeEngine.ENGINE_CONNECTION_OBJECT, conn);
		} else {
			map.put(RDBMSNativeEngine.CONNECTION_OBJECT, conn);
			map.put(RDBMSNativeEngine.ENGINE_CONNECTION_OBJECT, null);
		}
		if(this.dataSource != null) {
			map.put(RDBMSNativeEngine.DATASOURCE_POOLING_OBJECT, this.dataSource);
		}
		map.put(RDBMSNativeEngine.STATEMENT_OBJECT, stmt);
		return map;
	}

	/**
	 * Method to execute Update/Delete statements with the option of closing the Statement object.
	 * 
	 * @param query					Query to execute
	 * @param autoCloseStatement	Option to automatically close the Statement object after query execution
	 * @return
	 */
	public Statement execUpdateAndRetrieveStatement(String query, boolean autoCloseStatement) {
		Connection conn = null;
		PreparedStatement statement = null;
		try {
			conn = getConnection();
			statement = conn.prepareStatement(query);
			statement.executeUpdate();
		} catch (SQLException e) {
			statement = null;
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(autoCloseStatement) {
				closeConnections(conn,null,statement);
			} else {
				closeConnections(conn,null,null);
			}
		}

		return statement;
	}

	@Override
	public boolean isConnected() {
		return engineConn !=null && this.engineConnected;
	}

	@Override
	public boolean isConnectionPooling() {
		return this.useConnectionPooling;
	}

	@Override
	public void closeDB() {
		super.closeDB();
		try {
			if(this.useConnectionPooling){
				this.engineConnected = false;
				ConnectionUtils.closeConnection(this.engineConn);
				closeDataSource();
			} else {
				if(this.engineConn != null && !this.engineConn.isClosed()) {
					if(!this.engineConn.getAutoCommit()) {
						this.engineConn.commit();
					}
					this.shutdown();
					this.engineConn.close();
					this.engineConnected = false;
				}
			}

		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		}
	}

	private void closeDataSource(){
		if(this.dataSource != null) {
			try {
				this.dataSource.close();
				this.datasourceConnected = false;
			} catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
			}
		}
	}

	public Vector getColumnsFromResultSet(int columns, ResultSet rs)
	{
		Vector retVector = new Vector();
		// each value is an array in itself as well
		try {
			while(rs.next())
			{
				ArrayList list = new ArrayList();
				Object output = null;
				for(int colIndex = 1;colIndex <= columns;colIndex++)
				{					
					//					output = rs.getString(colIndex);
					output = rs.getObject(colIndex);
					//					System.out.print(rs.getObject(colIndex));
					list.add(output);
				}
				if(columns == 1)
					retVector.addElement(output);
				else
					retVector.addElement(list);
			}
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		}
		logger.info("Found " + retVector.size() + " elements in result set");
		return retVector;
	}

	/**
	 * Private method that returns a ResultSet object. If you choose to make this method public it make it harder to keep track of the Result set
	 * object and where you need to explicity close it
	 * 
	 * @param conn
	 * @param stmt
	 * @param query
	 * @return ResultSet object
	 * @throws Exception
	 */

	private ResultSet getResults(Statement stmt, String query) throws Exception {
		ResultSet rs = null;
		try {
			rs = stmt.executeQuery(query);
			// return to pool
		} catch (Exception e) {
			logger.error("Error occured in getResults method of RDBMSNativeEngine", e);
		}
		return rs;
	}

	public AbstractQueryParser getQueryParser() {
		return new SQLQueryParser();
	}

	protected void shutdown() {
		// shift to grabing from query util
		if(this.dbType == RdbmsTypeEnum.H2_DB) {
			Connection conn = null;
			Statement stmt = null;
			try {
				conn = getConnection();
				stmt = conn.createStatement();
				stmt.execute("SHUTDOWN");
				// return to pool
			} catch (Exception e) {
				logger.error("Unable to shutdown.", e);
			} finally {
				if(stmt != null) {
					try {
						stmt.close();
					} catch (SQLException e) {
						logger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}
	}

	@Override
	public void removeData(String query) throws SQLException {
		Connection conn = getConnection();
		try(PreparedStatement statement = conn.prepareStatement(query)){
			statement.execute();
		} catch(SQLException e){
			logger.error(Constants.STACKTRACE, e);
			throw e;
		}
		// if datasource, give back the conn to the pool
		if(this.datasourceConnected) {
			// you have to commit on the connection itself
			if(!autoCommit) {
				conn.commit();
			}
			conn.close();
		}
	}

	@Override
	public void commit() {
		try {
			if(!autoCommit) {
				getConnection().commit();
			}
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		}
	}

	public void commitRDBMS() {
		System.out.println("Before commit.. concept id hash size is.. "+ conceptIdHash.thisHash.size());
		conceptIdHash.persistBack();
		System.out.println("Once committed.. concept id hash size is.. "+ conceptIdHash.thisHash.size());
	}

	// traverse from a type to a type
	public String traverseOutputQuery(String fromType, String toType, List <String> fromInstances)
	{
		/*
		 * 1. Get the relation for the type
		 * 2. For every relation create a join
		 * 3. If Properties are included get the properties
		 * 4. Add the properties
		 * 5. For every, type 
		 */
		IQueryInterpreter builder = getQueryInterpreter();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.setEngine(this);

		String fromTableName = Utility.getInstanceName(fromType);
		String toTableName = Utility.getInstanceName(toType);
		qs.addSelector(fromTableName, SelectQueryStruct.PRIM_KEY_PLACEHOLDER);
		qs.addSelector(toTableName, SelectQueryStruct.PRIM_KEY_PLACEHOLDER);

		// determine relationship order
		String relationQuery = "SELECT ?relation WHERE {"
				+ "{" + "<" + fromType + "> ?relation <" + toType +">}"
				+ "{?relation <" + RDFS.SUBPROPERTYOF + "> <http://semoss.org/ontologies/Relation>}"
				+ "}";

		String relationName = getRelation(relationQuery);
		if(relationName != null && relationName.length() != 0) {
			qs.addRelation(fromTableName, toTableName, "inner.join");
		} else {
			qs.addRelation(toTableName, fromTableName, "inner.join");
		}

		if(fromInstances != null) {
			// convert instances to simple instance
			List<String> simpleFromInstances = new Vector<String>();
			for(int fromIndex = 0;fromIndex < fromInstances.size();fromIndex++) {
				simpleFromInstances.add(Utility.getInstanceName(fromInstances.get(fromIndex)));
			}
			NounMetadata lComparison = new NounMetadata(fromTableName, PixelDataType.COLUMN);
			NounMetadata rComparison = new NounMetadata(simpleFromInstances, PixelDataType.CONST_STRING);
			IQueryFilter simple = new SimpleQueryFilter(lComparison, "==", rComparison);
			qs.addExplicitFilter(simple);
		}

		String retQuery = builder.composeQuery();
		return retQuery;
	}

	private String getRelation(String query)
	{
		String relation = null;
		try {
			TupleQueryResult tqr = (TupleQueryResult)execOntoSelectQuery(query);
			while(tqr.hasNext())
			{
				BindingSet bs = tqr.next();
				relation = bs.getBinding("relation").getValue() + "";
				if(!relation.equalsIgnoreCase("http://semoss.org/ontologies/Relation"))
					break;
			}
		} catch (QueryEvaluationException e) {
			logger.error(Constants.STACKTRACE, e);
		}

		return relation;
	}

	public void deleteDB() {
		logger.debug("Deleting RDBMS Engine: " + this.engineName);

		// If this DB is not an H2, just delete the schema the data was added into, not the existing DB instance
		//WHY ARE WE DELETING THE SOURCE DATABSE????
		//COMMENTING THIS OUT FOR NOW
		//		if (this.getDbType() != SQLQueryUtil.DB_TYPE.H2_DB) {
		//			String deleteText = SQLQueryUtil.initialize(dbType).getDialectDeleteDBSchema(this.engineName);
		//			insertData(deleteText);
		//		}

		// Close the Insights RDBMS connection, the actual connection, and delete the folders
		try {
			//			this.insightRdbms.getConnection().close();
			closeDB();

			DeleteDbFiles.execute(DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "/db/" + this.engineName, "database", false);
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		}

		// Clean up SMSS and DB files/folder
		super.deleteDB();
	}

	public RdbmsTypeEnum getDbType() {
		return this.dbType;
	}

	public void setAutoCommit(boolean autoCommit) {
		if(engineConn != null) {
			try {
				engineConn.setAutoCommit(autoCommit);
				this.autoCommit = autoCommit;
			} catch (SQLException e) {
				logger.error(Constants.STACKTRACE, e);
			}
		}
	}

	@Override
	public java.sql.PreparedStatement bulkInsertPreparedStatement(Object[] args) throws SQLException {
		// if a table name and a column name are not specified, do nothing
		// not enough information to be meaningful
		if(args.length < 2) {
			return null;
		}

		// generate the sql for the prepared statement
		String tableName = (String) args[0];
		if(this.queryUtil.isSelectorKeyword(tableName)) {
			tableName = this.queryUtil.getEscapeKeyword(tableName);
		}
		
		StringBuilder sql = new StringBuilder("INSERT INTO ");
		sql.append(tableName).append(" (");
		for (int colIndex = 1; colIndex < args.length; colIndex++) {
			String columnName = (String) args[colIndex];
			if(this.queryUtil.isSelectorKeyword(columnName)) {
				columnName = this.queryUtil.getEscapeKeyword(columnName);
			}
			sql.append(columnName);
			if( (colIndex+1) != args.length) {
				sql.append(", ");
			}
		}
		sql.append(") VALUES (?"); 
		// remember, we already assumed one col and first index is table name
		for (int colIndex = 1; colIndex < args.length-1; colIndex++) {
			sql.append(", ?");
		}
		sql.append(")");
		
		return getPreparedStatement(sql.toString());
	}

	@Override
	public java.sql.PreparedStatement getPreparedStatement(String sql) throws SQLException {
		return makeConnection().prepareStatement(sql);
	}

	@Override
	public DatabaseMetaData getConnectionMetadata() {
		try {
			return getConnection().getMetaData();
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		}
		return null;
	}

	@Override
	public Connection makeConnection() throws SQLException {
		Connection retObject = getConnection();
		if(conceptIdHash == null && Constants.LOCAL_MASTER_DB_NAME.equals(this.engineId)) {
			if(PersistentHash.canInit(this)) {
				conceptIdHash = new PersistentHash();
				try {
					conceptIdHash.setEngine(this);
					conceptIdHash.load();
				} catch(Exception ex) {
					ex.printStackTrace();
				}
			}
		}
		return retObject;
	}

	public PersistentHash getConceptIdHash() {
		return this.conceptIdHash;
	}

	@Override
	public IQueryInterpreter getQueryInterpreter(){
		return this.queryUtil.getInterpreter(this);
	}

	public void setConnection(Connection engineConn) {
		this.engineConn = engineConn;
		try {
			this.engineConnected = !this.engineConn.isClosed();
			this.autoCommit = this.engineConn.getAutoCommit();
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		}
	}

	public Clob createClob(Connection connection) throws SQLException {
		return connection.createClob();
	}

	public void setQueryUtil(AbstractSqlQueryUtil queryUtil) {
		this.queryUtil = queryUtil;
	}

	@Override
	public String getConnectionUrl() {
		return this.connectionURL;
	}

	///////////////////////////////////////////////////////
	///////////////////////////////////////////////////////
	///////////////////////////////////////////////////////
	///////////////////////////////////////////////////////

}
