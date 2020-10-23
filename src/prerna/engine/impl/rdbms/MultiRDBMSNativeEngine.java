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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.h2.tools.DeleteDbFiles;

import prerna.auth.User;
import prerna.engine.api.IEngine;
import prerna.engine.impl.AbstractEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.om.ThreadStore;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RDBMSUtility;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SqlQueryUtilFactory;

public class MultiRDBMSNativeEngine extends AbstractEngine {

	private static final Logger logger = LogManager.getLogger(MultiRDBMSNativeEngine.class);

	// schema1 : connectionurl1/schema1 - snowflake
	// schema2 : connectionurl1/schema2 - teradata
	// schema3 : connectionurl2/schema3 - mysql
	private Map<String, RDBMSNativeEngine> contextToConnectionMap = new HashMap<String, RDBMSNativeEngine>();
	
	boolean engineConnected = false;
	boolean datasourceConnected = false;
	private RdbmsTypeEnum dbType;
	private BasicDataSource dataSource = null;
	private Connection engineConn = null;
	private boolean useConnectionPooling = false;
	private boolean autoCommit = false;
	
	private RdbmsConnectionBuilder connBuilder;
	private String userName = null;
	private String password = null;
	private String driver = null;
	private String connectionURL = null;
	private String schema = null;
	// parameterized in SMSS
	// fetch size -1 which will 
	private int fetchSize = -1;
	private int poolMinSize = 1;
	private int poolMaxSize = 16;
	
	private AbstractSqlQueryUtil queryUtil = null;

	private String fileDB = null;

	@Override
	public void openDB(String propFile)
	{
		/*
		 * contextToConnectionMap needs to be built
		 * and the keys need to be stored
		 * we will grab all the connection urls
		 * and populate the map
		 */
		
		if(propFile == null && prop == null){
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
			if(prop == null) {
				prop = Utility.loadProperties(propFile);
				// if this is not a temp then open the super
				if(!prop.containsKey("TEMP")) { 
					// not temp, in which case, this engine has a insights rdbms and an owl
					// so call super to open them and set them in the engine
					super.openDB(propFile);
				}
			}
			
			// grab the values from the prop file 
			String dbTypeString = prop.getProperty(Constants.RDBMS_TYPE);
			if(dbTypeString == null) {
				dbTypeString = prop.getProperty(AbstractSqlQueryUtil.DRIVER_NAME);
			}
			this.connectionURL = prop.getProperty(Constants.CONNECTION_URL);
			this.userName = prop.getProperty(Constants.USERNAME);
			
			if(propFile != null) {
				this.password = decryptPass(propFile, false);
			} 
			if(this.password == null) {
				this.password = (prop.containsKey(Constants.PASSWORD)) ? prop.getProperty(Constants.PASSWORD) : "";
			}
			this.driver = prop.getProperty(Constants.DRIVER);
			
			// make a check to see if it is asking to use file
			boolean useFile = false;
			if(prop.containsKey(USE_FILE)) {
				useFile = Boolean.valueOf(prop.getProperty(USE_FILE));
			}
			this.useConnectionPooling = Boolean.valueOf(prop.getProperty(Constants.USE_CONNECTION_POOLING));

			// get the dbType from the input or from the driver itself
			this.dbType = (dbTypeString != null) ? RdbmsTypeEnum.getEnumFromString(dbTypeString) : RdbmsTypeEnum.getEnumFromDriver(driver);
			if(this.dbType == null) {
				this.dbType = RdbmsTypeEnum.H2_DB;
			}
			if(this.dbType == RdbmsTypeEnum.H2_DB || this.dbType == RdbmsTypeEnum.SQLITE) {
				this.connectionURL = RDBMSUtility.fillParameterizedFileConnectionUrl(this.connectionURL, this.engineId, this.engineName);
			}
			
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
			if(prop.getProperty(Constants.POOL_MIN_SIZE) != null) {
				String strMinPoolSize = prop.getProperty(Constants.POOL_MIN_SIZE);
				try {
					this.poolMinSize = Integer.parseInt(strMinPoolSize);
				} catch(Exception e) {
					System.out.println("Error occured trying to parse and get the min pool size");
					logger.error(Constants.STACKTRACE, e);
				}
			}
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
			
			try {
				this.queryUtil = SqlQueryUtilFactory.initialize(this.dbType, this.connectionURL, this.userName, this.password);
				this.engineConn = connBuilder.build();
				if(useConnectionPooling) {
					this.dataSource = connBuilder.getDataSource();
					this.dataSource.setMinIdle(this.poolMinSize);
					this.dataSource.setMaxIdle(this.poolMaxSize);
					this.datasourceConnected = true;
				}
				this.engineConnected = true;
				this.autoCommit = this.engineConn.getAutoCommit();
				this.queryUtil.enhanceConnection(this.engineConn);
			} catch (SQLException e) {
				logger.error(Constants.STACKTRACE, e);
			}
		}
	}	
	
	/**
	 * For this user in the thread
	 * get the correct engine
	 * @return
	 */
	public RDBMSNativeEngine getContext() {
		User user = ThreadStore.getUser();
		return lookUpContext(user);
	}
	
	/**
	 * Needs to be parameterized
	 * Such that we can perform a lookup based on different queries
	 * @param user
	 * @return
	 */
	public RDBMSNativeEngine lookUpContext(User user) {
		// TODO
		// execute query against base connection url
		// get back a single valued string
		// go to contextToConnectionMap with the string
		// to get the correct rdbms native engine
		
		return null;
	}

	public AbstractSqlQueryUtil getQueryUtil() {
		return getContext().getQueryUtil();
	}
	
	public String getSchema() {
		return getContext().getSchema();
	}
	
	/**
	 * Get the data source
	 * @return
	 */
	public BasicDataSource getDataSource() {
		return getContext().getDataSource();
	}

	/**
	 * Get the connection
	 * @return
	 * @throws SQLException
	 */
	public Connection getConnection() throws SQLException {
		return getContext().getConnection();
	}

	@Override
	// need to clean up the exception it will never be thrown
	public void insertData(String query) throws SQLException {
		getContext().insertData(query);
	}

	@Override
	public ENGINE_TYPE getEngineType() {
		return IEngine.ENGINE_TYPE.RDBMS;
	}

	@Override
	public Vector<Object> getEntityOfType(String type)
	{
		return getContext().getEntityOfType(type);
	}

	public Vector<Object> getCleanSelect(String query){
		return getContext().getCleanSelect(query);
	}

	public Map<String, Object> execQuery(String query) throws SQLException
	{
		return getContext().execQuery(query);
	}

	/**
	 * Method to execute Update/Delete statements with the option of closing the Statement object.
	 * 
	 * @param query					Query to execute
	 * @param autoCloseStatement	Option to automatically close the Statement object after query execution
	 * @return
	 */
	public Statement execUpdateAndRetrieveStatement(String query, boolean autoCloseStatement) {
		return getContext().execUpdateAndRetrieveStatement(query, autoCloseStatement);
	}

	@Override
	public boolean isConnected() {
		return getContext().isConnected();
	}

	@Override
	public void closeDB() {
		super.closeDB();
		for(String key : this.contextToConnectionMap.keySet()) {
			RDBMSNativeEngine contextE = this.contextToConnectionMap.get(key);
			try {
				contextE.closeDB();
			} catch(Exception e) {
				logger.error(Constants.STACKTRACE, e);
			}
		}
	}
	
	@Override
	public void removeData(String query) throws SQLException {
		getContext().removeData(query);
	}

	@Override
	public void commit() {
		getContext().commit();
	}
	
	public void deleteDB() {
		// TODO
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
			this.insightRdbms.getConnection().close();
			closeDB();

			DeleteDbFiles.execute(DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "/db/" + this.engineName, "database", false);
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		}

		// Clean up SMSS and DB files/folder
		super.deleteDB();
	}

	public RdbmsTypeEnum getDbType() {
		return this.dbType;
	}

	public void setAutoCommit(boolean autoCommit) {
		getContext().setAutoCommit(autoCommit);
	}

	/**
	 * This is intended to be executed via doAction
	 * @param args			Object[] where the first index is the table name
	 * 						and every other entry are the column names
	 * @return				PreparedStatement to perform a bulk insert
	 */
	public java.sql.PreparedStatement bulkInsertPreparedStatement(Object[] args) {
		return getContext().bulkInsertPreparedStatement(args);
	}
	
	/**
	 * This is to get a prepared statement based on the input query
	 * @param query
	 * @return
	 */
	public java.sql.PreparedStatement bulkInsertPreparedStatement(String sql) {
		return getContext().bulkInsertPreparedStatement(sql);
	}

	/**
	 * Return the engine metadata
	 * @return
	 */
	public DatabaseMetaData getConnectionMetadata() {
		return getContext().getConnectionMetadata();
	}
	
	// does not account for a pooled connection need to ensure
	public Connection makeConnection() throws SQLException {
		return getContext().makeConnection();
	}
	
	@Override
	public IQueryInterpreter getQueryInterpreter(){
		return getContext().getQueryInterpreter();
	}

	public String getConnectionUrl() {
		return getContext().getConnectionUrl();
	}
	
	///////////////////////////////////////////////////////
	///////////////////////////////////////////////////////
	///////////////////////////////////////////////////////
	///////////////////////////////////////////////////////

}
