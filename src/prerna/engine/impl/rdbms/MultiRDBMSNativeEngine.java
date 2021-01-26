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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.h2.tools.DeleteDbFiles;

import com.zaxxer.hikari.HikariDataSource;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.engine.api.IEngine;
import prerna.engine.impl.AbstractEngine;
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.om.ThreadStore;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RdbmsTypeEnum;

public class MultiRDBMSNativeEngine extends AbstractEngine {

	// TODO: NEED TO ACCOUNT FOR PASSWORD ENCRYPTION
	// TODO: NEED TO DETERMINE IF DELETE DB NEEDS ANYTHING DIFFERENT
	
	private static final Logger logger = LogManager.getLogger(MultiRDBMSNativeEngine.class);

	public static final String DEFAULT_CONTEXT_KEY = "DEFAULT_CONTEXT";
	public static final String CONNECTIONS_TO_FILL = "CONNECTIONS_TO_FILL";
	public static final String SETUP_PREFIX = "SETUP_";
	public static final String SETUP_QUERY_KEY = "SETUP_QUERY";
	
	private String defaultContext = null;
	private String setupQuery = null;
	private Properties contextProperties = new Properties();
	private RDBMSNativeEngine contextEngine = null;
	
	// schema1 : connectionurl1/schema1 - snowflake
	// schema2 : connectionurl1/schema2 - teradata
	// schema3 : connectionurl2/schema3 - mysql
	private Map<String, Properties> contextToProperties = new HashMap<>();
	private Map<String, RDBMSNativeEngine> contextToConnectionMap = new HashMap<>();
	
	@Override
	public void openDB(String propFile)
	{
		super.openDB(propFile);
		
		/*
		 * contextToConnectionMap needs to be built
		 * and the keys need to be stored
		 * we will grab all the connection urls
		 * and populate the map
		 */
		
		// this will contain something like 1,2
		// which tells us there is 1_ and 2_ prefixes 
		// for the options around how to connect to the data sources
		String prefixes = prop.getProperty(CONNECTIONS_TO_FILL);
		String[] prefixIds = prefixes.split(",");
		
		this.setupQuery = prop.getProperty(SETUP_QUERY_KEY);
		if(this.setupQuery == null) {
			throw new NullPointerException("Could not find the user defined query to determine the engine context");
		}
		
		// if this exists...
		this.defaultContext = prop.getProperty(DEFAULT_CONTEXT_KEY);
		
		// really easy way to go about this
		// just loop through everything
		// and make a new prop file that is temp
		// this will create all the property files we need
		for(Object key : this.prop.keySet()) {
			// if it starts with our prefix
			// we will separate it out into its own prop file
			for(String prefix : prefixIds) {
				if(key.toString().startsWith(prefix + "_")) {
					// we found a match
					Properties thisPropInput = null;
					if(contextToProperties.containsKey(prefix)) {
						thisPropInput = this.contextToProperties.get(prefix);
					} else {
						thisPropInput = new Properties();
						thisPropInput.put("TEMP", true);
						this.contextToProperties.put(prefix, thisPropInput);
					}
					
					// now store the key without the prefix + "_"
					// in thisPropInput object
					String inputKey = key.toString().replaceFirst(prefix + "_", "");
					thisPropInput.put(inputKey, this.prop.get(key));
				}
			}
			
			if(key.toString().startsWith(SETUP_PREFIX)) {
				String inputKey = key.toString().replaceFirst(SETUP_PREFIX, "");
				this.contextProperties.put(inputKey, this.prop.get(key));
			}
		}
		
		// load in the SETUP engine
		this.contextProperties.put("TEMP", true);
		this.contextEngine = new RDBMSNativeEngine();
		this.contextEngine.setProp(contextProperties);
		this.contextEngine.openDB(null);
		
		// load all the other engines
		for(String contextName : this.contextToProperties.keySet()) {
			Properties thisProp = this.contextToProperties.get(contextName);
			RDBMSNativeEngine engine = new RDBMSNativeEngine();
			// set the OWL for each engine
			engine.setBaseDataEngine(this.baseDataEngine);
			engine.setProp(thisProp);
			engine.openDB(null);
			
			contextToConnectionMap.put(contextName, engine);
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
		// execute query against base connection url
		// get back a single valued string
		// go to contextToConnectionMap with the string
		// to get the correct rdbms native engine
		
		Object contextLookup = null;

		AccessToken token = user.getAccessToken(user.getPrimaryLogin());
		String userId = token.getId();
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			// use the setup query that was provided in the smss
			ps = contextEngine.getPreparedStatement(this.setupQuery);
			// it should have 1 ? to fill in with the userid
			ps.setString(1, userId);
			rs = ps.executeQuery();
			if(rs.next()) {
				contextLookup = rs.getObject(1);
			}
		} catch (SQLException e1) {
			e1.printStackTrace();
		} finally {
			try {
				rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			try {
				ps.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
		// if nothing defined - do we have a default?
		if(contextLookup == null) {
			contextLookup = this.defaultContext;
		}
		
		// still nothing - you are screwed....
		if(contextLookup == null) {
			throw new IllegalArgumentException("User has not been provisioned to any context for this app");
		}
		
		// give the context that was found
		return this.contextToConnectionMap.get(contextLookup);
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
	public HikariDataSource getDataSource() {
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
	public Vector<Object> getEntityOfType(String type) {
		return getContext().getEntityOfType(type);
	}

	public Vector<Object> getCleanSelect(String query) {
		return getContext().getCleanSelect(query);
	}

	public Map<String, Object> execQuery(String query) throws SQLException {
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
		// close the setup engine
		this.contextEngine.closeDB();
		// close for all the engines we have
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
		logger.debug("Deleting Multi RDBMS Engine: " + this.engineName);

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
	
	@Override
	public void setBaseData(RDFFileSesameEngine eng) {
		super.setBaseData(eng);
		// also set for all the inner ones
		for(String contextName : this.contextToConnectionMap.keySet()) {
			RDBMSNativeEngine engine = this.contextToConnectionMap.get(contextName);
			engine.setBaseDataEngine(this.baseDataEngine);
		}
	}

	public RdbmsTypeEnum getDbType() {
		return getContext().getDbType();
	}

	public void setAutoCommit(boolean autoCommit) {
		getContext().setAutoCommit(autoCommit);
	}

	/**
	 * This is intended to be executed via doAction
	 * @param args			Object[] where the first index is the table name
	 * 						and every other entry are the column names
	 * @return				PreparedStatement to perform a bulk insert
	 * @throws SQLException 
	 */
	public java.sql.PreparedStatement bulkInsertPreparedStatement(Object[] args) throws SQLException {
		return getContext().bulkInsertPreparedStatement(args);
	}
	
	/**
	 * This is to get a prepared statement based on the input query
	 * @param query
	 * @return
	 * @throws SQLException 
	 */
	public java.sql.PreparedStatement getPreparedStatement(String sql) throws SQLException {
		return getContext().getPreparedStatement(sql);
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
