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
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.h2.tools.DeleteDbFiles;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;

import prerna.engine.api.IEngine;
import prerna.engine.impl.AbstractEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.interpreters.sql.H2SqlInterpreter;
import prerna.query.interpreters.sql.MicrosoftSqlServerInterpreter;
import prerna.query.interpreters.sql.PostgresInterpreter;
import prerna.query.interpreters.sql.SqlInterpreter;
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
import prerna.util.sql.RDBMSUtility;
import prerna.util.sql.RdbmsTypeEnum;

public class RDBMSNativeEngine extends AbstractEngine {

	private static final Logger LOGGER = LogManager.getLogger(RDBMSNativeEngine.class.getName());
	
	public static final String STATEMENT_OBJECT = "STATEMENT_OBJECT";
	public static final String RESULTSET_OBJECT = "RESULTSET_OBJECT";
	public static final String CONNECTION_OBJECT = "CONNECTION_OBJECT";
	public static final String ENGINE_CONNECTION_OBJECT = "ENGINE_CONNECTION_OBJECT";
	
	public static final String USE_FILE = "USE_FILE";
	public static final String DATA_FILE = "DATA_FILE";

	boolean engineConnected = false;
	boolean datasourceConnected = false;
	private RdbmsTypeEnum dbType;
	private BasicDataSource dataSource = null;
	Connection engineConn = null;
	private boolean useConnectionPooling = false;
	public PersistentHash conceptIdHash = null;
	
	private RdbmsConnectionBuilder connBuilder;
	private String userName = null;
	private String password = null;
	private String driver = null;
	private String connectionURL = null;
	
	private String fileDB = null;
	private String createString = null;

	/**
	 * This is used for tracking audit modifications 
	 */
	private AuditDatabase auditDatabase = null;
	
	@Override
	public void openDB(String propFile)
	{
		if(propFile == null && prop == null){
			if(dataSource!= null){
				try{
					engineConn = getConnection();
					this.engineConnected = true;
				} catch (Exception e){
					LOGGER.error("error RDBMS opening database", e);
				}
			} else {
				LOGGER.info("using engine connection");
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
			this.connectionURL = prop.getProperty(Constants.CONNECTION_URL);
			this.userName = prop.getProperty(Constants.USERNAME);
			this.password = (prop.containsKey(Constants.PASSWORD)) ? prop.getProperty(Constants.PASSWORD) : "";
			this.driver = prop.getProperty(Constants.DRIVER);

			
			// make a check to see if it is asking to use file
			boolean useFile = false;
			if(prop.containsKey(USE_FILE)) {
				useFile = Boolean.valueOf(prop.getProperty(USE_FILE));
			}
			this.useConnectionPooling = Boolean.valueOf(prop.getProperty(Constants.USE_CONNECTION_POOLING));

			this.dbType = (dbTypeString != null) ? RdbmsTypeEnum.getEnumFromString(dbTypeString) : RdbmsTypeEnum.H2_DB;
			if(this.dbType == null) {
				this.dbType = RdbmsTypeEnum.H2_DB;
			}
			if(this.dbType == RdbmsTypeEnum.H2_DB) {
				this.connectionURL = RDBMSUtility.fillH2ConnectionURL(this.connectionURL, this.engineId, this.engineName);
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
					List <String> propList = getProperties4Concept(conceptsArray[conceptIndex], false);
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
				this.engineConn = connBuilder.build();
				if(useConnectionPooling) {
					this.dataSource = connBuilder.getDataSource();
					this.datasourceConnected = true;
				}
				this.engineConnected = true;
			} catch (SQLException e) {
				e.printStackTrace();
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
	}

	private void makeConnection(String driver, String userName, String password, String url, String createString) {
		try {
			Class.forName("org.h2.Driver");
			engineConn = DriverManager.getConnection(url, userName, password);
			engineConn.createStatement().execute(createString);
			engineConnected = true;
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	// you cant change owl right now
	public void reloadFile() {
		try {
			engineConn.close();
			makeConnection(driver, userName, password, connectionURL, createString);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
	}

	public boolean makeConnection(String url, String userName, String password)
	{
		try {
			Class.forName("org.h2.Driver");
			engineConn = DriverManager.getConnection(url, userName, password);
			engineConnected = true;

		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return engineConnected;
	}

	private Connection getConnection(){
		Connection connObj = null;
		if(isConnected()) {
			try {
				// re-establish bad connections
				if(this.engineConn.isClosed() || !this.engineConn.isValid(1)) {
					this.engineConn = connBuilder.build();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
			connObj = engineConn;
		}
		if(this.dataSource != null){
			try {
				connObj = dataSource.getConnection();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return connObj;
	}

	@Override
	// need to clean up the exception it will never be thrown
	public void insertData(String query) throws SQLException {
		Connection conn = null;
		Statement stmt = null;
		try {
			conn = getConnection();
			stmt = conn.createStatement();
			LOGGER.debug("Executing RDBMS query: " + query);
			if(query.startsWith("CREATE") && !(query.startsWith("CREATE DATABASE"))){ // this is create statement"
				stmt.execute(query);
			} else {
				stmt.executeUpdate(query);
			}
		} finally {
			closeConnections(conn, null, stmt);
		}
	}

	private void closeConnections(Connection conn, ResultSet rs, Statement stmt){
		if(isConnected()){
			conn = null;
		}
		ConnectionUtils.closeAllConnections(conn, null, stmt);
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
				// TODO Auto-generated catch block
				e.printStackTrace();
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
			// TODO Auto-generated catch block
			e.printStackTrace();
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
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			closeConnections(conn,rs,stmt);	
		}
		return null;
	}

	public Map<String, Object> execQuery(String query)
	{
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		try {
			conn = getConnection();
			stmt = conn.createStatement();
			Map<String, Object> map = new HashMap();
			rs = getResults(stmt, query);
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
			map.put(RDBMSNativeEngine.STATEMENT_OBJECT, stmt);
			return map;
		} catch (Exception e) {
			LOGGER.error("Error executing SQL query = " + query);
			LOGGER.error("Error message = " + e.getMessage());
			e.printStackTrace();
		}
		return null;
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
		Statement stmt = null;

		try {
			conn = getConnection();
			stmt = conn.createStatement();
			stmt.executeUpdate(query);
		} catch (SQLException e) {
			stmt = null;
			e.printStackTrace();
		} finally {
			if(autoCloseStatement) {
				closeConnections(conn,null,stmt);
			} else {
				closeConnections(conn,null,null);
			}
		}

		return stmt;
	}

	@Override
	public boolean isConnected() {
		return engineConn !=null && this.engineConnected;
	}

	@Override
	public void closeDB() {
		super.closeDB();
		try {
			if(this.useConnectionPooling){
				this.engineConnected = false;
				ConnectionUtils.closeConnection(this.engineConn);
			} else {
				if(this.engineConn != null && !this.engineConn.isClosed()) {
					if(!this.engineConn.getAutoCommit()) {
						this.engineConn.commit();
					}
					this.engineConn.close();
					this.engineConnected = false;
				}
			}
			closeDataSource();
			if(auditDatabase != null) {
				auditDatabase.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private void closeDataSource(){
		if(this.dataSource != null) {
			try {
				this.dataSource.close();
				this.datasourceConnected = false;
			} catch (SQLException e) {
				e.printStackTrace();
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
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		LOGGER.info("Found " + retVector.size() + " elements in result set");
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
			LOGGER.error("Error occured in getResults method of RDBMSNativeEngine", e);
		}
		return rs;
	}

	public AbstractQueryParser getQueryParser() {
		return new SQLQueryParser();
	}

	
	public void shutdown() {
		try {
			Connection conn = getConnection();
			Statement stmt = conn.createStatement();
			stmt.execute("SHUTDOWN");
			// return to pool
		} catch (Exception e) {
			LOGGER.error("Unable to shutdown.", e);
		}
		return;
	}
	
	@Override
	public void removeData(String query) throws SQLException {
		Connection conn = null;
		Statement stmt = null;
		try {
			conn = getConnection();
			stmt = conn.createStatement();
			stmt.execute(query);
			// return to pool
		} finally {
			closeConnections(conn, null, stmt);
		}
	}

	@Override
	public void commit() {
		try {
			getConnection().commit();
		} catch (SQLException e) {
			e.printStackTrace();
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
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return relation;
	}

	public void deleteDB() {
		LOGGER.debug("Deleting RDBMS Engine: " + this.engineName);

		// If this DB is not an H2, just delete the schema the data was added into, not the existing DB instance
		//WHY ARE WE DELETING THE SOURCE DATABSE????
		//COMMENTING THIS OUT FOR NOW
//		if (this.getDbType() != SQLQueryUtil.DB_TYPE.H2_DB) {
//			String deleteText = SQLQueryUtil.initialize(dbType).getDialectDeleteDBSchema(this.engineName);
//			insertData(deleteText);
//		}

		// Close the Insights RDBMS connection, the actual connection, and delete the folders
		try {
			this.insightRDBMS.getConnection().close();
			closeDB();

			DeleteDbFiles.execute(DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "/db/" + this.engineName, "database", false);
		} catch (SQLException e) {
			e.printStackTrace();
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
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * This is intended to be executed via doAction
	 * @param args			Object[] where the first index is the table name
	 * 						and every other entry are the column names
	 * @return				PreparedStatement to perform a bulk insert
	 */
	public java.sql.PreparedStatement bulkInsertPreparedStatement(Object[] args) {
		// if a table name and a column name are not specified, do nothing
		// not enough information to be meaningful
		if(args.length < 2) {
			return null;
		}

		// generate the sql for the prepared statement
		StringBuilder sql = new StringBuilder("INSERT INTO ");
		sql.append(args[0]).append(" (").append(args[1]);
		for(int colIndex = 2; colIndex < args.length; colIndex++) {
			sql.append(", ");
			sql.append(args[colIndex]);
		}
		sql.append(") VALUES (?"); // remember, we already assumed one col
		for(int colIndex = 2; colIndex < args.length; colIndex++) {
			sql.append(", ?");
		}
		sql.append(")");

		java.sql.PreparedStatement ps = null;
		try {
			// create the prepared statement using the sql query defined
			ps = this.engineConn.prepareStatement(sql.toString());
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return ps;
	}

	/**
	 * Return the engine metadata
	 * @return
	 */
	public DatabaseMetaData getConnectionMetadata() {
		try {
			return this.engineConn.getMetaData();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	// does not account for a pooled connection need to ensure
	public Connection makeConnection() {
		Connection retObject = getConnection();
		if(conceptIdHash == null) {
			conceptIdHash = new PersistentHash(this.engineId);
			try {
				conceptIdHash.setConnection(retObject);
				conceptIdHash.load();
			} catch(Exception ex) {
				ex.printStackTrace();
			}
		}
		return retObject;
	}
	
	public PersistentHash getConceptIdHash() {
		return this.conceptIdHash;
	}
	
	@Override
	public IQueryInterpreter getQueryInterpreter(){
		if(dbType == null || dbType == RdbmsTypeEnum.H2_DB) {
			return new H2SqlInterpreter(this);
		} else if(dbType == RdbmsTypeEnum.SQLSERVER) {
			return new MicrosoftSqlServerInterpreter(this);
		} else if(dbType == RdbmsTypeEnum.POSTGRES) {
			return new PostgresInterpreter(this);
		}
		// defualt ansi sql 
		return new SqlInterpreter(this);
	}

	public void setConnection(Connection engineConn) {
		this.engineConn = engineConn;
		try {
			this.engineConnected = !this.engineConn.isClosed();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public String getConnectionUrl() {
		return this.connectionURL;
	}
	
	///////////////////////////////////////////////////////
	///////////////////////////////////////////////////////
	///////////////////////////////////////////////////////
	///////////////////////////////////////////////////////

	/**
	 * Get an audit database for making modifications in a relational database
	 */
	
	/**
	 * Generate an audit database
	 * @param appId
	 */
	public synchronized AuditDatabase generateAudit() {
		if(this.auditDatabase == null) {
			this.auditDatabase = new AuditDatabase();
			this.auditDatabase.init(this, this.engineId, this.engineName);
		}
		return this.auditDatabase;
	}
}
