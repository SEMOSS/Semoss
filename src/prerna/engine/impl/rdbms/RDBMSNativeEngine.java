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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.h2.tools.DeleteDbFiles;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;

import prerna.engine.api.IEngine;
import prerna.engine.impl.AbstractEngine;
import prerna.rdf.query.builder.IQueryBuilder;
import prerna.rdf.query.builder.SQLQueryTableBuilder;
import prerna.rdf.util.AbstractQueryParser;
import prerna.rdf.util.SQLQueryParser;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.sql.H2QueryUtil;
import prerna.util.sql.RDBMSUtility;
import prerna.util.sql.SQLQueryUtil;

public class RDBMSNativeEngine extends AbstractEngine {
	
	public static final String STATEMENT_OBJECT = "STATEMENT_OBJECT";
	public static final String RESULTSET_OBJECT = "RESULTSET_OBJECT";
	public static final String CONNECTION_OBJECT = "CONNECTION_OBJECT";
	public static final String ENGINE_CONNECTION_OBJECT = "ENGINE_CONNECTION_OBJECT";
	
	static final Logger logger = LogManager.getLogger(RDBMSNativeEngine.class.getName());
	DriverManager manager = null;
	boolean engineConnected = false;
	boolean datasourceConnected = false;
	private SQLQueryUtil.DB_TYPE dbType;
	private BasicDataSource dataSource = null;
	Connection engineConn = null;
	private boolean useConnectionPooling = false;
	
	@Override
	public void openDB(String propFile)
	{
		if(propFile == null && prop == null){
			if(dataSource!= null){
				try{
					engineConn = getConnection();
					this.engineConnected = true;
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
				try {
					prop = loadProp(propFile);
					if(!prop.containsKey("TEMP")) {
						super.openDB(propFile);
					}
				} catch (FileNotFoundException e1) {
					logger.error("error in RDBMS openDB processing prop file", e1);
				} catch (IOException e1) {
					logger.error("error in RDBMS openDB processing prop file", e1);
				}
			}
			
			String tempEngineName = prop.getProperty(Constants.ENGINE);
			String tempConnectionURL = prop.getProperty(Constants.TEMP_CONNECTION_URL);
			String userName = prop.getProperty(Constants.USERNAME);
			String password = "";
			String dbTypeString = prop.getProperty(Constants.RDBMS_TYPE);
			useConnectionPooling = Boolean.valueOf(prop.getProperty(Constants.USE_CONNECTION_POOLING));
			dbType = SQLQueryUtil.DB_TYPE.H2_DB;
			if (dbTypeString != null) {
				dbType = (SQLQueryUtil.DB_TYPE.valueOf(dbTypeString));
			}
			String connectionURL = prop.getProperty(Constants.CONNECTION_URL);
			if(dbType == SQLQueryUtil.DB_TYPE.H2_DB) {
				if(engineName != null) {
					connectionURL = RDBMSUtility.fillH2ConnectionURL(connectionURL, engineName);
					try {
						Class.forName(H2QueryUtil.DATABASE_DRIVER);
						if(!(new File(RDBMSUtility.getH2ConnectionURLAbsolutePath(connectionURL)).exists()) || 
								!RDBMSUtility.isValidConnection(DriverManager.getConnection(connectionURL, "sa" , ""))) {
							connectionURL = resetH2ConnectionURL();System.out.println();
						}
					} catch (ClassNotFoundException | SQLException e) {
						logger.error("H2 Connection Error: " + e.getMessage() + " for Connection URL: " + connectionURL);
						connectionURL = resetH2ConnectionURL();
					}
				}
				prop.setProperty(Constants.CONNECTION_URL, connectionURL);
			}
			if(prop.containsKey(Constants.PASSWORD))
				password = prop.getProperty(Constants.PASSWORD);
			String driver = prop.getProperty(Constants.DRIVER);
			try {
				Class.forName(driver);
				//if the tempConnectionURL is set, connect to mysql, create the database, disconnect then reconnect to the database you created
				if((!dbType.equals(SQLQueryUtil.DB_TYPE.H2_DB)) && (tempConnectionURL != null && tempConnectionURL.length()>0)){
					if(useConnectionPooling){
						dataSource = setupDataSource(driver, tempConnectionURL, userName, password);
						engineConn = getConnection();
					} else {
						engineConn = DriverManager.getConnection(tempConnectionURL, userName, password);
					}

					this.engineConnected = true;
					//create database
					createDatabase(tempEngineName);
					//
					closeEngine();
					if(useConnectionPooling){
						closeDataSource();
					}
				}
				if(!isConnected()){
					if(useConnectionPooling){
						dataSource = setupDataSource(driver, connectionURL, userName, password);
//						engineConn = getConnection();
					} else if(userName != null && !userName.isEmpty()){
						engineConn = DriverManager.getConnection(connectionURL, userName, password);
					} else {
						engineConn = DriverManager.getConnection(connectionURL);
					}
					this.engineConnected = true;
				}
			} catch (ClassNotFoundException | SQLException e) {
				// TODO Auto-generated catch block
				logger.error("Exception occured opening database", e);
				this.engineConnected = false;
			}
		}
	}
	
	private Connection getConnection(){
		Connection connObj = null;
		if(isConnected()){
			//System.out.println("use engine connection");
			return engineConn;
		}
		if(this.dataSource!=null){
			try {
				connObj= dataSource.getConnection();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		//System.out.println("use datasource connection");
		return connObj;
	}
	
    private BasicDataSource setupDataSource(String driver, String connectURI, String userName, String password) {
    	//System.out.println("setupDataSource:: driver [" + driver +"] connectURI [" +  connectURI + "] userName ["+ userName+"] password [" + password + "]");
        BasicDataSource ds = new BasicDataSource();
        ds.setDriverClassName(driver);
        ds.setUrl(connectURI);
        ds.setUsername(userName);
        ds.setPassword(password);
        ds.setDefaultAutoCommit(true);//set autocommits to true...
        this.datasourceConnected = true;
        return ds;
    }
    
	private void createDatabase(String engineName){
		insertData(SQLQueryUtil.initialize(dbType).getDialectCreateDatabase(engineName));
	}
	
	@Override
	// need to clean up the exception it will never be thrown
	public void insertData(String query) 
	{
		Connection conn = null;
		Statement stmt = null;
		try {
			conn = getConnection();
			stmt = conn.createStatement();
			if(query.startsWith("CREATE") && !(query.startsWith("CREATE DATABASE"))){ // this is create statement"
				stmt.execute(query);
			} else {
				stmt.executeUpdate(query);
			}
		}catch(Exception ex)
		{
			ex.printStackTrace();
		} finally {
			closeConnections(conn,null,stmt);
		}
	}
	
	private void closeConnections(Connection conn, ResultSet rs, Statement stmt){
		if(isConnected()){
			conn = null;
		}
		ConnectionUtils.closeAllConnections(conn, null, stmt);
	}
	
	@Override
	public ENGINE_TYPE getEngineType()
	{
		return IEngine.ENGINE_TYPE.RDBMS;
	}
	
	@Override
	public Vector<Object> getEntityOfType(String type)
	{
        String table; // table in RDBMS
        String column; // column of table in RDBMS
        String query;

        if(type.contains("http://semoss.org/ontologies/Concept")){
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
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	public Statement execUpdateAndRetrieveStatement(String query, boolean autoCloseStatement) {
		Connection conn = null;
		Statement stmt = null;
		
		try {
			conn = getConnection();
			stmt = conn.createStatement();
			stmt.executeUpdate(query);
		} catch (SQLException e) {
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
	public boolean isConnected()
	{
		return engineConn!=null && this.engineConnected;
	}

	@Override
	public void closeDB() {
		super.closeDB();
		if(useConnectionPooling){
			closeEngine();
		} else {
			if(engineConn != null) {
				try {
					engineConn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			closeDataSource();
		}
	}
	
	private void closeEngine(){
		this.engineConnected = false;
		ConnectionUtils.closeConnection(engineConn);
	}
	
	private void closeDataSource(){
		if(dataSource != null) {
			try {
				dataSource.close();
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
	
	public IQueryBuilder getQueryBuilder(){
		return new SQLQueryTableBuilder(this);
	}
	
	public AbstractQueryParser getQueryParser() {
		return new SQLQueryParser();
	}

	@Override
	public void removeData(String query) {
		try {
			Connection conn = getConnection();
			Statement stmt = conn.createStatement();
			stmt.execute(query);
			// return to pool
		} catch (Exception e) {
			logger.error("Error occured in getResults method of RDBMSNativeEngine", e);
		}
		return;
	}

	@Override
	public void commit() {
		//we set autocommit when we init the data source, see setupDataSource
	}
	
	// traverse from a type to a type, optionally include properties
	public String traverseOutputQuery(String fromType, String toType, boolean isProperties, List <String> fromInstances)
	{
		/*
		 * 1. Get the relation for the type
		 * 2. For every relation create a join
		 * 3. If Properties are included get the properties
		 * 4. Add the properties
		 * 5. For every, type 
		 * 
		 * 
		 */
		SQLQueryTableBuilder builder = (SQLQueryTableBuilder) getQueryBuilder();
		Vector <String> neighBors = getNeighbors(fromType, 0);
		
		// get the properties for the tables	
		List <String> properties = new Vector<String>();
		if(isProperties)
			properties = getProperties4Concept(toType, new Boolean(false));
		
		// string relation selector
		String relationQuery = "SELECT ?relation WHERE {"
				+ "{" + "<" + fromType + "> ?relation <" + toType +">}"
				+ "{?relation <" + RDFS.SUBPROPERTYOF + "> <http://semoss.org/ontologies/Relation>}"
				+ "}";

		String relationName = getRelation(relationQuery);
		
		if(relationName == null || relationName.length() == 0)
		{
			relationQuery = "SELECT ?relation WHERE {"
					+ "{" + "<" + toType + "> ?relation <" + fromType +">}"
					+ "{?relation <" + RDFS.SUBPROPERTYOF + "> <http://semoss.org/ontologies/Relation>}"
					+ "}";
			relationName = getRelation(relationQuery);
			
		}
		
		String fromTableName = Utility.getInstanceName(fromType);
		builder.addSelector(fromTableName, fromTableName);

		String toTableName = Utility.getInstanceName(toType);
		// get the relation name for this from and to
		String relationClassName = Utility.getInstanceName(relationName);
		builder.addRelation(relationClassName,relationName, " AND ", true);
		builder.addTable(fromTableName, properties, properties);
		
		// need something that will identify the main identifier instead of it being always the same as table name
		builder.addSelector(toTableName, toTableName);
		
		
		if(fromInstances != null)
		{
			String propertyAsName = Utility.getInstanceName(fromType); // play on the type
			// convert instances to simple instance
			Vector <String> simpleFromInstances = new Vector<String>();
			for(int fromIndex = 0;fromIndex <fromInstances.size();fromIndex++)
				simpleFromInstances.add(Utility.getInstanceName(fromInstances.get(fromIndex)));
			builder.addStringFilter(propertyAsName, propertyAsName, simpleFromInstances);
		}		
		builder.makeQuery();
		String retQuery = builder.getQuery();
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
		if (this.getDbType() != SQLQueryUtil.DB_TYPE.H2_DB) {
			String deleteText = SQLQueryUtil.initialize(dbType).getDialectDeleteDBSchema(this.engineName);
			insertData(deleteText);
		}
		
//		try {
//			this.insightRDBMS.getConnection().close();
//			closeDB();
//			
//			try {
//				System.out.println("Conn obj closed: " + this.engineConn.isClosed());
//			} catch (SQLException e) {
//				e.printStackTrace();
//			}
//			System.out.println("Datasource: " + this.dataSource);
//			
//			DeleteDbFiles delete = new DeleteDbFiles();
//			delete.execute(DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "/db/" + this.engineName, "database", false);
//			
//			String location = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "/db/" + this.engineName + "/database.mv.db";
//			try {
//				FileUtils.forceDelete(new File(location));
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
//		} catch (SQLException e) {
//			e.printStackTrace();
//		}
		
		super.deleteDB();
	}

	public SQLQueryUtil.DB_TYPE getDbType() {
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
	
	private String resetH2ConnectionURL() {
		String baseH2URL = RDBMSUtility.getH2BaseConnectionURL();
		return RDBMSUtility.fillH2ConnectionURL(baseH2URL, engineName);
	}
}
