/*******************************************************************************
 * Copyright 2015 SEMOSS.ORG
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
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;

import prerna.engine.api.IEngine;
import prerna.engine.impl.AbstractEngine;
import prerna.rdf.query.builder.IQueryBuilder;
import prerna.rdf.query.builder.SQLQueryTableBuilder;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.Utility;
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
	
	@Override
	public void openDB(String propFile)
	{
		if(dataSource!= null){
			try{
				engineConn = getConnection();
				this.engineConnected = true;
			} catch (Exception e){
				logger.error("error RDBMS opening database", e);
			}
		} else {
		
			// will mostly be sent the connection string and I will connect here
			// I need to see if the connection pool has been initiated
			// if not initiate the connection pool
			try {
				prop = loadProp(propFile);
				if(!prop.containsKey("TEMP"))
					super.openDB(propFile);
			} catch (FileNotFoundException e1) {
				// TODO Auto-generated catch block
				logger.error("error in RDBMS openDB processing prop file", e1);
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				logger.error("error in RDBMS openDB processing prop file", e1);
			}
			String connectionURL = prop.getProperty(Constants.CONNECTION_URL);
			String tempEngineName = "";
			String tempConnectionURL = prop.getProperty(Constants.TEMP_CONNECTION_URL);
			String userName = prop.getProperty(Constants.USERNAME);
			String password = "";
			String dbTypeString = prop.getProperty(Constants.RDBMS_TYPE);
			dbType = dbType = SQLQueryUtil.DB_TYPE.H2_DB;
			if (dbTypeString != null) {
				dbType = (SQLQueryUtil.DB_TYPE.valueOf(dbTypeString));
			}
			if(dbType.equals(SQLQueryUtil.DB_TYPE.SQL_SERVER) || dbType == SQLQueryUtil.DB_TYPE.MARIA_DB){
				tempEngineName = SQLQueryUtil.initialize(dbType).getEngineNameFromConnectionURL(connectionURL);
			}
			if(prop.containsKey(Constants.PASSWORD))
				password = prop.getProperty(Constants.PASSWORD);
			String driver = prop.getProperty(Constants.DRIVER);
			try {
				Class.forName(driver);
				//if the tempConnectionURL is set, connect to mysql, create the database, disconnect then reconnect to the database you created
				if((dbType == SQLQueryUtil.DB_TYPE.MARIA_DB || dbType == SQLQueryUtil.DB_TYPE.SQL_SERVER) && (tempConnectionURL != null && tempConnectionURL.length()>0)){
					dataSource = setupDataSource(driver, tempConnectionURL, userName, password);
					engineConn = getConnection();
					this.engineConnected = true;
					//create database
					createDatabase(tempEngineName);
					closeDB();
					closeDataSource();
				}
				if(!isConnected()){
					dataSource = setupDataSource(driver, connectionURL, userName, password);
					engineConn = getConnection();
					this.engineConnected = true;
				}
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				logger.error("Database driver class not found", e);
				this.engineConnected = false;
			}
		}
	}
	
	private Connection getConnection(){
		Connection connObj = null;
		if(isConnected()){
			return engineConn;
		}
		if(this.dataSource!=null){
			try {
				connObj= dataSource.getConnection();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
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
		String createDB = "CREATE DATABASE " + engineName;
		insertData(createDB);
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
	public Vector<String> getEntityOfType(String type)
	{
        String table; // table in RDBMS
        String column; // column of table in RDBMS
        String query;

        if(type.contains(":")) {
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
        	rs = getResults(conn, stmt, query);
        	Vector<String> columnsFromResult = getColumnsFromResultSet(1, rs);
        	return columnsFromResult;
        } catch (Exception e) {
               // TODO Auto-generated catch block
               e.printStackTrace();
        } finally {
        	closeConnections(conn,rs,stmt);
        }
        return null;

	}
	
	public Vector<String> getCleanSelect(String query){
		Connection conn = null;
        ResultSet rs = null;
		Statement stmt = null;
		try {
			conn = getConnection();
			stmt = conn.createStatement();
			rs = getResults(conn, stmt, query);
    		Vector<String> columnsFromResult = getColumnsFromResultSet(1, rs);
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
			rs = getResults(conn, stmt, query);
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

	
	@Override
	public boolean isConnected()
	{
		return engineConn!=null && this.engineConnected;
	}

	@Override
	public void closeDB() {
		this.engineConnected = false;
		ConnectionUtils.closeConnection(engineConn);
		//closeDataSource();
	}
	
	private void closeDataSource(){
        try {
			dataSource.close();
			this.datasourceConnected = false;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
				String output = null;
				for(int colIndex = 1;colIndex <= columns;colIndex++)
				{					
					output = rs.getString(colIndex);
					System.out.print(rs.getObject(colIndex));
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

	private ResultSet getResults(Connection conn, Statement stmt, String query) throws Exception {
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
	
	@Override
	public void removeData(String query) {
		//not sure here
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
		Vector <String> properties = new Vector<String>();
		if(isProperties)
			properties = getProperties4Concept(toType);
		
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
		String relationClassName = Utility.getClassName(relationName);
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
	}

	public SQLQueryUtil.DB_TYPE getDbType() {
		return this.dbType;
	}
}
