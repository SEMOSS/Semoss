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
import java.util.Vector;

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
	
	DriverManager manager = null;
	Connection conn = null;
	boolean connected = false;
	private ResultSet rs = null;
	private Statement stmt = null;
	
	@Override
	public void openDB(String propFile)
	{
		// will mostly be sent the connection string and I will connect here
		// I need to see if the connection pool has been initiated
		// if not initiate the connection pool
		try {
			prop = loadProp(propFile);
			if(!prop.containsKey("TEMP"))
				super.openDB(propFile);
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		String connectionURL = prop.getProperty(Constants.CONNECTION_URL);
		String tempEngineName = "";
		String tempConnectionURL = prop.getProperty(Constants.TEMP_CONNECTION_URL);
		String userName = prop.getProperty(Constants.USERNAME);
		String password = "";
		SQLQueryUtil.DB_TYPE dbType = SQLQueryUtil.DB_TYPE.valueOf((prop.getProperty(Constants.RDBMS_TYPE)));
		//special treatment for mariadb
		if(dbType == SQLQueryUtil.DB_TYPE.MARIA_DB){
			String splitConnectionURL[] = connectionURL.split("/");
			tempEngineName = splitConnectionURL[splitConnectionURL.length - 1];
		}
		if(prop.containsKey(Constants.PASSWORD))
			password = prop.getProperty(Constants.PASSWORD);
		String driver = prop.getProperty(Constants.DRIVER);
        try {
			Class.forName(driver);
			//if the tempConnectionURL is set, connect to mysql, create the database, disconnect then reconnect to the database you created
			if(this.conn == null && dbType == SQLQueryUtil.DB_TYPE.MARIA_DB && (tempConnectionURL != null && tempConnectionURL.length()>0)){
				conn = DriverManager.getConnection(tempConnectionURL, userName, password);
				//create database
				createDatabase(tempEngineName);
				closeDB();
				this.conn = null; //reset back to null;
			}
			if(this.conn == null)
				conn = DriverManager.getConnection(connectionURL, userName, password);
			connected = true;
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			this.connected = false;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void createDatabase(String engineName){
		String createDB = "CREATE DATABASE " + engineName;
		insertData(createDB);
	}
	
	@Override
	// need to clean up the exception it will never be thrown
	public void insertData(String query) 
	{
		try {
			Statement stmt = conn.createStatement();
			if(query.startsWith("CREATE") && !(query.startsWith("CREATE DATABASE"))){ // this is create statement"
				stmt.execute(query);
			} else {
				stmt.executeUpdate(query);
			}
		}catch(Exception ex)
		{
			ex.printStackTrace();
		}
	}
	
	@Override
	public ENGINE_TYPE getEngineType()
	{
		return IEngine.ENGINE_TYPE.RDBMS;
	}
	
	@Override
	public Vector<String> getEntityOfType(String type)
	{
		if(type.contains(":"))
			type = Utility.getInstanceName(type);
		String query = "SELECT DISTINCT " + type + " FROM " + type;
		try {
			return getColumnsFromResultSet(1, getResults(query));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	public Vector<String> getCleanSelect(String query){
		try {
			return getColumnsFromResultSet(1, getResults(query));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public Object execQuery(String query)
	{
		try {
			rs = getResults(query);
			return rs;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	

	@Override
	public boolean isConnected()
	{
		return connected;
	}

	@Override
	public void closeDB() {
		// do nothing
		try {
			conn.commit();
			ConnectionUtils.closeAllConnections(conn, rs, stmt);
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
	 * @param query
	 * @return ResultSet object
	 * @throws Exception
	 */
	private ResultSet getResults(String query) throws Exception {
		stmt = conn.createStatement();
		rs = stmt.executeQuery(query);
		return rs;
	}
	
	public IQueryBuilder getQueryBuilder(){
		//return new SQLQueryBuilder();
		return new SQLQueryTableBuilder(this);
	}
	
	@Override
	public void removeData(String query) {
		//not sure here
		
	}

	@Override
	public void commit() {
		try {
			conn.commit();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	// traverse from a type to a type, optionally include properties
	public String traverseOutputQuery(String fromType, String toType, boolean isProperties, Vector <String> fromInstances)
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
		relationName = Utility.getClassName(relationName);
		builder.addRelation(relationName, " AND ", true);
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
}
