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
package prerna.rdf.engine.impl;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Vector;

import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.UpdateExecutionException;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.query.builder.IQueryBuilder;
import prerna.rdf.query.builder.SQLQueryBuilder;
import prerna.util.Constants;

public class RDBMSNativeEngine extends AbstractEngine {
	
	DriverManager manager = null;
	Connection conn = null;
	boolean connected = false;

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
		String userName = prop.getProperty(Constants.USERNAME);
		String password = "";
		if(prop.containsKey(Constants.PASSWORD))
			password = prop.getProperty(Constants.PASSWORD);
		String driver = prop.getProperty(Constants.DRIVER);
        try {
			Class.forName(driver);
			if(this.conn == null)
			conn = DriverManager.
			    getConnection(connectionURL, userName, password);
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

	@Override
	// need to clean up the exception it will never be thrown
	public void execInsertQuery(String query) throws SailException,
			UpdateExecutionException, RepositoryException,
			MalformedQueryException 
	{
		try {
			Statement stmt = conn.createStatement();
			if(query.startsWith("CREATE")) // this is create statement"
				stmt.execute(query);
			else
				stmt.executeUpdate(query);
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
	public Vector<String> getEntityOfType(String sparqlQuery)
	{
		try {
			return getColumnsFromResultSet(1, getResults(sparqlQuery));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public Object execSelectQuery(String query)
	{
		try {
			ResultSet rs = getResults(query);
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
	public void closeDB()
	{
		// do nothing
		try {
			conn.commit();
			conn.close();
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
	
	public ResultSet getResults(String query) throws Exception
	{
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery(query);
		return rs;
	}
	
	public IQueryBuilder getQueryBuilder(){
		return new SQLQueryBuilder();
		//return new SQLQueryTableBuilder(this);
	}
}
