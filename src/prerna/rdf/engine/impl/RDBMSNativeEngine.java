package prerna.rdf.engine.impl;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Vector;

import prerna.rdf.engine.api.IEngine;
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
		super.openDB(propFile);
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
				for(int colIndex = 1;colIndex < columns;colIndex++)
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
}
