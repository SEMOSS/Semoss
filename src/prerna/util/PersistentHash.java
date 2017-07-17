package prerna.util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.Hashtable;

public class PersistentHash {
	
	// simple hash table that saves and gets values from the database
	Connection conn = null;
	String tableName = "KVStore";
	public Hashtable <String, String> thisHash = new Hashtable<String, String>();
	boolean dirty = false;
	
	public void setConnection(Connection conn)
	{
		this.conn = conn;
	}

	public void setTableName(String tableName)
	{
		this.tableName = tableName;
	}

	
	public void load()
	{
		try {
			if(conn != null)
			{
				ResultSet rs = conn.createStatement().executeQuery("SELECT K, V from " + tableName);
				while(rs.next())
				{
					thisHash.put(rs.getString(1),rs.getString(2));
				}	
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void put(String key, String value)
	{
		thisHash.put(key, value);
		dirty = true;
	}
	
	public boolean containsKey(String key)
	{
		return thisHash.containsKey(key);
				
	}

	public String get(String key)
	{
		return thisHash.get(key);
				
	}

	public void persistBack()
	{
		if(dirty)
		{
			try {
				String [] colNames = {"K","V"};
				String [] types = {"varchar(800)", "varchar(800)"};
				Enumeration <String> keys = thisHash.keys();
				String createString = makeCreate(tableName, colNames, types);
				conn.createStatement().execute(createString);
				conn.createStatement().execute("DELETE from " + tableName);
//				conn.createStatement().execute(createString);
				while(keys.hasMoreElements())
				{
					String key = keys.nextElement();
					String value = thisHash.get(key);
					String [] values = {key, value};
					String insertString = makeInsert(tableName, colNames, types, values);
					conn.createStatement().execute(insertString);
				}
				dirty = false;
				
				//load();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	}
	
	private String makeCreate(String tableName, String [] colNames, String [] types)
	{
		StringBuilder retString = new StringBuilder("CREATE TABLE IF NOT EXISTS "+ tableName + " (" + colNames[0] + " " + types[0]);
		for(int colIndex = 1;colIndex < colNames.length;colIndex++)
			retString = retString.append(" , " + colNames[colIndex] + "  " + types[colIndex]);
		retString = retString.append(")");
		
		//System.out.println("CREATE >>> " + retString);
		
		return retString.toString();
		
	}

	
	private String makeInsert(String tableName, String [] colNames, String [] types, Object [] data)
	{
		StringBuilder retString = new StringBuilder("INSERT INTO "+ tableName + " (" + colNames[0]);
		for(int colIndex = 1;colIndex < colNames.length;colIndex++)
			retString = retString.append(" , " + colNames[colIndex]);

		String prefix = "'";
		
		retString = retString.append(") VALUES (" + prefix + data[0] + prefix);
		for(int colIndex = 1;colIndex < colNames.length;colIndex++)
		{
			if(types[colIndex].contains("varchar") || types[colIndex].toLowerCase().contains("timestamp") || types[colIndex].toLowerCase().contains("date"))
				prefix = "'";
			else
				prefix = "";
			retString = retString.append(" , " + prefix + data[colIndex] + prefix);
		}
		retString = retString.append(")");
		
		//System.out.println("INSERT >>> " + retString);
		
		return retString.toString();
	}

	
	

}
