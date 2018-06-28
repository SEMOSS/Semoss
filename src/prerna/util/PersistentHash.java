package prerna.util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.Hashtable;

import prerna.ds.util.RdbmsQueryBuilder;

public class PersistentHash {
	
	// simple hash table that saves and gets values from the database
	private final String tableName = "kvstore";
	private Connection conn = null;
	private String engineId = null;
	private boolean validEngine = false;
	
	public Hashtable<String, String> thisHash = new Hashtable<String, String>();
	boolean dirty = false;
	
	public PersistentHash(String engineId) {
		this.engineId = engineId;
		this.validEngine = Constants.LOCAL_MASTER_DB_NAME.equals(this.engineId);
	}
	
	public void setConnection(Connection conn) {
		this.conn = conn;
	}

	public void load() {
		// this is only for local master!!!
		if(validEngine) {
			try {
				if(conn != null) {
					ResultSet rs = conn.createStatement().executeQuery("SELECT K, V from " + this.tableName);
					while(rs.next()) {
						this.thisHash.put(rs.getString(1),rs.getString(2));
					}	
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void put(String key, String value) {
		if(validEngine) {
			this.thisHash.put(key, value);
			dirty = true;
		}
	}
	
	public boolean containsKey(String key) {
		return this.thisHash.containsKey(key);
				
	}

	public String get(String key) {
		return this.thisHash.get(key);
	}

	public void persistBack() {
		if(this.validEngine && this.dirty) {
			try {
				String [] colNames = {"K","V"};
				String [] types = {"varchar(800)", "varchar(800)"};
				Enumeration <String> keys = thisHash.keys();
				this.conn.createStatement().execute("DELETE from " + this.tableName);
				while(keys.hasMoreElements()) {
					String key = keys.nextElement();
					String value = thisHash.get(key);
					String [] values = {key, value};
					String insertString = RdbmsQueryBuilder.makeInsert(this.tableName, colNames, types, values);
					this.conn.createStatement().execute(insertString);
				}
				this.dirty = false;
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
}
