package prerna.util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.Hashtable;

import prerna.ds.util.RdbmsQueryBuilder;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.sql.AbstractSqlQueryUtil;

public class PersistentHash {

	// simple hash table that saves and gets values from the database
	private static final String TABLE_NAME = "KVSTORE";

	private Connection conn = null;

	public Hashtable<String, String> thisHash = new Hashtable<String, String>();
	boolean dirty = false;

	public PersistentHash() {

	}

	public void setConnection(Connection conn) {
		this.conn = conn;
	}

	public void load() {
		// this is only for local master!!!
		try {
			if(conn != null) {
				ResultSet rs = conn.createStatement().executeQuery("SELECT K, V from " + TABLE_NAME);
				while(rs.next()) {
					this.thisHash.put(rs.getString(1),rs.getString(2));
				}	
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void put(String key, String value) {
		this.thisHash.put(key, value);
		dirty = true;
	}

	public boolean containsKey(String key) {
		return this.thisHash.containsKey(key);

	}

	public String get(String key) {
		return this.thisHash.get(key);
	}

	public void persistBack() {
		if(this.dirty) {
			try {
				String [] colNames = {"K","V"};
				String [] types = {"varchar(800)", "varchar(800)"};
				Enumeration <String> keys = thisHash.keys();
				this.conn.createStatement().execute("DELETE from " + TABLE_NAME);
				while(keys.hasMoreElements()) {
					String key = keys.nextElement();
					String value = thisHash.get(key);
					String [] values = {key, value};
					String insertString = RdbmsQueryBuilder.makeInsert(TABLE_NAME, colNames, types, values);
					this.conn.createStatement().execute(insertString);
				}
				this.dirty = false;
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * See if we can init the persistent hash
	 * @param engine
	 * @return
	 */
	public static boolean canInit(RDBMSNativeEngine engine) {
		AbstractSqlQueryUtil queryUtil = engine.getQueryUtil();
		if(queryUtil == null) {
			return false;
		}

		// make sure the KVSTORE table exists
		String tableExistsQuery = queryUtil.tableExistsQuery(TABLE_NAME, engine.getSchema());
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(engine, tableExistsQuery);
		try {
			return wrapper.hasNext();
		} finally {
			wrapper.cleanUp();
		}
	}
}
