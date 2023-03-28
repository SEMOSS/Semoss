package prerna.util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Enumeration;
import java.util.Hashtable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.ds.util.RdbmsQueryBuilder;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.util.sql.AbstractSqlQueryUtil;

public class PersistentHash {

	private static final Logger logger = LogManager.getLogger(PersistentHash.class);

	// simple hash table that saves and gets values from the database
	private static final String TABLE_NAME = "KVSTORE";

	private RDBMSNativeEngine engine = null;

	public Hashtable<String, String> thisHash = new Hashtable<String, String>();
	boolean dirty = false;

	public PersistentHash() {

	}

	public void setEngine(RDBMSNativeEngine engine) {
		this.engine = engine;
	}

	public void load() {
		if (engine == null) {
			return;
		}
		
		// this is only for local master!!!
		Connection conn = null;
		try {
			conn = engine.getConnection();
			try (Statement stmt = conn.createStatement()) {
				ResultSet rs = stmt.executeQuery("SELECT K, V from " + TABLE_NAME);
				while(rs.next()) {
					this.thisHash.put(rs.getString(1),rs.getString(2));
				}	
			}
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			try {
				if (engine.isConnectionPooling() && conn != null) {
					conn.close();
				}
			} catch (SQLException e) {
				logger.error(Constants.STACKTRACE, e);
			}
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
		if(engine != null && this.dirty) {
			Connection conn = null;
			try {
				conn = engine.getConnection();
				try(Statement stmt = conn.createStatement()) {
					String [] colNames = {"K","V"};
					String [] types = {"varchar(800)", "varchar(800)"};
					Enumeration <String> keys = thisHash.keys();
					stmt.execute("DELETE from " + TABLE_NAME);
					while(keys.hasMoreElements()) {
						String key = keys.nextElement();
						String value = thisHash.get(key);
						String [] values = {key, value};
						String insertString = RdbmsQueryBuilder.makeInsert(TABLE_NAME, colNames, types, values);
						stmt.execute(insertString);
					}
					this.dirty = false;
				}
			} catch (SQLException e) {
				logger.error(Constants.STACKTRACE, e);
			} finally {
				try {
					if (engine.isConnectionPooling() && conn != null) {
						conn.close();
					}
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}

	/**
	 * See if we can init the persistent hash
	 * @param engine
	 * @return
	 */
	public static boolean canInit(RDBMSNativeEngine engine) {
		if(engine == null) {
			return false;
		}
		
		AbstractSqlQueryUtil queryUtil = engine.getQueryUtil();
		if(queryUtil == null) {
			return false;
		}

		Connection conn = null;
		try {
			conn = engine.getConnection();
			// make sure the KVSTORE table exists
			return queryUtil.tableExists(conn, TABLE_NAME, engine.getDatabase(), engine.getSchema());
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			try {
				if(engine.isConnectionPooling() && conn != null) {
					conn.close();
				}
			} catch (SQLException e) {
				logger.error(Constants.STACKTRACE, e);
			}
		}
		return false;
	}
}
