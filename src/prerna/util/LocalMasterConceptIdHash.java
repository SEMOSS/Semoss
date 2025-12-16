package prerna.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IRDBMSEngine;

public class LocalMasterConceptIdHash {

	private static final Logger classLogger = LogManager.getLogger(LocalMasterConceptIdHash.class);

	// simple hash table that saves and gets values from the database
	private static final String TABLE_NAME = "KVSTORE";

	private static LocalMasterConceptIdHash instance = null;

	private Map<String, String> dataHash = Collections.synchronizedMap(new HashMap<String, String>());
	private boolean dirty = false;

	private LocalMasterConceptIdHash() {

	}

	public static LocalMasterConceptIdHash getInstance() {
		if (instance != null) {
			return instance;
		}

		synchronized (LocalMasterConceptIdHash.class) {
			if (instance != null) {
				return instance;
			}

			instance = new LocalMasterConceptIdHash();
			instance.load();
		}

		return instance;
	}

	private void load() {
		IRDBMSEngine engine = (IRDBMSEngine) Utility.getDatabase(Constants.LOCAL_MASTER_DB);

		// this is only for local master!!!
		Connection conn = null;
		try {
			conn = engine.getConnection();
			try (Statement stmt = conn.createStatement()) {
				ResultSet rs = stmt.executeQuery("SELECT K, V from " + TABLE_NAME);
				while (rs.next()) {
					this.dataHash.put(rs.getString(1), rs.getString(2));
				}
			}
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			try {
				if (engine.isConnectionPooling() && conn != null) {
					conn.close();
				}
			} catch (SQLException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		}
	}

	public void put(String key, String value) {
		this.dataHash.put(key, value);
		this.dirty = true;
	}

	public boolean containsKey(String key) {
		return this.dataHash.containsKey(key);
	}

	public String get(String key) {
		return this.dataHash.get(key);
	}

	public void clear() {
		this.dataHash.clear();
	}

	public void persistBack() {
		IRDBMSEngine engine = (IRDBMSEngine) Utility.getDatabase(Constants.LOCAL_MASTER_DB);

		if (this.dirty) {
			Connection conn = null;
			Statement stmt = null;
			PreparedStatement ps = null;
			try {
				conn = engine.getConnection();
				stmt = conn.createStatement();
				stmt.execute("DELETE FROM " + TABLE_NAME);
				Iterator<String> keys = dataHash.keySet().iterator();
				ps = conn.prepareStatement("INSERT KVSTORE(K, V) VALUES(?, ?)");
				while (keys.hasNext()) {
					String key = keys.next();
					String value = dataHash.get(key);
					int parameterIndex = 1;
					ps.setString(parameterIndex++, key);
					ps.setString(parameterIndex++, value);
					ps.addBatch();
				}
				ps.executeBatch();
				if (!ps.getConnection().getAutoCommit()) {
					ps.getConnection().commit();
				}
				this.dirty = false;
			} catch (Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
			} finally {
				ConnectionUtils.closeStatement(stmt);
				ConnectionUtils.closeAllConnectionsIfPooling(engine, ps);
			}
		}
	}

}
