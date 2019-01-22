package prerna.engine.impl.rdbms;

import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Vector;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.engine.api.IEngine;
import prerna.test.TestUtilityMethods;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class AuditDatabaseUtility {
	/**
	 * Get the list of modifications made by all users
	 * @param audit
	 * @param user
	 * @return
	 */
	public static List<Map<String, Map<String, Object>>> getEdits(AuditDatabase audit) {
		String query = "SELECT timestamp, id, type, table, key_column, key_column_value, altered_column, "
				+ "old_value, new_value, user FROM AUDIT_TABLE ORDER BY timestamp ASC;";
		Connection conn = audit.getConnection();
		Map<String, Map<String, Map<String, Map<String, Object>>>> userMap = new HashMap<>();
		Map<String, Map<String, Map<String, Object>>> retMap = new TreeMap<>();
		try {
			Statement statement = conn.createStatement();
			ResultSet rs = statement.executeQuery(query);
			while (rs.next()) {
				int i = 1;
				Timestamp timeStamp = rs.getTimestamp(i++);
				String time = timeStamp.toString();
				String id = rs.getString(i++);
				String type = rs.getString(i++);
				String table = rs.getString(i++);
				String keyCol = rs.getString(i++);
				String keyColValue = rs.getString(i++);
				String alteredCol = rs.getString(i++);
				String oldValue = rs.getString(i++);
				String newValue = rs.getString(i++);
				String user = rs.getString(i++);
				Map<String, Map<String, Object>> alteredColumns = new TreeMap<>();
				if (retMap.containsKey(time)) {
					alteredColumns = retMap.get(time);
				}
				Map<String, Object> colMap = new HashMap<>();
				colMap.put("oldValue", oldValue);
				colMap.put("newValue", newValue);
				colMap.put("type", type);
				colMap.put("table", table);
				colMap.put("keyColumn", keyCol);
				colMap.put("keyColumnValue", keyColValue);
				colMap.put("timestamp", time);
				colMap.put("id", id);
				colMap.put("user", user);
				alteredColumns.put(alteredCol, colMap);
				retMap.put(time, alteredColumns);
//				Map<String, Map<String, Map<String, Object>>> userMods = new HashMap<>(); 
//				if(userMap.containsKey(user)) {
//					userMods = userMap.get(user);
//				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

		List<Map<String, Map<String, Object>>> list = new Vector<Map<String, Map<String, Object>>>(retMap.values());
		return list;
	}
	
	

	/**
	 * Get the list of modifications made by a user
	 * @param audit
	 * @param user
	 * @return
	 */
	public static List<Map<String, Map<String, Object>>> getEditsByUser(AuditDatabase audit, String user) {
		String query = "SELECT timestamp, id, type, table, key_column, key_column_value, altered_column, "
				+ "old_value, new_value FROM AUDIT_TABLE "
				+ "WHERE user = '"+user+"' ORDER BY timestamp ASC;";
		System.out.println(query);
		Connection conn = audit.getConnection();
		Map<String, Map<String, Map<String, Object>>> retMap = new TreeMap<>();
		try {
			Statement statement = conn.createStatement();
			ResultSet rs = statement.executeQuery(query);
			while (rs.next()) {
				int i = 1;
				Timestamp timeStamp = rs.getTimestamp(i++);
				String time = timeStamp.toString();
				String id = rs.getString(i++);
				String type = rs.getString(i++);
				String table = rs.getString(i++);
				String keyCol = rs.getString(i++);
				String keyColValue = rs.getString(i++);
				String alteredCol = rs.getString(i++);
				String oldValue = rs.getString(i++);
				String newValue = rs.getString(i++);
				Map<String, Map<String, Object>> alteredColumns = new TreeMap<>();
				if (retMap.containsKey(time)) {
					alteredColumns = retMap.get(time);
				}
				Map<String, Object> colMap = new HashMap<>();
				colMap.put("oldValue", oldValue);
				colMap.put("newValue", newValue);
				colMap.put("type", type);
				colMap.put("table", table);
				colMap.put("keyColumn", keyCol);
				colMap.put("keyColumnValue", keyColValue);
				colMap.put("timestamp", time);
				colMap.put("id", id);
				alteredColumns.put(alteredCol, colMap);
				retMap.put(time, alteredColumns);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

		List<Map<String, Map<String, Object>>> list = new Vector<Map<String, Map<String, Object>>>(retMap.values());
		return list;
	}

	public static List<Map<String, Object>> getColumnUpdates(AuditDatabase audit, String keyColValue, String column) {
		String query = "SELECT timestamp, id, type, table, key_column, key_column_value, "
				+ "altered_column, old_value, new_value, user " + "FROM AUDIT_TABLE where altered_Column ='" + column
				+ "' and key_column_value = '" + keyColValue + "' ORDER BY timestamp asc;";
		Connection conn = audit.getConnection();
		List<Map<String, Object>> updates = new ArrayList<>();
		try {
			Statement statement = conn.createStatement();
			ResultSet rs = statement.executeQuery(query);
			while (rs.next()) {
				int i = 1;
				Timestamp timeStamp = rs.getTimestamp(i++);
				String time = timeStamp.toString();
				String id = rs.getString(i++);
				String type = rs.getString(i++);
				String table = rs.getString(i++);
				String keyCol = rs.getString(i++);
				String kcValue = rs.getString(i++);
				String alteredCol = rs.getString(i++);
				String oldValue = rs.getString(i++);
				String newValue = rs.getString(i++);
				String user = rs.getString(i++);
				Map<String, Object> colMap = new HashMap<>();
				colMap.put("oldValue", oldValue);
				colMap.put("newValue", newValue);
				colMap.put("type", type);
				colMap.put("table", table);
				colMap.put("keyColumn", kcValue);
				colMap.put("keyColumnValue", keyColValue);
				colMap.put("timestamp", time);
				colMap.put("id", id);
				colMap.put("user", user);
				updates.add(colMap);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return updates;
	}

	public static Timestamp getColumnLastModifiedTime(AuditDatabase audit, String keyColValue, String column) {
		String query = "SELECT timestamp FROM AUDIT_TABLE WHERE altered_column = '" + column
				+ "' AND key_column_value='" + keyColValue + "' ORDER BY timestamp DESC limit 1;";
		Connection conn = audit.getConnection();
		Timestamp lastModified = null;
		try {
			Statement statement = conn.createStatement();
			ResultSet rs = statement.executeQuery(query);
			while (rs.next()) {
				lastModified = rs.getTimestamp(1);
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}
		return lastModified;
	}

	public static int getTotalChangesOverTime(AuditDatabase audit, String dateTimeField, int timeDiff) {
		String query = "SELECT Count( *) FROM AUDIT_TABLE WHERE TIMESTAMP > DATEADD('" + dateTimeField + "',-"
				+ timeDiff + ", CURRENT_DATE);";
		Connection conn = audit.getConnection();
		int updateCount = -1;
		try {
			Statement statement = conn.createStatement();
			ResultSet rs = statement.executeQuery(query);
			while (rs.next()) {
				updateCount = rs.getInt(1);
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}
		return updateCount;
	}

	public static int getTotalColumnChangesOverTime(AuditDatabase audit, String dateTimeField, int timeDiff,
			String keyColValue, String column) {
		String query = "SELECT Count( *) FROM AUDIT_TABLE WHERE TIMESTAMP > DATEADD('" + dateTimeField + "',-"
				+ timeDiff + ", CURRENT_DATE) AND altered_column = '" + column + "' AND key_column_value='"
				+ keyColValue + "';";
		Connection conn = audit.getConnection();
		int updateCount = -1;
		try {
			Statement statement = conn.createStatement();
			ResultSet rs = statement.executeQuery(query);
			while (rs.next()) {
				updateCount = rs.getInt(1);
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}
		return updateCount;
	}

	public static int getTotalChangesOverTimeByUser(AuditDatabase audit, String dateTimeField, int timeDiff,
			String user) {
		String query = "SELECT Count( *) FROM AUDIT_TABLE WHERE TIMESTAMP > DATEADD('" + dateTimeField + "',-"
				+ timeDiff + ", CURRENT_DATE) and user = '" + user + "';";
		Connection conn = audit.getConnection();
		int updateCount = -1;
		try {
			Statement statement = conn.createStatement();
			ResultSet rs = statement.executeQuery(query);
			while (rs.next()) {
				updateCount = rs.getInt(1);
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}
		return updateCount;
	}

	public static int getTotalColumnChangesOverTimeByUser(AuditDatabase audit, String dateTimeField, int timeDiff,
			String keyColValue, String column, String user) {
		String query = "SELECT Count( *) FROM AUDIT_TABLE WHERE TIMESTAMP > DATEADD('" + dateTimeField + "',-"
				+ timeDiff + ", CURRENT_DATE) AND altered_column = '" + column + "' AND key_column_value='"
				+ keyColValue + "' and user = '" + user + "';";
		Connection conn = audit.getConnection();
		int updateCount = -1;
		try {
			Statement statement = conn.createStatement();
			ResultSet rs = statement.executeQuery(query);
			while (rs.next()) {
				updateCount = rs.getInt(1);
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}
		return updateCount;
	}
	
	/**
	 * Get the list of usernames that have made modifications to the database
	 * @param audit
	 * @return
	 */
	public static List<String> getUserList(AuditDatabase audit) {
		String query = "Select distinct user from Audit_Table;";
		Connection conn = audit.getConnection();
		List<String> users = new Vector<>();
		try {
			Statement statement = conn.createStatement();
			ResultSet rs = statement.executeQuery(query);
			while (rs.next()) {
				users.add(rs.getString(1));
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}
		return users;
	}
	
	

	public static void main(String[] args) throws Exception {
		Gson gson = new GsonBuilder().disableHtmlEscaping()
				.excludeFieldsWithModifiers(Modifier.STATIC, Modifier.TRANSIENT).setPrettyPrinting().create();
		TestUtilityMethods.loadDIHelper("C:\\Users\\rramirezjimenez\\Documents\\workspace\\Semoss\\RDF_Map.prop");
		String engineProp = "C:\\Users\\rramirezjimenez\\Documents\\workspace\\Semoss\\db\\LocalMasterDatabase.smss";
		IEngine coreEngine = new RDBMSNativeEngine();
		coreEngine.setEngineId(Constants.LOCAL_MASTER_DB_NAME);
		coreEngine.openDB(engineProp);
		DIHelper.getInstance().setLocalProperty(Constants.LOCAL_MASTER_DB_NAME, coreEngine);

		engineProp = "C:\\Users\\rramirezjimenez\\Documents\\workspace\\Semoss\\db\\Movie__424f8bdc-6f19-4aab-87de-bd70acaa9ae7.smss";
		RDBMSNativeEngine movie = new RDBMSNativeEngine();
		String engineID = "424f8bdc-6f19-4aab-87de-bd70acaa9ae7";
		movie.setEngineId(engineID);
		movie.setEngineName("Movie");
		movie.openDB(engineProp);
		DIHelper.getInstance().setLocalProperty(engineID, movie);
		RDBMSNativeEngine test = (RDBMSNativeEngine) Utility.getEngine(engineID);
		AuditDatabase audit = test.generateAudit();
		List userEdits = getEditsByUser(audit, "require login for user");
		List edits = getEdits(audit);
		 System.out.println(gson.toJson(edits));

		List columnChanges = getColumnUpdates(audit, "97", "Genre");
		// System.out.println(gson.toJson(columnChanges));

		Timestamp ts = getColumnLastModifiedTime(audit, "97", "Genre");
		// System.out.println(ts);

		int count = getTotalChangesOverTime(audit, "DAY", 7);
		count = getTotalChangesOverTime(audit, "MONTH", 7);
//		System.out.println(count);
		System.exit(0);

	}
}
