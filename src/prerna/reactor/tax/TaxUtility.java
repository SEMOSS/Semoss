package prerna.reactor.tax;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.util.Utility;

public class TaxUtility {

	private TaxUtility() {

	}

	/**
	 * Execute a query to get the hashcode used from the alias
	 * @param aliasList
	 * @return
	 */
	public static Map<String, String> mapAliasToHash(List<String> aliasList) {
		String filterQuery = getInFilter(aliasList);

		Map<String, String> aliasHashMap = new Hashtable<String, String>();
		try {
			// execute the query on both databases
			String sql = "SELECT ALIAS_1, HASHCODE FROM INPUTCSV WHERE ALIAS_1 " + filterQuery;
			execAliasToHashCodeQuery(Utility.getDatabase("MinInput"), sql, aliasHashMap);
			sql = "SELECT ALIAS_1, HASHCODE FROM IMPACTCSV WHERE ALIAS_1 " + filterQuery;
			execAliasToHashCodeQuery(Utility.getDatabase("MinImpact"), sql, aliasHashMap);
			sql = "SELECT ALIAS_1, HASHCODE FROM OUTPUTCSV WHERE ALIAS_1 " + filterQuery;
			execAliasToHashCodeQuery(Utility.getDatabase("MinOutput"), sql, aliasHashMap);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return aliasHashMap;
	}
	
	/**
	 * Execute the query on the engines to get the conversion from alias to hashcode
	 * @param engine
	 * @param sql
	 * @param aliasHashMap
	 * @throws Exception 
	 */
	private static void execAliasToHashCodeQuery(IDatabaseEngine engine, String sql, Map<String, String> aliasHashMap) throws Exception {
		if(engine == null) {
			return;
		}
		Map<String, Object> queryRet = (Map<String, Object>)engine.execQuery(sql);
		Statement stmt = (Statement) queryRet.get(RDBMSNativeEngine.STATEMENT_OBJECT);
		ResultSet rs = (ResultSet) queryRet.get(RDBMSNativeEngine.RESULTSET_OBJECT);
		try {
			flushRsToMap(rs, aliasHashMap);
		} catch(SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				stmt.close();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
			try {
				rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Flush a result set into a map
	 * Assumption that rs only returns 2 columns
	 * Assumption that column1 is the key, column2 is the value
	 * Assumption that rs only returns strings
	 * @param rs
	 * @param map
	 * @throws SQLException 
	 */
	private static void flushRsToMap(ResultSet rs, Map<String, String> map) throws SQLException {
		while(rs.next()) {
			map.put(rs.getString(1), rs.getString(2));
		}
	}

	/**
	 * Generate a string for the SQL IN operator
	 * assumes all inputs are strings
	 * @param aliasList
	 * @return
	 */
	private static String getInFilter(List<String> aliasList) {
		StringBuilder sql = new StringBuilder(" IN (");
		sql.append("'").append(aliasList.get(0)).append("'");
		for(int i = 1; i < aliasList.size(); i++) {
			sql.append(",'").append(aliasList.get(i)).append("'");
		}
		sql.append(")");
		return sql.toString();
	}
	
	public static double getLatestVersionForScenario(IDatabaseEngine engine, String clientID, double scenarioID) throws Exception {
		double scenarioRet = 1.0;
		String sql = "SELECT VERSION FROM INPUTCSV WHERE CLIENT_ID='" + "' AND SCENARIO=" + scenarioID + " ORDER BY VERSION DESC LIMIT 1";
		Map<String, Object> queryRet = (Map<String, Object>)engine.execQuery(sql);
		Statement stmt = (Statement) queryRet.get(RDBMSNativeEngine.STATEMENT_OBJECT);
		ResultSet rs = (ResultSet) queryRet.get(RDBMSNativeEngine.RESULTSET_OBJECT);
		try {
			while(rs.next()) {
				scenarioRet = rs.getDouble(1);
			}
		} catch(SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				stmt.close();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
			try {
				rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
		return scenarioRet;
	}
}
