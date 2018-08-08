package prerna.auth;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.stream.Collectors;

import com.google.gson.internal.StringMap;

import jodd.util.BCrypt;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;

public class SecurityQueryUtils extends AbstractSecurityUtils {
	
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	
	/**
	 * Try to reconcile and get the engine id
	 * @return
	 */
	public static String testUserEngineIdForAlias(User user, String potentialId) {
		String userFilters = getUserFilters(user);
		List<String> ids = new Vector<String>();
		String query = "SELECT DISTINCT ENGINEPERMISSION.ENGINEID "
				+ "FROM ENGINEPERMISSION INNER JOIN ENGINE ON ENGINE.ENGINEID=ENGINEPERMISSION.ENGINEID "
				+ "WHERE ENGINE.ENGINENAME='" + potentialId + "' AND ENGINEPERMISSION.USERID IN " + userFilters;
		
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		ids = flushToListString(wrapper);
		if(ids.isEmpty()) {
			query = "SELECT DISTINCT ENGINE.ENGINEID FROM ENGINE WHERE ENGINE.ENGINENAME='" + potentialId + "' AND ENGINE.GLOBAL=TRUE";
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
			ids = flushToListString(wrapper);
		}
		
		if(ids.size() == 1) {
			potentialId = ids.get(0);
		} else if(ids.size() > 1) {
			throw new IllegalArgumentException("There are 2 databases with the name " + potentialId + ". Please pass in the correct id to know which source you want to load from");
		}
		
		return potentialId;
	}
	
	/**
	 * Get a list of the engine ids
	 * @return
	 */
	public static List<String> getEngineIds() {
		String query = "SELECT DISTINCT ENGINEID FROM ENGINE";
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		return flushToListString(wrapper);
	}
	
	/**
	 * Get the engine alias for a id
	 * @return
	 */
	public static String getEngineAliasForId(String id) {
		String query = "SELECT ENGINENAME FROM ENGINE WHERE ENGINEID='" + id + "'";
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		List<String> results = flushToListString(wrapper);
		if(results.isEmpty()) {
			return null;
		}
		return results.get(0);
	}
	
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	
	/*
	 * Querying engine data
	 */

	/**
	 * Get the list of the engine information that the user has access to
	 * @param userId
	 * @return
	 */
	public static List<Map<String, Object>> getUserDatabaseList(User user) {
		String userFilters = getUserFilters(user);
		String query = "SELECT DISTINCT ENGINE.ENGINEID as \"app_id\", ENGINE.ENGINENAME as \"app_name\", ENGINE.TYPE as \"app_type\", ENGINE.COST as \"app_cost\" "
				+ "FROM ENGINE "
				+ "LEFT JOIN ENGINEPERMISSION ON ENGINE.ENGINEID=ENGINEPERMISSION.ENGINEID "
				+ "WHERE (ENGINEPERMISSION.USERID IN " + userFilters + " OR ENGINE.GLOBAL=TRUE) "
				+ "ORDER BY ENGINE.ENGINENAME";
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		return flushRsToMap(wrapper);
	}
	
	/**
	 * Get the list of the engine information that the user has access to
	 * @param userId
	 * @return
	 */
	public static List<Map<String, Object>> getAllDatabaseList() {
		String query = "SELECT DISTINCT ENGINE.ENGINEID as \"app_id\", ENGINE.ENGINENAME as \"app_name\", ENGINE.TYPE as \"app_type\", ENGINE.COST as \"app_cost\" "
				+ "FROM ENGINE "
				+ "LEFT JOIN ENGINEPERMISSION ON ENGINE.ENGINEID=ENGINEPERMISSION.ENGINEID "
				+ "ORDER BY ENGINE.ENGINENAME";
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		return flushRsToMap(wrapper);
	}
	
	/**
	 * Get the list of the engine information that the user has access to
	 * @param userId
	 * @return
	 */
	public static List<Map<String, Object>> getUserDatabaseList(User user, String... engineFilter) {
		String userFilters = getUserFilters(user);
		String filter = createFilter(engineFilter); 
		String query = "SELECT DISTINCT ENGINE.ENGINEID as \"app_id\", ENGINE.ENGINENAME as \"app_name\", ENGINE.TYPE as \"app_type\", ENGINE.COST as \"app_cost\" "
				+ "FROM ENGINE "
				+ "LEFT JOIN ENGINEPERMISSION ON ENGINE.ENGINEID=ENGINEPERMISSION.ENGINEID "
				+ "WHERE "
				+ (!filter.isEmpty() ? ("ENGINE.ENGINEID " + filter + " AND ") : "")
				+ "(ENGINEPERMISSION.USERID IN " + userFilters + " OR ENGINE.GLOBAL=TRUE) "
				+ "ORDER BY ENGINE.ENGINENAME";
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		return flushRsToMap(wrapper);
	}
	
	/**
	 * Get the list of the engine information that the user has access to
	 * @param userId
	 * @return
	 */
	public static List<Map<String, Object>> getAllDatabaseList(String... engineFilter) {
		String filter = createFilter(engineFilter); 
		String query = "SELECT DISTINCT ENGINE.ENGINEID as \"app_id\", ENGINE.ENGINENAME as \"app_name\", ENGINE.TYPE as \"app_type\", ENGINE.COST as \"app_cost\" "
				+ "FROM ENGINE "
				+ "LEFT JOIN ENGINEPERMISSION ON ENGINE.ENGINEID=ENGINEPERMISSION.ENGINEID "
 				+ (!filter.isEmpty() ? ("WHERE ENGINE.ENGINEID " + filter + " ") : "")
				+ "ORDER BY ENGINE.ENGINENAME";
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		return flushRsToMap(wrapper);
	}
	
	public static Map<String, List<String>> getAggregateEngineMetadata(String engineId) {
		Map<String, List<String>> engineMeta = new HashMap<String, List<String>>();
		String query = "SELECT KEY, VALUE FROM ENGINEMETA WHERE ENGINEID='" + engineId + "'";
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		while(wrapper.hasNext()) {
			Object[] data = wrapper.next().getValues();
			String key = data[0].toString();
			String value = data[1].toString();
			if(!engineMeta.containsKey(key)) {
				engineMeta.put(key, new Vector<String>());
			}
			engineMeta.get(key).add(value);
		}
		return engineMeta;
	}
	
	/**
	 * Get user engines + global engines 
	 * @param userId
	 * @return
	 */
	public static List<String> getUserEngineIds(User user) {
		String userFilters = getUserFilters(user);
		String query = "SELECT DISTINCT ENGINEID FROM ENGINEPERMISSION WHERE USERID IN " + userFilters;
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		List<String> engineList = flushToListString(wrapper);
		engineList.addAll(getGlobalEngineIds());
		return engineList.stream().distinct().sorted().collect(Collectors.toList());
	}
	
	/**
	 * Get global engines
	 * @return
	 */
	public static Set<String> getGlobalEngineIds() {
		String query = "SELECT ENGINEID FROM ENGINE WHERE GLOBAL=TRUE";
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		return flushToSetString(wrapper, false);
	}
	
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	
	/**
	 * Determine if the user is the owner
	 * @param userId
	 * @param engineId
	 * @return
	 */
	public static boolean userIsOwner(User user, String engineId) {
		String userFilters = getUserFilters(user);
		return userIsOwner(userFilters, engineId);
	}
	
	/**
	 * Determine if the user is the owner
	 * @param userId
	 * @param engineId
	 * @return
	 */
	private static boolean userIsOwner(String userFilters, String engineId) {
		String query = "SELECT DISTINCT ENGINEPERMISSION.PERMISSION FROM ENGINEPERMISSION "
				+ "WHERE ENGINEID='" + engineId + "' AND USERID IN " + userFilters;
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		while(wrapper.hasNext()) {
			int permission = ((Number) wrapper.next().getValues()[0]).intValue();
			if(EnginePermission.isOwner(permission)) {
				return true;
			}
		}
		return false;
	}
	
	/**
	 * Determine if the user can modify the database
	 * @param engineId
	 * @param userId
	 * @return
	 */
	public static boolean userCanEditEngine(User user, String engineId) {
		String userFilters = getUserFilters(user);
		String query = "SELECT DISTINCT ENGINEPERMISSION.PERMISSION FROM ENGINEPERMISSION "
				+ "WHERE ENGINEID='" + engineId + "' AND USERID IN " + userFilters;
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		while(wrapper.hasNext()) {
			int permission = ((Number) wrapper.next().getValues()[0]).intValue();
			if(EnginePermission.canModify(permission)) {
				return true;
			}
		}
		return false;
	}
	
	/**
	 * Determine if the user can edit the insight
	 * User must be database owner OR be given explicit permissions on the insight
	 * @param userId
	 * @param engineId
	 * @param insightId
	 * @return
	 */
	public static boolean userCanEditInsight(User user, String engineId, String insightId) {
		String userFilters = getUserFilters(user);

		// if user is owner
		// they can do whatever they want
		if(userIsOwner(userFilters, engineId)) {
			return true;
		}
		
		// else query the database
		String query = "SELECT DISTINCT USERINSIGHTPERMISSION.PERMISSION FROM USERINSIGHTPERMISSION "
				+ "WHERE ENGINEID='" + engineId + "' AND INSIGHTID='" + insightId + "' AND USERID IN " + userFilters;
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		while(wrapper.hasNext()) {
			int permission = ((Number) wrapper.next().getValues()[0]).intValue();
			if(EnginePermission.canModify(permission)) {
				return true;
			}
		}
		
		return false;
	}
	
	/**
	 * Determine if the user can edit the insight
	 * User must be database owner OR be given explicit permissions on the insight
	 * @param userId
	 * @param engineId
	 * @param insightId
	 * @return
	 */
	public static boolean userCanViewInsight(User user, String engineId, String insightId) {
		String userFilters = getUserFilters(user);

		// if user is owner
		// they can do whatever they want
		if(userIsOwner(userFilters, engineId)) {
			return true;
		}
		
		if(insightIsGlobal(engineId, insightId)) {
			return true;
		}
		
		// else query the database
		String query = "SELECT DISTINCT USERINSIGHTPERMISSION.PERMISSION FROM USERINSIGHTPERMISSION  "
				+ "WHERE ENGINEID='" + engineId + "' AND INSIGHTID='" + insightId + "' AND USERID IN " + userFilters;
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		try {
			if(wrapper.hasNext()) {
				// do not care if owner/edit/read
				return true;
			}
		} finally {
			wrapper.cleanUp();
		}
		
		return false;
	}
	
	public static boolean insightIsGlobal(String engineId, String insightId) {
		String query = "SELECT DISTINCT INSIGHT.GLOBAL FROM INSIGHT  "
				+ "WHERE ENGINEID='" + engineId + "' AND INSIGHTID='" + insightId + "' AND INSIGHT.GLOBAL=TRUE";
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		try {
			if(wrapper.hasNext()) {
				// i already bound that global must be true
				return true;
			}
		} finally {
			wrapper.cleanUp();
		}
		
		return false;
	}
	
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	
	/*
	 * Querying insight data
	 */
	
	/**
	 * User has access to specific insights within a database
	 * User can access if:
	 * 	1) Is Owner, Editer, or Reader of insight
	 * 	2) Insight is global
	 * 	3) Is Owner of database
	 * 
	 * @param engineId
	 * @param userId
	 * @param searchTerm
	 * @param limit
	 * @param offset
	 * @return
	 */
	public static List<Map<String, Object>> searchUserInsights(User user, String engineId, String searchTerm, String limit, String offset) {
		String userFilters = getUserFilters(user);

		String query = "SELECT DISTINCT "
				+ "INSIGHT.ENGINEID AS \"app_id\", "
				+ "ENGINE.ENGINENAME AS \"app_name\", "
				+ "INSIGHT.INSIGHTID as \"app_insight_id\", "
				+ "INSIGHT.INSIGHTNAME as \"name\", "
				+ "INSIGHT.LAYOUT as \"layout\", "
				+ "CONCAT(INSIGHT.ENGINEID, '_', INSIGHT.INSIGHTID) AS \"id\" "
				+ "FROM INSIGHT "
				+ "INNER JOIN ENGINE ON ENGINE.ENGINEID=INSIGHT.ENGINEID "
				+ "LEFT JOIN ENGINEPERMISSION ON ENGINE.ENGINEID=ENGINEPERMISSION.ENGINEID "
				+ "LEFT JOIN USERINSIGHTPERMISSION ON USERINSIGHTPERMISSION.ENGINEID=INSIGHT.ENGINEID "
				+ "WHERE "
				+ "INSIGHT.ENGINEID='" + engineId + "' "
				+ "AND (USERINSIGHTPERMISSION.USERID IN " + userFilters + " OR INSIGHT.GLOBAL=TRUE OR "
						+ "(ENGINEPERMISSION.PERMISSION=1 AND ENGINEPERMISSION.USERID IN " + userFilters + ") ) "
				+ ( (searchTerm != null && !searchTerm.trim().isEmpty()) ? "AND REGEXP_LIKE(INSIGHT.INSIGHTNAME, '"+ escapeRegexCharacters(searchTerm) + "', 'i')" : "")
				+ "ORDER BY INSIGHT.INSIGHTNAME "
				+ ( (limit != null && !limit.trim().isEmpty()) ? "LIMIT " + limit + " " : "")
				+ ( (offset != null && !offset.trim().isEmpty()) ? "OFFSET " + offset + " ": "")
				;
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		return flushRsToMap(wrapper);
	}
	
	public static List<Map<String, Object>> searchInsights(String engineId, String searchTerm, String limit, String offset) {
		String query = "SELECT DISTINCT "
				+ "INSIGHT.ENGINEID AS \"app_id\", "
				+ "ENGINE.ENGINENAME AS \"app_name\", "
				+ "INSIGHT.INSIGHTID as \"app_insight_id\", "
				+ "INSIGHT.INSIGHTNAME as \"name\", "
				+ "INSIGHT.LAYOUT as \"layout\", "
				+ "CONCAT(INSIGHT.ENGINEID, '_', INSIGHT.INSIGHTID) AS \"id\" "
				+ "FROM INSIGHT "
				+ "INNER JOIN ENGINE ON ENGINE.ENGINEID=INSIGHT.ENGINEID "
				+ "LEFT JOIN USERINSIGHTPERMISSION ON USERINSIGHTPERMISSION.ENGINEID=INSIGHT.ENGINEID "
				+ "WHERE "
				+ "INSIGHT.ENGINEID='" + engineId + "' "
				+ ( (searchTerm != null && !searchTerm.trim().isEmpty()) ? "AND REGEXP_LIKE(INSIGHT.INSIGHTNAME, '"+ escapeRegexCharacters(searchTerm) + "', 'i')" : "")
				+ "ORDER BY INSIGHT.INSIGHTNAME "
				+ ( (limit != null && !limit.trim().isEmpty()) ? "LIMIT " + limit + " " : "")
				+ ( (offset != null && !offset.trim().isEmpty()) ? "OFFSET " + offset + " ": "")
				;
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		return flushRsToMap(wrapper);
	}

	/**
	 * Return top executed insights for engine as a map
	 * TODO: THIS IS A WEIRD FORMAT BUT CURRENTLY WHAT FE IS EXEPCTING....
	 * @param engineId
	 * @param limit
	 * @return
	 */
	public static Map<String, List<String>> getTopExecutedInsightsForEngine(String engineId, long limit) {
		String query = "SELECT DISTINCT INSIGHT.INSIGHTID, INSIGHT.INSIGHTNAME, INSIGHT.EXECUTIONCOUNT "
				+ "FROM INSIGHT "
				+ "WHERE INSIGHT.ENGINEID='" + engineId + "'"
				+ "ORDER BY INSIGHT.EXECUTIONCOUNT"
				;
		
		Vector<String> ids = new Vector<String>();
		Vector<String> names = new Vector<String>();

		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		while(wrapper.hasNext()) {
			Object[] row = wrapper.next().getValues();
			ids.add(row[0].toString());
			names.add(row[1].toString());
		}
		
		Map<String, List<String>> retMap = new HashMap<String, List<String>>();
		retMap.put("rdbmsId", ids);
		retMap.put("insightName", names);
		return retMap;
	}
	
	/**
	 * Get user insights + global insights in engine
	 * @param userId
	 * @param engineId
	 * @return
	 */
	public static List<String> getUserInsightsForEngine(User user, String engineId) {
		String userFilters = getUserFilters(user);
		String query = "SELECT INSIGHTID FROM USERINSIGHTPERMISSION WHERE ENGINEID='" + engineId + "' AND USER IN " + userFilters;
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		List<String> insightList = flushToListString(wrapper);
		insightList.addAll(getGlobalInsightIdsForEngine(engineId));
		return insightList.stream().distinct().sorted().collect(Collectors.toList());
	}
	
	public static Set<String> getGlobalInsightIdsForEngine(String engineId) {
		String query = "SELECT INSIGHTID FROM INSIGHT WHERE ENGINEID='" + engineId + "' AND GLOBAL=TRUE";
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		return flushToSetString(wrapper, false);
	}
	
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	
	/*
	 * Querying user data
	 */
	public static boolean userExists(String userId) {
		String query = "SELECT ID FROM USER WHERE ID='" + userId + "'";
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		try {
			if(wrapper.hasNext()) {
				return true;
			} else {
				return false;
			}
		}finally {

		}
	}
	
	//////////////////////////////////////////////////////////////////
	
	/*
	 * For autocompletion of user searching
	 */
	
	/**
	 * User will see specific insight predictions for their searches
	 * User can see records if:
	 * 	1) Is Owner, Editer, or Reader of insight
	 * 	2) Insight is global
	 * 	3) Is Owner of database
	 * 
	 * @param userId
	 * @param searchTerm
	 * @param limit
	 * @param offset
	 * @return
	 */
	public static List<String> predictUserInsightSearch(User user, String searchTerm, String limit, String offset) {
		String userFilters = getUserFilters(user);

		String query = "SELECT DISTINCT "
				+ "INSIGHT.INSIGHTNAME as \"name\" "
				+ "FROM INSIGHT "
				+ "LEFT JOIN ENGINEPERMISSION ON INSIGHT.ENGINEID=ENGINEPERMISSION.ENGINEID "
				+ "LEFT JOIN USERINSIGHTPERMISSION ON USERINSIGHTPERMISSION.ENGINEID=INSIGHT.ENGINEID "
				+ "WHERE "
				+ "(USERINSIGHTPERMISSION.USERID IN " + userFilters + " OR INSIGHT.GLOBAL=TRUE OR "
						+ "(ENGINEPERMISSION.PERMISSION=1 AND ENGINEPERMISSION.USERID IN " + userFilters + ") ) "
				+ ( (searchTerm != null && !searchTerm.trim().isEmpty()) ? "AND REGEXP_LIKE(INSIGHT.INSIGHTNAME, '"+ escapeRegexCharacters(searchTerm) + "', 'i')" : "")
				+ "ORDER BY INSIGHT.INSIGHTNAME "
				+ ( (limit != null && !limit.trim().isEmpty()) ? "LIMIT " + limit + " " : "")
				+ ( (offset != null && !offset.trim().isEmpty()) ? "OFFSET " + offset + " ": "")
				;
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		return flushToListString(wrapper);
	}
	
	public static List<String> predictInsightSearch(String searchTerm, String limit, String offset) {
		String query = "SELECT DISTINCT INSIGHT.INSIGHTNAME as \"name\" "
				+ "FROM INSIGHT "
				+ ( (searchTerm != null && !searchTerm.trim().isEmpty()) ? "WHERE REGEXP_LIKE(INSIGHT.INSIGHTNAME, '"+ escapeRegexCharacters(searchTerm) + "', 'i')" : "")
				+ "ORDER BY INSIGHT.INSIGHTNAME "
				+ ( (limit != null && !limit.trim().isEmpty()) ? "LIMIT " + limit + " " : "")
				+ ( (offset != null && !offset.trim().isEmpty()) ? "OFFSET " + offset + " ": "")
				;
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		return flushToListString(wrapper);
	}
	
	/*
	 * For searching in search bar
	 */
	
	/**
	 * 
	 * @param searchTerm
	 * @param limit
	 * @param offset
	 * @param sortOrder
	 * @param sortField
	 * @param engineFilter
	 * @return
	 */
	public static List<Map<String, Object>> searchUserInsightDataByName(User user, String searchTerm, String limit, String offset) {
		String userFilters = getUserFilters(user);

		String query = "SELECT DISTINCT "
				+ "INSIGHT.ENGINEID AS \"app_id\", "
				+ "ENGINE.ENGINENAME AS \"app_name\", "
				+ "INSIGHT.INSIGHTID as \"app_insight_id\", "
				+ "INSIGHT.INSIGHTNAME as \"name\", "
				+ "INSIGHT.EXECUTIONCOUNT as \"view_count\", "
				+ "INSIGHT.LAYOUT as \"layout\", "
				+ "CONCAT(INSIGHT.ENGINEID, '_', INSIGHT.INSIGHTID) AS \"id\" "
				+ "FROM INSIGHT "
				+ "INNER JOIN ENGINE ON ENGINE.ENGINEID=INSIGHT.ENGINEID "
				+ "LEFT JOIN ENGINEPERMISSION ON ENGINE.ENGINEID=ENGINEPERMISSION.ENGINEID "
				+ "LEFT JOIN USERINSIGHTPERMISSION ON USERINSIGHTPERMISSION.ENGINEID=INSIGHT.ENGINEID "
				+ "WHERE "
				+ "(USERINSIGHTPERMISSION.USERID IN " + userFilters + " OR INSIGHT.GLOBAL=TRUE OR "
						+ "(ENGINEPERMISSION.PERMISSION=1 AND ENGINEPERMISSION.USERID IN " + userFilters + ") ) "
				+ ( (searchTerm != null && !searchTerm.trim().isEmpty()) ? "AND REGEXP_LIKE(INSIGHT.INSIGHTNAME, '"+ escapeRegexCharacters(searchTerm) + "', 'i')" : "")
				+ "ORDER BY INSIGHT.INSIGHTNAME "
				+ ( (limit != null && !limit.trim().isEmpty()) ? "LIMIT " + limit + " " : "")
				+ ( (offset != null && !offset.trim().isEmpty()) ? "OFFSET " + offset + " ": "")
				;
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		return flushRsToMap(wrapper);
	}
	
	/**
	 * 
	 * @param searchTerm
	 * @param limit
	 * @param offset
	 * @param sortOrder
	 * @param sortField
	 * @param engineFilter
	 * @return
	 */
	public static List<Map<String, Object>> searchAllInsightDataByName(String searchTerm, String limit, String offset) {
		String query = "SELECT DISTINCT "
				+ "INSIGHT.ENGINEID AS \"app_id\", "
				+ "ENGINE.ENGINENAME AS \"app_name\", "
				+ "INSIGHT.INSIGHTID as \"app_insight_id\", "
				+ "INSIGHT.INSIGHTNAME as \"name\", "
				+ "INSIGHT.EXECUTIONCOUNT as \"view_count\", "
				+ "INSIGHT.LAYOUT as \"layout\", "
				+ "CONCAT(INSIGHT.ENGINEID, '_', INSIGHT.INSIGHTID) AS \"id\" "
				+ "FROM INSIGHT INNER JOIN ENGINE ON ENGINE.ENGINEID=INSIGHT.ENGINEID "
				+ ( (searchTerm != null && !searchTerm.trim().isEmpty()) ? "WHERE REGEXP_LIKE(INSIGHT.INSIGHTNAME, '"+ escapeRegexCharacters(searchTerm) + "', 'i')" : "")
				+ "ORDER BY INSIGHT.INSIGHTNAME "
				+ ( (limit != null && !limit.trim().isEmpty()) ? "LIMIT " + limit + " " : "")
				+ ( (offset != null && !offset.trim().isEmpty()) ? "OFFSET " + offset + " ": "")
				;
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		return flushRsToMap(wrapper);
	}
	
	/**
	 * 
	 * @param searchTerm
	 * @param engineFilter
	 * @return
	 */
	public static List<Map<String, Object>> getInsightFacetDataByName(String searchTerm, String[] engineFilter) {
		String filter = createFilter(engineFilter); 
		String query = "SELECT DISTINCT "
				+ "ENGINEID, "
				+ "LAYOUT, "
				+ "COUNT(ENGINEID) "
				+ "FROM INSIGHT LEFT JOIN ENGINE ON INSIGHT.ENGINEID=ENGINE.ENGINEID "
				+ "WHERE "
				+ "REGEXP_LIKE(INSIGHT.INSIGHTNAME, '"+ escapeRegexCharacters(searchTerm) + "', 'i') " 
				+ "AND (INSIGHT.ENGINEID " + filter + " OR ENGINE.GLOBAL=TRUE) "
				+ "GROUP BY LAYOUT, ENGINEID;";
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		return flushRsToMap(wrapper);
	}
	
	private static String escapeRegexCharacters(String s) {
		s = s.trim();
		s = s.replace("(", "\\(");
		s = s.replace(")", "\\)");
		return s;
	}
	
	/**
	 * Get all ids from user object
	 * @param user
	 * @return
	 */
	private static String getUserFilters(User user) {
		StringBuilder b = new StringBuilder();
		b.append("(");
		if(user != null) {
			List<AuthProvider> logins = user.getLogins();
			if(!logins.isEmpty()) {
				int numLogins = logins.size();
				b.append("'").append(user.getAccessToken(logins.get(0)).getId()).append("'");
				for(int i = 1; i < numLogins; i++) {
					b.append(", '").append(user.getAccessToken(logins.get(0)).getId()).append("'");
				}
			}
		}
		b.append(")");
		return b.toString();
	}
	
	
	//TODO:
	//TODO:
	//TODO:
	//TODO:
	//TODO:
	//TODO:
	//TODO:
	//TODO:
	//TODO:
	//TODO:
	//TODO:
	//TODO:
	
//	protected static String convertArrayToDbString(ArrayList<String> list, boolean stringList){
//		String listString = "";
//		String quotes = "'";
//		if(!stringList) {
//			quotes = "";	
//		}
//		for(String groupId : list){
//			if(listString.isEmpty())
//				listString += quotes + groupId + quotes;
//			else 
//				listString += ", " + quotes + groupId + quotes;
//		}
//		return listString.equals("") && stringList ? "''" : listString;
//	}
//
//	/**
//	 * Returns a list of values given a query with one column/variable.
//	 * 
//	 * @param query		Query to be executed to retrieve engine names
//	 * @return			List of engine names
//	 */
//	private static ArrayList<String[]> runQuery(String query) {
//		System.out.println("Executing security query: " + query);
//		IRawSelectWrapper sjsw = WrapperManager.getInstance().getRawWrapper(securityDb, query);
//		String[] names = sjsw.getHeaders();
//		ArrayList<String[]> ret = new ArrayList<String[]>();
//		while(sjsw.hasNext()) {
//			IHeadersDataRow sjss = sjsw.next();
//			String[] rowValues = new String[names.length];
//			Object[] values = sjss.getValues();
//			for(int i = 0; i < names.length; i++) {
//				rowValues[i] = values[i].toString();
//			}
//			ret.add(rowValues);
//		}
//
//		return ret;
//	}
//
//	/**
//	 * Returns a list of values given a query with one column/variable.
//	 * 
//	 * @param query		Query to be executed to retrieve engine names
//	 * @return			List of engine names
//	 */
//	protected static List<Object[]> runQuery2(String query) {
//		System.out.println("Executing security query: " + query);
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
//		List<Object[]> ret = new ArrayList<Object[]>();
//		while(wrapper.hasNext()) {
//			IHeadersDataRow row = wrapper.next();
//			ret.add(row.getValues());
//		}
//
//		return ret;
//	}
	
	/**
	 * Check if a user (user name or email) exist in the security database
	 * @param username
	 * @param email
	 * @return true if user is found otherwise false.
	 */
	public static boolean checkUserExist(String username, String email){
		String query = "SELECT * FROM USER WHERE USERNAME='" + username + "' OR EMAIL='" + email + "'";
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		try {
			return wrapper.hasNext();
		} finally {
			wrapper.cleanUp();
		}
	}

	/**
	 * Verifies user information provided in the log in screen to allow or not 
	 * the entry in the application.
	 * @param user user name
	 * @param password
	 * @return true if user exist and password is correct otherwise false.
	 */
	public static boolean logIn(String user, String password){
		Map<String, String> databaseUser = getUserFromDatabase(user);
		if(!databaseUser.isEmpty()){
			String typedHash = hash(password, databaseUser.get("SALT"));
			return databaseUser.get("PASSWORD").equals(typedHash);
		} else {
			return false;
		}
	}

	/**
	 * Brings all the user basic information from the database.
	 * @param username 
	 * @return User retrieved from the database otherwise null.
	 */
	private static Map<String, String> getUserFromDatabase(String username) {
		Map<String, String> user = new HashMap<String, String>();
		String query = "SELECT ID, NAME, USERNAME, EMAIL, TYPE, ADMIN, PASSWORD, SALT FROM USER WHERE USERNAME='" + username + "'";
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		String[] names = wrapper.getHeaders();
		if(wrapper.hasNext()) {
			Object[] values = wrapper.next().getValues();
			user.put(names[0], values[0].toString());
			user.put(names[1], values[1].toString());
			user.put(names[2], values[2].toString());
			user.put(names[3], values[3].toString());
			user.put(names[4], values[4].toString());
			user.put(names[5], values[5].toString());
			user.put(names[6], values[6].toString());
			user.put(names[7], values[7].toString());
		}
		return user;
	}

	/**
	 * Current salt generation by BCrypt
	 * @return salt
	 */
	protected static String generateSalt(){
		return BCrypt.gensalt();
	}

	/**
	 * Create the password hash based on the password and salt provided.
	 * @param password
	 * @param salt
	 * @return hash
	 */
	protected static String hash(String password, String salt) {
		return BCrypt.hashpw(password, salt);
	}

	/**
	 * Check if the user is an admin
	 * 
	 * @param userId	String representing the id of the user to check
	 */
	public static Boolean isUserAdmin(String userId) {
		String query = "SELECT ADMIN FROM USER WHERE ID='" + userId + "';";
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		List<String[]> ret = flushRsToListOfStrArray(wrapper);
		if(!ret.isEmpty()) {
			return Boolean.parseBoolean(ret.get(0)[0]);
		}
		return false;
	}

	/**
	 * Returns a list of all engines that a given user is listed as an owner for.
	 * @param userId	ID of the user
	 * @return			List of engine names
	 */
	public static List<String> getUserOwnedEngines(String userId) {
		String query = "SELECT DISTINCT ENGINE.ENGINEID "
				+ "FROM ENGINE, ENGINEPERMISSION "
				+ "WHERE "
				+ "ENGINEPERMISSION.USERID='" + userId + "' "
				+ "AND ENGINEPERMISSION.PERMISSION=" + EnginePermission.OWNER.getId() + " "
				+ "AND ENGINE.ENGINEID=ENGINEPERMISSION.ENGINEID";
		
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		return flushToListString(wrapper);
	}

	/**
	 * Returns a list of all engines that a given user can see (has been given any level of permission).
	 * @param userId	ID of the user
	 * @return			List of engine names
	 */
	public static Set<String> getUserAccessibleEngines(String userId) {
		Set<String> ret = null;
		String query = null;

		if(isUserAdmin(userId)) {
			query = "SELECT DISTINCT ENGINE.ENGINEID FROM ENGINE";
			IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
			ret = flushToSetString(wrapper, false);
		} else {
			// get the engine ids this user has access to
			query = "SELECT DISTINCT ENGINE.ENGINEID "
					+ "FROM ENGINE, ENGINEPERMISSION "
					+ "WHERE "
					+ "ENGINEPERMISSION.USERID='" + userId + "' "
					+ "AND ENGINE.ENGINEID=ENGINEPERMISSION.ENGINEID;";
			IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
			ret = flushToSetString(wrapper, false);

			// add the engine ids your group has access to
			query = "SELECT DISTINCT ENGINE.ENGINEID "
					+ "FROM ENGINE, USER, GROUPENGINEPERMISSION, GROUPMEMBERS "
					+ "WHERE "
					+ "USER.ID='" + userId + "' "
					+ "AND GROUPMEMBERS.MEMBERID=USER.ID "
					+ "AND GROUPMEMBERS.GROUPID=GROUPENGINEPERMISSION.GROUPID";
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
			ret.addAll(flushToSetString(wrapper, false));
		}

		return ret;
	}

//	/**
//	 * 
//	 * @param userId
//	 * @param isAdmin
//	 * @return
//	 */
//	public static List<Map<String, String>> getUserDatabases(String userId, boolean isAdmin){
//		List<Map<String, String>> ret = new ArrayList<Map<String, String>>();
//
//		if(isAdmin && !isUserAdmin(userId)){
//			throw new IllegalArgumentException("The user isn't an admin");
//		}
//
//		String query = "";
//		List<String[]> engines = new ArrayList<>();
//
//		if(!isAdmin){
//			query = "SELECT e.ENGINEID AS DB_ID, e.ENGINENAME AS DB_NAME, e.global AS PUBLIC, ep.permission AS DB_PERMISSION, ep.visibility AS VISIBILITY "
//					+ "FROM ENGINEPERMISSION ep JOIN ENGINE e ON(ep.engineid = e.engineid)  "
//					+ "WHERE ep.userid = '?1'";
//			query = query.replace("?1", userId);
//
//			engines = runQuery(query);
//
//			query = "SELECT ge.engine AS DB_ID, e.ENGINENAME AS DB_NAME, e.global AS PUBLIC, ge.permission AS DB_PERMISSION, v.visibility AS VISIBILITY "
//					+ "FROM GROUPMEMBERS gm JOIN GROUPENGINEPERMISSION ge ON (ge.groupid = gm.groupid) JOIN ENGINE e ON (ge.engine = e.engineid) "
//					+ "JOIN ENGINEGROUPMEMBERVISIBILITY v ON(gm.id = v.groupmembersid AND ge.id = groupenginepermissionid)  "
//					+ "WHERE gm.memberid = '?1'";
//			query = query.replace("?1", userId);
//			engines.addAll(runQuery(query));
//
//			engines.addAll(getPublicEngines(engines, null));
//		} else {
//			query = "SELECT e.engineid AS DB_ID, e.enginename AS DB_NAME, e.global AS PUBLIC "
//					+ "FROM ENGINE e "
//					+ "WHERE e.engineid != '1'";
//
//			engines = runQuery(query);	
//		}
//
//		for(String[] engine : engines) {
//			StringMap<String> dbProp = new StringMap<>();
//
//			dbProp.put("db_id", engine[0]);
//			dbProp.put("db_name", engine[1]);
//			dbProp.put("db_public", engine[2]);
//			if(!isAdmin){
//				if(engine[3] == null){
//					dbProp.put("db_permission", EnginePermission.EDIT.getPermission());
//				} else {
//					dbProp.put("db_permission", EnginePermission.getPermissionValueById(engine[3]));
//				}
//				dbProp.put("db_visibility", engine[4]);
//			}
//			ret.add(dbProp);
//		}
//		return ret;
//	}
//
//	/**
//	 * Get only the public engines that the user don't have a straight relationship established 
//	 * @param engines other engines the user is related to
//	 * @return public engines
//	 */
//	private static List<String[]> getPublicEngines(ArrayList<String[]> engines, String userId) {
//
//		String query = "";
//		ArrayList<String> allEngines = new ArrayList<>();
//		for(String[] engine : engines){
//			allEngines.add(engine[0]);
//		}
//
//		if(userId != null){
//
//			query = "SELECT e.ENGINEID FROM ENGINEPERMISSION ep JOIN ENGINE e ON (e.engineid = ep.engineid) WHERE ep.userid = '?1' AND ep.visibility = FALSE AND e.global =TRUE";
//			query = query.replace("?1", userId);
//
//			List<Object[]> publicHiddenEngines = runQuery2(query);
//
//			query = "SELECT ENGINE_ID FROM (SELECT e.ENGINEID AS ENGINE_ID, gep.ID AS ID1, temp.AID AS ID2 FROM GROUPENGINEPERMISSION gep JOIN ENGINE e ON (gep.engine = e.engineid) JOIN (SELECT ID AS AID, GROUPID FROM GROUPMEMBERS WHERE MEMBERID = '?1') temp ON (gep.groupid = temp.GROUPID) WHERE ENGINE = '?1' AND e.global = TRUE)  b JOIN ENGINEGROUPMEMBERVISIBILITY v ON (b.id1 = v.GROUPENGINEPERMISSIONID AND b.id2 = v.GROUPMEMBERSID) WHERE v.visibility = FALSE;";
//			query = query.replace("?1", userId);
//			publicHiddenEngines.addAll(runQuery2(query));
//
//			for(Object[] p : publicHiddenEngines){
//				String[] engine = Arrays.stream(p).map(Object::toString).
//						toArray(String[]::new);
//				allEngines.add(engine[0]);
//			}
//
//		}
//
//		//TAP_Site_Data
//		//allEngines.add("1");
//		query = "";
//
//		query = "SELECT e.engineid AS ID, e.enginename AS NAME, e.global AS PUBLIC, 'Edit' AS PERMISSIONS, 'true' AS VISIBILITY FROM ENGINE e WHERE e.engineid NOT IN ?1 AND global = TRUE";	
//
//		query = query.replace("?1", "(" + convertArrayToDbString(allEngines, true) + ")");
//
//		List<Object[]> res = runQuery2(query);
//		List<String[]> resPrimitive = new ArrayList<>();
//		for(Object[] r : res){
//			String[] engine = Arrays.stream(r).map(Object::toString).
//					toArray(String[]::new);
//			resPrimitive.add(engine);
//		}
//		return resPrimitive;
//	}
//
//	public static StringMap<ArrayList<StringMap<String>>> getDatabaseUsersAndGroups(String userId, String engineId, boolean isAdmin){
//
//		StringMap<ArrayList<StringMap<String>>> ret = new StringMap<>();
//		ret.put("groups", new ArrayList<>());
//		ret.put("users", new ArrayList<>());
//
//		if(isAdmin && !isUserAdmin(userId)){
//			throw new IllegalArgumentException("This user is not an admin. ");
//		}
//
//		//TODO add check if user can access this engine as owner
//
//		String query = "SELECT u.id as ID, u.name as NAME, ep.permission as PERMISSION "
//				+ "FROM ENGINEPERMISSION ep JOIN User u ON (ep.userid = u.id) "
//				+ "WHERE ep.engineid = '?1'";
//		query = query.replace("?1", engineId);
//
//		ArrayList<String[]> users = runQuery(query);
//
//		for(String[] user : users) {
//			StringMap<String> userInfo = new StringMap<>();
//
//			userInfo.put("id", user[0]);
//			userInfo.put("name", user[1]);
//			userInfo.put("permission", EnginePermission.getPermissionValueById(user[2]));	
//
//			ret.get("users").add(userInfo);
//		}
//
//		String groupQuery = "SELECT ug.id AS ID, ug.name as NAME, ge.permission AS PERMISSION "
//				+ "FROM GROUPENGINEPERMISSION ge JOIN USERGROUP ug ON (ge.groupid = ug.id) "
//				+ "WHERE ge.engine = '?1'";
//		groupQuery = groupQuery.replace("?1", engineId);
//
//		ArrayList<String[]> groups = runQuery(groupQuery);
//
//		for(String[] group : groups) {
//			StringMap<String> userInfo = new StringMap<>();
//
//			userInfo.put("id", group[0]);
//			userInfo.put("name", group[1]);
//			userInfo.put("permission", EnginePermission.getPermissionValueById(group[2]));	
//
//			ret.get("groups").add(userInfo);
//		}
//
//		return ret;
//	}
//
//
//	private static void getGroupsWithoutMembers(ArrayList<HashMap<String, Object>> ret, String query, String userId){
//		ArrayList<String[]> groupsWithoutMembers = runQuery(query);
//
//		for(String[] groupsWithoutMember : groupsWithoutMembers) {
//			String groupId = groupsWithoutMember[0];
//			String groupName = groupsWithoutMember[1];
//
//			HashMap<String, Object> groupObject = new HashMap<>();
//			groupObject.put("group_id", groupId);
//			groupObject.put("group_name", groupName);
//			groupObject.put("group_users", new ArrayList<StringMap<String>>());
//
//			ret.add(groupObject);
//		}
//	}
//
//	private static int indexGroup(ArrayList<HashMap<String, Object>> ret, String groupId){
//		for (int i = 0; i < ret.size(); i++) {
//			if(ret.get(i).get("group_id").equals(groupId)){
//				return i;
//			}
//		}
//		return -1;
//	}
//
//	private static void getGroupsAndMembers(ArrayList<HashMap<String, Object>> ret, String query, String userId){
//		ArrayList<String[]> groupsAndMembers = runQuery(query);
//
//		for(String[] groupAndMember : groupsAndMembers) {
//			String groupId = groupAndMember[0];
//			String groupName = groupAndMember[1];
//
//			StringMap<ArrayList<StringMap<String>>> groupUsersObject = new StringMap<>();
//
//			StringMap<String> user = new StringMap<String>();
//			user.put("id", groupAndMember[2]);
//			user.put("name", groupAndMember[3]);
//			user.put("email", groupAndMember[4]);
//
//			int indexGroup = indexGroup(ret, groupId);
//
//			if(indexGroup == -1) {
//				ArrayList<StringMap<String>> newGroup = new ArrayList<StringMap<String>>();
//				newGroup.add(user);
//				HashMap<String, Object> groupObject = new HashMap<>();
//				groupObject.put("group_id", groupId);
//				groupObject.put("group_name", groupName);
//				groupObject.put("group_users", newGroup);
//
//				ret.add(groupObject);
//			} else {
//				ArrayList<StringMap<String>> updateGroup = (ArrayList<StringMap<String>>) ret.get(indexGroup).get("group_users");
//				updateGroup.add(user);
//			}
//		}
//	}
//
//	/**
//	 * Get all Groups and list of user for each group,
//	 * if the user is admin returns all the groups.
//	 * @param userId
//	 * @return all groups and list of users for each group
//	 */
//	public static ArrayList<HashMap<String, Object>> getGroupsAndMembersForUser(String userId) {
//
//		ArrayList<HashMap<String, Object>> ret = new ArrayList<>();
//
//		String query = "SELECT ug.ID AS GROUP_ID, ug.NAME AS GROUP_NAME FROM USERGROUP ug LEFT JOIN GROUPMEMBERS gm ON(ug.ID = gm.GROUPID) WHERE GROUPID IS NULL AND ug.owner = '?1'";
//		query = query.replace("?1", userId);
//		getGroupsWithoutMembers(ret, query, userId);
//
//		query = "SELECT ug.ID AS GROUP_ID, ug.NAME AS GroupName, u.ID AS MEMBER_ID, u.NAME AS MEMBERNAME, u.EMAIL AS EMAIL FROM UserGroup ug JOIN GroupMembers gm ON(gm.groupid = ug.id) JOIN User u ON(gm.memberid = u.id) WHERE ug.owner = '?1'";
//		query = query.replace("?1", userId);
//		getGroupsAndMembers(ret, query, userId);
//
//		return ret;
//	}
//
//	public static ArrayList<String[]> getAllEnginesOwnedByUser(String userId) {
//		String query = "SELECT DISTINCT Engine.ENGINENAME AS EngineName FROM Engine, User, UserGroup, GroupMembers, Permission, EnginePermission, GroupEnginePermission "
//				+ "WHERE User.ID='" + userId + "' "
//				+ "AND ((User.ID=EnginePermission.USERID AND EnginePermission.PERMISSION=" + EnginePermission.OWNER.getId() + ") "
//				+ "OR (UserGroup.ID=GroupEnginePermission.GROUPID AND GroupEnginePermission.PERMISSION=" + EnginePermission.OWNER.getId() + " "
//				+ "AND UserGroup.ID=GroupMembers.GROUPID AND GroupMembers.MEMBERID='" + userId + "') )";
//
//		return runQuery(query);
//	}
//
//	public static HashMap<String, ArrayList<StringMap<String>>> getAllPermissionsGrantedByEngine(String userId, String engineName) {
//		String query = "SELECT DISTINCT ug.NAME AS GROUPNAME, p.NAME AS PERMISSIONNAME FROM User u, UserGroup ug, Engine e, EnginePermission ep, GroupEnginePermission gep, Permission p "
//				+ "WHERE ug.ID=gep.GROUPID "
//				+ "AND e.ENGINENAME='" + engineName + "' "
//				+ "AND gep.ENGINE=e.ENGINEID "
//				+ "AND gep.PERMISSION=p.ID "
//				+ "AND ep.ENGINEID=e.ENGINEID AND ep.PERMISSION=" + EnginePermission.OWNER.getId() + " AND ep.USERID='" + userId + "';";
//
//		ArrayList<StringMap<String>> groups = new ArrayList<StringMap<String>>();
//		for(String[] groupPermissions : runQuery(query)) {
//			StringMap<String> map = new StringMap<String>();
//			map.put("name", groupPermissions[0]);
//			map.put("permission", groupPermissions[1]);
//			groups.add(map);
//		}
//
//
//		ArrayList<String> engines = getUserOwnedEngines(userId);
//		query = "SELECT DISTINCT u.ID AS ID, u.NAME AS USERNAME, p.NAME AS PERMISSIONNAME, u.EMAIL AS EMAIL FROM User u, Engine e, EnginePermission ep, Permission p "
//				+ "WHERE u.ID=ep.USERID "
//				+ "AND ep.ENGINEID=e.ENGINEID "
//				+ "AND ep.PERMISSION=p.ID "
//				+ "AND e.ENGINENAME IN ('";
//
//		for(int i = 0; i < engines.size(); i++) {
//			if(i != engines.size()-1) {
//				query += engines.get(i) + "', '";
//			} else {
//				query += engines.get(i);
//			}
//		}
//		query += "');";
//
//		ArrayList<StringMap<String>> users = new ArrayList<StringMap<String>>();
//		for(String[] userPermissions : runQuery(query)) {
//			StringMap<String> map = new StringMap<String>();
//			map.put("id", userPermissions[0]);
//			map.put("name", userPermissions[1]);
//			map.put("permission", userPermissions[2]);
//			map.put("email", userPermissions[3]);
//			users.add(map);
//		}
//
//		HashMap<String, ArrayList<StringMap<String>>> ret = new HashMap<String, ArrayList<StringMap<String>>>();
//		ret.put("groups", groups);
//		ret.put("users", users);
//
//		return ret;
//	}
//
//	public static ArrayList<StringMap<String>> searchForUser(String searchTerm) {
//		String query = "SELECT DISTINCT User.ID AS ID, User.NAME AS NAME, User.EMAIL AS EMAIL FROM User WHERE UPPER(User.NAME) LIKE UPPER('%" + searchTerm + "%') OR UPPER(User.EMAIL) LIKE UPPER('%" + searchTerm + "%') AND TYPE != 'anonymous';";
//		ArrayList<StringMap<String>> users = new ArrayList<StringMap<String>>();
//
//		for(String[] s : runQuery(query)) {
//			StringMap<String> map = new StringMap<String>();
//			map.put("id", s[0]);
//			map.put("name", s[1]);
//			map.put("email", s[2]);
//			users.add(map);
//		}
//
//		return users;
//	}
//
//	/**
//	 * Get all engines associated with the userId and the permission that the user has for the engine,
//	 * @param userId
//	 * @return List of "EngineName, UserName and Permission" for the specific user.
//	 */
//	public static ArrayList<StringMap<String>> getAllEnginesAndPermissionsForUser(String userId) {
//		ArrayList<String[]> ret = new ArrayList<String[]>();
//
//		String query = "SELECT e.ENGINENAME AS ENGINENAME, u.NAME AS OWNER, p.NAME AS PERMISSION "
//				+ "FROM Engine e JOIN EnginePermission ep ON (e.engineid = ep.engineid) "
//				+ "JOIN User u ON(ep.user = u.id) "
//				+ "JOIN Permission p ON(ep.permission = p.id) "
//				+ "WHERE ep.userid='?1'";
//		query = query.replace("?1", userId);
//
//		ret = runQuery(query);
//
//		query = "SELECT DISTINCT e.ENGINENAME AS ENGINENAME, u.NAME AS OWNER, p.NAME AS PERMISSIONNAME "
//				+ "FROM Engine e JOIN GroupEnginePermission gep ON (e.engineid =gep.engine) "
//				+ "JOIN GroupMembers gm ON(gep.groupid = gm.groupid) "
//				+ "JOIN User u ON(gm.memberid = u.id) "
//				+ "JOIN Permission p ON(gep.permission = p.id) "
//				+ "WHERE u.ID='?1'";
//		query = query.replace("?1", userId);
//
//		ret.addAll(runQuery(query));
//
//		ArrayList<StringMap<String>> list = new ArrayList<StringMap<String>>();
//		for(String[] eng : ret) {
//			StringMap<String> map = new StringMap<String>();
//			map.put("name", eng[0]);
//			map.put("owner", eng[1]);
//			map.put("permission", eng[2]);
//			list.add(map);
//		}
//
//		return list;
//	}
//
//	public static String isUserWithDatabasePermissionAlready(String userId, ArrayList<String> groupsId, ArrayList<String> usersId){
//		String ret = "";
//
//		String username = getUsernameByUserId(userId);
//
//		if(usersId.contains(userId)){
//			ret += "The user " + username + " already has a direct relationship with the database. ";
//		}
//
//		String query = "SELECT gm.groupid AS ID, ug.name AS NAME, u.name AS OWNER FROM GROUPMEMBERS gm JOIN USERGROUP ug ON (gm.groupid = ug.id) JOIN User u ON(ug.owner = u.id) WHERE gm.groupid IN ?1 AND gm.memberid = '?2'";	
//		query = query.replace("?1", "(" + convertArrayToDbString(groupsId, false) + ")");
//		query = query.replace("?2", userId);
//
//		ArrayList<String[]> result = runQuery(query);
//
//		for(String[] row : result){
//			String groupId = row[0];
//			String groupName = row[1];
//			String groupOwner = row[2];
//
//			ret += "The user " + username + " is in " + groupName + " owned by " + groupOwner + ". "; 
//
//		}
//
//		return ret.isEmpty() ? "true" : ret;
//	}
//
//	private static String getUsernameByUserId(String userId) {
//		// TODO Auto-generated method stub
//		String query = "SELECT NAME FROM USER WHERE ID = '?1'";
//		query = query.replace("?1", userId);
//
//		IRawSelectWrapper sjsw = WrapperManager.getInstance().getRawWrapper(securityDb, query);
//		if(sjsw.hasNext()) {
//			IHeadersDataRow sjss = sjsw.next();
//			return sjss.getValues()[0].toString();
//		}
//		return null;
//	}
//
//	/**
//	 * Get all database users who aren't "Anonymous" type
//	 * @param userId
//	 * @return
//	 * @throws IllegalArgumentException
//	 */
//	public static ArrayList<StringMap<String>> getAllDbUsers(String userId) throws IllegalArgumentException{
//		ArrayList<StringMap<String>> ret = new ArrayList<>();  
//		if(isUserAdmin(userId)){
//			String query = "SELECT ID, NAME, USERNAME, EMAIL, TYPE, ADMIN FROM USER WHERE TYPE != 'anonymous'";
//			ret = getSimpleQuery(query);
//		} else {
//			throw new IllegalArgumentException("The user can't access to this resource. ");
//		}
//		return ret;
//	}
//
//	/**
//	 * Returns a list of values given a query with one column/variable.
//	 * 
//	 * @param query		Query to be executed to retrieve engine names
//	 * @return			List of engine names
//	 */
//	private static ArrayList<StringMap<String>> getSimpleQuery(String query) {
//		System.out.println("Executing security query: " + query);
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
//		ArrayList<StringMap<String>> ret = new ArrayList<>();
//		while(wrapper.hasNext()) {
//			IHeadersDataRow row = wrapper.next();
//			Object[] headers = row.getHeaders();
//			Object[] values = row.getValues();
//			StringMap<String> rowData = new StringMap<>();
//			for(int idx = 0; idx < headers.length; idx++){
//				if(headers[idx].toString().toLowerCase().equals("type") && values[idx].toString().equals("NATIVE")){
//					rowData.put(headers[idx].toString().toLowerCase(), "Default");
//				} else {
//					rowData.put(headers[idx].toString().toLowerCase(), values[idx].toString());
//				}
//			}
//			ret.add(rowData);
//		}
//
//		return ret;
//	}
//
//	/**
//	 * Brings the user id from database.
//	 * @param username
//	 * @return userId if it exists otherwise null
//	 */
//	public static String getUserId(String username) {
//		// TODO Auto-generated method stub
//		String query = "SELECT ID FROM USER WHERE USERNAME = '?1'";
//		query = query.replace("?1", username);
//
//		IRawSelectWrapper sjsw = WrapperManager.getInstance().getRawWrapper(securityDb, query);
//		if(sjsw.hasNext()) {
//			IHeadersDataRow sjss = sjsw.next();
//			return sjss.getValues()[0].toString();
//		}
//		return null;
//
//	}
//
//	/**
//	 * Brings the user name from database.
//	 * @param username
//	 * @return userId if it exists otherwise null
//	 */
//	public static String getNameUser(String username) {
//		// TODO Auto-generated method stub
//		String query = "SELECT NAME FROM USER WHERE USERNAME = '?1'";
//		query = query.replace("?1", username);
//
//		IRawSelectWrapper sjsw = WrapperManager.getInstance().getRawWrapper(securityDb, query);
//		if(sjsw.hasNext()) {
//			IHeadersDataRow sjss = sjsw.next();
//			return sjss.getValues()[0].toString();
//		}
//		return null;
//
//	}
//
//	/**
//	 * Check if user is allowed to use a certain database (owner/edit permissions).
//	 * @param userId
//	 * @param engineName
//	 * @return 
//	 */
//	public static boolean isUserAllowedToUseEngine(String userId, String engineId){
//		/*if(isUserAdmin(userId)){
//			return true;
//		}*/
//
//		String query = "SELECT e.ENGINEID FROM ENGINE e WHERE e.enginename = '?3' AND e.global = TRUE ";
//		List<Object[]> res = runQuery2(query);
//		if(!res.isEmpty()){
//			return true;
//		}
//
//		query = "SELECT e.ENGINEID FROM ENGINEPERMISSION ep "
//				+ "JOIN ENGINE e ON(ep.engineid = e.engineid) WHERE USERID = '?1' AND e.engineid = '?2' AND (ep.permission = '1'  OR ep.permission = '3') "
//				+ "UNION "
//				+ "SELECT gep.engine "
//				+ "FROM GROUPENGINEPERMISSION gep JOIN GROUPMEMBERS gm ON (gep.groupid = gm.groupid) JOIN engine e ON (e.engineid = gep.engine) "
//				+ "WHERE e.engineid = '?2'  AND gm.memberid = '?1' AND (gep.PERMISSION = '1' OR  gep.PERMISSION = '3')";
//
//		query = query.replace("?1", userId);
//		query = query.replace("?2", engineId);
//
//		res = runQuery2(query);
//		return !res.isEmpty();
//	}
//
//	/**
//	 * Get a list of the users o a certain group.
//	 * @param groupId
//	 * @return list of users.
//	 */
//	public static ArrayList<String[]> getAllUserFromGroup(String groupId){
//		String query = 	"SELECT gm.groupid AS GROUP_ID, ug.name AS GROUP_NAME, ug.owner AS GROUP_OWNER, gm.memberid AS USER_ID, u.name AS USER_NAME  FROM GROUPMEMBERS gm JOIN USERGROUP ug ON (ug.id = gm.groupid) JOIN USER u ON (u.id = gm.memberid) WHERE gm.groupid = ?1";
//		query = query.replace("?1", groupId);
//		return runQuery(query);
//	}
//
//	/**
//	 * Get a list of all the users of a list of groups
//	 * @param groups
//	 * @return list of users.
//	 */
//	public static StringMap<ArrayList<String[]>> getAllUserFromGroups(ArrayList<String> groups){
//		String query = 	"SELECT gm.groupid AS GROUP_ID, ug.name AS GROUP_NAME, ug.owner AS GROUP_OWNER, gm.memberid AS USER_ID, u.name AS USER_NAME  FROM GROUPMEMBERS gm JOIN USERGROUP ug ON (ug.id = gm.groupid) JOIN USER u ON (u.id = gm.memberid) WHERE gm.groupid IN ?1";
//		query = query.replace("?1", "(" + convertArrayToDbString(groups, false) + ")");
//		List<Object[]> users = runQuery2(query);
//		StringMap<ArrayList<String[]>> ret = new StringMap<>();
//		for(Object[] userObject : users){
//			String[] user = Arrays.stream(userObject).map(Object::toString).
//					toArray(String[]::new);
//			String userId = user[3];
//			if(ret.get(userId) == null){
//				ArrayList<String[]> userNodeList = new ArrayList<>();
//				userNodeList.add(user);
//				ret.put(userId, userNodeList);
//			} else {
//				ret.get(userId).add(user);
//			}
//		}
//		return ret;
//	}
//
//
//	/**
//	 * Check if the users from a group already have access to the database through another group.  
//	 * @param usersFromGroup
//	 * @param otherGroupsUserList
//	 * @return
//	 */
//	private static String isGroupWithInvalidUsersFromOtherGroups(ArrayList<String[]> usersFromGroup, StringMap<ArrayList<String[]>> otherGroupsUserList){
//		String ret = "";
//		for(String[] user : usersFromGroup){
//			String userId = user[3];
//			if(otherGroupsUserList.get(userId) != null){
//				ArrayList<String[]> otherCoincidences = otherGroupsUserList.get(userId);
//				for(String[] coincidence : otherCoincidences){
//					String groupName = coincidence[1];
//					String groupOwner = getUsernameByUserId(coincidence[2]);
//					ret += "The user " + user[4] + " in " + user[1] + " already has access to the database through " + groupName + " owned by " + groupOwner + ". ";
//				}
//			}
//		}
//		return ret;
//	}
//
//	/**
//	 * Check if the user from a group have already direct access to a database. 
//	 * Checking over the parameter usersList
//	 * @param usersFromGroup
//	 * @param users
//	 * @return
//	 */
//	private static String isGroupWithInvalidUsersFromList(ArrayList<String[]> usersFromGroup, ArrayList<String> users){
//		String ret = "";
//		for(String[] user : usersFromGroup){
//			if(users.contains(user[3])){
//				ret += "The user " + user[4] + " in the group " + user[1] + " already has direct access to the database. ";
//			}
//		}
//		return ret;
//	}
//
//	/**
//	 * Check if the group to be added has already permissions to a database.
//	 * @param groupId
//	 * @param groups
//	 * @param users
//	 * @return
//	 */
//	public static String isGroupUsersWithDatabasePermissionAlready(String groupId, ArrayList<String> groups,
//			ArrayList<String> users) {
//
//		String ret = "";
//		// TODO Security check if the user logged in is the owner of the group being added (?)
//
//		ArrayList<String[]> allUserFromGroup = getAllUserFromGroup(groupId);
//		ret += isGroupWithInvalidUsersFromList(allUserFromGroup, users);
//		StringMap<ArrayList<String[]>> allUsersFromOtherGroups = getAllUserFromGroups(groups);
//		ret += isGroupWithInvalidUsersFromOtherGroups(allUserFromGroup, allUsersFromOtherGroups);
//
//		return ret.isEmpty() ? "true" : ret;
//	}
//
//	/**
//	 * Get all groups associated with a database. To that list adds groupsToAdd and remove groupsToRemove 
//	 * @param engineId
//	 * @param groupsToAdd
//	 * @param groupsToRemove
//	 * @return list of groups id.
//	 */
//	public static ArrayList<String> getAllDbGroupsById(String engineId, ArrayList<String> groupsToAdd,
//			ArrayList<String> groupsToRemove) {
//
//		ArrayList<String> ret = new ArrayList<>();
//		String query = "SELECT GROUPENGINEPERMISSION.GROUPID FROM GROUPENGINEPERMISSION WHERE GROUPENGINEPERMISSION.ENGINE = '?1'";
//		query = query.replace("?1", engineId);
//		ArrayList<String[]> rows = runQuery(query);
//
//		for(String[] row : rows){
//			if(row[0] != null && !row[0].isEmpty())
//				ret.add(row[0]);
//		}
//
//		ret.removeAll(groupsToRemove);
//		ret.addAll(groupsToAdd);
//		return ret;
//	}
//
//	/***
//	 * Get all users that have a direct relationship with a database to that list adds userToAdd and 
//	 * removes usersToRemove
//	 * @param engineId
//	 * @param usersToAdd
//	 * @param usersToRemove
//	 * @return list of users id.
//	 */
//	public static ArrayList<String> getAllDbUsersById(String engineId, ArrayList<String> usersToAdd,
//			ArrayList<String> usersToRemove) {
//
//		ArrayList<String> ret = new ArrayList<>();
//		String query = "SELECT ENGINEPERMISSION.USERID FROM ENGINEPERMISSION WHERE ENGINEPERMISSION.ENGINEID = '?1'";
//		query = query.replace("?1", engineId);
//		ArrayList<String[]> rows = runQuery(query);
//
//		for(String[] row : rows){
//			ret.add(row[0]);
//		}
//		ret.removeAll(usersToRemove);
//		ret.addAll(usersToAdd);
//		return ret;
//	}
//
//	/**
//	 * Check if the user is already associated from another group 
//	 * to the databases that the group he wants to be part of is associated. 
//	 * @param userId
//	 * @param groupId
//	 * @return
//	 */
//	public static String isUserInAnotherDbGroup(String userId, String groupId) {
//
//		String ret = "";
//		String query = "SELECT gm.groupid as GROUPID, ug.name as GROUPNAME, u.name as OWNER, gr.enginename AS ENGINENAME "
//				+ "FROM GROUPMEMBERS gm JOIN USERGROUP ug ON (ug.id = gm.groupid) JOIN USER u ON(u.id = ug.owner) JOIN "
//				+ "(SELECT ge.groupid as GROUPID, en.name AS ENGINENAME FROM GROUPENGINEPERMISSION ge JOIN "
//				+ "(SELECT ge.engine AS ENGINE, e.enginename AS NAME "
//				+ "FROM GROUPENGINEPERMISSION ge JOIN ENGINE e ON(ge.engine = e.engineid) WHERE ge.groupid = '?2') en ON (ge.engine = en.engine) WHERE ge.groupid != '?2') gr ON (gm.groupid = gr.groupid) "
//				+ "WHERE gm.memberid = '?1'";
//
//		query = query.replace("?1", userId);
//		query = query.replace("?2", groupId);
//
//		List<Object[]> rows = runQuery2(query);
//
//		for(Object[] row : rows){
//			String groupName = row[1].toString();
//			String ownerName = row[2].toString();
//			String engineName = row[3].toString();
//			ret += "The user is already associated with the database " + engineName + " in the group " + groupName + " owned by " + ownerName + ". ";
//		}
//
//		return ret;
//	}
//
//	/**
//	 * Check if a user already have a relationship with a database that the group already have access.
//	 * @param userId
//	 * @param groupId
//	 * @return blank if there was no relationships otherwise a message explaining other databases associated with the user.
//	 */
//	public static String isUserWithAccessToGroupDb(String userId, String groupId) {
//
//		String ret = "";
//		String query = "SELECT en.engine AS ENGINE, en.name AS ENGINENAME "
//				+ "FROM ENGINEPERMISSION ep JOIN (SELECT ge.engine AS ENGINE, e.enginename AS NAME FROM GROUPENGINEPERMISSION ge JOIN ENGINE e ON(ge.engine = e.engineid) WHERE ge.groupid = '?1') en ON (ep.engineid = en.engine) "
//				+ "WHERE ep.userid = '?2'";
//
//		query = query.replace("?1", groupId);
//		query = query.replace("?2", userId);
//
//		List<Object[]> rows = runQuery2(query);
//
//		for(Object[] row : rows){
//			String engineName = row[1].toString();
//			ret += "The user is already associated with the database " + engineName + ". ";
//		}
//
//		return ret;
//	}
//
//	/**
//	 * Check if a user can be added or not to a group based on its database
//	 * permission on another groups and the database permissions from the group
//	 * it's going ot be added.
//	 * @param userId
//	 * @param groupId
//	 * @return true if user can be added otherwise a message explainig why not.
//	 */
//	public static String isUserAddedToGroupValid(String userId, String groupId){
//		String ret = "";
//
//		ret += isUserWithAccessToGroupDb(userId, groupId);
//		ret += isUserInAnotherDbGroup(userId, groupId);
//
//		return ret.isEmpty() ? "true" : ret;
//	}
//
//	/**
//	 * Returns a list of all engines that a given user can see (has been given any level of permission).
//	 * 
//	 * @param userId	ID of the user
//	 * @return			List of engine names
//	 */
//	public static List<Map<String, Object>> getUserVisibleEngines(String userId) {
//		HashSet<String> engines = new HashSet<String>();
//
//		//boolean isAdmin = isUserAdmin(userId);
//		boolean isAdmin = false;
//
//		String query = "SELECT e.engineid AS ID "
//				+ "FROM ENGINEPERMISSION ep JOIN ENGINE e ON(ep.engineid = e.engineid) ";
//
//		if(!isAdmin){
//			query = "SELECT e.engineid AS ID "
//					+ "FROM ENGINEPERMISSION ep JOIN ENGINE e ON(ep.engineid = e.engineid)  "
//					+ "WHERE ep.userid = '?1' AND ep.visibility = 'TRUE'";
//			query = query.replace("?1", userId);
//		}
//
//		ArrayList<String[]> dbEngines = runQuery(query);
//
//		query = "SELECT e.engineid AS ID "
//				+ "FROM GROUPMEMBERS gm JOIN GROUPENGINEPERMISSION ge ON (ge.groupid = gm.groupid) JOIN ENGINE e ON (ge.engine = e.engineid) ";
//
//		if(!isAdmin){
//			query = "SELECT e.engineid AS ID "
//					+ "FROM GROUPMEMBERS gm JOIN GROUPENGINEPERMISSION ge ON (ge.groupid = gm.groupid) JOIN ENGINE e ON (ge.engine = e.engineid) "
//					+ "JOIN ENGINEGROUPMEMBERVISIBILITY v ON(gm.id = v.groupmembersid AND ge.id = groupenginepermissionid)  "
//					+ "WHERE gm.memberid = '?1' AND v.visibility = 'TRUE'";
//			query = query.replace("?1", userId);
//		}
//
//		dbEngines.addAll(runQuery(query));
//
//		dbEngines.addAll(getPublicEngines(dbEngines, userId));
//
//		ArrayList<String> visibleEngines = new ArrayList<>();
//
//		for(String[] engine : dbEngines) {
//			if(!engines.contains(engine[0])){
//				//engines.add(engine[0]);
//				visibleEngines.add(engine[0]);
//			}	
//		}
//
//		query = "SELECT e.ENGINEID AS ID, e.ENGINENAME AS NAME, e.TYPE AS TYPE, e.COST AS COST "
//				+ "FROM engine e "
//				+ "WHERE e.engineid IN ?1";
//		query = query.replace("?1", "(" + convertArrayToDbString(visibleEngines, true) + ")");
//
//		ArrayList<String[]> visibleEnginesAttributes = runQuery(query);
//
//		List<Map<String, Object>> enginesObject = new ArrayList<>();
//
//		for(String[] engineAttributes : visibleEnginesAttributes) {
//			Map<String, Object> temp = new HashMap<>();
//			temp.put("app_id", engineAttributes[0]);
//			temp.put("app_name", engineAttributes[1]);
//			temp.put("app_type", engineAttributes[2]);
//			temp.put("app_cost", engineAttributes[3] == null ? "" : engineAttributes[3]);
//			enginesObject.add(temp);
//		}
//
//		return enginesObject;
//
//	}
//
//	/**
//	 * Check if the user has owner permission of a certain database
//	 * @param userId
//	 * @param engineId
//	 * @return true or false
//	 */
//	public static boolean isUserDatabaseOwner(String userId, String engineId){
//		String query = "SELECT ID FROM ENGINEPERMISSION WHERE USERID = '?1' AND ENGINEID = '?2' AND PERMISSION = '1' UNION SELECT gep.ID FROM GROUPENGINEPERMISSION gep JOIN GROUPMEMBERS gm ON (gep.groupid = gm.groupid) WHERE gep.engine = '?2'  AND gm.memberid = '?1' AND gep.PERMISSION = '1'";
//		query = query.replace("?1", userId);
//		query = query.replace("?2", engineId);
//		List<Object[]> res = runQuery2(query);
//		return !res.isEmpty();
//	}
//
//	/**
//	 * Get the id list of groups owned by an user
//	 * @param userId
//	 * @return List<String> with the id of the groups owned by an user
//	 */
//	public static List<String> getGroupsOwnedByUser(String userId){
//		String query = "SELECT ID FROM USERGROUP WHERE OWNER = '?1'";
//		query = query.replace("?1", userId);
//		ArrayList<String[]> res = runQuery(query);
//		List<String> groupList = new ArrayList<>();
//		for(String[] row : res){
//			groupList.add(row[0]);
//		}
//		return groupList;
//	}
}
