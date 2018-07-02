package prerna.auth;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.stream.Collectors;

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
		String query = "SELECT DISTINCT ENGINEPERMISSION.ENGINE "
				+ "FROM ENGINEPERMISSION INNER JOIN ENGINE ON ENGINE.ID=ENGINEPERMISSION.ENGINE "
				+ "WHERE ENGINE.NAME='" + potentialId + "' AND ENGINEPERMISSION.USER IN " + userFilters;
		
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		ids = flushToListString(wrapper);
		if(ids.isEmpty()) {
			query = "SELECT DISTINCT ENGINE.ID FROM ENGINE WHERE ENGINE.NAME='" + potentialId + "' AND ENGINE.GLOBAL=TRUE";
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
		String query = "SELECT DISTINCT ID FROM ENGINE";
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		return flushToListString(wrapper);
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
		String query = "SELECT DISTINCT ENGINE.ID as \"app_id\", ENGINE.NAME as \"app_name\", ENGINE.TYPE as \"app_type\", ENGINE.COST as \"app_cost\" "
				+ "FROM ENGINE "
				+ "LEFT JOIN ENGINEPERMISSION ON ENGINE.ID=ENGINEPERMISSION.ENGINE "
				+ "WHERE (ENGINEPERMISSION.USER IN " + userFilters + " OR ENGINE.GLOBAL=TRUE) "
				+ "ORDER BY ENGINE.NAME";
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		return flushRsToMap(wrapper);
	}
	
	/**
	 * Get the list of the engine information that the user has access to
	 * @param userId
	 * @return
	 */
	public static List<Map<String, Object>> getAllDatabaseList() {
		String query = "SELECT DISTINCT ENGINE.ID as \"app_id\", ENGINE.NAME as \"app_name\", ENGINE.TYPE as \"app_type\", ENGINE.COST as \"app_cost\" "
				+ "FROM ENGINE "
				+ "LEFT JOIN ENGINEPERMISSION ON ENGINE.ID=ENGINEPERMISSION.ENGINE "
				+ "ORDER BY ENGINE.NAME";
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
		String query = "SELECT DISTINCT ENGINE.ID as \"app_id\", ENGINE.NAME as \"app_name\", ENGINE.TYPE as \"app_type\", ENGINE.COST as \"app_cost\" "
				+ "FROM ENGINE "
				+ "LEFT JOIN ENGINEPERMISSION ON ENGINE.ID=ENGINEPERMISSION.ENGINE "
				+ "WHERE "
				+ (!filter.isEmpty() ? ("ENGINE.ID " + filter + " AND ") : "")
				+ "(ENGINEPERMISSION.USER IN " + userFilters + " OR ENGINE.GLOBAL=TRUE) "
				+ "ORDER BY ENGINE.NAME";
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
		String query = "SELECT DISTINCT ENGINE.ID as \"app_id\", ENGINE.NAME as \"app_name\", ENGINE.TYPE as \"app_type\", ENGINE.COST as \"app_cost\" "
				+ "FROM ENGINE "
				+ "LEFT JOIN ENGINEPERMISSION ON ENGINE.ID=ENGINEPERMISSION.ENGINE "
 				+ (!filter.isEmpty() ? ("WHERE ENGINE.ID " + filter + " ") : "")
				+ "ORDER BY ENGINE.NAME";
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
	public static List<String> getUserEngines(User user) {
		String userFilters = getUserFilters(user);
		String query = "SELECT DISTINCT ENGINE FROM ENGINEPERMISSION WHERE USER IN " + userFilters;
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
		String query = "SELECT ID FROM ENGINE WHERE GLOBAL=TRUE";
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
				+ "WHERE ENGINE='" + engineId + "' AND USER IN " + userFilters;
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
				+ "WHERE ENGINE='" + engineId + "' AND USER IN " + userFilters;
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
			if(EnginePermission.isOwner(permission)) {
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
				+ "INSIGHT.INSIGHTID as \"app_insight_id\", "
				+ "INSIGHT.INSIGHTNAME as \"name\", "
				+ "CONCAT(INSIGHT.ENGINEID, '_', INSIGHT.INSIGHTID) AS \"id\" "
				+ "FROM INSIGHT "
				+ "INNER JOIN ENGINE ON ENGINE.ID=INSIGHT.ENGINEID "
				+ "LEFT JOIN ENGINEPERMISSION ON ENGINE.ID=ENGINEPERMISSION.ENGINE "
				+ "LEFT JOIN USERINSIGHTPERMISSION ON USERINSIGHTPERMISSION.ENGINEID=INSIGHT.ENGINEID "
				+ "WHERE "
				+ "INSIGHT.ENGINEID='" + engineId + "' "
				+ "AND (USERINSIGHTPERMISSION.USERID IN " + userFilters + " OR INSIGHT.GLOBAL=TRUE OR "
						+ "(ENGINEPERMISSION.PERMISSION=1 AND ENGINEPERMISSION.USER IN " + userFilters + ") ) "
				+ ( (searchTerm != null && !searchTerm.trim().isEmpty()) ? "AND REGEXP_LIKE(INSIGHT.INSIGHTNAME, '"+ escapeRegexCharacters(searchTerm) + "', 'i')" : "")
				+ "ORDER BY INSIGHT.INSIGHTNAME "
				+ ( (limit != null && !limit.trim().isEmpty()) ? "LIMIT " + limit + " " : "")
				+ ( (offset != null && !offset.trim().isEmpty()) ? "OFFSET " + offset + " ": "")
				;
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		return flushRsToMap(wrapper);
	}
	
	public static List<Map<String, Object>> searchInsights(String engineId, String searchTerm, String limit, String offset) {
		String query = "SELECT DISTINCT INSIGHT.ENGINEID AS \"app_id\", INSIGHT.INSIGHTID as \"app_insight_id\", INSIGHT.INSIGHTNAME as \"name\" "
				+ ", CONCAT(INSIGHT.ENGINEID, '_', INSIGHT.INSIGHTID) AS \"id\" "
				+ "FROM INSIGHT "
				+ "INNER JOIN ENGINE ON ENGINE.ID=INSIGHT.ENGINEID "
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
				+ "LEFT JOIN ENGINEPERMISSION ON INSIGHT.ENGINEID=ENGINEPERMISSION.ENGINE "
				+ "LEFT JOIN USERINSIGHTPERMISSION ON USERINSIGHTPERMISSION.ENGINEID=INSIGHT.ENGINEID "
				+ "WHERE "
				+ "(USERINSIGHTPERMISSION.USERID IN " + userFilters + " OR INSIGHT.GLOBAL=TRUE OR "
						+ "(ENGINEPERMISSION.PERMISSION=1 AND ENGINEPERMISSION.USER IN " + userFilters + ") ) "
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
	public static List<Map<String, Object>> getInsightDataByName(String searchTerm, String limit, String offset, String[] engineFilter) {
		String filter = createFilter(engineFilter); 
		String query = "SELECT DISTINCT "
				+ "INSIGHT.ENGINEID AS \"app_id\", "
				+ "ENGINE.NAME as \"app_name\", "
				+ "INSIGHT.INSIGHTID as \"app_insight_id\", "
				+ "INSIGHT.LAYOUT as \"layout\", "
				+ "INSIGHT.INSIGHTNAME as \"name\", "
				+ "INSIGHT.EXECUTIONCOUNT as \"view_count\" "
				+ "FROM INSIGHT LEFT JOIN ENGINE ON INSIGHT.ENGINEID=ENGINE.ID "
				+ "WHERE "
				+ "REGEXP_LIKE(INSIGHT.INSIGHTNAME, '"+ escapeRegexCharacters(searchTerm) + "', 'i') " 
				+ "AND (INSIGHT.ENGINEID " + filter + " OR ENGINE.GLOBAL=TRUE) "
				+ "ORDER BY INSIGHT.EXECUTIONCOUNT DESC, ENGINE.NAME, INSIGHT.INSIGHTNAME  "
				+ ( (limit != null && !limit.trim().isEmpty()) ? "LIMIT " + limit + " " : "")
				+ ( (offset != null && !offset.trim().isEmpty()) ? "OFFSET " + offset + " ": "");
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
				+ "FROM INSIGHT LEFT JOIN ENGINE ON INSIGHT.ENGINEID=ENGINE.ID "
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
}
