package prerna.auth.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.stream.Collectors;

import prerna.auth.AccessPermission;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.date.SemossDate;
import prerna.ds.util.RdbmsQueryBuilder;
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
	 * Get all databases for setting options that the user has access to
	 * @param usersId
	 * @param isAdmin
	 * @return
	 */
	public static List<Map<String, Object>> getAllUserDatabaseSettings(User user) {
		String userFilters = getUserFilters(user);
		
		// get user specific databases
		String query = "SELECT DISTINCT "
				+ "ENGINE.ENGINEID as \"app_id\", "
				+ "ENGINE.ENGINENAME as \"app_name\", "
				+ "ENGINE.GLOBAL as \"app_global\", "
				+ "COALESCE(ENGINEPERMISSION.VISIBILITY, TRUE) as \"app_visibility\", "
				+ "COALESCE(PERMISSION.NAME, 'READ_ONLY') as \"app_permission\" "
				+ "FROM ENGINE "
				+ "INNER JOIN ENGINEPERMISSION ON ENGINE.ENGINEID=ENGINEPERMISSION.ENGINEID "
				+ "LEFT JOIN PERMISSION ON PERMISSION.ID=ENGINEPERMISSION.PERMISSION "
				+ "WHERE ENGINEPERMISSION.USERID IN " + userFilters;
		
		Set<String> engineIdsIncluded = new HashSet<String>();
		
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		List<Map<String, Object>> result = new Vector<Map<String, Object>>();
		while(wrapper.hasNext()) {
			IHeadersDataRow headerRow = wrapper.next();
			String[] headers = headerRow.getHeaders();
			Object[] values = headerRow.getValues();
			
			// store the engine ids
			// we will exclude these later
			// engine id is the first one to be returned
			engineIdsIncluded.add(values[0].toString());
			
			Map<String, Object> map = new HashMap<String, Object>();
			for(int i = 0; i < headers.length; i++) {
				map.put(headers[i], values[i]);
			}
			result.add(map);
		}
		
		// now need to add the global ones
		// that DO NOT sit in the engine permission
		// (this is because we do not update that table when a user modifies the global)
		query = "SELECT DISTINCT "
				+ "ENGINE.ENGINEID as \"app_id\", "
				+ "ENGINE.ENGINENAME as \"app_name\" "
				+ "FROM ENGINE WHERE ENGINE.GLOBAL=TRUE AND ENGINE.ENGINEID NOT " + createFilter(engineIdsIncluded);
		
		wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		while(wrapper.hasNext()) {
			IHeadersDataRow headerRow = wrapper.next();
			String[] headers = headerRow.getHeaders();
			Object[] values = headerRow.getValues();
			
			Map<String, Object> map = new HashMap<String, Object>();
			for(int i = 0; i < headers.length; i++) {
				map.put(headers[i], values[i]);
			}
			// add the others which we know
			map.put("app_global", true);
			map.put("app_permission", "READ_ONLY");
			map.put("app_visibility", true);
			result.add(map);
		}
		
		// now we need to loop through and order the results
		Collections.sort(result, new Comparator<Map<String, Object>>() {

			@Override
			public int compare(Map<String, Object> o1, Map<String, Object> o2) {
				String appName1 = o1.get("app_name").toString().toLowerCase();
				String appName2 = o2.get("app_name").toString().toLowerCase();
				return appName1.compareTo(appName2);
			}
		
		});
		
		return result;
	}

	/**
	 * Get the list of the engine information that the user has access to
	 * @param userId
	 * @return
	 */
	public static List<Map<String, Object>> getUserDatabaseList(User user) {
		String userFilters = getUserFilters(user);
		String query = "SELECT DISTINCT "
				+ "ENGINE.ENGINEID as \"app_id\", "
				+ "ENGINE.ENGINENAME as \"app_name\", "
				+ "ENGINE.TYPE as \"app_type\", "
				+ "ENGINE.COST as \"app_cost\","
				+ "LOWER(ENGINE.ENGINENAME) as \"low_app_name\" "
				+ "FROM ENGINE "
				+ "LEFT JOIN ENGINEPERMISSION ON ENGINE.ENGINEID=ENGINEPERMISSION.ENGINEID "
				+ "LEFT JOIN USER ON ENGINEPERMISSION.USERID=USER.ID "
				+ "WHERE "
				+ "( ENGINE.GLOBAL=TRUE "
				+ "OR ENGINEPERMISSION.USERID IN " + userFilters + " ) "
				+ "AND ENGINE.ENGINEID NOT IN (SELECT ENGINEID FROM ENGINEPERMISSION WHERE VISIBILITY=FALSE AND USERID IN " + userFilters + ") "
				+ "ORDER BY LOWER(ENGINE.ENGINENAME)";	
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		return flushRsToMap(wrapper);
	}
	
	/**
	 * Get the list of the engine information that the user has access to
	 * @param userId
	 * @return
	 */
	public static List<Map<String, Object>> getAllDatabaseList() {
		String query = "SELECT DISTINCT "
				+ "ENGINE.ENGINEID as \"app_id\", "
				+ "ENGINE.ENGINENAME as \"app_name\", "
				+ "ENGINE.TYPE as \"app_type\", "
				+ "ENGINE.COST as \"app_cost\", "
				+ "LOWER(ENGINE.ENGINENAME) as \"low_app_name\" "
				+ "FROM ENGINE "
				+ "LEFT JOIN ENGINEPERMISSION ON ENGINE.ENGINEID=ENGINEPERMISSION.ENGINEID "
				+ "ORDER BY LOWER(ENGINE.ENGINENAME)";
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
		String query = "SELECT DISTINCT "
				+ "ENGINE.ENGINEID as \"app_id\", "
				+ "ENGINE.ENGINENAME as \"app_name\", "
				+ "ENGINE.TYPE as \"app_type\", "
				+ "ENGINE.COST as \"app_cost\", "
				+ "LOWER(ENGINE.ENGINENAME) as \"low_app_name\" "
				+ "FROM ENGINE "
				+ "LEFT JOIN ENGINEPERMISSION ON ENGINE.ENGINEID=ENGINEPERMISSION.ENGINEID "
				+ "WHERE "
				+ (!filter.isEmpty() ? ("ENGINE.ENGINEID " + filter + " AND ") : "")
				+ "(ENGINEPERMISSION.USERID IN " + userFilters + " OR ENGINE.GLOBAL=TRUE) "
				+ "ORDER BY LOWER(ENGINE.ENGINENAME)";
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
		String query = "SELECT DISTINCT "
				+ "ENGINE.ENGINEID as \"app_id\", "
				+ "ENGINE.ENGINENAME as \"app_name\", "
				+ "ENGINE.TYPE as \"app_type\", "
				+ "ENGINE.COST as \"app_cost\", "
				+ "LOWER(ENGINE.ENGINENAME) as \"low_app_name\" "
				+ "FROM ENGINE "
 				+ (!filter.isEmpty() ? ("WHERE ENGINE.ENGINEID " + filter + " ") : "")
				+ "ORDER BY LOWER(ENGINE.ENGINENAME)";
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
	
	//////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////////

	/*
	 * Get all the users for a database
	 */
	
	public static List<Map<String, Object>> getAllDatabaseOwnersAndEditors(String engineId) {
		List<Map<String, Object>> users = null;
		if(getGlobalEngineIds().contains(engineId)) {
			String query = "SELECT DISTINCT "
					+ "USER.NAME AS \"name\", "
					+ "PERMISSION.NAME as \"permission\" "
					+ "FROM USER "
					+ "INNER JOIN ENGINEPERMISSION ON USER.ID=ENGINEPERMISSION.USERID "
					+ "INNER JOIN PERMISSION ON ENGINEPERMISSION.PERMISSION=PERMISSION.ID "
					+ "WHERE PERMISSION.ID IN (1,2) AND ENGINEPERMISSION.ENGINEID='" + engineId + "'";
			IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
			users = flushRsToMap(wrapper);
			
			Map<String, Object> globalMap = new HashMap<String, Object>();
			globalMap.put("name", "PUBLIC DATABASE");
			globalMap.put("permission", "READ_ONLY");
			users.add(globalMap);
		} else {
			String query = "SELECT DISTINCT "
					+ "USER.NAME AS \"name\", "
					+ "PERMISSION.NAME as \"permission\" "
					+ "FROM USER "
					+ "INNER JOIN ENGINEPERMISSION ON USER.ID=ENGINEPERMISSION.USERID "
					+ "INNER JOIN PERMISSION ON ENGINEPERMISSION.PERMISSION=PERMISSION.ID "
					+ "WHERE ENGINEPERMISSION.ENGINEID='" + engineId + "'";
			IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
			users = flushRsToMap(wrapper);
		}
		return users;
	}
	
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	
	@Deprecated
	public static Boolean userIsAdmin(String userId) {
		String query = "SELECT ADMIN FROM USER WHERE ID ='" + userId + "';";
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		List<String[]> ret = flushRsToListOfStrArray(wrapper);
		if(!ret.isEmpty()) {
			return Boolean.parseBoolean(ret.get(0)[0]);
		}
		return false;
	}
	
	
	/**
	 * Determine if the user is the owner
	 * @param user
	 * @param engineId
	 * @return
	 */
	public static boolean userIsOwner(User user, String engineId) {
		String userFilters = getUserFilters(user);
		return userIsOwner(userFilters, engineId);
	}
	
	/**
	 * Determine if the user is the owner
	 * @param userFilters
	 * @param engineId
	 * @return
	 */
	static boolean userIsOwner(String userFilters, String engineId) {
		String query = "SELECT DISTINCT ENGINEPERMISSION.PERMISSION FROM ENGINEPERMISSION "
				+ "WHERE ENGINEID='" + engineId + "' AND USERID IN " + userFilters;
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		while(wrapper.hasNext()) {
			Object val = wrapper.next().getValues()[0];
			if(val == null) {
				return false;
			}
			int permission = ((Number) val).intValue();
			if(AccessPermission.isOwner(permission)) {
				return true;
			}
		}
		return false;
	}
	
	/**
	 * Determine if a user can view an engine
	 * @param user
	 * @param engineId
	 * @return
	 */
	public static boolean userCanViewEngine(User user, String engineId) {
		String userFilters = getUserFilters(user);
		String query = "SELECT * "
				+ "FROM ENGINE "
				+ "LEFT JOIN ENGINEPERMISSION ON ENGINE.ENGINEID=ENGINEPERMISSION.ENGINEID "
				+ "WHERE ("
				+ "ENGINE.GLOBAL=TRUE "
				+ "OR ENGINEPERMISSION.USERID IN " + userFilters + ") AND ENGINE.ENGINEID='" + engineId + "'"
				;
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		try {
			// if you are here, you can view
			while(wrapper.hasNext()) {
				return true;
			} 
		} finally {
			wrapper.cleanUp();
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
		try {
			while(wrapper.hasNext()) {
				Object val = wrapper.next().getValues()[0];
				if(val == null) {
					return false;
				}
				int permission = ((Number) val).intValue();
				if(AccessPermission.isEditor(permission)) {
					return true;
				}
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
	
	public static SemossDate getLastExecutedInsightInApp(String engineId) {
		String query = "SELECT DISTINCT INSIGHT.LASTMODIFIEDON "
				+ "FROM INSIGHT "
				+ "WHERE INSIGHT.ENGINEID='" + engineId + "'"
				+ "ORDER BY INSIGHT.LASTMODIFIEDON DESC LIMIT 1"
				;
		
		SemossDate date = null;
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		while(wrapper.hasNext()) {
			Object[] row = wrapper.next().getValues();
			try {
				date = (SemossDate) row[0];
			} catch(Exception e) {
				// ignore
			}
		}
		
		return date;
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
	
	/**
	 * Get user info from ids
	 * 
	 * @param userIds
	 * @return
	 * @throws IllegalArgumentException
	 */
	public static Map<String, Map<String, Object>> getUserInfo(List<String> userIds) throws IllegalArgumentException {
		String query = "SELECT DISTINCT ID, NAME, USERNAME, EMAIL, TYPE, ADMIN FROM USER ";
		String userFilter = createFilter(userIds);
		query += " WHERE ID " + userFilter + ";";
		Map<String, Map<String, Object>> userMap = new HashMap<>();
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		String[] names = wrapper.getHeaders();
		if (wrapper.hasNext()) {
			Object[] values = wrapper.next().getValues();
			Map<String, Object> userInfo = new HashMap<>();
			String userId = values[0].toString();
			userInfo.put(names[0], userId);
			userInfo.put(names[1], values[1].toString());
			userInfo.put(names[2], values[2].toString());
			userInfo.put(names[3], values[3].toString());
			userInfo.put(names[4], values[4].toString());
			userInfo.put(names[5], values[5].toString());
			userMap.put(userId, userInfo);
		}
		return userMap;
	}
	

	
	//////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////

	/**
	 * Get all current user requests
	 * @param user
	 * @return
	 */
	public static List<Map<String, Object>> getUserAccessRequests(User user) {
		String filter = getUserFilters(user);
		String query = "SELECT DISTINCT ID, ENGINE, PERMISSION FROM ACCESSREQUEST "
				+ "WHERE SUBMITTEDBY IN " + filter;
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		return flushRsToMap(wrapper);
	}
	
	/**
	 * Get all user requests including specific user id
	 * @param user
	 * @return
	 */
	public static List<Map<String, Object>> getUserAccessRequestsByProvider(User user, String engineFilter) {
		String filter = getUserFilters(user);
		String query = "SELECT DISTINCT ID, SUBMITTEDBY, ENGINE, PERMISSION FROM ACCESSREQUEST "
				+ "WHERE ENGINE='" + engineFilter + "' AND SUBMITTEDBY IN " + filter;
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		return flushRsToMap(wrapper);
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
	
	/**
	 * Search if there's users containing 'searchTerm' in their email or name
	 * @param searchTerm
	 * @return
	 */
	public static List<Map<String, String>> searchForUser(String searchTerm) {
		String query = "SELECT DISTINCT USER.ID AS ID, USER.NAME AS NAME, USER.EMAIL AS EMAIL FROM USER "
				+ "WHERE UPPER(USER.NAME) LIKE UPPER('%" + searchTerm + "%') "
				+ "OR UPPER(USER.EMAIL) LIKE UPPER('%" + searchTerm + "%') "
				+ "OR UPPER(USER.ID) LIKE UPPER('%" + searchTerm + "%');";
		List<Map<String, String>> users = new ArrayList<>();

		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		for(String[] s : flushRsToListOfStrArray(wrapper)) {
			Map<String, String> map = new HashMap<String, String>();
			map.put("id", s[0]);
			map.put("name", s[1]);
			map.put("email", s[2]);
			users.add(map);
		}

		return users;
	}
	
	/*
	 * Check properties of user
	 */
	
	/**
	 * Check if a user (user name or email) exist in the security database
	 * @param username
	 * @param email
	 * @return true if user is found otherwise false.
	 */
	public static boolean checkUserExist(String id) {
		String query = "SELECT * FROM USER WHERE ID='" + RdbmsQueryBuilder.escapeForSQLStatement(id) + "'";
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		try {
			return wrapper.hasNext();
		} finally {
			wrapper.cleanUp();
		}
	}
	
	/**
	 * Check if a user (user name or email) exist in the security database
	 * @param username
	 * @param email
	 * @return true if user is found otherwise false.
	 */
	public static boolean checkUserExist(String username, String email){
		String query = "SELECT * FROM USER WHERE USERNAME='" + RdbmsQueryBuilder.escapeForSQLStatement(username) + 
				"' OR EMAIL='" + RdbmsQueryBuilder.escapeForSQLStatement(email) + "'";
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		try {
			return wrapper.hasNext();
		} finally {
			wrapper.cleanUp();
		}
	}
	
	/**
	 * Check if the user is of the type requested
	 * @param userId	String representing the id of the user to check
	 */
	public static Boolean isUserType(String userId, AuthProvider type) {
		String query = "SELECT NAME FROM USER WHERE ID='" + RdbmsQueryBuilder.escapeForSQLStatement(userId) + "' AND TYPE = '"+ type + "';";
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		List<String[]> ret = flushRsToListOfStrArray(wrapper);
		if(!ret.isEmpty()) {
			return Boolean.parseBoolean(ret.get(0)[0]);
		}
		return false;
	}
	
	/**
	 * Check if a user can be added or not to a group based on its database
	 * permission on another groups and the database permissions from the group
	 * it's going ot be added.
	 * @param userId
	 * @param groupId
	 * @return true if user can be added otherwise a message explainig why not.
	 */
	public static String isUserAddedToGroupValid(String userId, String groupId){
		String ret = "";

		ret += isUserWithAccessToGroupDb(userId, groupId);
		ret += isUserInAnotherDbGroup(userId, groupId);

		return ret.isEmpty() ? "true" : ret;
	}

	/**
	 * Check if the user has owner permission of a certain database
	 * @param userId
	 * @param engineId
	 * @return true or false
	 */
	public static boolean isUserDatabaseOwner(String userId, String engineId){
		String query = "SELECT ENGINEID FROM ENGINEPERMISSION WHERE USERID = '?1' AND ENGINEID = '?2' AND PERMISSION = '1' UNION SELECT GROUPENGINEPERMISSION.ENGINE FROM GROUPENGINEPERMISSION JOIN GROUPMEMBERS ON (GROUPENGINEPERMISSION.GROUPID = GROUPMEMBERS.GROUPID) WHERE GROUPENGINEPERMISSION.ENGINE = '?2'  AND GROUPMEMBERS.USERID = '?1' AND GROUPENGINEPERMISSION.PERMISSION = '1'";
		query = query.replace("?1", userId);
		query = query.replace("?2", engineId);
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		List<Object[]> res = flushRsToMatrix(wrapper);
		return !res.isEmpty();
	}

	/**
	 * Check if user is allowed to use a certain database (owner/edit permissions).
	 * @param userId
	 * @param engineName
	 * @return 
	 */
	public static boolean isUserAllowedToUseEngine(String userId, String engineId){
		/*if(isUserAdmin(userId)){
			return true;
		}*/

		//Check if engine is global
		String query = "SELECT ENGINE.ENGINEID FROM ENGINE WHERE ENGINE.ENGINEID = '?3' AND ENGINE.GLOBAL = TRUE ";
		query = query.replace("?3", engineId);
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		List<Object[]> res = flushRsToMatrix(wrapper);
		if(!res.isEmpty()){
			return true;
		}

		query = "SELECT ENGINE.ENGINEID FROM ENGINEPERMISSION"
				+ "JOIN ENGINE ON(ENGINEPERMISSION.ENGINEID = ENGINE.ENGINEID) WHERE USERID = '?1' AND ENGINE.ENGINEID = '?2' AND (ENGINEPERMISSION.PERMISSION = '1'  OR ENGINEPERMISSION.PERMISSION = '3') "
				+ "UNION "
				+ "SELECT GROUPENGINEPERMISSION.ENGINE "
				+ "FROM GROUPENGINEPERMISSION JOIN GROUPMEMBERS ON (GROUPENGINEPERMISSION.GROUPID = GROUPMEMBERS.GROUPID) JOIN ENGINE ON (ENGINE.ENGINEID = GROUPENGINEPERMISSION.ENGINE) "
				+ "WHERE ENGINE.ENGINEID = '?2'  AND GROUPMEMBERS.USERID = '?1' AND (GROUPENGINEPERMISSION.PERMISSION = '1' OR  GROUPENGINEPERMISSION.PERMISSION = '3')";

		query = query.replace("?1", userId);
		query = query.replace("?2", engineId);
		wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		res = flushRsToMatrix(wrapper);
		return !res.isEmpty();
	}
	
	/*
	 * Engines
	 */
	
	/**
	 * Get only the public engines that the user don't have a straight relationship established 
	 * @param engines other engines the user is related to
	 * @return public engines
	 */
	private static List<String[]> getPublicEngines(List<String[]> engines, String userId) {

		String query = "";
		IRawSelectWrapper wrapper = null;
		List<String> allEngines = new ArrayList<>();
		for(String[] engine : engines){
			allEngines.add(engine[0]);
		}

		if(userId != null){

			query = "SELECT ENGINE.ENGINEID FROM ENGINEPERMISSION JOIN ENGINE ON (ENGINE.ENGINEID = ENGINEPERMISSION.ENGINEID) WHERE ENGINEPERMISSION.USERID = '?1' AND ENGINEPERMISSION.VISIBILITY = FALSE AND ENGINE.GLOBAL =TRUE";
			query = query.replace("?1", userId);
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
			List<Object[]> publicHiddenEngines = flushRsToMatrix(wrapper);

			query = "SELECT ENGINE_ID FROM (SELECT ENGINE.ENGINEID AS ENGINE_ID, GROUPENGINEPERMISSION.GROUPENGINEPERMISSIONID AS ID1, TEMP.AID AS ID2 FROM GROUPENGINEPERMISSION JOIN ENGINE ON (GROUPENGINEPERMISSION.ENGINE = ENGINE.ENGINEID) JOIN (SELECT GROUPMEMBERSID AS AID, GROUPID FROM GROUPMEMBERS WHERE USERID = '?1') TEMP ON (GROUPENGINEPERMISSION.GROUPID = TEMP.GROUPID) WHERE ENGINE.GLOBAL = TRUE) B JOIN ENGINEGROUPMEMBERVISIBILITY ON (B.ID1 = ENGINEGROUPMEMBERVISIBILITY.GROUPENGINEPERMISSIONID AND B.ID2 = ENGINEGROUPMEMBERVISIBILITY.GROUPMEMBERSID) WHERE ENGINEGROUPMEMBERVISIBILITY.VISIBILITY = FALSE;";
			query = query.replace("?1", userId);
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
			publicHiddenEngines.addAll(flushRsToMatrix(wrapper));

			for(Object[] p : publicHiddenEngines){
				String[] engine = Arrays.stream(p).map(Object::toString).
						toArray(String[]::new);
				allEngines.add(engine[0]);
			}

		}

		//TAP_Site_Data
		//allEngines.add("1");
		query = "";

		query = "SELECT ENGINE.ENGINEID AS ID, ENGINE.ENGINENAME AS NAME, ENGINE.GLOBAL AS PUBLIC, 'Edit' AS PERMISSIONS, 'true' AS VISIBILITY FROM ENGINE WHERE ENGINE.ENGINEID NOT ?1 AND GLOBAL = TRUE";	
		query = query.replace("?1", createFilter(allEngines));

		wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		List<Object[]> res = flushRsToMatrix(wrapper);
		List<String[]> resPrimitive = new ArrayList<>();
		for(Object[] r : res){
			String[] engine = Arrays.stream(r).map(Object::toString).
					toArray(String[]::new);
			resPrimitive.add(engine);
		}
		return resPrimitive;
	}
	
	/**
	 * Returns a list of all engines that a given user can see (has been given any level of permission).
	 * 
	 * @param userId	ID of the user
	 * @return			List of engine names
	 */
	public static List<Map<String, Object>> getUserVisibleEngines(String userId) {
		HashSet<String> engines = new HashSet<String>();

		//boolean isAdmin = isUserAdmin(userId);
		boolean isAdmin = false;

		String query = "SELECT ENGINE.ENGINEID AS ID "
				+ "FROM ENGINEPERMISSION JOIN ENGINE ON(ENGINEPERMISSION.ENGINEID = ENGINE.ENGINEID) ";

		if(!isAdmin){
			query = "SELECT ENGINE.ENGINEID AS ID "
					+ "FROM ENGINEPERMISSION JOIN ENGINE ON(ENGINEPERMISSION.ENGINEID = ENGINE.ENGINEID)  "
					+ "WHERE ENGINEPERMISSION.USERID = '?1' AND ENGINEPERMISSION.VISIBILITY = 'TRUE'";
			query = query.replace("?1", userId);
		}
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		List<String[]> dbEngines = flushRsToListOfStrArray(wrapper);

		query = "SELECT ENGINE.ENGINEID AS ID "
				+ "FROM GROUPMEMBERS JOIN GROUPENGINEPERMISSION ON (GROUPENGINEPERMISSION.GROUPID = GROUPMEMBERS.GROUPID) JOIN ENGINE ON (GROUPENGINEPERMISSION.ENGINE = ENGINE.ENGINEID) ";

		if(!isAdmin){
			query = "SELECT ENGINE.ENGINEID AS ID "
					+ "FROM GROUPMEMBERS JOIN GROUPENGINEPERMISSION ON (GROUPENGINEPERMISSION.GROUPID = GROUPMEMBERS.GROUPID) JOIN ENGINE ON (GROUPENGINEPERMISSION.ENGINE = ENGINE.ENGINEID) "
					+ "JOIN ENGINEGROUPMEMBERVISIBILITY ON(GROUPMEMBERS .GROUPMEMBERSID = ENGINEGROUPMEMBERVISIBILITY.GROUPMEMBERSID AND GROUPENGINEPERMISSION.GROUPENGINEPERMISSIONID = ENGINEGROUPMEMBERVISIBILITY.GROUPENGINEPERMISSIONID)  "
					+ "WHERE GROUPMEMBERS.USERID = '?1' AND ENGINEGROUPMEMBERVISIBILITY.VISIBILITY = 'TRUE'";
			query = query.replace("?1", userId);
		}

		wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		dbEngines.addAll(flushRsToListOfStrArray(wrapper));

		dbEngines.addAll(getPublicEngines(dbEngines, userId));

		List<String> visibleEngines = new ArrayList<>();

		for(String[] engine : dbEngines) {
			if(!engines.contains(engine[0])){
				//engines.add(engine[0]);
				visibleEngines.add(engine[0]);
			}	
		}

		query = "SELECT ENGINE.ENGINEID AS ID, ENGINE.ENGINENAME AS NAME, ENGINE.TYPE AS TYPE, ENGINE.COST AS COST "
				+ "FROM ENGINE "
				+ "WHERE ENGINE.ENGINEID ?1";
		query = query.replace("?1", createFilter(visibleEngines));

		wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		List<String[]> visibleEnginesAttributes = flushRsToListOfStrArray(wrapper);

		List<Map<String, Object>> enginesObject = new ArrayList<>();

		for(String[] engineAttributes : visibleEnginesAttributes) {
			Map<String, Object> temp = new HashMap<>();
			temp.put("app_id", engineAttributes[0]);
			temp.put("app_name", engineAttributes[1]);
			temp.put("app_type", engineAttributes[2]);
			temp.put("app_cost", engineAttributes[3] == null ? "" : engineAttributes[3]);
			enginesObject.add(temp);
		}

		return enginesObject;

	}

	/*
	 * Groups
	 */
	
	public static Map<String, List<Map<String, String>>> getDatabaseUsersAndGroups(String userId, String engineId, boolean isAdmin){

		Map<String, List<Map<String, String>>> ret = new HashMap<>();
		ret.put("groups", new ArrayList<>());
		ret.put("users", new ArrayList<>());

		if(isAdmin && !userIsAdmin(userId)){
			throw new IllegalArgumentException("This user is not an admin. ");
		}

		//TODO add check if user can access this engine as owner

		String query = "SELECT USER.ID AS ID, USER.NAME AS NAME, ENGINEPERMISSION.PERMISSION AS PERMISSION "
				+ "FROM ENGINEPERMISSION JOIN USER ON (ENGINEPERMISSION.USERID = USER.ID) "
				+ "WHERE ENGINEPERMISSION.ENGINEID = '?1'";
		query = query.replace("?1", engineId);

		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		List<String[]> users = flushRsToListOfStrArray(wrapper);

		for(String[] user : users) {
			Map<String, String> userInfo = new HashMap<>();

			userInfo.put("id", user[0]);
			userInfo.put("name", user[1]);
			userInfo.put("permission", AccessPermission.getPermissionValueById(user[2]));	

			ret.get("users").add(userInfo);
		}

		String groupQuery = "SELECT USERGROUP.GROUPID AS ID, USERGROUP.NAME AS NAME, GROUPENGINEPERMISSION.PERMISSION AS PERMISSION "
				+ "FROM GROUPENGINEPERMISSION JOIN USERGROUP ON (GROUPENGINEPERMISSION.GROUPID = USERGROUP.GROUPID) "
				+ "WHERE GROUPENGINEPERMISSION.ENGINE = '?1'";
		groupQuery = groupQuery.replace("?1", engineId);

		wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, groupQuery);
		List<String[]> groups = flushRsToListOfStrArray(wrapper);

		for(String[] group : groups) {
			Map<String, String> userInfo = new HashMap<>();

			userInfo.put("id", group[0]);
			userInfo.put("name", group[1]);
			userInfo.put("permission", AccessPermission.getPermissionValueById(group[2]));	

			ret.get("groups").add(userInfo);
		}

		return ret;
	}

	/***
	 * Add to the object ret the groups without users than the user is owner.
	 * @param ret
	 * @param query
	 * @param userId
	 */
	private static void getGroupsWithoutMembers(List<Map<String, Object>> ret, String query, String userId){
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		List<String[]> groupsWithoutMembers = flushRsToListOfStrArray(wrapper);

		for(String[] groupsWithoutMember : groupsWithoutMembers) {
			String groupId = groupsWithoutMember[0];
			String groupName = groupsWithoutMember[1];

			Map<String, Object> groupObject = new HashMap<>();
			groupObject.put("group_id", groupId);
			groupObject.put("group_name", groupName);
			groupObject.put("group_users", new ArrayList<HashMap<String, String>>());

			ret.add(groupObject);
		}
	}

	/***
	 * Get index of a group in the object ret  
	 * @param ret
	 * @param groupId
	 * @return
	 */
	private static int indexGroup(List<Map<String, Object>> ret, String groupId){
		for (int i = 0; i < ret.size(); i++) {
			if(ret.get(i).get("group_id").equals(groupId)){
				return i;
			}
		}
		return -1;
	}

	/**
	 * Get the groups with members for a user.
	 * @param ret
	 * @param query
	 */
	private static void getGroupsAndMembers(List<Map<String, Object>> ret, String query, String userId){
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		List<String[]> groupsAndMembers = flushRsToListOfStrArray(wrapper);

		for(String[] groupAndMember : groupsAndMembers) {
			String groupId = groupAndMember[0];
			String groupName = groupAndMember[1];
			Map<String, String> user = new HashMap<String, String>();
			user.put("id", groupAndMember[2]);
			user.put("name", groupAndMember[3]);
			user.put("email", groupAndMember[4]);

			int indexGroup = indexGroup(ret, groupId);

			if(indexGroup == -1) {
				List<Map<String, String>> newGroup = new ArrayList<>();
				newGroup.add(user);
				Map<String, Object> groupObject = new HashMap<>();
				groupObject.put("group_id", groupId);
				groupObject.put("group_name", groupName);
				groupObject.put("group_users", newGroup);

				ret.add(groupObject);
			} else {
				List<Map<String, String>> updateGroup = (List<Map<String, String>>) ret.get(indexGroup).get("group_users");
				updateGroup.add(user);
			}
		}
	}

	/**
	 * Get all Groups and list of user for each group,
	 * @param userId
	 * @return all groups and list of users for each group
	 */
	public static List<Map<String, Object>> getGroupsAndMembersForUser(String userId) {

		List<Map<String, Object>> ret = new ArrayList<>();

		String query = "SELECT USERGROUP.GROUPID AS GROUP_ID, USERGROUP.NAME AS GROUP_NAME FROM USERGROUP LEFT JOIN GROUPMEMBERS ON(USERGROUP.GROUPID = GROUPMEMBERS.GROUPID) WHERE GROUPMEMBERS.GROUPID IS NULL AND USERGROUP.OWNER = '?1'";
		query = query.replace("?1", userId);
		getGroupsWithoutMembers(ret, query, userId);

		query = "SELECT USERGROUP.GROUPID AS GROUP_ID, USERGROUP.NAME AS GROUPNAME, USER.ID AS MEMBER_ID, USER.NAME AS MEMBERNAME, USER.EMAIL AS EMAIL FROM USERGROUP JOIN GROUPMEMBERS ON(GROUPMEMBERS.GROUPID = USERGROUP.GROUPID) JOIN USER ON(GROUPMEMBERS.USERID = USER.ID) WHERE USERGROUP.OWNER = '?1'";
		query = query.replace("?1", userId);
		getGroupsAndMembers(ret, query, userId);

		return ret;
	}


	/**
	 * Get a list of the users o a certain group.
	 * @param groupId
	 * @return list of users.
	 */
	public static List<String[]> getAllUserFromGroup(String groupId){
		String query = 	"SELECT GROUPMEMBERS.GROUPID AS GROUP_ID, USERGROUP.NAME AS GROUP_NAME, USERGROUP.OWNER AS GROUP_OWNER, GROUPMEMBERS.USERID AS USER_ID, USER.NAME AS USER_NAME  FROM GROUPMEMBERS JOIN USERGROUP ON (USERGROUP.GROUPID = GROUPMEMBERS.GROUPID) JOIN USER ON (USER.ID = GROUPMEMBERS.USERID) WHERE GROUPMEMBERS.GROUPID = ?1";
		query = query.replace("?1", groupId);
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		return flushRsToListOfStrArray(wrapper);
	}

	/**
	 * Get a list of all the users of a list of groups
	 * @param groups
	 * @return list of users.
	 */
	public static Map<String, List<String[]>> getAllUserFromGroups(List<String> groups){
		String query = 	"SELECT GROUPMEMBERS.GROUPID AS GROUP_ID, USERGROUP.NAME AS GROUP_NAME, USERGROUP.OWNER AS GROUP_OWNER, GROUPMEMBERS.USERID AS USER_ID, USER.NAME AS USER_NAME  FROM GROUPMEMBERS JOIN USERGROUP ON (USERGROUP.GROUPID = GROUPMEMBERS.GROUPID) JOIN USER ON (USER.ID = GROUPMEMBERS.USERID) WHERE GROUPMEMBERS.GROUPID ?1";
		query = query.replace("?1", createFilter(groups));
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		List<Object[]> users = flushRsToMatrix(wrapper);
		Map<String, List<String[]>> ret = new HashMap<>();
		for(Object[] userObject : users){
			String[] user = Arrays.stream(userObject).map(Object::toString).
					toArray(String[]::new);
			String userId = user[3];
			if(ret.get(userId) == null){
				List<String[]> userNodeList = new ArrayList<>();
				userNodeList.add(user);
				ret.put(userId, userNodeList);
			} else {
				ret.get(userId).add(user);
			}
		}
		return ret;
	}


	/**
	 * Check if the users from a group already have access to the database through another group.  
	 * @param usersFromGroup
	 * @param otherGroupsUserList
	 * @return
	 */
	private static String isGroupWithInvalidUsersFromOtherGroups(List<String[]> usersFromGroup, Map<String, List<String[]>> otherGroupsUserList){
		String ret = "";
		for(String[] user : usersFromGroup){
			String userId = user[3];
			if(otherGroupsUserList.get(userId) != null){
				List<String[]> otherCoincidences = otherGroupsUserList.get(userId);
				for(String[] coincidence : otherCoincidences){
					String groupName = coincidence[1];
					String groupOwner = NativeUserSecurityUtils.getUsernameByUserId(coincidence[2]);
					ret += "The user " + user[4] + " in " + user[1] + " already has access to the database through " + groupName + " owned by " + groupOwner + ". ";
				}
			}
		}
		return ret;
	}

	/**
	 * Check if the user from a group have already direct access to a database. 
	 * Checking over the parameter usersList
	 * @param usersFromGroup
	 * @param users
	 * @return
	 */
	private static String isGroupWithInvalidUsersFromList(List<String[]> usersFromGroup, List<String> users){
		String ret = "";
		for(String[] user : usersFromGroup){
			if(users.contains(user[3])){
				ret += "The user " + user[4] + " in the group " + user[1] + " already has direct access to the database. ";
			}
		}
		return ret;
	}

	/**
	 * Check if the group to be added has already permissions to a database.
	 * @param groupId
	 * @param groups
	 * @param users
	 * @return
	 */
	public static String isGroupUsersWithDatabasePermissionAlready(String groupId, List<String> groups, List<String> users) {
		String ret = "";
		// TODO Security check if the user logged in is the owner of the group being added (?)

		List<String[]> allUserFromGroup = getAllUserFromGroup(groupId);
		ret += isGroupWithInvalidUsersFromList(allUserFromGroup, users);
		Map<String, List<String[]>> allUsersFromOtherGroups = getAllUserFromGroups(groups);
		ret += isGroupWithInvalidUsersFromOtherGroups(allUserFromGroup, allUsersFromOtherGroups);

		return ret.isEmpty() ? "true" : ret;
	}
	

	/**
	 * Get the id list of groups owned by an user
	 * @param userId
	 * @return List<String> with the id of the groups owned by an user
	 */
	public static List<String> getGroupsOwnedByUser(String userId){
		String query = "SELECT GROUPID FROM USERGROUP WHERE OWNER = '?1'";
		query = query.replace("?1", userId);
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		List<String[]> res = flushRsToListOfStrArray(wrapper);
		List<String> groupList = new ArrayList<>();
		for(String[] row : res){
			groupList.add(row[0]);
		}
		return groupList;
	}
	
	/*
	 * Engines
	 */

	/**
	 * Check if user already has a relationship with a certain database
	 * @param userId
	 * @param groupsId 
	 * @param usersId
	 * @return if the user has a relationship with a database this message explains how.
	 */
	public static String isUserWithDatabasePermissionAlready(String userId, List<String> groupsId, List<String> usersId){
		String ret = "";

		String username = NativeUserSecurityUtils.getUsernameByUserId(userId);

		if(usersId.contains(userId)){
			ret += "The user " + username + " already has a direct relationship with the database. ";
		}

		String query = "SELECT GROUPMEMBERS.GROUPID AS ID, USERGROUP.NAME AS NAME, USER.NAME AS OWNER FROM GROUPMEMBERS JOIN USERGROUP ON (GROUPMEMBERS.GROUPID = USERGROUP.GROUPID) JOIN USER ON(USERGROUP.OWNER = USER.ID) WHERE GROUPMEMBERS.GROUPID ?1 AND GROUPMEMBERS.USERID = '?2'";	
		query = query.replace("?1", createFilter(groupsId));
		query = query.replace("?2", userId);

		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		List<String[]> result = flushRsToListOfStrArray(wrapper);

		for(String[] row : result){
			//String groupId = row[0];
			String groupName = row[1];
			String groupOwner = row[2];

			ret += "The user " + username + " is in " + groupName + " owned by " + groupOwner + ". "; 

		}

		return ret.isEmpty() ? "true" : ret;
	}
	
	/**
	 * Get all groups associated with a database. To that list adds groupsToAdd and remove groupsToRemove 
	 * @param engineId
	 * @param groupsToAdd
	 * @param groupsToRemove
	 * @return list of groups id.
	 */
	public static List<String> getAllDbGroupsById(String engineId, List<String> groupsToAdd,
			List<String> groupsToRemove) {

		List<String> ret = new ArrayList<>();
		String query = "SELECT GROUPENGINEPERMISSION.GROUPID FROM GROUPENGINEPERMISSION WHERE GROUPENGINEPERMISSION.ENGINE = '?1'";
		query = query.replace("?1", engineId);
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		List<String[]> rows = flushRsToListOfStrArray(wrapper);

		for(String[] row : rows){
			if(row[0] != null && !row[0].isEmpty())
				ret.add(row[0]);
		}

		ret.removeAll(groupsToRemove);
		ret.addAll(groupsToAdd);
		return ret;
	}

	/***
	 * Get all users that have a direct relationship with a database to that list adds userToAdd and 
	 * removes usersToRemove
	 * @param engineId
	 * @param usersToAdd
	 * @param usersToRemove
	 * @return list of users id.
	 */
	public static List<String> getAllDbUsersById(String engineId, List<String> usersToAdd,
			List<String> usersToRemove) {

		List<String> ret = new ArrayList<>();
		String query = "SELECT ENGINEPERMISSION.USERID FROM ENGINEPERMISSION WHERE ENGINEPERMISSION.ENGINEID = '?1'";
		query = query.replace("?1", engineId);
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		List<String[]> rows = flushRsToListOfStrArray(wrapper);

		for(String[] row : rows){
			ret.add(row[0]);
		}
		ret.removeAll(usersToRemove);
		ret.addAll(usersToAdd);
		return ret;
	}

	/**
	 * Check if the user is already associated from another group 
	 * to the databases that the group he wants to be part of is associated. 
	 * @param userId
	 * @param groupId
	 * @return
	 */
	public static String isUserInAnotherDbGroup(String userId, String groupId) {

		String ret = "";
		String query = "SELECT GROUPMEMBERS.GROUPID AS GROUPID, USERGROUP.NAME AS GROUPNAME, USER.NAME AS OWNER, GR.ENGINENAME AS ENGINENAME "
				+ "FROM GROUPMEMBERS JOIN USERGROUP ON (USERGROUP.GROUPID = GROUPMEMBERS.GROUPID) JOIN USER ON(USER.ID = USERGROUP.OWNER) JOIN "
				+ "(SELECT GROUPENGINEPERMISSION.GROUPID AS GROUPID, EN.NAME AS ENGINENAME FROM GROUPENGINEPERMISSION JOIN "
				+ "(SELECT GROUPENGINEPERMISSION.ENGINE AS ENGINE, ENGINE.ENGINENAME AS NAME "
				+ "FROM GROUPENGINEPERMISSION JOIN ENGINE ON(GROUPENGINEPERMISSION.ENGINE = ENGINE.ENGINEID) WHERE GROUPENGINEPERMISSION.GROUPID = '?2') EN ON (GROUPENGINEPERMISSION.ENGINE = EN.ENGINE) WHERE GROUPENGINEPERMISSION.GROUPID != '?2') GR ON (GROUPMEMBERS.GROUPID = GR.GROUPID) "
				+ "WHERE GROUPMEMBERS.USERID = '?1'";

		query = query.replace("?1", userId);
		query = query.replace("?2", groupId);

		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		List<Object[]> rows = flushRsToMatrix(wrapper);

		for(Object[] row : rows){
			String groupName = row[1].toString();
			String ownerName = row[2].toString();
			String engineName = row[3].toString();
			ret += "The user is already associated with the database " + engineName + " in the group " + groupName + " owned by " + ownerName + ". ";
		}

		return ret;
	}

	/**
	 * Check if a user already have a relationship with a database that the group already have access.
	 * @param userId
	 * @param groupId
	 * @return blank if there was no relationships otherwise a message explaining other databases associated with the user.
	 */
	public static String isUserWithAccessToGroupDb(String userId, String groupId) {

		String ret = "";
		String query = "SELECT EN.ENGINE AS ENGINE, EN.NAME AS ENGINENAME "
				+ "FROM ENGINEPERMISSION JOIN (SELECT GROUPENGINEPERMISSION.ENGINE AS ENGINE, ENGINE.ENGINENAME AS NAME FROM GROUPENGINEPERMISSION JOIN ENGINE ON(GROUPENGINEPERMISSION.ENGINE = ENGINE.ENGINEID) WHERE GROUPENGINEPERMISSION.GROUPID = '?1') EN ON (ENGINEPERMISSION.ENGINEID = EN.ENGINE) "
				+ "WHERE ENGINEPERMISSION.USERID = '?2'";

		query = query.replace("?1", groupId);
		query = query.replace("?2", userId);

		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		List<Object[]> rows = flushRsToMatrix(wrapper);

		for(Object[] row : rows){
			String engineName = row[1].toString();
			ret += "The user is already associated with the database " + engineName + ". ";
		}

		return ret;
	}

}
