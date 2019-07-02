package prerna.auth.utils;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.stream.Collectors;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.date.SemossDate;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryConstantSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SecurityQueryUtils extends AbstractSecurityUtils {
	
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	
	/**
	 * Try to reconcile and get the engine id
	 * @return
	 */
	public static String testUserEngineIdForAlias(User user, String potentialId) {
		List<String> ids = new Vector<String>();
		
//		String userFilters = getUserFilters(user);
//		String query = "SELECT DISTINCT ENGINEPERMISSION.ENGINEID "
//				+ "FROM ENGINEPERMISSION INNER JOIN ENGINE ON ENGINE.ENGINEID=ENGINEPERMISSION.ENGINEID "
//				+ "WHERE ENGINE.ENGINENAME='" + potentialId + "' AND ENGINEPERMISSION.USERID IN " + userFilters;
//		
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__ENGINEID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINENAME", "==", potentialId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", getUserFiltersQs(user)));
		qs.addRelation("ENGINE", "ENGINEPERMISSION", "inner.join");

		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
		ids = flushToListString(wrapper);
		if(ids.isEmpty()) {
//			query = "SELECT DISTINCT ENGINE.ENGINEID FROM ENGINE WHERE ENGINE.ENGINENAME='" + potentialId + "' AND ENGINE.GLOBAL=TRUE";

			qs = new SelectQueryStruct();
			qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID"));
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINENAME", "==", potentialId));
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__GLOBAL", "==", true, PixelDataType.BOOLEAN));
			
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
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
//		String query = "SELECT DISTINCT ENGINEID FROM ENGINE";
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID"));
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
		return flushToListString(wrapper);
	}
	
	/**
	 * Get the engine alias for a id
	 * @return
	 */
	public static String getEngineAliasForId(String id) {
//		String query = "SELECT ENGINENAME FROM ENGINE WHERE ENGINEID='" + id + "'";
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINENAME"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGIENID", "==", id));
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
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
//		String userFilters = getUserFilters(user);
//		
//		// get user specific databases
//		String query = "SELECT DISTINCT "
//				+ "ENGINE.ENGINEID as \"app_id\", "
//				+ "ENGINE.ENGINENAME as \"app_name\", "
//				+ "ENGINE.GLOBAL as \"app_global\", "
//				+ "COALESCE(ENGINEPERMISSION.VISIBILITY, TRUE) as \"app_visibility\", "
//				+ "COALESCE(PERMISSION.NAME, 'READ_ONLY') as \"app_permission\" "
//				+ "FROM ENGINE "
//				+ "INNER JOIN ENGINEPERMISSION ON ENGINE.ENGINEID=ENGINEPERMISSION.ENGINEID "
//				+ "LEFT JOIN PERMISSION ON PERMISSION.ID=ENGINEPERMISSION.PERMISSION "
//				+ "WHERE ENGINEPERMISSION.USERID IN " + userFilters;
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID", "app_id"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINENAME", "app_name"));
		qs.addSelector(new QueryColumnSelector("ENGINE__GLOBAL", "app_global"));
		{
			QueryFunctionSelector fun = new QueryFunctionSelector();
			fun.setFunction(QueryFunctionHelper.COALESCE);
			fun.addInnerSelector(new QueryColumnSelector("ENGINEPERMISSION__VISIBILITY"));
			fun.addInnerSelector(new QueryConstantSelector(true));
			fun.setAlias("app_visibility");
			qs.addSelector(fun);
		}
		{
			QueryFunctionSelector fun = new QueryFunctionSelector();
			fun.setFunction(QueryFunctionHelper.COALESCE);
			fun.addInnerSelector(new QueryColumnSelector("PERMISSION__NAME"));
			fun.addInnerSelector(new QueryConstantSelector("READ_ONLY"));
			fun.setAlias("app_permission");
			qs.addSelector(fun);
		}
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", getUserFiltersQs(user)));
		qs.addRelation("ENGINE", "ENGINEPERMISSION", "inner.join");
		qs.addRelation("ENGINEPERMISSION", "PERMISSION", "left.outer.join");
		
		Set<String> engineIdsIncluded = new HashSet<String>();
		
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
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
//		query = "SELECT DISTINCT "
//				+ "ENGINE.ENGINEID as \"app_id\", "
//				+ "ENGINE.ENGINENAME as \"app_name\" "
//				+ "FROM ENGINE WHERE ENGINE.GLOBAL=TRUE AND ENGINE.ENGINEID NOT " + createFilter(engineIdsIncluded);
//		
//		wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);

		qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID", "app_id"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINENAME", "app_name"));
		{
			QueryFunctionSelector fun = new QueryFunctionSelector();
			fun.setFunction(QueryFunctionHelper.COALESCE);
			fun.addInnerSelector(new QueryColumnSelector("ENGINEPERMISSION__VISIBILITY"));
			fun.addInnerSelector(new QueryConstantSelector(true));
			fun.setAlias("app_visibility");
			qs.addSelector(fun);
		}
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__GLOBAL", "==", true, PixelDataType.BOOLEAN));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINEID", "!=", new Vector<String>(engineIdsIncluded)));
		qs.addRelation("ENGINE", "ENGINEPERMISSION", "left.outer.join");
		wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
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
//		String userFilters = getUserFilters(user);
//		String query = "SELECT DISTINCT "
//				+ "ENGINE.ENGINEID as \"app_id\", "
//				+ "ENGINE.ENGINENAME as \"app_name\", "
//				+ "ENGINE.TYPE as \"app_type\", "
//				+ "ENGINE.COST as \"app_cost\","
//				+ "LOWER(ENGINE.ENGINENAME) as \"low_app_name\" "
//				+ "FROM ENGINE "
//				+ "LEFT JOIN ENGINEPERMISSION ON ENGINE.ENGINEID=ENGINEPERMISSION.ENGINEID "
//				+ "LEFT JOIN USER ON ENGINEPERMISSION.USERID=USER.ID "
//				+ "WHERE "
//				+ "( ENGINE.GLOBAL=TRUE "
//				+ "OR ENGINEPERMISSION.USERID IN " + userFilters + " ) "
//				+ "AND ENGINE.ENGINEID NOT IN (SELECT ENGINEID FROM ENGINEPERMISSION WHERE VISIBILITY=FALSE AND USERID IN " + userFilters + ") "
//				+ "ORDER BY LOWER(ENGINE.ENGINENAME)";	
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		List<String> userIds = getUserFiltersQs(user);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		// selectors
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID", "app_id"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINENAME", "app_name"));
		qs.addSelector(new QueryColumnSelector("ENGINE__TYPE", "app_type"));
		qs.addSelector(new QueryColumnSelector("ENGINE__COST", "app_cost"));
		QueryFunctionSelector fun = new QueryFunctionSelector();
		fun.setFunction(QueryFunctionHelper.LOWER);
		fun.addInnerSelector(new QueryColumnSelector("ENGINE__ENGINENAME"));
		fun.setAlias("low_app_name");
		qs.addSelector(fun);
		// filters
		{
			OrQueryFilter orFilter = new OrQueryFilter();
			orFilter.addFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__GLOBAL", "==", true, PixelDataType.BOOLEAN));
			orFilter.addFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", userIds));
			qs.addExplicitFilter(orFilter);
		}
		{
			SelectQueryStruct subQs = new SelectQueryStruct();
			subQs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__ENGINEID"));
			subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__VISIBILITY", "==", false, PixelDataType.BOOLEAN));
			subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", userIds));
			
			NounMetadata subQueryLHS = new NounMetadata(new QueryColumnSelector("ENGINE__ENGINEID"), PixelDataType.COLUMN);
			NounMetadata subQueryRHS = new NounMetadata(subQs, PixelDataType.QUERY_STRUCT);
			SimpleQueryFilter subQueryFilter = new SimpleQueryFilter(subQueryLHS, "!=", subQueryRHS);
			qs.addExplicitFilter(subQueryFilter);
		}
		// joins
		qs.addRelation("ENGINE", "ENGINEPERMISSION", "left.outer.join");
		qs.addRelation("USER", "ENGINEPERMISSION", "left.outer.join");
		// sorts
		qs.addOrderBy(new QueryColumnOrderBySelector("low_app_name"));
		
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
		return flushRsToMap(wrapper);
	}
	
	/**
	 * Get the list of the engine information that the user has access to
	 * @param userId
	 * @return
	 */
	public static List<Map<String, Object>> getAllDatabaseList() {
//		String query = "SELECT DISTINCT "
//				+ "ENGINE.ENGINEID as \"app_id\", "
//				+ "ENGINE.ENGINENAME as \"app_name\", "
//				+ "ENGINE.TYPE as \"app_type\", "
//				+ "ENGINE.COST as \"app_cost\", "
//				+ "LOWER(ENGINE.ENGINENAME) as \"low_app_name\" "
//				+ "FROM ENGINE "
//				+ "LEFT JOIN ENGINEPERMISSION ON ENGINE.ENGINEID=ENGINEPERMISSION.ENGINEID "
//				+ "ORDER BY LOWER(ENGINE.ENGINENAME)";
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID", "app_id"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINENAME", "app_name"));
		qs.addSelector(new QueryColumnSelector("ENGINE__TYPE", "app_type"));
		qs.addSelector(new QueryColumnSelector("ENGINE__COST", "app_cost"));
		QueryFunctionSelector fun = new QueryFunctionSelector();
		fun.setFunction(QueryFunctionHelper.LOWER);
		fun.addInnerSelector(new QueryColumnSelector("ENGINE__ENGINENAME"));
		fun.setAlias("low_app_name");
		qs.addSelector(fun);
		qs.addRelation("ENGINE", "ENGINEPERMISSION", "left.outer.join");
		qs.addOrderBy(new QueryColumnOrderBySelector("low_app_name"));
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
		return flushRsToMap(wrapper);
	}
	
	/**
	 * Get the list of the engine information that the user has access to
	 * @param userId
	 * @return
	 */
	public static List<Map<String, Object>> getUserDatabaseList(User user, String engineFilter) {
//		String userFilters = getUserFilters(user);
//		String filter = createFilter(engineFilter); 
//		String query = "SELECT DISTINCT "
//				+ "ENGINE.ENGINEID as \"app_id\", "
//				+ "ENGINE.ENGINENAME as \"app_name\", "
//				+ "ENGINE.TYPE as \"app_type\", "
//				+ "ENGINE.COST as \"app_cost\", "
//				+ "LOWER(ENGINE.ENGINENAME) as \"low_app_name\" "
//				+ "FROM ENGINE "
//				+ "LEFT JOIN ENGINEPERMISSION ON ENGINE.ENGINEID=ENGINEPERMISSION.ENGINEID "
//				+ "WHERE "
//				+ (!filter.isEmpty() ? ("ENGINE.ENGINEID " + filter + " AND ") : "")
//				+ "(ENGINEPERMISSION.USERID IN " + userFilters + " OR ENGINE.GLOBAL=TRUE) "
//				+ "ORDER BY LOWER(ENGINE.ENGINENAME)";
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID", "app_id"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINENAME", "app_name"));
		qs.addSelector(new QueryColumnSelector("ENGINE__TYPE", "app_type"));
		qs.addSelector(new QueryColumnSelector("ENGINE__COST", "app_cost"));
		QueryFunctionSelector fun = new QueryFunctionSelector();
		fun.setFunction(QueryFunctionHelper.LOWER);
		fun.addInnerSelector(new QueryColumnSelector("ENGINE__ENGINENAME"));
		fun.setAlias("low_app_name");
		qs.addSelector(fun);
		if(engineFilter != null && !engineFilter.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINEID", "==", engineFilter));
		}
		{
			OrQueryFilter orFilter = new OrQueryFilter();
			orFilter.addFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__GLOBAL", "==", true, PixelDataType.BOOLEAN));
			orFilter.addFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", getUserFiltersQs(user)));
			qs.addExplicitFilter(orFilter);
		}
		qs.addRelation("ENGINE", "ENGINEPERMISSION", "left.outer.join");
		qs.addOrderBy(new QueryColumnOrderBySelector("low_app_name"));
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
		return flushRsToMap(wrapper);
	}
	
	/**
	 * Get the list of the engine information that the user has access to
	 * @param userId
	 * @return
	 */
	public static List<Map<String, Object>> getAllDatabaseList(String engineFilter) {
//		String filter = createFilter(engineFilter); 
//		String query = "SELECT DISTINCT "
//				+ "ENGINE.ENGINEID as \"app_id\", "
//				+ "ENGINE.ENGINENAME as \"app_name\", "
//				+ "ENGINE.TYPE as \"app_type\", "
//				+ "ENGINE.COST as \"app_cost\", "
//				+ "LOWER(ENGINE.ENGINENAME) as \"low_app_name\" "
//				+ "FROM ENGINE "
// 				+ (!filter.isEmpty() ? ("WHERE ENGINE.ENGINEID " + filter + " ") : "")
//				+ "ORDER BY LOWER(ENGINE.ENGINENAME)";
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID", "app_id"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINENAME", "app_name"));
		qs.addSelector(new QueryColumnSelector("ENGINE__TYPE", "app_type"));
		qs.addSelector(new QueryColumnSelector("ENGINE__COST", "app_cost"));
		QueryFunctionSelector fun = new QueryFunctionSelector();
		fun.setFunction(QueryFunctionHelper.LOWER);
		fun.addInnerSelector(new QueryColumnSelector("ENGINE__ENGINENAME"));
		fun.setAlias("low_app_name");
		qs.addSelector(fun);
		if(engineFilter != null && !engineFilter.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINEID", "==", engineFilter));
		}
		qs.addOrderBy(new QueryColumnOrderBySelector("low_app_name"));
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
		return flushRsToMap(wrapper);
	}
	
	public static Map<String, List<String>> getAggregateEngineMetadata(String engineId) {
//		String query = "SELECT KEY, VALUE FROM ENGINEMETA WHERE ENGINEID='" + engineId + "'";
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINEMETA__KEY"));
		qs.addSelector(new QueryColumnSelector("ENGINEMETA__VALUE"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEMETA__ENGINEID", "==", engineId));
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
		
		Map<String, List<String>> engineMeta = new HashMap<String, List<String>>();
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
//		String userFilters = getUserFilters(user);
//		String query = "SELECT DISTINCT ENGINEID FROM ENGINEPERMISSION WHERE USERID IN " + userFilters;
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__ENGINEID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", getUserFiltersQs(user)));
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
		List<String> engineList = flushToListString(wrapper);
		engineList.addAll(getGlobalEngineIds());
		return engineList.stream().distinct().sorted().collect(Collectors.toList());
	}
	
	/**
	 * Get global engines
	 * @return
	 */
	public static Set<String> getGlobalEngineIds() {
//		String query = "SELECT ENGINEID FROM ENGINE WHERE GLOBAL=TRUE";
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__GLOBAL", "==", true, PixelDataType.BOOLEAN));
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
		return flushToSetString(wrapper, false);
	}
		
	//////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////////

	/*
	 * Get all the users for a database
	 */
	
	public static List<Map<String, Object>> getDisplayDatabaseOwnersAndEditors(String engineId) {
		List<Map<String, Object>> users = null;
		if(getGlobalEngineIds().contains(engineId)) {
//			String query = "SELECT DISTINCT "
//					+ "USER.NAME AS \"name\", "
//					+ "PERMISSION.NAME as \"permission\" "
//					+ "FROM USER "
//					+ "INNER JOIN ENGINEPERMISSION ON USER.ID=ENGINEPERMISSION.USERID "
//					+ "INNER JOIN PERMISSION ON ENGINEPERMISSION.PERMISSION=PERMISSION.ID "
//					+ "WHERE PERMISSION.ID IN (1,2) AND ENGINEPERMISSION.ENGINEID='" + engineId + "'";
//			IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
						
			SelectQueryStruct qs = new SelectQueryStruct();
			qs.addSelector(new QueryColumnSelector("USER__NAME", "name"));
			qs.addSelector(new QueryColumnSelector("PERMISSION__NAME", "permission"));
			List<Integer> permissionValues = new Vector<Integer>(2);
			permissionValues.add(new Integer(1));
			permissionValues.add(new Integer(2));
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PERMISSION__ID", "==", permissionValues));
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__ENGINEID", "==", engineId));
			qs.addRelation("USER", "ENGINEPERMISSION", "inner.join");
			qs.addRelation("ENGINEPERMISSION", "PERMISSION", "inner.join");
			IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			users = flushRsToMap(wrapper);
			
			Map<String, Object> globalMap = new HashMap<String, Object>();
			globalMap.put("name", "PUBLIC DATABASE");
			globalMap.put("permission", "READ_ONLY");
			users.add(globalMap);
		} else {
//			String query = "SELECT DISTINCT "
//					+ "USER.NAME AS \"name\", "
//					+ "PERMISSION.NAME as \"permission\" "
//					+ "FROM USER "
//					+ "INNER JOIN ENGINEPERMISSION ON USER.ID=ENGINEPERMISSION.USERID "
//					+ "INNER JOIN PERMISSION ON ENGINEPERMISSION.PERMISSION=PERMISSION.ID "
//					+ "WHERE ENGINEPERMISSION.ENGINEID='" + engineId + "'";
//			IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
			
			users = getFullDatabaseOwnersAndEditors(engineId);
		}
		return users;
	}
	
	public static List<Map<String, Object>> getFullDatabaseOwnersAndEditors(String engineId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("USER__ID", "id"));
		qs.addSelector(new QueryColumnSelector("USER__NAME", "name"));
		qs.addSelector(new QueryColumnSelector("PERMISSION__NAME", "permission"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__ENGINEID", "==", engineId));
		qs.addRelation("USER", "ENGINEPERMISSION", "inner.join");
		qs.addRelation("ENGINEPERMISSION", "PERMISSION", "inner.join");
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
		List<Map<String, Object>> users = flushRsToMap(wrapper);
		return users;
	}
	
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	
//	@Deprecated
//	public static Boolean userIsAdmin(String userId) {
//		String query = "SELECT ADMIN FROM USER WHERE ID ='" + userId + "';";
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
//		List<String[]> ret = flushRsToListOfStrArray(wrapper);
//		if(!ret.isEmpty()) {
//			return Boolean.parseBoolean(ret.get(0)[0]);
//		}
//		return false;
//	}
	
	public static boolean insightIsGlobal(String engineId, String insightId) {
//		String query = "SELECT DISTINCT INSIGHT.GLOBAL FROM INSIGHT  "
//				+ "WHERE ENGINEID='" + engineId + "' AND INSIGHTID='" + insightId + "' AND INSIGHT.GLOBAL=TRUE";
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("INSIGHT__GLOBAL"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHT__ENGINEID", "==", engineId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHT__INSIGHTID", "==", insightId));
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
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

	public static SemossDate getLastExecutedInsightInApp(String engineId) {
//		String query = "SELECT DISTINCT INSIGHT.LASTMODIFIEDON "
//				+ "FROM INSIGHT "
//				+ "WHERE INSIGHT.ENGINEID='" + engineId + "'"
//				+ "ORDER BY INSIGHT.LASTMODIFIEDON DESC LIMIT 1"
//				;
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("INSIGHT__LASTMODIFIEDON"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHT__ENGINEID", "==", engineId));
		qs.addOrderBy(new QueryColumnOrderBySelector("INSIGHT__LASTMODIFIEDON", "DESC"));
		qs.setLimit(1);
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);

		SemossDate date = null;
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
	
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	
	/*
	 * Querying user data
	 */
	
	/**
	 * Get user info from ids
	 * 
	 * @param userIds
	 * @return
	 * @throws IllegalArgumentException
	 */
	public static Map<String, Map<String, Object>> getUserInfo(List<String> userIds) throws IllegalArgumentException {
//		String query = "SELECT DISTINCT ID, NAME, USERNAME, EMAIL, TYPE, ADMIN FROM USER ";
//		String userFilter = createFilter(userIds);
//		query += " WHERE ID " + userFilter + ";";
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("USER__ID"));
		qs.addSelector(new QueryColumnSelector("USER__NAME"));
		qs.addSelector(new QueryColumnSelector("USER__USERNAME"));
		qs.addSelector(new QueryColumnSelector("USER__EMAIL"));
		qs.addSelector(new QueryColumnSelector("USER__TYPE"));
		qs.addSelector(new QueryColumnSelector("USER__ADMIN"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USER__ID", "==", userIds));
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
		
		Map<String, Map<String, Object>> userMap = new HashMap<>();
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
//		String filter = getUserFilters(user);
//		String query = "SELECT DISTINCT ID, ENGINE, PERMISSION FROM ACCESSREQUEST "
//				+ "WHERE SUBMITTEDBY IN " + filter;
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ACCESSREQUEST__ID"));
		qs.addSelector(new QueryColumnSelector("ACCESSREQUEST__ENGINE"));
		qs.addSelector(new QueryColumnSelector("ACCESSREQUEST__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ACCESSREQUEST__SUBMITTEDBY", "==", getUserFiltersQs(user)));
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
		return flushRsToMap(wrapper);
	}
	
	/**
	 * Get all user requests including specific user id
	 * @param user
	 * @return
	 */
	public static List<Map<String, Object>> getUserAccessRequestsByProvider(User user, String engineFilter) {
//		String filter = getUserFilters(user);
//		String query = "SELECT DISTINCT ID, SUBMITTEDBY, ENGINE, PERMISSION FROM ACCESSREQUEST "
//				+ "WHERE ENGINE='" + engineFilter + "' AND SUBMITTEDBY IN " + filter;
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ACCESSREQUEST__ID"));
		qs.addSelector(new QueryColumnSelector("ACCESSREQUEST__SUBMITTEDBY"));
		qs.addSelector(new QueryColumnSelector("ACCESSREQUEST__ENGINE"));
		qs.addSelector(new QueryColumnSelector("ACCESSREQUEST__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ACCESSREQUEST__ENGINE", "==", engineFilter));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ACCESSREQUEST__SUBMITTEDBY", "==", getUserFiltersQs(user)));
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
		return flushRsToMap(wrapper);
	}
	
	/**
	 * Determine if the user has publisher rights
	 * @param user
	 * @return
	 */
	public static boolean userIsPublisher(User user) {
//		String userFilters = getUserFilters(user);
//		String query = "SELECT * FROM USER WHERE PUBLISHER=TRUE AND ID IN " + userFilters + " LIMIT 1;";
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("USER__ID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USER__PUBLISHER", "==", "TRUE"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USER__ID", "==", getUserFiltersQs(user)));
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
		try {
			return wrapper.hasNext();
		} finally {
			wrapper.cleanUp();
		}
	}
	
	/**
	 * Search if there's users containing 'searchTerm' in their email or name
	 * @param searchTerm
	 * @return
	 */
	public static List<Map<String, Object>> searchForUser(String searchTerm) {
//		String query = "SELECT DISTINCT USER.ID AS ID, USER.NAME AS NAME, USER.EMAIL AS EMAIL FROM USER "
//				+ "WHERE UPPER(USER.NAME) LIKE UPPER('%" + searchTerm + "%') "
//				+ "OR UPPER(USER.EMAIL) LIKE UPPER('%" + searchTerm + "%') "
//				+ "OR UPPER(USER.ID) LIKE UPPER('%" + searchTerm + "%');";
//
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("USER__ID", "id"));
		qs.addSelector(new QueryColumnSelector("USER__NAME", "name"));
		qs.addSelector(new QueryColumnSelector("USER__EMAIL", "email"));

		OrQueryFilter orFilter = new OrQueryFilter();
		orFilter.addFilter(SimpleQueryFilter.makeColToValFilter("USER__NAME", "?like", searchTerm));
		orFilter.addFilter(SimpleQueryFilter.makeColToValFilter("USER__EMAIL", "?like", searchTerm));
		orFilter.addFilter(SimpleQueryFilter.makeColToValFilter("USER__ID", "?like", searchTerm));
		qs.addExplicitFilter(orFilter);
		
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
		List<Map<String, Object>> users = flushRsToMap(wrapper);
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
	public static boolean checkUserExist(String userId) {
//		String query = "SELECT * FROM USER WHERE ID='" + RdbmsQueryBuilder.escapeForSQLStatement(userId) + "'";
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("USER__ID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USER__ID", "==", userId));
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
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
//		String query = "SELECT * FROM USER WHERE USERNAME='" + RdbmsQueryBuilder.escapeForSQLStatement(username) + 
//				"' OR EMAIL='" + RdbmsQueryBuilder.escapeForSQLStatement(email) + "'";
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("USER__USERNAME"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USER__USERNAME", "==", username));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USER__EMAIL", "==", email));
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
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
//		String query = "SELECT NAME FROM USER WHERE ID='" + RdbmsQueryBuilder.escapeForSQLStatement(userId) + "' AND TYPE = '"+ type + "';";
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("USER__NAME"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USER__ID", "==", userId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USER__TYPE", "==", type.toString()));
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
		List<String[]> ret = flushRsToListOfStrArray(wrapper);
		if(!ret.isEmpty()) {
			return Boolean.parseBoolean(ret.get(0)[0]);
		}
		return false;
	}
}
