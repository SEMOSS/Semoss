package prerna.auth.utils;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
import prerna.util.Constants;
import prerna.util.QueryExecutionUtility;

public class SecurityQueryUtils extends AbstractSecurityUtils {
	
	private static final Logger logger = LogManager.getLogger(SecurityQueryUtils.class);
	
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

		ids = QueryExecutionUtility.flushToListString(securityDb, qs);
		if(ids.isEmpty()) {
//			query = "SELECT DISTINCT ENGINE.ENGINEID FROM ENGINE WHERE ENGINE.ENGINENAME='" + potentialId + "' AND ENGINE.GLOBAL=TRUE";

			qs = new SelectQueryStruct();
			qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID"));
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINENAME", "==", potentialId));
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__GLOBAL", "==", true, PixelDataType.BOOLEAN));
			
			ids = QueryExecutionUtility.flushToListString(securityDb, qs);
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
		return QueryExecutionUtility.flushToListString(securityDb, qs);
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
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINEID", "==", id));
		List<String> results = QueryExecutionUtility.flushToListString(securityDb, qs);
		if(results.isEmpty()) {
			return null;
		}
		return results.get(0);
	}
	
	/**
	 * Get the engine alias for a id
	 * @return
	 */
	public static String getInsightNameForId(String engineId, String insightId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("INSIGHT__INSIGHTNAME"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHT__ENGINEID", "==", engineId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHT__INSIGHTID", "==", insightId));
		List<String> results = QueryExecutionUtility.flushToListString(securityDb, qs);
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
		
		List<Map<String, Object>> result = new Vector<Map<String, Object>>();

		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
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
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
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
		try {
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
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
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
		
		Collection<String> userIds = getUserFiltersQs(user);
		
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
			// store first and fill in sub query after
			qs.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery("ENGINE__ENGINEID", "!=", subQs));
			
			// fill in the sub query with the necessary column output + filters
			subQs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__ENGINEID"));
			subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__VISIBILITY", "==", false, PixelDataType.BOOLEAN));
			subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", userIds));
		}
		// joins
		qs.addRelation("ENGINE", "ENGINEPERMISSION", "left.outer.join");
		// sorts
		qs.addOrderBy(new QueryColumnOrderBySelector("low_app_name"));
		
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}
	
	/**
	 * Get all user engines and engine Ids regardless of it being hidden or not 
	 * @param userId
	 * @return
	 */
	public static List<Map<String, Object>> getAllUserDatabaseList(User user) {	
		SelectQueryStruct qs = new SelectQueryStruct();

		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID", "app_id"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINENAME", "app_name"));
		qs.addSelector(new QueryColumnSelector("ENGINE__TYPE", "app_type"));
		qs.addSelector(new QueryColumnSelector("ENGINE__COST", "app_cost"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__GLOBAL", "==", true, PixelDataType.BOOLEAN));
		List<Map<String, Object>> allGlobalEnginesMap = QueryExecutionUtility.flushRsToMap(securityDb, qs);

		SelectQueryStruct qs2 = new SelectQueryStruct();
		qs2.addSelector(new QueryColumnSelector("ENGINE__ENGINEID", "app_id"));
		qs2.addSelector(new QueryColumnSelector("ENGINE__ENGINENAME", "app_name"));
		qs2.addSelector(new QueryColumnSelector("ENGINE__TYPE", "app_type"));
		qs2.addSelector(new QueryColumnSelector("ENGINE__COST", "app_cost"));
		qs2.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", getUserFiltersQs(user)));
		qs2.addRelation("ENGINE", "ENGINEPERMISSION", "inner.join");
		
		List<Map<String, Object>> engineMap = QueryExecutionUtility.flushRsToMap(securityDb, qs2);
		engineMap.addAll(allGlobalEnginesMap);
		return engineMap;
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
		
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
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
		
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}
	
	/**
	 * Get the list of the engine information that the user has access to filter appType
	 * @param user
	 * @param appTypeFilter
	 * @return
	 */
	public static List<Map<String, Object>> getUserDatabaseList(User user, List<String> appTypeFilter) {
		Collection<String> userIds = getUserFiltersQs(user);

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
		if(appTypeFilter != null && !appTypeFilter.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__TYPE", "==", appTypeFilter));
		}
		{
			OrQueryFilter orFilter = new OrQueryFilter();
			orFilter.addFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__GLOBAL", "==", true, PixelDataType.BOOLEAN));
			orFilter.addFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", userIds));
			qs.addExplicitFilter(orFilter);
		}
		{
			SelectQueryStruct subQs = new SelectQueryStruct();
			// store first and fill in sub query after
			qs.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery("ENGINE__ENGINEID", "!=", subQs));
			
			// fill in the sub query with the necessary column output + filters
			subQs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__ENGINEID"));
			subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__VISIBILITY", "==", false, PixelDataType.BOOLEAN));
			subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", userIds));
		}
		// joins
		qs.addRelation("ENGINE", "ENGINEPERMISSION", "left.outer.join");
		qs.addOrderBy(new QueryColumnOrderBySelector("low_app_name"));
		
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
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
		
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}
	
	/**
	 * Get the list of the engine information that the user has access to
	 * @param engineTypeFilter
	 * @return
	 */
	public static List<Map<String, Object>> getAllDatabaseList(List<String> engineTypeFilter) {
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
		if(engineTypeFilter != null && !engineTypeFilter.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__TYPE", "==", engineTypeFilter));
		}
		qs.addOrderBy(new QueryColumnOrderBySelector("low_app_name"));
		
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}
	
	/**
	 * Get user engines + global engines 
	 * @param userId
	 * @return
	 */
	public static List<String> getFullUserEngineIds(User user) {
//		String userFilters = getUserFilters(user);
//		String query = "SELECT DISTINCT ENGINEID FROM ENGINEPERMISSION WHERE USERID IN " + userFilters;
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__ENGINEID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", getUserFiltersQs(user)));
		List<String> engineList = QueryExecutionUtility.flushToListString(securityDb, qs);
		engineList.addAll(getGlobalEngineIds());
		return engineList.stream().distinct().sorted().collect(Collectors.toList());
	}
	
	/**
	 * Get the visual user engines
	 * @param userId
	 * @return
	 */
	public static List<String> getVisibleUserEngineIds(User user) {
		Collection<String> userIds = getUserFiltersQs(user);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID"));
		{
			OrQueryFilter orFilter = new OrQueryFilter();
			orFilter.addFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__GLOBAL", "==", true, PixelDataType.BOOLEAN));
			orFilter.addFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", userIds));
			qs.addExplicitFilter(orFilter);
		}
		{
			SelectQueryStruct subQs = new SelectQueryStruct();
			// store first and fill in sub query after
			qs.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery("ENGINE__ENGINEID", "!=", subQs));
			
			// fill in the sub query with the necessary column output + filters
			subQs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__ENGINEID"));
			subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__VISIBILITY", "==", false, PixelDataType.BOOLEAN));
			subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", userIds));
		}
		// joins
		qs.addRelation("ENGINE", "ENGINEPERMISSION", "left.outer.join");
		return QueryExecutionUtility.flushToListString(securityDb, qs);
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
		return QueryExecutionUtility.flushToSetString(securityDb, qs, false);
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
			
			users = QueryExecutionUtility.flushRsToMap(securityDb, qs);
			
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
		qs.addSelector(new QueryColumnSelector("USER__EMAIL", "email"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__ENGINEID", "==", engineId));
		qs.addRelation("USER", "ENGINEPERMISSION", "inner.join");
		qs.addRelation("ENGINEPERMISSION", "PERMISSION", "inner.join");
		
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
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
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHT__GLOBAL", "==", true, PixelDataType.BOOLEAN));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if(wrapper.hasNext()) {
				// i already bound that global must be true
				return true;
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
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
		
		SemossDate date = null;
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if(wrapper.hasNext()) {
				Object[] row = wrapper.next().getValues();
				try {
					date = (SemossDate) row[0];
				} catch(Exception e) {
					// ignore
				}
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
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

		Map<String, Map<String, Object>> userMap = new HashMap<>();
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
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
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
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
		
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
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
		
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
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
		
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			return wrapper.hasNext();
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
		}

		return false;
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
		
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
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
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			return wrapper.hasNext();
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
		}

		return false;
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
		
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			return wrapper.hasNext();
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
		}
		
		return false;
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
		
		List<String[]> ret = QueryExecutionUtility.flushRsToListOfStrArray(securityDb, qs);
		if(!ret.isEmpty()) {
			return Boolean.parseBoolean(ret.get(0)[0]);
		}
		return false;
	}


}
