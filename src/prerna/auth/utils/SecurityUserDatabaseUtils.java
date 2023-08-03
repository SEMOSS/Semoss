package prerna.auth.utils;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessPermissionEnum;
import prerna.auth.User;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.PixelDataType;
import prerna.util.Constants;
import prerna.util.QueryExecutionUtility;

class SecurityUserDatabaseUtils extends AbstractSecurityUtils {

	private static final Logger logger = LogManager.getLogger(SecurityUserDatabaseUtils.class);

	/**
	 * Get what permission the user has for a given database
	 * @param userId
	 * @param databaseId
	 * @param insightId
	 * @return
	 */
	public static String getActualUserDatabasePermission(User user, String databaseId) {
//		String userFilters = getUserFilters(user);
//		String query = "SELECT DISTINCT ENGINEPERMISSION.PERMISSION FROM ENGINEPERMISSION "
//				+ "WHERE ENGINEID='" + databaseId + "' AND USERID IN " + userFilters;
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__ENGINEID", "==", databaseId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", getUserFiltersQs(user)));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			while(wrapper.hasNext()) {
				Object val = wrapper.next().getValues()[0];
				if(val != null) {
					int permission = ((Number) val).intValue();
					return AccessPermissionEnum.getPermissionValueById(permission);
				}
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
		}
		
		// see if database is public
		if(SecurityEngineUtils.databaseIsGlobal(databaseId)) {
			return AccessPermissionEnum.READ_ONLY.getPermission();
		}
		
		return null;
	}
	
	/**
	 * Get the database permissions for a specific user
	 * @param singleUserId
	 * @param databaseId
	 * @return
	 */
	public static Integer getUserDatabasePermission(String singleUserId, String databaseId) {
//		String query = "SELECT DISTINCT ENGINEPERMISSION.PERMISSION FROM ENGINEPERMISSION  "
//				+ "WHERE ENGINEID='" + databaseId + "' AND USERID='" + singleUserId + "'";
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__ENGINEID", "==", databaseId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", singleUserId));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if(wrapper.hasNext()) {
				Object val = wrapper.next().getValues()[0];
				if(val != null && val instanceof Number) {
					return ((Number) val).intValue();
				}
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
		}
		
		return null;
	}
	
	/**
	 * Determine if the user is the owner of an database
	 * @param userFilters
	 * @param databaseId
	 * @return
	 */
	public static boolean userIsOwner(User user, String databaseId) {
		return userIsOwner(getUserFiltersQs(user), databaseId);
	}
	
	static boolean userIsOwner(Collection<String> userIds, String databaseId) {
//		String query = "SELECT DISTINCT ENGINEPERMISSION.PERMISSION FROM ENGINEPERMISSION "
//				+ "WHERE ENGINEID='" + databaseId + "' AND USERID IN " + userFilters;
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__ENGINEID", "==", databaseId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", userIds));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			while(wrapper.hasNext()) {
				Object val = wrapper.next().getValues()[0];
				if(val == null) {
					return false;
				}
				int permission = ((Number) val).intValue();
				if(AccessPermissionEnum.isOwner(permission)) {
					return true;
				}
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
	
	/**
	 * Determine if a user can view a database
	 * @param user
	 * @param databaseId
	 * @return
	 */
	public static boolean userCanViewEngine(User user, String databaseId) {
//		String userFilters = getUserFilters(user);
//		String query = "SELECT * "
//				+ "FROM ENGINE "
//				+ "LEFT JOIN ENGINEPERMISSION ON ENGINE.ENGINEID=ENGINEPERMISSION.ENGINEID "
//				+ "WHERE ("
//				+ "ENGINE.GLOBAL=TRUE "
//				+ "OR ENGINEPERMISSION.USERID IN " + userFilters + ") AND ENGINE.ENGINEID='" + databaseId + "'"
//				;
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID"));
		OrQueryFilter orFilter = new OrQueryFilter();
		orFilter.addFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__GLOBAL", "==", true, PixelDataType.BOOLEAN));
		orFilter.addFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", getUserFiltersQs(user)));
		qs.addExplicitFilter(orFilter);
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINEID", "==", databaseId));
		qs.addRelation("ENGINE", "ENGINEPERMISSION", "left.outer.join");
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if(wrapper.hasNext()) {
				// if you are here, you can view
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
	
	/**
	 * Determine if the user can modify the database
	 * @param databaseId
	 * @param userId
	 * @return
	 */
	public static boolean userCanEditEngine(User user, String databaseId) {
//		String userFilters = getUserFilters(user);
//		String query = "SELECT DISTINCT ENGINEPERMISSION.PERMISSION FROM ENGINEPERMISSION "
//				+ "WHERE ENGINEID='" + databaseId + "' AND USERID IN " + userFilters;
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__ENGINEID", "==", databaseId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", getUserFiltersQs(user)));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			while(wrapper.hasNext()) {
				Object val = wrapper.next().getValues()[0];
				if(val == null) {
					return false;
				}
				int permission = ((Number) val).intValue();
				if(AccessPermissionEnum.isEditor(permission)) {
					return true;
				}
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
	
	/**
	 * Determine if the user can edit the database
	 * @param userId
	 * @param databaseId
	 * @return
	 */
	static int getMaxUserDatabasePermission(User user, String databaseId) {
//		String userFilters = getUserFilters(user);
//		// query the database
//		String query = "SELECT DISTINCT ENGINEPERMISSION.PERMISSION FROM ENGINEPERMISSION "
//				+ "WHERE ENGINEID='" + databaseId + "' AND USERID IN " + userFilters + " ORDER BY PERMISSION";
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__ENGINEID", "==", databaseId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", getUserFiltersQs(user)));
		qs.addOrderBy(new QueryColumnOrderBySelector("ENGINEPERMISSION__PERMISSION"));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			while(wrapper.hasNext()) {
				Object val = wrapper.next().getValues()[0];
				if(val == null) {
					return AccessPermissionEnum.READ_ONLY.getId();
				}
				int permission = ((Number) val).intValue();
				return permission;
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
		}		
		return AccessPermissionEnum.READ_ONLY.getId();
	}
	
	/**
	 * Check if the user has access to the database
	 * @param databaseId
	 * @param userId
	 * @return
	 * @throws Exception
	 */
	public static boolean checkUserHasAccessToDatabase(String databaseId, String userId) throws Exception {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__ENGINEID"));
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__USERID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__ENGINEID", "==", databaseId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", userId));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			return wrapper.hasNext();
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
			throw e;
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
		}
	} 	
	
	/**
	 * Get the database permissions for a specific user
	 * @param singleUserId
	 * @param databaseId
	 * @return
	 */
	public static Map<String, Integer> getUserDatabasePermissions(List<String> userIds, String databaseId) {
		Map<String, Integer> retMap = new HashMap<String, Integer>();
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = getUserDatabasePermissionsWrapper(userIds, databaseId);
			while(wrapper.hasNext()) {
				Object[] data = wrapper.next().getValues();
				String userId = (String) data[0];
				Integer permission = (Integer) data[1];
				retMap.put(userId, permission);
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
		}
		return retMap;
	}
	
	/**
	 * Get the database permissions for a specific user
	 * @param singleUserId
	 * @param databaseId
	 * @return
	 */
	public static IRawSelectWrapper getUserDatabasePermissionsWrapper(List<String> userIds, String databaseId) throws Exception {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__USERID"));
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__ENGINEID", "==", databaseId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", userIds));
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
		return wrapper;
	}
	
	/**
	 * 
	 * @param databaseId
	 * @return
	 */
	public static List<Map<String, Object>> getDisplayDatabaseOwnersAndEditors(String databaseId) {
		List<Map<String, Object>> users = null;
		if(SecurityEngineUtils.getGlobalDatabaseIds().contains(databaseId)) {
//			String query = "SELECT DISTINCT "
//					+ "SMSS_USER.NAME AS \"name\", "
//					+ "PERMISSION.NAME as \"permission\" "
//					+ "FROM SMSS_USER "
//					+ "INNER JOIN ENGINEPERMISSION ON USER.ID=ENGINEPERMISSION.USERID "
//					+ "INNER JOIN PERMISSION ON ENGINEPERMISSION.PERMISSION=PERMISSION.ID "
//					+ "WHERE PERMISSION.ID IN (1,2) AND ENGINEPERMISSION.ENGINEID='" + engineId + "'";
//			IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
						
			SelectQueryStruct qs = new SelectQueryStruct();
			qs.addSelector(new QueryColumnSelector("SMSS_USER__NAME", "name"));
			qs.addSelector(new QueryColumnSelector("PERMISSION__NAME", "permission"));
			List<Integer> permissionValues = new Vector<Integer>(2);
			permissionValues.add(new Integer(1));
			permissionValues.add(new Integer(2));
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PERMISSION__ID", "==", permissionValues, PixelDataType.CONST_INT));
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__ENGINEID", "==", databaseId));
			qs.addRelation("SMSS_USER", "ENGINEPERMISSION", "inner.join");
			qs.addRelation("ENGINEPERMISSION", "PERMISSION", "inner.join");
			
			users = QueryExecutionUtility.flushRsToMap(securityDb, qs);
			
			Map<String, Object> globalMap = new HashMap<String, Object>();
			globalMap.put("name", "PUBLIC DATABASE");
			globalMap.put("permission", "READ_ONLY");
			users.add(globalMap);
		} else {
//			String query = "SELECT DISTINCT "
//					+ "SMSS_USER.NAME AS \"name\", "
//					+ "PERMISSION.NAME as \"permission\" "
//					+ "FROM SMSS_USER "
//					+ "INNER JOIN ENGINEPERMISSION ON USER.ID=ENGINEPERMISSION.USERID "
//					+ "INNER JOIN PERMISSION ON ENGINEPERMISSION.PERMISSION=PERMISSION.ID "
//					+ "WHERE ENGINEPERMISSION.ENGINEID='" + engineId + "'";
//			IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
			
			users = getFullDatabaseOwnersAndEditors(databaseId);
		}
		return users;
	}
	
	/**
	 * 
	 * @param databaseId
	 * @return
	 */
	public static List<Map<String, Object>> getFullDatabaseOwnersAndEditors(String databaseId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__ID", "id"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__NAME", "name"));
		qs.addSelector(new QueryColumnSelector("PERMISSION__NAME", "permission"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__EMAIL", "email"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__ENGINEID", "==", databaseId));
		qs.addRelation("SMSS_USER", "ENGINEPERMISSION", "inner.join");
		qs.addRelation("ENGINEPERMISSION", "PERMISSION", "inner.join");
		qs.addOrderBy(new QueryColumnOrderBySelector("SMSS_USER__ID"));
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}
	
	/**
	 * 
	 * @param databaseId
	 * @param userId
	 * @param permission
	 * @param limit
	 * @param offset
	 * @return
	 */
	public static List<Map<String, Object>> getFullDatabaseOwnersAndEditors(String databaseId, String userId, String permission, long limit, long offset) {
		boolean hasUserId = userId != null && !(userId=userId.trim()).isEmpty();
		boolean hasPermission = permission != null && !(permission=permission.trim()).isEmpty();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__ID", "id"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__NAME", "name"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__EMAIL", "email"));
		qs.addSelector(new QueryColumnSelector("PERMISSION__NAME", "permission"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__ENGINEID", "==", databaseId));
		if (hasUserId) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "?like", userId));
		}
		if (hasPermission) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__PERMISSION", "==", AccessPermissionEnum.getIdByPermission(permission)));
		}
		qs.addRelation("SMSS_USER", "ENGINEPERMISSION", "inner.join");
		qs.addRelation("ENGINEPERMISSION", "PERMISSION", "inner.join");
		qs.addOrderBy(new QueryColumnOrderBySelector("PERMISSION__ID"));
		qs.addOrderBy(new QueryColumnOrderBySelector("SMSS_USER__ID"));
		if(limit > 0) {
			qs.setLimit(limit);
		}
		if(offset > 0) {
			qs.setOffSet(offset);
		}
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}
	
}
