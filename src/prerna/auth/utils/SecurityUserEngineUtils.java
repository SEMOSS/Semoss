package prerna.auth.utils;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;



import prerna.auth.AccessPermissionEnum;
import prerna.auth.User;
import prerna.date.SemossDate;
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
import prerna.util.Utility;



class SecurityUserEngineUtils extends AbstractSecurityUtils {

	private static final Logger classLogger = LogManager.getLogger(SecurityUserEngineUtils.class);

	/**
	 * Get what permission the user has for a given engine
	 * @param userId
	 * @param engineId
	 * @return
	 */
	public static String getActualUserEnginePermission(User user, String engineId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__ENGINEID", "==", engineId));
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
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		// see if engine is public
		if(SecurityEngineUtils.engineIsGlobal(engineId)) {
			return AccessPermissionEnum.READ_ONLY.getPermission();
		}
		
		return null;
	}
	
	/**
	 * Get the engine permissions for a specific user
	 * @param singleUserId
	 * @param engineId
	 * @return
	 */
	public static Integer getUserEnginePermission(String singleUserId, String engineId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__ENGINEID", "==", engineId));
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
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		return null;
	}
	
	/**
	 * Get the engine permissions for a specific user
	 * @param user
	 * @param engineId
	 * @return
	 */
	public static Integer getUserEnginePermission(User user, String engineId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__ENGINEID", "==", engineId));
		// TODO: account for different logins with different levels of access
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", getUserFiltersQs(user)));
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
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		return null;
	}
	
	/**
	 * Determine if the user is the owner of an engine
	 * @param userFilters
	 * @param engineId
	 * @return
	 */
	public static boolean userIsOwner(User user, String engineId) {
		try {
			boolean isExpired = enginePermissionIsExpired(User.getSingleLogginName(user), engineId);
			// If permission is expired remove permission
			if (isExpired) {
				SecurityEngineUtils.removeExpiredEngineUser(User.getSingleLogginName(user), engineId);
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		
		return userIsOwner(getUserFiltersQs(user), engineId);
	}
	
	static boolean userIsOwner(Collection<String> userIds, String engineId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__ENGINEID", "==", engineId));
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
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		return false;
	}
	
	/**
	 * Determine if a user can view a engine
	 * @param user
	 * @param engineId
	 * @return
	 */
	public static boolean userCanViewEngine(User user, String engineId) {
		// Check to see if permission has expired
		try {
			boolean isExpired = enginePermissionIsExpired(Utility.inputSanitizer(User.getSingleLogginName(user)), engineId);
			// If permission is expired remove permission
			if (isExpired) {
				SecurityEngineUtils.removeExpiredEngineUser(Utility.inputSanitizer(User.getSingleLogginName(user)), engineId);
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID"));
		OrQueryFilter orFilter = new OrQueryFilter();
		orFilter.addFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__GLOBAL", "==", true, PixelDataType.BOOLEAN));
		orFilter.addFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", getUserFiltersQs(user)));
		qs.addExplicitFilter(orFilter);
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINEID", "==", engineId));
		qs.addRelation("ENGINE", "ENGINEPERMISSION", "left.outer.join");
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if(wrapper.hasNext()) {
				// if you are here, you can view
				return true;
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		return false;
	}
	
	/**
	 * Determine if the user can modify the engine
	 * @param engineId
	 * @param userId
	 * @return
	 */
	public static boolean userCanEditEngine(User user, String engineId) {
		// Check to see if permission has expired
		try {
			boolean isExpired = enginePermissionIsExpired(User.getSingleLogginName(user), engineId);
			// If permission is expired remove permission
			if (isExpired) {
				SecurityEngineUtils.removeExpiredEngineUser(User.getSingleLogginName(user), engineId);
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__ENGINEID", "==", engineId));
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
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		return false;
	}
	
	/**
	 * Check if permission to engine has expired
	 * @param engineId
	 * @param userId
	 */
	public static boolean enginePermissionIsExpired(String userId, String engineId) throws Exception {
		LocalDateTime currentTime = LocalDateTime.now();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__ENDDATE"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__ENGINEID", "==", engineId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", userId));
		
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if (wrapper.hasNext()) {
				SemossDate endDate = (SemossDate) wrapper.next().getValues()[0];
				if (endDate == null) {
					return false;
				}
				LocalDateTime formattedEndDate = endDate.getLocalDateTime();
				return formattedEndDate.isBefore(currentTime);
			} else {
				return false;
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}
	
	/**
	 * Determine if the user can edit the engine
	 * @param userId
	 * @param engineId
	 * @return
	 */
	static int getMaxUserEnginePermission(User user, String engineId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__ENGINEID", "==", engineId));
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
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}		
		return AccessPermissionEnum.READ_ONLY.getId();
	}
	
	/**
	 * Check if the user has access to the engine
	 * @param engineId
	 * @param userId
	 * @return
	 * @throws Exception
	 */
	public static boolean checkUserHasAccessToEngine(String engineId, String userId) throws Exception {
		try {
			boolean isExpired = enginePermissionIsExpired(userId, engineId);
			if (isExpired) {
				SecurityProjectUtils.removeExpiredProjectUser(userId, engineId);
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
		}
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__ENGINEID"));
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__USERID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__ENGINEID", "==", engineId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", userId));
		
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			return wrapper.hasNext();
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
	} 	
	
	/**
	 * Get the engine permissions for a specific user
	 * @param singleUserId
	 * @param engineId
	 * @return
	 */
	public static Map<String, Integer> getUserEnginePermissions(List<String> userIds, String engineId) {
		Map<String, Integer> retMap = new HashMap<String, Integer>();
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = getUserEnginePermissionsWrapper(userIds, engineId);
			while(wrapper.hasNext()) {
				Object[] data = wrapper.next().getValues();
				String userId = (String) data[0];
				Integer permission = (Integer) data[1];
				retMap.put(userId, permission);
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		return retMap;
	}
	
	/**
	 * Get the engine permissions for a specific user
	 * @param singleUserId
	 * @param engineId
	 * @return
	 */
	public static IRawSelectWrapper getUserEnginePermissionsWrapper(List<String> userIds, String engineId) throws Exception {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__USERID"));
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__ENGINEID", "==", engineId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", userIds));
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
		return wrapper;
	}
	
	/**
	 * 
	 * @param engineId
	 * @return
	 */
	public static List<Map<String, Object>> getDisplayEngineOwnersAndEditors(String engineId) {
		List<Map<String, Object>> users = null;
		if(SecurityEngineUtils.engineIsGlobal(engineId)) {
			SelectQueryStruct qs = new SelectQueryStruct();
			qs.addSelector(new QueryColumnSelector("SMSS_USER__NAME", "name"));
			qs.addSelector(new QueryColumnSelector("PERMISSION__NAME", "permission"));
			// return the end date of the permission
			qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__ENDDATE", "end_date"));
			// also return who did this and when
			qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__PERMISSIONGRANTEDBY", "permission_granted_by"));
			qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__PERMISSIONGRANTEDBYTYPE", "permission_granted_by_type"));
			qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__DATEADDED", "date_added"));
			// filter to owners and editors
			List<Integer> permissionValues = new Vector<Integer>(2);
			permissionValues.add(new Integer(1));
			permissionValues.add(new Integer(2));
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PERMISSION__ID", "==", permissionValues, PixelDataType.CONST_INT));
			// filter to the engine
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__ENGINEID", "==", engineId));
			qs.addRelation("SMSS_USER", "ENGINEPERMISSION", "inner.join");
			qs.addRelation("ENGINEPERMISSION", "PERMISSION", "inner.join");
			
			users = QueryExecutionUtility.flushRsToMap(securityDb, qs);
			
			// since global just say all global
			Map<String, Object> globalMap = new HashMap<String, Object>();
			globalMap.put("name", "PUBLIC DATABASE");
			globalMap.put("permission", "READ_ONLY");
			users.add(globalMap);
		} else {
			users = getEngineUsers(engineId, null, null, -1, -1);
		}
		return users;
	}
	
	/**
	 * 
	 * @param engineId
	 * @param userId
	 * @param permission
	 * @param limit
	 * @param offset
	 * @return
	 */
	public static List<Map<String, Object>> getEngineUsers(String engineId, String searchParam, String permission, long limit, long offset) {
		boolean hasSearchParam = searchParam != null && !(searchParam=searchParam.trim()).isEmpty();
		boolean hasPermission = permission != null && !(permission=permission.trim()).isEmpty();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__ID", "id"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__TYPE", "type"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__NAME", "name"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__EMAIL", "email"));
		qs.addSelector(new QueryColumnSelector("PERMISSION__NAME", "permission"));
		// return the end date of the permission
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__ENDDATE", "end_date"));
		// also return who did this and when
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__PERMISSIONGRANTEDBY", "permission_granted_by"));
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__PERMISSIONGRANTEDBYTYPE", "permission_granted_by_type"));
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__DATEADDED", "date_added"));
		// filter to the engine
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__ENGINEID", "==", engineId));
		if (hasSearchParam) {
			OrQueryFilter or = new OrQueryFilter();
			or.addFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "?like", searchParam));
			or.addFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__NAME", "?like", searchParam));
			or.addFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__USERNAME", "?like", searchParam));
			or.addFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__EMAIL", "?like", searchParam));
			qs.addExplicitFilter(or);
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
