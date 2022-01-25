package prerna.auth.utils;

import java.util.Collection;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessPermission;
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

class SecurityUserProjectUtils extends AbstractSecurityUtils {

	private static final Logger logger = LogManager.getLogger(SecurityUserProjectUtils.class);

	
	/**
	 * Get user databases + global databases 
	 * @param userId
	 * @return
	 */
	public static List<String> getFullUserProjectIds(User user) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__PROJECTID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", getUserFiltersQs(user)));
		List<String> databaseList = QueryExecutionUtility.flushToListString(securityDb, qs);
		return databaseList;
	}
	
	/**
	 * Get what permission the user has for a given app
	 * @param userId
	 * @param projectId
	 * @param insightId
	 * @return
	 */
	public static String getActualUserProjectPermission(User user, String projectId) {
		//		String userFilters = getUserFilters(user);
		//		String query = "SELECT DISTINCT ENGINEPERMISSION.PERMISSION FROM ENGINEPERMISSION "
		//				+ "WHERE ENGINEID='" + engineId + "' AND USERID IN " + userFilters;
		//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", getUserFiltersQs(user)));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			while(wrapper.hasNext()) {
				Object val = wrapper.next().getValues()[0];
				if(val != null) {
					int permission = ((Number) val).intValue();
					return AccessPermission.getPermissionValueById(permission);
				}
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
		}

		// see if project is public
		if(SecurityProjectUtils.projectIsGlobal(projectId)) {
			return AccessPermission.READ_ONLY.getPermission();
		}

		return null;
	}
	
	/**
	 * Get the project permissions for a specific user
	 * @param singleUserId
	 * @param projectId
	 * @return
	 */
	public static Integer getUserProjectPermission(String singleUserId, String projectId) {
		//		String query = "SELECT DISTINCT ENGINEPERMISSION.PERMISSION FROM ENGINEPERMISSION  "
		//				+ "WHERE ENGINEID='" + engineId + "' AND USERID='" + singleUserId + "'";
		//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", singleUserId));
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
	 * Determine if the user is the owner of a project
	 * @param userFilters
	 * @param engineId
	 * @return
	 */
	public static boolean userIsOwner(User user, String projectId) {
		return userIsOwner(getUserFiltersQs(user), projectId);
	}

	static boolean userIsOwner(Collection<String> userIds, String projectId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", userIds));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
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
	 * Determine if a user can view a project
	 * @param user
	 * @param projectId
	 * @return
	 */
	public static boolean userCanViewProject(User user, String projectId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTID"));
		OrQueryFilter orFilter = new OrQueryFilter();
		orFilter.addFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__GLOBAL", "==", true, PixelDataType.BOOLEAN));
		orFilter.addFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", getUserFiltersQs(user)));
		qs.addExplicitFilter(orFilter);
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__PROJECTID", "==", projectId));
		qs.addRelation("PROJECT", "PROJECTPERMISSION", "left.outer.join");
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
	 * @param projectId
	 * @param userId
	 * @return
	 */
	public static boolean userCanEditProject(User user, String projectId) {

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", getUserFiltersQs(user)));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
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
	 * Get Project max permission for a user
	 * @param userId
	 * @param projectId
	 * @return
	 */
	static int getMaxUserProjectPermission(User user, String projectId) {
		//		String userFilters = getUserFilters(user);
		//		// query the database
		//		String query = "SELECT DISTINCT ENGINEPERMISSION.PERMISSION FROM ENGINEPERMISSION "
		//				+ "WHERE ENGINEID='" + engineId + "' AND USERID IN " + userFilters + " ORDER BY PERMISSION";
		//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", getUserFiltersQs(user)));
		qs.addOrderBy(new QueryColumnOrderBySelector("PROJECTPERMISSION__PERMISSION"));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			while(wrapper.hasNext()) {
				Object val = wrapper.next().getValues()[0];
				if(val == null) {
					return AccessPermission.READ_ONLY.getId();
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
		return AccessPermission.READ_ONLY.getId();
	}

	/**
	 * Check if the user has access to the project
	 * @param projectId
	 * @param userId
	 * @return
	 * @throws Exception
	 */
	public static boolean checkUserHasAccessToProject(String projectId, String userId) throws Exception {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__PROJECTID"));
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__USERID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", userId));
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
}
