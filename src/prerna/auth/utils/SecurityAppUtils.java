package prerna.auth.utils;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessPermission;
import prerna.auth.User;
import prerna.ds.util.RdbmsQueryBuilder;
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
import prerna.util.sql.AbstractSqlQueryUtil;

public class SecurityAppUtils extends AbstractSecurityUtils {

	private static final Logger logger = LogManager.getLogger(SecurityAppUtils.class);

	/**
	 * Get what permission the user has for a given app
	 * @param userId
	 * @param engineId
	 * @param insightId
	 * @return
	 */
	public static String getActualUserAppPermission(User user, String engineId) {
//		String userFilters = getUserFilters(user);
//		String query = "SELECT DISTINCT ENGINEPERMISSION.PERMISSION FROM ENGINEPERMISSION "
//				+ "WHERE ENGINEID='" + engineId + "' AND USERID IN " + userFilters;
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
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
		
		// see if engine is public
		if(appIsGlobal(engineId)) {
			return AccessPermission.READ_ONLY.getPermission();
		}
				
		return null;
	}
	
	/**
	 * Get the app permissions for a specific user
	 * @param singleUserId
	 * @param engineId
	 * @return
	 */
	public static Integer getUserAppPermission(String singleUserId, String engineId) {
//		String query = "SELECT DISTINCT ENGINEPERMISSION.PERMISSION FROM ENGINEPERMISSION  "
//				+ "WHERE ENGINEID='" + engineId + "' AND USERID='" + singleUserId + "'";
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
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
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
		}
		
		return null;
	}
	
	/**
	 * See if specific app is global
	 * @return
	 */
	public static boolean appIsGlobal(String engineId) {
//		String query = "SELECT ENGINEID FROM ENGINE WHERE GLOBAL=TRUE and ENGINEID='" + engineId + "'";
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__GLOBAL", "==", true, PixelDataType.BOOLEAN));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINEID", "==", engineId));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if(wrapper.hasNext()) {
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
	 * Determine if the user is the owner of an app
	 * @param userFilters
	 * @param engineId
	 * @return
	 */
	public static boolean userIsOwner(User user, String engineId) {
		return userIsOwner(getUserFiltersQs(user), engineId);
	}
	
	static boolean userIsOwner(Collection<String> userIds, String engineId) {
//		String query = "SELECT DISTINCT ENGINEPERMISSION.PERMISSION FROM ENGINEPERMISSION "
//				+ "WHERE ENGINEID='" + engineId + "' AND USERID IN " + userFilters;
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);

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
	 * Determine if a user can view an engine
	 * @param user
	 * @param engineId
	 * @return
	 */
	public static boolean userCanViewEngine(User user, String engineId) {
//		String userFilters = getUserFilters(user);
//		String query = "SELECT * "
//				+ "FROM ENGINE "
//				+ "LEFT JOIN ENGINEPERMISSION ON ENGINE.ENGINEID=ENGINEPERMISSION.ENGINEID "
//				+ "WHERE ("
//				+ "ENGINE.GLOBAL=TRUE "
//				+ "OR ENGINEPERMISSION.USERID IN " + userFilters + ") AND ENGINE.ENGINEID='" + engineId + "'"
//				;
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
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
	 * @param engineId
	 * @param userId
	 * @return
	 */
	public static boolean userCanEditEngine(User user, String engineId) {
//		String userFilters = getUserFilters(user);
//		String query = "SELECT DISTINCT ENGINEPERMISSION.PERMISSION FROM ENGINEPERMISSION "
//				+ "WHERE ENGINEID='" + engineId + "' AND USERID IN " + userFilters;
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
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
	 * Determine if the user can edit the app
	 * @param userId
	 * @param engineId
	 * @return
	 */
	static int getMaxUserAppPermission(User user, String engineId) {
//		String userFilters = getUserFilters(user);
//		// query the database
//		String query = "SELECT DISTINCT ENGINEPERMISSION.PERMISSION FROM ENGINEPERMISSION "
//				+ "WHERE ENGINEID='" + engineId + "' AND USERID IN " + userFilters + " ORDER BY PERMISSION";
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
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
	
	
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	
	/*
	 * Query for app users
	 */
	
	/**
	 * Retrieve the list of users for a given insight
	 * @param user
	 * @param engineId
	 * @param insightId
	 * @return
	 * @throws IllegalAccessException
	 */
	public static List<Map<String, Object>> getAppUsers(User user, String engineId) throws IllegalAccessException {
		if(!userCanViewEngine(user, engineId)) {
			throw new IllegalArgumentException("The user does not have access to view this app");
		}
		
//		String query = "SELECT USER.ID AS \"id\", "
//				+ "USER.NAME AS \"name\", "
//				+ "PERMISSION.NAME AS \"permission\" "
//				+ "FROM USER "
//				+ "INNER JOIN ENGINEPERMISSION ON (USER.ID = ENGINEPERMISSION.USERID) "
//				+ "INNER JOIN PERMISSION ON (ENGINEPERMISSION.PERMISSION = PERMISSION.ID) "
//				+ "WHERE ENGINEPERMISSION.ENGINEID='" + appId + "';"
//				;
//		
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("USER__ID", "id"));
		qs.addSelector(new QueryColumnSelector("USER__NAME", "name"));
		qs.addSelector(new QueryColumnSelector("PERMISSION__NAME", "permission"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__ENGINEID", "==", engineId));
		qs.addRelation("USER", "ENGINEPERMISSION", "inner.join");
		qs.addRelation("ENGINEPERMISSION", "PERMISSION", "inner.join");
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}
	
	
	/**
	 * 
	 * @param user
	 * @param newUserId
	 * @param engineId
	 * @param permission
	 * @return
	 * @throws IllegalAccessException 
	 */
	public static void addAppUser(User user, String newUserId, String engineId, String permission) throws IllegalAccessException {
		if(!userCanEditEngine(user, engineId)) {
			throw new IllegalAccessException("Insufficient privileges to modify this app's permissions.");
		}
		
		// make sure user doesn't already exist for this insight
		if(getUserAppPermission(newUserId, engineId) != null) {
			// that means there is already a value
			throw new IllegalArgumentException("This user already has access to this app. Please edit the existing permission level.");
		}
		
		String query = "INSERT INTO ENGINEPERMISSION (USERID, ENGINEID, VISIBILITY, PERMISSION) VALUES('"
				+ RdbmsQueryBuilder.escapeForSQLStatement(newUserId) + "', '"
				+ RdbmsQueryBuilder.escapeForSQLStatement(engineId) + "', "
				+ "TRUE, "
				+ AccessPermission.getIdByPermission(permission) + ");";

		try {
			securityDb.insertData(query);
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occured adding user permissions for this APP");
		}
	}
	
	/**
	 * 
	 * @param user
	 * @param existingUserId
	 * @param engineId
	 * @param newPermission
	 * @return
	 * @throws IllegalAccessException 
	 */
	public static void editAppUserPermission(User user, String existingUserId, String engineId, String newPermission) throws IllegalAccessException {
		// make sure user can edit the app
		int userPermissionLvl = getMaxUserAppPermission(user, engineId);
		if(!AccessPermission.isEditor(userPermissionLvl)) {
			throw new IllegalAccessException("Insufficient privileges to modify this app's permissions.");
		}
		
		// make sure we are trying to edit a permission that exists
		Integer existingUserPermission = getUserAppPermission(existingUserId, engineId);
		if(existingUserPermission == null) {
			throw new IllegalArgumentException("Attempting to modify app permission for a user who does not currently have access to the app");
		}
		
		int newPermissionLvl = AccessPermission.getIdByPermission(newPermission);
		
		// if i am not an owner
		// then i need to check if i can edit this users permission
		if(!AccessPermission.isOwner(userPermissionLvl)) {
			// not an owner, check if trying to edit an owner or an editor/reader
			// get the current permission
			if(AccessPermission.OWNER.getId() == existingUserPermission) {
				throw new IllegalAccessException("The user doesn't have the high enough permissions to modify this users app permission.");
			}
			
			// also, cannot give some owner permission if i am just an editor
			if(AccessPermission.OWNER.getId() == newPermissionLvl) {
				throw new IllegalAccessException("Cannot give owner level access to this insight since you are not currently an owner.");
			}
		}
		
		String query = "UPDATE ENGINEPERMISSION SET PERMISSION=" + newPermissionLvl
				+ " WHERE USERID='" + RdbmsQueryBuilder.escapeForSQLStatement(existingUserId) + "' "
				+ "AND ENGINEID='"	+ RdbmsQueryBuilder.escapeForSQLStatement(engineId) + "';";
		try {
			securityDb.insertData(query);
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occured updating the user permissions for this insight");
		}
	}
	
	/**
	 * 
	 * @param user
	 * @param editedUserId
	 * @param engineId
	 * @return
	 * @throws IllegalAccessException 
	 */
	public static void removeAppUser(User user, String existingUserId, String engineId) throws IllegalAccessException {
		// make sure user can edit the app
		int userPermissionLvl = getMaxUserAppPermission(user, engineId);
		if(!AccessPermission.isEditor(userPermissionLvl)) {
			throw new IllegalAccessException("Insufficient privileges to modify this app's permissions.");
		}
		
		// make sure we are trying to edit a permission that exists
		Integer existingUserPermission = getUserAppPermission(existingUserId, engineId);
		if(existingUserPermission == null) {
			throw new IllegalArgumentException("Attempting to modify user permission for a user who does not currently have access to the app");
		}
		
		// if i am not an owner
		// then i need to check if i can remove this users permission
		if(!AccessPermission.isOwner(userPermissionLvl)) {
			// not an owner, check if trying to edit an owner or an editor/reader
			// get the current permission
			if(AccessPermission.OWNER.getId() == existingUserPermission) {
				throw new IllegalAccessException("The user doesn't have the high enough permissions to modify this users app permission.");
			}
		}
		
		String query = "DELETE FROM ENGINEPERMISSION WHERE "
				+ "USERID='" + RdbmsQueryBuilder.escapeForSQLStatement(existingUserId) + "' "
				+ "AND ENGINEID='"	+ RdbmsQueryBuilder.escapeForSQLStatement(engineId) + "';";
		try {
			securityDb.insertData(query);
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occured removing the user permissions for this app");
		}
		
		// need to also delete all insight permissions for this app
		query = "DELETE FROM USERINSIGHTPERMISSION WHERE "
				+ "USERID='" + RdbmsQueryBuilder.escapeForSQLStatement(existingUserId) + "' "
				+ "AND ENGINEID='"	+ RdbmsQueryBuilder.escapeForSQLStatement(engineId) + "';";
		try {
			securityDb.insertData(query);
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occured removing the user permissions for the insights of this app");
		}
	}
	
	/**
	 * Set if the database is public to all users on this instance
	 * @param user
	 * @param appId
	 * @param isPublic
	 * @return
	 * @throws IllegalAccessException 
	 */
	public static boolean setAppGlobal(User user, String appId, boolean isPublic) throws IllegalAccessException {
		if(!SecurityAppUtils.userIsOwner(user, appId)) {
			throw new IllegalAccessException("The user doesn't have the permission to set this database as global. Only the owner or an admin can perform this action.");
		}
		appId = RdbmsQueryBuilder.escapeForSQLStatement(appId);
		String query = "UPDATE ENGINE SET GLOBAL = " + isPublic + " WHERE ENGINEID ='" + appId + "';";
		securityDb.execUpdateAndRetrieveStatement(query, true);
		securityDb.commit();
		return true;
	}
	
	/**
	 * update the app name
	 * @param user
	 * @param engineId
	 * @param isPublic
	 * @return
	 */
	public static boolean setAppName(User user, String appId, String newAppName) {
		if(!SecurityAppUtils.userIsOwner(user, appId)) {
			throw new IllegalArgumentException("The user doesn't have the permission to change the database name. Only the owner or an admin can perform this action.");
		}
		newAppName = RdbmsQueryBuilder.escapeForSQLStatement(newAppName);
		appId = RdbmsQueryBuilder.escapeForSQLStatement(appId);
		String query = "UPDATE ENGINE SET ENGINENAME = '" + newAppName + "' WHERE ENGINEID ='" + appId + "';";
		securityDb.execUpdateAndRetrieveStatement(query, true);
		securityDb.commit();
		return true;
	}
	
	//////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////

	/*
	 * App Metadata
	 */
	
	/**
	 * Update the app description
	 * Will perform an insert if the description doesn't currently exist
	 * @param engineId
	 * @param insideId
	 */
	public static void updateAppDescription(String engineId, String description) {
		// try to do an update
		// if nothing is updated
		// do an insert
		engineId = RdbmsQueryBuilder.escapeForSQLStatement(engineId);
		String query = "UPDATE ENGINEMETA SET METAVALUE='" 
				+ AbstractSqlQueryUtil.escapeForSQLStatement(description) + "' "
				+ "WHERE METAKEY='description' AND ENGINEID='" + engineId + "'";
		Statement stmt = null;
		try {
			stmt = securityDb.execUpdateAndRetrieveStatement(query, false);
			if(stmt.getUpdateCount() <= 0) {
				// need to perform an insert
				query = securityDb.getQueryUtil().insertIntoTable("ENGINEMETA", 
						new String[]{"ENGINEID", "METAKEY", "METAVALUE", "METAORDER"}, 
						new String[]{"varchar(255)", "varchar(255)", "clob", "int"}, 
						new Object[]{engineId, "description", description, 0});
				securityDb.insertData(query);
			}
		} catch(SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}
	
	/**
	 * Update the app tags
	 * Will delete existing values and then perform a bulk insert
	 * @param engineId
	 * @param insightId
	 * @param tags
	 */
	public static void updateAppTags(String engineId, List<String> tags) {
		// first do a delete
		String query = "DELETE FROM ENGINEMETA WHERE METAKEY='tag' AND ENGINEID='" + engineId + "'";
		try {
			securityDb.insertData(query);
			securityDb.commit();
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		}
		
		// now we do the new insert with the order of the tags
		query = securityDb.getQueryUtil().createInsertPreparedStatementString("ENGINEMETA", 
				new String[]{"ENGINEID", "METAKEY", "METAVALUE", "METAORDER"});
		PreparedStatement ps = securityDb.bulkInsertPreparedStatement(query);
		try {
			for(int i = 0; i < tags.size(); i++) {
				String tag = tags.get(i);
				ps.setString(1, engineId);
				ps.setString(2, "tag");
				ps.setString(3, tag);
				ps.setInt(4, i);
				ps.addBatch();;
			}
			
			ps.executeBatch();
		} catch(Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}
	
	/**
	 * Get the wrapper for additional app metadata
	 * @param engineId
	 * @param metaKeys
	 * @return
	 * @throws Exception 
	 */
	public static IRawSelectWrapper getAppMetadataWrapper(Collection<String> engineId, List<String> metaKeys) throws Exception {
		SelectQueryStruct qs = new SelectQueryStruct();
		// selectors
		qs.addSelector(new QueryColumnSelector("ENGINEMETA__ENGINEID"));
		qs.addSelector(new QueryColumnSelector("ENGINEMETA__METAKEY"));
		qs.addSelector(new QueryColumnSelector("ENGINEMETA__METAVALUE"));
		qs.addSelector(new QueryColumnSelector("ENGINEMETA__METAORDER"));
		// filters
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEMETA__ENGINEID", "==", engineId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEMETA__METAKEY", "==", metaKeys));
		// order
		qs.addSelector(new QueryColumnSelector("ENGINEMETA__METAORDER"));
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
		return wrapper;
	}
	
	/**
	 * Get the metadata for a specific app
	 * @param engineId
	 * @return
	 */
	public static Map<String, Object> getAggregateAppMetadata(String engineId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINEMETA__METAKEY"));
		qs.addSelector(new QueryColumnSelector("ENGINEMETA__METAVALUE"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEMETA__ENGINEID", "==", engineId));
		
		Map<String, Object> retMap = new HashMap<String, Object>();

		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			while(wrapper.hasNext()) {
				Object[] data = wrapper.next().getValues();
				String metaKey = data[0].toString();
				String metaValue = AbstractSqlQueryUtil.flushClobToString((java.sql.Clob) data[1]);

				// AS THIS LIST EXPANDS
				// WE NEED TO KNOW IF THESE ARE MULTI VALUED OR SINGLE
				if(metaKey.equals("tag")) {
					List<String> listVal = null;
					if(retMap.containsKey("tags")) {
						listVal = (List<String>) retMap.get("tags");
					} else {
						listVal = new Vector<String>();
						retMap.put("tags", listVal);
					}
					listVal.add(metaValue);
				}
				// these will be the single valued parameters
				else {
					retMap.put(metaKey, metaValue);
				}
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
	 * Check if the user has access to the app
	 * @param engineId
	 * @param userId
	 * @return
	 * @throws Exception
	 */
	public static boolean checkUserHasAccessToApp(String engineId, String userId) throws Exception {
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
			logger.error(Constants.STACKTRACE, e);
			throw e;
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
		}
	}
	
	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////

	/*
	 * Copying permissions
	 */
	
	/**
	 * Copy the app permissions from one app to another
	 * @param sourceEngineId
	 * @param targetEngineId
	 * @throws SQLException
	 */
	public static void copyAppPermissions(String sourceEngineId, String targetEngineId) throws Exception {
		String insertTargetAppPermissionSql = "INSERT INTO ENGINEPERMISSION (ENGINEID, USERID, PERMISSION, VISIBILITY) VALUES (?, ?, ?, ?)";
		PreparedStatement insertTargetAppPermissionStatement = securityDb.bulkInsertPreparedStatement(insertTargetAppPermissionSql);
		if(insertTargetAppPermissionStatement == null) {
			throw new IllegalArgumentException("An error occured trying to generate the appropriate query to copy the data");
		}
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__ENGINEID"));
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__USERID"));
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__PERMISSION"));
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__VISIBILITY"));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			while(wrapper.hasNext()) {
				Object[] row = wrapper.next().getValues();
				
				insertTargetAppPermissionStatement.setString(1, targetEngineId);
				insertTargetAppPermissionStatement.setString(2, (String) row[1]);
				insertTargetAppPermissionStatement.setInt(3, ((Number) row[2]).intValue() );
				insertTargetAppPermissionStatement.setBoolean(4, (Boolean) row[3]);
				// add to batch
				insertTargetAppPermissionStatement.addBatch();
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
			throw e;
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
		}
		
		// first delete the current app permissions on the database
		String deleteTargetAppPermissionsSql = "DELETE FROM ENGINEPERMISSION WHERE ENGINEID = '" + AbstractSqlQueryUtil.escapeForSQLStatement(targetEngineId) + "'";
		securityDb.removeData(deleteTargetAppPermissionsSql);
		// execute the query
		insertTargetAppPermissionStatement.executeBatch();
	}
}
