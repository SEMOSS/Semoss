package prerna.auth.utils;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.Vector;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessPermissionEnum;
import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.ds.util.RdbmsQueryBuilder;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.joins.IRelation;
import prerna.query.querystruct.joins.SubqueryRelationship;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.query.querystruct.selectors.QueryIfSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.PixelDataType;
import prerna.util.Constants;
import prerna.util.QueryExecutionUtility;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

public class SecurityDatabaseUtils extends AbstractSecurityUtils {

	private static final Logger logger = LogManager.getLogger(SecurityDatabaseUtils.class);
	
	/**
	 * Get the database alias for a id
	 * @return
	 */
	public static String getDatabaseAliasForId(String id) {
//		String query = "SELECT ENGINENAME FROM ENGINE WHERE ENGINEID='" + id + "'";
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINENAME"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINEID", "==", id));
		List<String> results = QueryExecutionUtility.flushToListString(securityDb, qs);
		if (results.isEmpty()) {
			return null;
		}
		return results.get(0);
	}
	
	/**
	 * Get what permission the user has for a given database
	 * @param userId
	 * @param databaseId
	 * @param insightId
	 * @return
	 */
	public static String getActualUserDatabasePermission(User user, String databaseId) {
		return SecurityUserDatabaseUtils.getActualUserDatabasePermission(user, databaseId);
	}
	
	/**
	 * Get a list of the database ids
	 * @return
	 */
	public static List<String> getAllDatabaseIds() {
//		String query = "SELECT DISTINCT ENGINEID FROM ENGINE";
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID"));
		return QueryExecutionUtility.flushToListString(securityDb, qs);
	}
	

	
	/**
	 * Get markdown for a given engine
	 * @param user, databaseId
	 * @return
	 */
	public static String getDatabaseMarkdown(User user, String databaseId) {
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINEMETA__METAVALUE"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEMETA__METAKEY", "==", Constants.MARKDOWN));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEMETA__ENGINEID", "==", databaseId));
		{
			SelectQueryStruct qs1 = new SelectQueryStruct();
			qs1.addSelector(new QueryColumnSelector("ENGINE__ENGINEID"));
			
			{
				OrQueryFilter orFilter = new OrQueryFilter();
				orFilter.addFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__DISCOVERABLE", "==", Arrays.asList(true, null), PixelDataType.BOOLEAN));
				orFilter.addFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", getUserFiltersQs(user)));
				qs1.addExplicitFilter(orFilter);
			}
			qs1.addRelation("ENGINE", "ENGINEPERMISSION", "join");
			IRelation subQuery = new SubqueryRelationship(qs1, "ENGINE", "join", new String[] {"ENGINE__ENGINEID", "ENGINEMETA__ENGINEID", "="});
			qs.addRelation(subQuery);
		}
		return QueryExecutionUtility.flushToString(securityDb, qs);
	}
	
	/**
	 * Get the database permissions for a specific user
	 * @param singleUserId
	 * @param databaseId
	 * @return
	 */
	public static Integer getUserDatabasePermission(String singleUserId, String databaseId) {
		return SecurityUserDatabaseUtils.getUserDatabasePermission(singleUserId, databaseId);
	}
	
	/**
	 * Get the request pending database permission for a specific user
	 * @param singleUserId
	 * @param databaseId
	 * @return
	 */
	public static Integer getUserAccessRequestDatabasePermission(String userId, String databaseId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("DATABASEACCESSREQUEST__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("DATABASEACCESSREQUEST__REQUEST_USERID", "==", userId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("DATABASEACCESSREQUEST__ENGINEID", "==", databaseId));
		return QueryExecutionUtility.flushToInteger(securityDb, qs);
	}
	
	/**
	 * Approving user access requests and giving user access in permissions
	 * @param userId
	 * @param userType
	 * @param databaseId
	 * @param requests
	 */
	public static void approveDatabaseUserAccessRequests(User user, String databaseId, List<Map<String, String>> requests) throws IllegalAccessException{
		// make sure user has right permission level to approve access requests
		int userPermissionLvl = getMaxUserDatabasePermission(user, databaseId);
		if(!AccessPermissionEnum.isEditor(userPermissionLvl)) {
			throw new IllegalAccessException("Insufficient privileges to modify this database's permissions.");
		}
		
		// get user permissions of all requests
		List<String> permissions = new ArrayList<String>();
	    for(Map<String,String> i:requests){
	    	permissions.add(i.get("permission"));
	    }

		// if user is not an owner, check to make sure they cannot grant owner access
		if(!AccessPermissionEnum.isEditor(userPermissionLvl)) {
			throw new IllegalArgumentException("You cannot grant user access to others.");
		} else {
			if(!AccessPermissionEnum.isOwner(userPermissionLvl) && permissions.contains("OWNER")) {
				throw new IllegalArgumentException("As a non-owner, you cannot grant owner access.");
			}
		}
				
		// bulk delete
		String deleteQ = "DELETE FROM ENGINEPERMISSION WHERE USERID=? AND ENGINEID=?";
		PreparedStatement deletePs = null;
		try {
			deletePs = securityDb.getPreparedStatement(deleteQ);
			for(int i=0; i<requests.size(); i++) {
				int parameterIndex = 1;
				deletePs.setString(parameterIndex++, (String) requests.get(i).get("userid"));
				deletePs.setString(parameterIndex++, databaseId);
				deletePs.addBatch();
			}
			deletePs.executeBatch();
			if(!deletePs.getConnection().getAutoCommit()) {
				deletePs.getConnection().commit();
			}
		} catch(Exception e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred while deleting enginepermission with detailed message = " + e.getMessage());
		} finally {
			if(deletePs != null) {
				try {
					deletePs.close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
				if(securityDb.isConnectionPooling()) {
					try {
						deletePs.getConnection().close();
					} catch (SQLException e) {
						logger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}
		// insert new user permissions in bulk
		String insertQ = "INSERT INTO ENGINEPERMISSION (USERID, ENGINEID, PERMISSION, VISIBILITY) VALUES(?,?,?,?)";
		PreparedStatement insertPs = null;
		try {
			insertPs = securityDb.getPreparedStatement(insertQ);
			for(int i=0; i<requests.size(); i++) {
				int parameterIndex = 1;
				insertPs.setString(parameterIndex++, (String) requests.get(i).get("userid"));
				insertPs.setString(parameterIndex++, databaseId);
				insertPs.setInt(parameterIndex++, AccessPermissionEnum.getIdByPermission(requests.get(i).get("permission")));
				insertPs.setBoolean(parameterIndex++, true);
				insertPs.addBatch();
			}
			insertPs.executeBatch();
			if(!insertPs.getConnection().getAutoCommit()) {
				insertPs.getConnection().commit();
			}
		} catch(Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(insertPs != null) {
				try {
					insertPs.close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
				if(securityDb.isConnectionPooling()) {
					try {
						insertPs.getConnection().close();
					} catch (SQLException e) {
						logger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}

		// now we do the new bulk update to databaseaccessrequest table
		String updateQ = "UPDATE DATABASEACCESSREQUEST SET PERMISSION = ?, APPROVER_USERID = ?, APPROVER_TYPE = ?, APPROVER_DECISION = ?, APPROVER_TIMESTAMP = ? WHERE REQUEST_USERID = ? AND ENGINEID = ?";
		PreparedStatement updatePs = null;
		try {
			Calendar cal = Calendar.getInstance(TimeZone.getTimeZone(Utility.getApplicationTimeZoneId()));
			java.sql.Timestamp timestamp = java.sql.Timestamp.valueOf(LocalDateTime.now());
			updatePs = securityDb.getPreparedStatement(updateQ);
			AccessToken token = user.getAccessToken(user.getPrimaryLogin());
			String userId = token.getId();
			String userType = token.getProvider().toString();
			for(int i=0; i<requests.size(); i++) {
				int index = 1;
				// set
				updatePs.setInt(index++, AccessPermissionEnum.getIdByPermission(requests.get(i).get("permission")));
				updatePs.setString(index++, userId);
				updatePs.setString(index++, userType);
				updatePs.setString(index++, "APPROVED");
				updatePs.setTimestamp(index++, timestamp, cal);
				// where
				updatePs.setString(index++, (String) requests.get(i).get("userid"));
				updatePs.setString(index++, databaseId);
				updatePs.addBatch();
			}
			updatePs.executeBatch();
			if(!updatePs.getConnection().getAutoCommit()) {
				updatePs.getConnection().commit();
			}
		} catch(Exception e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred while updating user access request detailed message = " + e.getMessage());
		} finally {
			if(updatePs != null) {
				try {
					updatePs.close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
				if(securityDb.isConnectionPooling()) {
					try {
						updatePs.getConnection().close();
					} catch (SQLException e) {
						logger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}
	}
	
	/**
	 * Denying user access requests to database
	 * @param userId
	 * @param userType
	 * @param databaseId
	 * @param requests
	 */
	public static void denyDatabaseUserAccessRequests(User user, String databaseId, List<String> UserIdList) throws IllegalAccessException {
		// make sure user has right permission level to approve acces requests
		int userPermissionLvl = getMaxUserDatabasePermission(user, databaseId);
		if(!AccessPermissionEnum.isEditor(userPermissionLvl)) {
			throw new IllegalAccessException("Insufficient privileges to modify this database's permissions.");
		}

		// only database owners can deny user access requests
		if(!AccessPermissionEnum.isOwner(userPermissionLvl)) {
			throw new IllegalArgumentException("Insufficient privileges to deny user access requests.");
		}
		
		// bulk update to databaseaccessrequest table
		String updateQ = "UPDATE DATABASEACCESSREQUEST SET APPROVER_USERID = ?, APPROVER_TYPE = ?, APPROVER_DECISION = ?, APPROVER_TIMESTAMP = ? WHERE REQUEST_USERID = ? AND ENGINEID = ?";
		PreparedStatement updatePs = null;
		try {
			Calendar cal = Calendar.getInstance(TimeZone.getTimeZone(Utility.getApplicationTimeZoneId()));
			java.sql.Timestamp timestamp = java.sql.Timestamp.valueOf(LocalDateTime.now());
			updatePs = securityDb.getPreparedStatement(updateQ);
			AccessToken token = user.getAccessToken(user.getPrimaryLogin());
			String userId = token.getId();
			String userType = token.getProvider().toString();
			for(int i=0; i<UserIdList.size(); i++) {
				int index = 1;
				//set
				updatePs.setString(index++, userId);
				updatePs.setString(index++, userType);
				updatePs.setString(index++, "DENIED");
				updatePs.setTimestamp(index++, timestamp, cal);
				//where
				updatePs.setString(index++, UserIdList.get(i));
				updatePs.setString(index++, databaseId);
				updatePs.addBatch();
			}
			updatePs.executeBatch();
			if(!updatePs.getConnection().getAutoCommit()) {
				updatePs.getConnection().commit();
			}
		} catch(Exception e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred while updating user access request detailed message = " + e.getMessage());
		} finally {
			if(updatePs != null) {
				try {
					updatePs.close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
				if(securityDb.isConnectionPooling()) {
					try {
						updatePs.getConnection().close();
					} catch (SQLException e) {
						logger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}
	}
	
	/**
	 * Get the request pending database permission for a specific user
	 * @param singleUserId
	 * @param databaseId
	 * @return
	 */
	public static List<Map<String, Object>> getUserAccessRequestsByDatabase(String databaseId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("DATABASEACCESSREQUEST__REQUEST_USERID"));
		qs.addSelector(new QueryColumnSelector("DATABASEACCESSREQUEST__REQUEST_TYPE"));
		qs.addSelector(new QueryColumnSelector("DATABASEACCESSREQUEST__REQUEST_TIMESTAMP"));
		qs.addSelector(new QueryColumnSelector("DATABASEACCESSREQUEST__ENGINEID"));
		qs.addSelector(new QueryColumnSelector("DATABASEACCESSREQUEST__PERMISSION"));
		qs.addSelector(new QueryColumnSelector("DATABASEACCESSREQUEST__APPROVER_USERID"));
		qs.addSelector(new QueryColumnSelector("DATABASEACCESSREQUEST__APPROVER_TYPE"));
		qs.addSelector(new QueryColumnSelector("DATABASEACCESSREQUEST__APPROVER_DECISION"));
		qs.addSelector(new QueryColumnSelector("DATABASEACCESSREQUEST__APPROVER_TIMESTAMP"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("DATABASEACCESSREQUEST__ENGINEID", "==", databaseId));
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}
	
	/**
	 * See if specific database is global
	 * @return
	 */
	public static boolean databaseIsGlobal(String databaseId) {
//		String query = "SELECT ENGINEID FROM ENGINE WHERE GLOBAL=TRUE and ENGINEID='" + databaseId + "'";
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__GLOBAL", "==", true, PixelDataType.BOOLEAN));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINEID", "==", databaseId));
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
	 * Determine if the user is the owner of an database
	 * @param userFilters
	 * @param databaseId
	 * @return
	 */
	public static boolean userIsOwner(User user, String databaseId) {
		return SecurityUserDatabaseUtils.userIsOwner(getUserFiltersQs(user), databaseId)
				|| SecurityGroupDatabaseUtils.userGroupIsOwner(user, databaseId);
	}
	
	/**
	 * Determine if the user can modify the database
	 * @param databaseId
	 * @param userId
	 * @return
	 */
	public static boolean userCanEditDatabase(User user, String databaseId) {
		return SecurityUserDatabaseUtils.userCanEditDatabase(user, databaseId)
				|| SecurityGroupDatabaseUtils.userGroupCanEditDatabase(user, databaseId);
	}
	
	/**
	 * Determine if a user can view a database
	 * @param user
	 * @param databaseId
	 * @return
	 */
	public static boolean userCanViewDatabase(User user, String databaseId) {
		return SecurityUserDatabaseUtils.userCanViewDatabase(user, databaseId)
				|| SecurityGroupDatabaseUtils.userGroupCanViewDatabase(user, databaseId);
	}
	
	/**
	 * Determine if the user can edit the database
	 * @param userId
	 * @param databaseId
	 * @return
	 */
	static int getMaxUserDatabasePermission(User user, String databaseId) {
		return SecurityUserDatabaseUtils.getMaxUserDatabasePermission(user, databaseId);
	}
	
	
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	
	/*
	 * Query for database users
	 */
	
	/**
	 * Retrieve the list of users for a given database
	 * @param user
	 * @param databaseId
	 * @param insightId
	 * @return
	 * @throws IllegalAccessException
	 */
	public static List<Map<String, Object>> getDatabaseUsers(User user, String databaseId) throws IllegalAccessException {
		if(!userCanViewDatabase(user, databaseId)) {
			throw new IllegalArgumentException("The user does not have access to view this database");
		}
		
//		String query = "SELECT SMSS_USER.ID AS \"id\", "
//				+ "SMSS_USER.NAME AS \"name\", "
//				+ "PERMISSION.NAME AS \"permission\" "
//				+ "FROM SMSS_USER "
//				+ "INNER JOIN ENGINEPERMISSION ON (USER.ID = ENGINEPERMISSION.USERID) "
//				+ "INNER JOIN PERMISSION ON (ENGINEPERMISSION.PERMISSION = PERMISSION.ID) "
//				+ "WHERE ENGINEPERMISSION.ENGINEID='" + databaseId + "';"
//				;
//		
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__ID", "id"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__NAME", "name"));
		qs.addSelector(new QueryColumnSelector("PERMISSION__NAME", "permission"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__ENGINEID", "==", databaseId));
		qs.addRelation("SMSS_USER", "ENGINEPERMISSION", "inner.join");
		qs.addRelation("ENGINEPERMISSION", "PERMISSION", "inner.join");
		qs.addOrderBy(new QueryColumnOrderBySelector("SMSS_USER__ID"));
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}
	
	/**
	 * 
	 * @param user
	 * @param newUserId
	 * @param databaseId
	 * @param permission
	 * @return
	 * @throws IllegalAccessException 
	 */
	public static void addDatabaseUser(User user, String newUserId, String databaseId, String permission) throws IllegalAccessException {
		// make sure user can edit the database
		int userPermissionLvl = getMaxUserDatabasePermission(user, databaseId);
		if(!AccessPermissionEnum.isEditor(userPermissionLvl)) {
			throw new IllegalAccessException("Insufficient privileges to modify this database's permissions.");
		}
		
		// make sure user doesn't already exist for this database
		if(getUserDatabasePermission(newUserId, databaseId) != null) {
			// that means there is already a value
			throw new IllegalArgumentException("This user already has access to this database. Please edit the existing permission level.");
		}
		
		// if i am not an owner
		// then i need to check if i can edit this users permission
		if(!AccessPermissionEnum.isOwner(userPermissionLvl)) {
			int newPermissionLvl = AccessPermissionEnum.getIdByPermission(permission);

			// cannot give some owner permission if i am just an editor
			if(AccessPermissionEnum.OWNER.getId() == newPermissionLvl) {
				throw new IllegalAccessException("Cannot give owner level access to this database since you are not currently an owner.");
			}
		}
		
		String query = "INSERT INTO ENGINEPERMISSION (USERID, ENGINEID, VISIBILITY, PERMISSION) VALUES('"
				+ RdbmsQueryBuilder.escapeForSQLStatement(newUserId) + "', '"
				+ RdbmsQueryBuilder.escapeForSQLStatement(databaseId) + "', "
				+ "TRUE, "
				+ AccessPermissionEnum.getIdByPermission(permission) + ");";

		try {
			securityDb.insertData(query);
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occured adding user permissions for this APP");
		}
	}
	
	
	/**
	 * 
	 * @param newUserId
	 * @param databaseId
	 * @param permission
	 * @return
	 */
	public static void addDatabaseUserPermissions(User user, String databaseId, List<Map<String,String>> permission) throws IllegalAccessException {
		
		// make sure user can edit the database
		int userPermissionLvl = getMaxUserDatabasePermission(user, databaseId);
		if(!AccessPermissionEnum.isEditor(userPermissionLvl)) {
			throw new IllegalAccessException("Insufficient privileges to modify this database's permissions.");
		}
		
		// check to make sure these users do not already have permissions to database
		// get list of userids from permission list map
		List<String> userIds = permission.stream().map(map -> map.get("userid")).collect(Collectors.toList());
		// this returns a list of existing permissions
		Map<String, Integer> existingUserPermission = SecurityUserDatabaseUtils.getUserDatabasePermissions(userIds, databaseId);
		if (!existingUserPermission.isEmpty()) {
			throw new IllegalArgumentException("The following users already have access to this database. Please edit the existing permission level: "+String.join(",", existingUserPermission.keySet()));
		}
		
		// if user is not an owner, check to make sure they are not adding owner access
		if(!AccessPermissionEnum.isOwner(userPermissionLvl)) {
			List<String> permissionList = permission.stream().map(map -> map.get("permission")).collect(Collectors.toList());
			if(permissionList.contains("OWNER")) {
				throw new IllegalArgumentException("As a non-owner, you cannot add owner user access.");
			}
		}
		
		// insert new user permissions in bulk
		String insertQ = "INSERT INTO ENGINEPERMISSION (USERID, ENGINEID, PERMISSION, VISIBILITY) VALUES(?,?,?,?)";
		PreparedStatement insertPs = null;
		try {
			insertPs = securityDb.getPreparedStatement(insertQ);
			for(int i=0; i<permission.size(); i++) {
				int parameterIndex = 1;
				insertPs.setString(parameterIndex++, permission.get(i).get("userid"));
				insertPs.setString(parameterIndex++, databaseId);
				insertPs.setInt(parameterIndex++, AccessPermissionEnum.getIdByPermission(permission.get(i).get("permission")));
				insertPs.setString(parameterIndex++, "TRUE");
				insertPs.addBatch();
			}
			insertPs.executeBatch();
			if(!insertPs.getConnection().getAutoCommit()) {
				insertPs.getConnection().commit();
			}
		} catch(Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(insertPs != null) {
				try {
					insertPs.close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
				if(securityDb.isConnectionPooling()) {
					try {
						insertPs.getConnection().close();
					} catch (SQLException e) {
						logger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}
	}
	
	/**
	 * 
	 * @param user
	 * @param existingUserId
	 * @param databaseId
	 * @param newPermission
	 * @return
	 * @throws IllegalAccessException 
	 */
	public static void editDatabaseUserPermission(User user, String existingUserId, String databaseId, String newPermission) throws IllegalAccessException {
		// make sure user can edit the database
		int userPermissionLvl = getMaxUserDatabasePermission(user, databaseId);
		if(!AccessPermissionEnum.isEditor(userPermissionLvl)) {
			throw new IllegalAccessException("Insufficient privileges to modify this database's permissions.");
		}
		
		// make sure we are trying to edit a permission that exists
		Integer existingUserPermission = getUserDatabasePermission(existingUserId, databaseId);
		if(existingUserPermission == null) {
			throw new IllegalArgumentException("Attempting to modify database permission for a user who does not currently have access to the database");
		}
		
		int newPermissionLvl = AccessPermissionEnum.getIdByPermission(newPermission);
		
		// if i am not an owner
		// then i need to check if i can edit this users permission
		if(!AccessPermissionEnum.isOwner(userPermissionLvl)) {
			// not an owner, check if trying to edit an owner or an editor/reader
			// get the current permission
			if(AccessPermissionEnum.OWNER.getId() == existingUserPermission) {
				throw new IllegalAccessException("The user doesn't have the high enough permissions to modify this users database permission.");
			}
			
			// also, cannot give some owner permission if i am just an editor
			if(AccessPermissionEnum.OWNER.getId() == newPermissionLvl) {
				throw new IllegalAccessException("Cannot give owner level access to this insight since you are not currently an owner.");
			}
		}
		
		String query = "UPDATE ENGINEPERMISSION SET PERMISSION=" + newPermissionLvl
				+ " WHERE USERID='" + RdbmsQueryBuilder.escapeForSQLStatement(existingUserId) + "' "
				+ "AND ENGINEID='"	+ RdbmsQueryBuilder.escapeForSQLStatement(databaseId) + "';";
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
	 * @param databaseId
	 * @return
	 * @throws IllegalAccessException 
	 */
	public static void removeDatabaseUser(User user, String existingUserId, String databaseId) throws IllegalAccessException {
		// make sure user can edit the database
		int userPermissionLvl = getMaxUserDatabasePermission(user, databaseId);
		if(!AccessPermissionEnum.isEditor(userPermissionLvl)) {
			throw new IllegalAccessException("Insufficient privileges to modify this database's permissions.");
		}
		
		// make sure we are trying to edit a permission that exists
		Integer existingUserPermission = getUserDatabasePermission(existingUserId, databaseId);
		if(existingUserPermission == null) {
			throw new IllegalArgumentException("Attempting to modify user permission for a user who does not currently have access to the database");
		}
		
		// if i am not an owner
		// then i need to check if i can remove this users permission
		if(!AccessPermissionEnum.isOwner(userPermissionLvl)) {
			// not an owner, check if trying to edit an owner or an editor/reader
			// get the current permission
			if(AccessPermissionEnum.OWNER.getId() == existingUserPermission) {
				throw new IllegalAccessException("The user doesn't have the high enough permissions to modify this users database permission.");
			}
		}
		
		String query = "DELETE FROM ENGINEPERMISSION WHERE USERID='" 
				+ RdbmsQueryBuilder.escapeForSQLStatement(existingUserId) + "' "
				+ "AND ENGINEID='"	+ RdbmsQueryBuilder.escapeForSQLStatement(databaseId) + "';";
		try {
			securityDb.insertData(query);
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occured removing the user permissions for this database");
		}
		
		
		//TODO >>> Kunal: There are no more insights in an database. likely need to clean whole file
		// need to also delete all insight permissions for this database
//		query = "DELETE FROM USERINSIGHTPERMISSION WHERE USERID='" 
//				+ RdbmsQueryBuilder.escapeForSQLStatement(existingUserId) + "' "
//				+ "AND ENGINEID='"	+ RdbmsQueryBuilder.escapeForSQLStatement(databaseId) + "';";
//		try {
//			securityDb.insertData(query);
//		} catch (SQLException e) {
//			logger.error(Constants.STACKTRACE, e);
//			throw new IllegalArgumentException("An error occured removing the user permissions for the insights of this database");
//		}
	}
	
	/**
	 * 
	 * @param editedUserId
	 * @param databaseId
	 * @return
	 */
	public static void removeDatabaseUsers(User user, List<String> existingUserIds, String databaseId)  throws IllegalAccessException {
		// make sure user can edit the database
		int userPermissionLvl = getMaxUserDatabasePermission(user, databaseId);
		if(!AccessPermissionEnum.isEditor(userPermissionLvl)) {
			throw new IllegalAccessException("Insufficient privileges to modify this database's permissions.");
		}
		
		// get user permissions to remove
		Map<String, Integer> existingUserPermission = SecurityUserDatabaseUtils.getUserDatabasePermissions(existingUserIds, databaseId);
		
		// make sure all users to remove currently has access to database
		Set<String> toRemoveUserIds = new HashSet<String>(existingUserIds);
		toRemoveUserIds.removeAll(existingUserPermission.keySet());
		if (!toRemoveUserIds.isEmpty()) {
			throw new IllegalArgumentException("Attempting to modify user permission for the following users who do not currently have access to the database: "+String.join(",", toRemoveUserIds));
		}
		
		// if user is not an owner, check to make sure they are not removing owner access
		if(!AccessPermissionEnum.isOwner(userPermissionLvl)) {
			List<Integer> permissionList = new ArrayList<Integer>(existingUserPermission.values());
			if(permissionList.contains(AccessPermissionEnum.OWNER.getId())) {
				throw new IllegalArgumentException("As a non-owner, you cannot remove access of an owner.");
			}
		}
		
		// first do a delete
		String deleteQ = "DELETE FROM ENGINEPERMISSION WHERE USERID=? AND ENGINEID=?";
		PreparedStatement deletePs = null;
		try {
			deletePs = securityDb.getPreparedStatement(deleteQ);
			for(int i=0; i<existingUserIds.size(); i++) {
				int parameterIndex = 1;
				deletePs.setString(parameterIndex++, existingUserIds.get(i));
				deletePs.setString(parameterIndex++, databaseId);
				deletePs.addBatch();
			}
			deletePs.executeBatch();
			if(!deletePs.getConnection().getAutoCommit()) {
				deletePs.getConnection().commit();
			}
		} catch(Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(deletePs != null) {
				try {
					deletePs.close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
				if(securityDb.isConnectionPooling()) {
					try {
						deletePs.getConnection().close();
					} catch (SQLException e) {
						logger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}
	}
	
	/**
	 * Set if the database is public to all users on this instance
	 * @param user
	 * @param databaseId
	 * @param global
	 * @return
	 * @throws IllegalAccessException 
	 */
	public static boolean setDatabaseGlobal(User user, String databaseId, boolean global) throws IllegalAccessException {
		if(!SecurityUserDatabaseUtils.userIsOwner(user, databaseId)) {
			throw new IllegalAccessException("The user doesn't have the permission to set this database as global. Only the owner or an admin can perform this action.");
		}

		String updateQ = "UPDATE ENGINE SET GLOBAL=? WHERE ENGINEID=?";
		PreparedStatement updatePs = null;
		try {
			updatePs = securityDb.getPreparedStatement(updateQ);
			updatePs.setBoolean(1, global);
			updatePs.setString(2, databaseId);
			updatePs.execute();
		} catch(Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(updatePs != null) {
				try {
					updatePs.close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
				if(securityDb.isConnectionPooling()) {
					try {
						updatePs.getConnection().close();
					} catch (SQLException e) {
						logger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}
		return true;
	}
	
	/**
	 * Set if the database is discoverable to all users on this instance
	 * @param user
	 * @param databaseId
	 * @param discoverable
	 * @return
	 * @throws IllegalAccessException
	 */
	public static boolean setDatabaseDiscoverable(User user, String databaseId, boolean discoverable) throws IllegalAccessException {
		if(!SecurityUserDatabaseUtils.userIsOwner(user, databaseId)) {
			throw new IllegalAccessException("The user doesn't have the permission to set this database as global. Only the owner or an admin can perform this action.");
		}
		
		String updateQ = "UPDATE ENGINE SET DISCOVERABLE=? WHERE ENGINEID=?";
		PreparedStatement updatePs = null;
		try {
			updatePs = securityDb.getPreparedStatement(updateQ);
			updatePs.setBoolean(1, discoverable);
			updatePs.setString(2, databaseId);
			updatePs.execute();
		} catch(Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(updatePs != null) {
				try {
					updatePs.close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
				if(securityDb.isConnectionPooling()) {
					try {
						updatePs.getConnection().close();
					} catch (SQLException e) {
						logger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}
		return true;
	}
	
	/**
	 * update the database name
	 * @param user
	 * @param databaseId
	 * @param isPublic
	 * @return
	 */
	public static boolean setDatabaseName(User user, String databaseId, String newDatabaseName) {
		if(!SecurityUserDatabaseUtils.userIsOwner(user, databaseId)) {
			throw new IllegalArgumentException("The user doesn't have the permission to change the database name. Only the owner or an admin can perform this action.");
		}
		newDatabaseName = RdbmsQueryBuilder.escapeForSQLStatement(newDatabaseName);
		databaseId = RdbmsQueryBuilder.escapeForSQLStatement(databaseId);
		String query = "UPDATE ENGINE SET ENGINENAME = '" + newDatabaseName + "' WHERE ENGINEID ='" + databaseId + "';";
		securityDb.execUpdateAndRetrieveStatement(query, true);
		securityDb.commit();
		return true;
	}
	
	//////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////

	/*
	 * Database Metadata
	 */
	
	/**
	 * 
	 * @return
	 */
	public static List<String> getAllMetakeys() {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINEMETAKEYS__METAKEY"));
		List<String> metakeys = QueryExecutionUtility.flushToListString(securityDb, qs);
		return metakeys;
	}
	
	/**
	 * Update the database metadata
	 * Will delete existing values and then perform a bulk insert
	 * @param databaseId
	 * @param insightId
	 * @param tags
	 */
	public static void updateDatabaseMetadata(String databaseId, Map<String, Object> metadata) {
		// first do a delete
		String deleteQ = "DELETE FROM ENGINEMETA WHERE METAKEY=? AND ENGINEID=?";
		PreparedStatement deletePs = null;
		try {
			deletePs = securityDb.getPreparedStatement(deleteQ);
			for(String field : metadata.keySet()) {
				int parameterIndex = 1;
				deletePs.setString(parameterIndex++, field);
				deletePs.setString(parameterIndex++, databaseId);
				deletePs.addBatch();
			}
			deletePs.executeBatch();
			if(!deletePs.getConnection().getAutoCommit()) {
				deletePs.getConnection().commit();
			}
		} catch(Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(deletePs != null) {
				try {
					deletePs.close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
				if(securityDb.isConnectionPooling()) {
					try {
						deletePs.getConnection().close();
					} catch (SQLException e) {
						logger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}
		
		// now we do the new insert with the order of the tags
		String query = securityDb.getQueryUtil().createInsertPreparedStatementString("ENGINEMETA", new String[]{"ENGINEID", "METAKEY", "METAVALUE", "METAORDER"});
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			for(String field : metadata.keySet()) {
				Object val = metadata.get(field);
				List<Object> values = new ArrayList<>();
				if(val instanceof List) {
					values = (List<Object>) val;
				} else if(val instanceof Collection) {
					values.addAll( (Collection<Object>) val);
				} else {
					values.add(val);
				}
				
				for(int i = 0; i < values.size(); i++) {
					int parameterIndex = 1;
					Object fieldVal = values.get(i);
					
					ps.setString(parameterIndex++, databaseId);
					ps.setString(parameterIndex++, field);
					ps.setString(parameterIndex++, fieldVal + "");
					ps.setInt(parameterIndex++, i);
					ps.addBatch();
				}
			}
			ps.executeBatch();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch(Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
				if(securityDb.isConnectionPooling()) {
					try {
						ps.getConnection().close();
					} catch (SQLException e) {
						logger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}
	}
	
	
//	/**
//	 * Update the database description
//	 * Will perform an insert if the description doesn't currently exist
//	 * @param databaseId
//	 * @param insideId
//	 */
//	public static void updateDatabaseDescription(String databaseId, String description) {
//		// try to do an update
//		// if nothing is updated
//		// do an insert
//		databaseId = RdbmsQueryBuilder.escapeForSQLStatement(databaseId);
//		String query = "UPDATE ENGINEMETA SET METAVALUE='" 
//				+ AbstractSqlQueryUtil.escapeForSQLStatement(description) + "' "
//				+ "WHERE METAKEY='description' AND ENGINEID='" + databaseId + "'";
//		Statement stmt = null;
//		try {
//			stmt = securityDb.execUpdateAndRetrieveStatement(query, false);
//			if(stmt.getUpdateCount() <= 0) {
//				// need to perform an insert
//				query = securityDb.getQueryUtil().insertIntoTable("ENGINEMETA", 
//						new String[]{"ENGINEID", "METAKEY", "METAVALUE", "METAORDER"}, 
//						new String[]{"varchar(255)", "varchar(255)", "clob", "int"}, 
//						new Object[]{databaseId, "description", description, 0});
//				securityDb.insertData(query);
//			}
//		} catch(SQLException e) {
//			logger.error(Constants.STACKTRACE, e);
//		} finally {
//			if(stmt != null) {
//				try {
//					stmt.close();
//				} catch (SQLException e) {
//					logger.error(Constants.STACKTRACE, e);
//				}
//				if(securityDb.isConnectionPooling()) {
//					try {
//						stmt.getConnection().close();
//					} catch (SQLException e) {
//						logger.error(Constants.STACKTRACE, e);
//					}
//				}
//			}
//		}
//	}
	
//	/**
//	 * Update the database tags
//	 * Will delete existing values and then perform a bulk insert
//	 * @param databaseId
//	 * @param insightId
//	 * @param tags
//	 */
//	public static void updateDatabaseTags(String databaseId, List<String> tags) {
//		// first do a delete
//		String query = "DELETE FROM ENGINEMETA WHERE METAKEY='tag' AND ENGINEID='" + databaseId + "'";
//		try {
//			securityDb.insertData(query);
//			securityDb.commit();
//		} catch (SQLException e) {
//			logger.error(Constants.STACKTRACE, e);
//		}
//		
//		// now we do the new insert with the order of the tags
//		query = securityDb.getQueryUtil().createInsertPreparedStatementString("ENGINEMETA", 
//				new String[]{"ENGINEID", "METAKEY", "METAVALUE", "METAORDER"});
//		PreparedStatement ps = null;
//		try {
//			ps = securityDb.getPreparedStatement(query);
//			for(int i = 0; i < tags.size(); i++) {
//				String tag = tags.get(i);
//				ps.setString(1, databaseId);
//				ps.setString(2, "tag");
//				ps.setString(3, tag);
//				ps.setInt(4, i);
//				ps.addBatch();;
//			}
//			
//			ps.executeBatch();
//		} catch(Exception e) {
//			logger.error(Constants.STACKTRACE, e);
//		} finally {
//			if(ps != null) {
//				try {
//					ps.close();
//				} catch (SQLException e) {
//					logger.error(Constants.STACKTRACE, e);
//				}
//				if(securityDb.isConnectionPooling()) {
//					try {
//						ps.getConnection().close();
//					} catch (SQLException e) {
//						logger.error(Constants.STACKTRACE, e);
//					}
//				}
//			}
//		}
//	}
	
	/**
	 * Get the wrapper for additional database metadata
	 * @param databaseIds
	 * @param metaKeys
	 * @param ignoreMarkdown
	 * @return
	 * @throws Exception
	 */
	public static IRawSelectWrapper getDatabaseMetadataWrapper(Collection<String> databaseIds, List<String> metaKeys, boolean ignoreMarkdown) throws Exception {
		SelectQueryStruct qs = new SelectQueryStruct();
		// selectors
		qs.addSelector(new QueryColumnSelector("ENGINEMETA__ENGINEID"));
		qs.addSelector(new QueryColumnSelector("ENGINEMETA__METAKEY"));
		qs.addSelector(new QueryColumnSelector("ENGINEMETA__METAVALUE"));
		qs.addSelector(new QueryColumnSelector("ENGINEMETA__METAORDER"));
		// filters
		if(databaseIds != null && !databaseIds.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEMETA__ENGINEID", "==", databaseIds));
		}
		if(metaKeys != null && !metaKeys.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEMETA__METAKEY", "==", metaKeys));
		}
		// exclude markdown metadata due to potential large data size
		if(ignoreMarkdown) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEMETA__METAKEY", "!=", Constants.MARKDOWN));
		}
		// order
		qs.addOrderBy("ENGINEMETA__METAORDER");
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
		return wrapper;
	}
	
	/**
	 * Get the metadata for a specific database
	 * @param databaseId
	 * @param metaKeys
	 * @param ignoreMarkdown
	 * @return
	 */
	public static Map<String, Object> getAggregateDatabaseMetadata(String databaseId, List<String> metaKeys, boolean ignoreMarkdown) {
		Map<String, Object> retMap = new HashMap<String, Object>();

		List<String> databaseIds = new ArrayList<>();
		databaseIds.add(databaseId);

		IRawSelectWrapper wrapper = null;
		try {
			wrapper = getDatabaseMetadataWrapper(databaseIds, metaKeys, ignoreMarkdown);
			while(wrapper.hasNext()) {
				Object[] data = wrapper.next().getValues();
				String metaKey = (String) data[1];
				String metaValue = (String) data[2];

				// always send as array
				// if multi, send as array
				if(retMap.containsKey(metaKey)) {
					Object obj = retMap.get(metaKey);
					if(obj instanceof List) {
						((List) obj).add(metaValue);
					} else {
						List<Object> newList = new ArrayList<>();
						newList.add(obj);
						newList.add(metaValue);
						retMap.put(metaKey, newList);
					}
				} else {
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
	 * Check if the user has access to the database
	 * @param databaseId
	 * @param userId
	 * @return
	 * @throws Exception
	 */
	public static boolean checkUserHasAccessToDatabase(String databaseId, String userId) throws Exception {
		return SecurityUserDatabaseUtils.checkUserHasAccessToDatabase(databaseId, userId);
	}
	
	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////

	/*
	 * Copying permissions
	 */
	
	/**
	 * Copy the database permissions from one database to another
	 * @param sourceDatabaseId
	 * @param targetDatabaseId
	 * @throws SQLException
	 */
	public static void copyDatabasePermissions(String sourceDatabaseId, String targetDatabaseId) throws Exception {
		String insertTargetDbPermissionSql = "INSERT INTO ENGINEPERMISSION (ENGINEID, USERID, PERMISSION, VISIBILITY) VALUES (?, ?, ?, ?)";
		PreparedStatement insertTargetDbPermissionStatement = securityDb.getPreparedStatement(insertTargetDbPermissionSql);
		
		// grab the permissions, filtered on the source database id
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__ENGINEID"));
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__USERID"));
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__PERMISSION"));
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__VISIBILITY"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__ENGINEID", "==", sourceDatabaseId));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			while(wrapper.hasNext()) {
				Object[] row = wrapper.next().getValues();
				// now loop through all the permissions
				// but with the target engine id instead of the source engine id
				insertTargetDbPermissionStatement.setString(1, targetDatabaseId);
				insertTargetDbPermissionStatement.setString(2, (String) row[1]);
				insertTargetDbPermissionStatement.setInt(3, ((Number) row[2]).intValue() );
				insertTargetDbPermissionStatement.setBoolean(4, (Boolean) row[3]);
				// add to batch
				insertTargetDbPermissionStatement.addBatch();
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
			throw e;
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
		}
		
		// first delete the current database permissions on the database
		String deleteTargetDbPermissionsSql = "DELETE FROM ENGINEPERMISSION WHERE ENGINEID = '" + AbstractSqlQueryUtil.escapeForSQLStatement(targetDatabaseId) + "'";
		securityDb.removeData(deleteTargetDbPermissionsSql);
		// execute the query
		insertTargetDbPermissionStatement.executeBatch();
	}
	
	/**
	 * Returns List of users that have no access credentials to a given database.
	 * @param databaseId
	 * @return 
	 */
	public static List<Map<String, Object>> getDatabaseUsersNoCredentials(User user, String databaseId) throws IllegalAccessException {
		/*
		 * Security check to make sure that the user can view the application provided. 
		 */
		if (!userCanViewDatabase(user, databaseId)) {
			throw new IllegalArgumentException("The user does not have access to view this database");
		}
		
		/*
		 * String Query = 
		 * "SELECT SMSS_USER.ID, SMSS_USER.USERNAME, SMSS_USER.NAME, SMSS_USER.EMAIL FROM SMSS_USER WHERE ID NOT IN 
		 * (SELECT e.USERID FROM ENGINEPERMISSION e WHERE e.ENGINEID = '"+ appID + "' e.PERMISSION IS NOT NULL);"
		 */
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__ID", "id"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__USERNAME", "username"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__NAME", "name"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__EMAIL", "email"));
		//Filter for sub-query
		{
			SelectQueryStruct subQs = new SelectQueryStruct();
			qs.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery("SMSS_USER__ID", "!=", subQs));
			//Sub-query itself
			subQs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__USERID"));
			subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__ENGINEID","==",databaseId));
			subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__PERMISSION", "!=", null, PixelDataType.NULL_VALUE));
		}
		
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}
	
	/**
	 * Return the databases the user has explicit access to
	 * @param singleUserId
	 * @return
	 */
	public static Set<String> getDatabasesUserHasExplicitAccess(User user) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID"));
		OrQueryFilter orFilter = new OrQueryFilter();
		orFilter.addFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__GLOBAL", "==", true, PixelDataType.BOOLEAN));
		orFilter.addFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", getUserFiltersQs(user)));
		qs.addExplicitFilter(orFilter);
		qs.addRelation("ENGINE", "ENGINEPERMISSION", "left.outer.join");
		return QueryExecutionUtility.flushToSetString(securityDb, qs, false);
	}
	
	/**
	 * Determine if a user can request a database
	 * @param databaseId
	 * @return
	 */
	public static boolean databaseIsDiscoverable(String databaseId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__DISCOVERABLE", "==", true, PixelDataType.BOOLEAN));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINEID", "==", databaseId));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if(wrapper.hasNext()) {
				// if you are here, you can request
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
	 * set user access request
	 * @param userId
	 * @param userType
	 * @param databaseId
	 * @param permission
	 * @return
	 */
	public static void setUserAccessRequest(String userId, String userType, String databaseId, int permission) {
		// first do a delete
		String deleteQ = "DELETE FROM DATABASEACCESSREQUEST WHERE REQUEST_USERID=? AND REQUEST_TYPE=? AND ENGINEID=?";
		PreparedStatement deletePs = null;
		try {
			int index = 1;
			deletePs = securityDb.getPreparedStatement(deleteQ);
			deletePs.setString(index++, userId);
			deletePs.setString(index++, userType);
			deletePs.setString(index++, databaseId);
			deletePs.execute();
		} catch(Exception e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred while deleting user access request with detailed message = " + e.getMessage());
		} finally {
			if(deletePs != null) {
				try {
					deletePs.close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
				if(securityDb.isConnectionPooling()) {
					try {
						deletePs.getConnection().close();
					} catch (SQLException e) {
						logger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}

		// now we do the new insert 
		String insertQ = "INSERT INTO DATABASEACCESSREQUEST (REQUEST_USERID, REQUEST_TYPE, REQUEST_TIMESTAMP, ENGINEID, PERMISSION) VALUES (?,?,?,?,?)";
		PreparedStatement insertPs = null;
		try {
			Calendar cal = Calendar.getInstance(TimeZone.getTimeZone(Utility.getApplicationTimeZoneId()));
			java.sql.Timestamp timestamp = java.sql.Timestamp.valueOf(LocalDateTime.now());

			int index = 1;
			insertPs = securityDb.getPreparedStatement(insertQ);
			insertPs.setString(index++, userId);
			insertPs.setString(index++, userType);
			insertPs.setTimestamp(index++, timestamp, cal);
			insertPs.setString(index++, databaseId);
			insertPs.setInt(index++, permission);
			insertPs.execute();
		} catch(Exception e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred while adding user access request detailed message = " + e.getMessage());
		} finally {
			if(insertPs != null) {
				try {
					insertPs.close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
				if(securityDb.isConnectionPooling()) {
					try {
						insertPs.getConnection().close();
					} catch (SQLException e) {
						logger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}
	}
	
	/**
	 * Get the list of databases the user does not have access to but can request
	 * @param allUserDbs 
	 * @throws Exception
	 */
	public static List<Map<String, Object>> getUserRequestableDatabases(Collection<String> allUserDbs) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINENAME"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINEID", "!=", allUserDbs));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__DISCOVERABLE", "==", true, PixelDataType.BOOLEAN));
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}	

	public static List<Map<String, Object>> getDatabaseInfo(Collection dbFilter) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINENAME"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINEID", "==", dbFilter));
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}
	
	/**
	 * Retrieve the database owner
	 * @param user
	 * @param databaseId
	 * @param insightId
	 * @return
	 * @throws IllegalAccessException
	 */
	public static List<String> getDatabaseOwners(String databaseId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__EMAIL", "email"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__ENGINEID", "==", databaseId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PERMISSION__ID", "==", AccessPermissionEnum.OWNER.getId()));
		qs.addRelation("SMSS_USER", "ENGINEPERMISSION", "inner.join");
		qs.addRelation("ENGINEPERMISSION", "PERMISSION", "inner.join");
		qs.addOrderBy(new QueryColumnOrderBySelector("SMSS_USER__ID"));
		return QueryExecutionUtility.flushToListString(securityDb, qs);
	}
	
	/**
	 * Get global databases
	 * @return
	 */
	public static Set<String> getGlobalDatabaseIds() {
//		String query = "SELECT ENGINEID FROM ENGINE WHERE GLOBAL=TRUE";
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__GLOBAL", "==", true, PixelDataType.BOOLEAN));
		return QueryExecutionUtility.flushToSetString(securityDb, qs, false);
	}
	
	/**
	 * Get all databases for setting options that the user has access to
	 * @param user
	 * @return
	 */
	public static List<Map<String, Object>> getUserDatabaseSettings(User user) {
		return getUserDatabaseSettings(user, null);
	}
	
	/**
	 * Get database settings - if databaseFitler passed will filter to that db otherwise returns all
	 * @param user
	 * @param dbFilter
	 * @return
	 */
	public static List<Map<String, Object>> getUserDatabaseSettings(User user, String databaseFilter) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID", "app_id"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINENAME", "app_name"));
		qs.addSelector(new QueryColumnSelector("ENGINE__GLOBAL", "app_global"));
		qs.addSelector(QueryFunctionSelector.makeCol2ValCoalesceSelector("ENGINEPERMISSION__VISIBILITY", true, "app_visibility"));
		qs.addSelector(QueryFunctionSelector.makeCol2ValCoalesceSelector("PERMISSION__NAME", "READ_ONLY", "app_permission"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", getUserFiltersQs(user)));
		if(databaseFilter != null && !databaseFilter.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINEID", "==", databaseFilter));
		}
		qs.addRelation("ENGINE", "ENGINEPERMISSION", "inner.join");
		qs.addRelation("ENGINEPERMISSION", "PERMISSION", "left.outer.join");
		
		Set<String> dbIdsIncluded = new HashSet<String>();
		List<Map<String, Object>> result = new Vector<Map<String, Object>>();
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			while (wrapper.hasNext()) {
				IHeadersDataRow headerRow = wrapper.next();
				String[] headers = headerRow.getHeaders();
				Object[] values = headerRow.getValues();
				
				// store the database ids
				// we will exclude these later
				// database id is the first one to be returned
				dbIdsIncluded.add(values[0].toString());
				
				Map<String, Object> map = new HashMap<String, Object>();
				for (int i = 0; i < headers.length; i++) {
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
		
		// we dont need to run 2nd query if we are filtering to one db and already have it
		if(databaseFilter != null && !databaseFilter.isEmpty() && !result.isEmpty()) {
			qs = new SelectQueryStruct();
			qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID", "app_id"));
			qs.addSelector(new QueryColumnSelector("ENGINE__ENGINENAME", "app_name"));
			qs.addSelector(QueryFunctionSelector.makeCol2ValCoalesceSelector("ENGINEPERMISSION__VISIBILITY", true, "app_visibility"));
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__GLOBAL", "==", true, PixelDataType.BOOLEAN));
			// since some rdbms do not allow "not in ()" - we will only add if necessary
			if (!dbIdsIncluded.isEmpty()) {
				qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINEID", "!=", new Vector<String>(dbIdsIncluded)));
			}
			if(databaseFilter != null && !databaseFilter.isEmpty()) {
				qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINEID", "==", databaseFilter));
			}
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
		}
		
		return result;
	}


	/**
	 * Get the list of the database information that the user has access to
	 * @param user
	 * @param databaseFilters
	 * @param favoritesOnly
	 * @param engineMetadataFilter
	 * @param searchTerm
	 * @param limit
	 * @param offset
	 * @return
	 */
	public static List<Map<String, Object>> getUserDatabaseList(User user, List<String> databaseFilters,
			Boolean favoritesOnly, Map<String, Object> engineMetadataFilter, 
			String searchTerm, String limit, String offset) {

		String enginePrefix = "ENGINE__";
		String groupEnginePermission = "GROUPENGINEPERMISSION__";
		Collection<String> userIds = getUserFiltersQs(user);
		boolean hasSearchTerm = searchTerm != null && !(searchTerm=searchTerm.trim()).isEmpty();
		
		SelectQueryStruct qs1 = new SelectQueryStruct();
		// selectors
		qs1.addSelector(new QueryColumnSelector("ENGINE__ENGINEID", "app_id"));
		qs1.addSelector(new QueryColumnSelector("ENGINE__ENGINENAME", "app_name"));
		qs1.addSelector(new QueryColumnSelector("ENGINE__TYPE", "app_type"));
		qs1.addSelector(new QueryColumnSelector("ENGINE__COST", "app_cost"));
		qs1.addSelector(new QueryColumnSelector("ENGINE__ENGINEID", "database_id"));
		qs1.addSelector(new QueryColumnSelector("ENGINE__ENGINENAME", "database_name"));
		qs1.addSelector(new QueryColumnSelector("ENGINE__TYPE", "database_type"));
		qs1.addSelector(new QueryColumnSelector("ENGINE__COST", "database_cost"));
		qs1.addSelector(new QueryColumnSelector("ENGINE__DISCOVERABLE", "database_discoverable"));
		qs1.addSelector(new QueryColumnSelector("ENGINE__GLOBAL", "database_global"));
		qs1.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.LOWER, "ENGINE__ENGINENAME", "low_database_name"));
		qs1.addSelector(new QueryColumnSelector("USER_PERMISSIONS__PERMISSION", "user_permission"));
		qs1.addSelector(new QueryColumnSelector("GROUP_PERMISSIONS__PERMISSION", "group_permission"));
		qs1.addSelector(new QueryColumnSelector("USER_PERMISSIONS__FAVORITE", "database_favorite"));
		qs1.addSelector(new QueryColumnSelector("USER_PERMISSIONS__FAVORITE", "app_favorite"));
		
		// this block is for max permissions
		// If both null - return null
		// if either not null - return the permission value that is not null
		// if both not null - return the max permissions (I.E lowest number)
		{
			AndQueryFilter and = new AndQueryFilter();
			and.addFilter(SimpleQueryFilter.makeColToValFilter("GROUP_PERMISSIONS__PERMISSION", "==", null, PixelDataType.CONST_INT));
			and.addFilter(SimpleQueryFilter.makeColToValFilter("USER_PERMISSIONS__PERMISSION", "==", null, PixelDataType.CONST_INT));
				
			AndQueryFilter and1 = new AndQueryFilter();
			and1.addFilter(SimpleQueryFilter.makeColToValFilter("GROUP_PERMISSIONS__PERMISSION", "!=", null, PixelDataType.CONST_INT));
			and1.addFilter(SimpleQueryFilter.makeColToValFilter("USER_PERMISSIONS__PERMISSION", "==", null, PixelDataType.CONST_INT));
		
			AndQueryFilter and2 = new AndQueryFilter();
			and2.addFilter(SimpleQueryFilter.makeColToValFilter("GROUP_PERMISSIONS__PERMISSION", "==", null, PixelDataType.CONST_INT));
			and2.addFilter(SimpleQueryFilter.makeColToValFilter("USER_PERMISSIONS__PERMISSION", "!=", null, PixelDataType.CONST_INT));
			
			SimpleQueryFilter maxPermFilter = SimpleQueryFilter.makeColToColFilter("USER_PERMISSIONS__PERMISSION", "<", "GROUP_PERMISSIONS__PERMISSION");
			
			QueryIfSelector qis3 = QueryIfSelector.makeQueryIfSelector(maxPermFilter,
						new QueryColumnSelector("USER_PERMISSIONS__PERMISSION"),
						new QueryColumnSelector("GROUP_PERMISSIONS__PERMISSION"),
						"permission"
					);

			QueryIfSelector qis2 = QueryIfSelector.makeQueryIfSelector(and2,
						new QueryColumnSelector("USER_PERMISSIONS__PERMISSION"),
						qis3,
						"permission"
					);
			
			QueryIfSelector qis1 = QueryIfSelector.makeQueryIfSelector(and1,
						new QueryColumnSelector("GROUP_PERMISSIONS__PERMISSION"),
						qis2,
						"permission"
					);
			
			QueryIfSelector qis = QueryIfSelector.makeQueryIfSelector(and,
						new QueryColumnSelector("USER_PERMISSIONS__PERMISSION"),
						qis1,
						"permission"
					);
			
			qs1.addSelector(qis);
		}
		
		// add a join to get the user permission level, if favorite, and the visibility
		{
			SelectQueryStruct qs2 = new SelectQueryStruct();
			qs2.addSelector(new QueryColumnSelector("ENGINEPERMISSION__ENGINEID", "ENGINEID"));
			qs2.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.MAX, "ENGINEPERMISSION__FAVORITE", "FAVORITE"));
			qs2.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.MIN, "ENGINEPERMISSION__PERMISSION", "PERMISSION"));
			qs2.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.MAX, "ENGINEPERMISSION__VISIBILITY", "VISIBILITY"));
			qs2.addGroupBy(new QueryColumnSelector("ENGINEPERMISSION__ENGINEID", "ENGINEID"));
			qs2.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", userIds));
			IRelation subQuery = new SubqueryRelationship(qs2, "USER_PERMISSIONS", "left.outer.join", new String[] {"USER_PERMISSIONS__ENGINEID", "ENGINE__ENGINEID", "="});
			qs1.addRelation(subQuery);
		}
		
		// add a join to get the group permission level
		{
			SelectQueryStruct qs3 = new SelectQueryStruct();
			qs3.addSelector(new QueryColumnSelector(groupEnginePermission + "ENGINEID", "ENGINEID"));
			qs3.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.MIN, groupEnginePermission + "PERMISSION", "PERMISSION"));
			qs3.addGroupBy(new QueryColumnSelector(groupEnginePermission + "ENGINEID", "ENGINEID"));
			
			// filter on groups
			OrQueryFilter groupEngineOrFilters = new OrQueryFilter();
			List<AuthProvider> logins = user.getLogins();
			for(AuthProvider login : logins) {
				if(user.getAccessToken(login).getUserGroups().isEmpty()) {
					continue;
				}
				
				AndQueryFilter andFilter = new AndQueryFilter();
				andFilter.addFilter(SimpleQueryFilter.makeColToValFilter(groupEnginePermission + "TYPE", "==", user.getAccessToken(login).getUserGroupType()));
				andFilter.addFilter(SimpleQueryFilter.makeColToValFilter(groupEnginePermission + "ID", "==", user.getAccessToken(login).getUserGroups()));
				groupEngineOrFilters.addFilter(andFilter);
			}
			
			if (!groupEngineOrFilters.isEmpty()) {
				qs3.addExplicitFilter(groupEngineOrFilters);
			} else {
				AndQueryFilter andFilter1 = new AndQueryFilter();
				andFilter1.addFilter(SimpleQueryFilter.makeColToValFilter(groupEnginePermission + "TYPE", "==", null));
				andFilter1.addFilter(SimpleQueryFilter.makeColToValFilter(groupEnginePermission + "ID", "==", null));
				qs3.addExplicitFilter(andFilter1);
			}
			
			IRelation subQuery = new SubqueryRelationship(qs3, "GROUP_PERMISSIONS", "left.outer.join", new String[] {"GROUP_PERMISSIONS__ENGINEID", "ENGINE__ENGINEID", "="});
			qs1.addRelation(subQuery);
		}
		
		// filters
		if(databaseFilters != null && !databaseFilters.isEmpty()) {
			qs1.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINEID", "==", databaseFilters));
		}
		OrQueryFilter orFilter = new OrQueryFilter();
		{
			orFilter.addFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__GLOBAL", "==", true, PixelDataType.BOOLEAN));
			orFilter.addFilter(SimpleQueryFilter.makeColToValFilter("USER_PERMISSIONS__PERMISSION", "!=", null, PixelDataType.CONST_INT));
			qs1.addExplicitFilter(orFilter);
		}
		// only show those that are visible
		qs1.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USER_PERMISSIONS__VISIBILITY", "==", Arrays.asList(new Object[] {true, null}), PixelDataType.BOOLEAN));
		// favorites only
		if(favoritesOnly) {
			qs1.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USER_PERMISSIONS__FAVORITE", "==", true, PixelDataType.BOOLEAN));
		}
		// optional word filter on the engine name
		if(hasSearchTerm) {
			securityDb.getQueryUtil().appendSearchRegexFilter(qs1, "ENGINE__ENGINENAME", searchTerm);
		}
		// filtering by enginemeta key-value pairs (i.e. <tag>:value): for each pair, add in-filter against engineids from subquery
		if (engineMetadataFilter!=null && !engineMetadataFilter.isEmpty()) {
			for (String k : engineMetadataFilter.keySet()) {
				SelectQueryStruct subQs = new SelectQueryStruct();
				subQs.addSelector(new QueryColumnSelector("ENGINEMETA__ENGINEID"));
				subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEMETA__METAKEY", "==", k));
				subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEMETA__METAVALUE", "==", engineMetadataFilter.get(k)));
				qs1.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery("ENGINE__ENGINEID", "==", subQs));
			}
		}
		
		// group permissions	
		{
			// first lets make sure we have any groups
			OrQueryFilter groupEngineOrFilters = new OrQueryFilter();
			List<AuthProvider> logins = user.getLogins();
			for(AuthProvider login : logins) {
				if(user.getAccessToken(login).getUserGroups().isEmpty()) {
					continue;
				}
				AndQueryFilter andFilter = new AndQueryFilter();
				andFilter.addFilter(SimpleQueryFilter.makeColToValFilter(groupEnginePermission + "TYPE", "==", user.getAccessToken(login).getUserGroupType()));
				andFilter.addFilter(SimpleQueryFilter.makeColToValFilter(groupEnginePermission + "ID", "==", user.getAccessToken(login).getUserGroups()));
				groupEngineOrFilters.addFilter(andFilter);
			}
			// 4.a does the group have explicit access
			if(!groupEngineOrFilters.isEmpty()) {
				SelectQueryStruct subQs = new SelectQueryStruct();
				// store first and fill in sub query after
				orFilter.addFilter(SimpleQueryFilter.makeColToSubQuery(enginePrefix + "ENGINEID", "==", subQs));
				
				// we need to have the insight filters
				subQs.addSelector(new QueryColumnSelector(groupEnginePermission + "ENGINEID"));
				subQs.addExplicitFilter(groupEngineOrFilters);
			}
		}
		
		// add the sort
		qs1.addOrderBy(new QueryColumnOrderBySelector("low_database_name"));

		Long long_limit = -1L;
		Long long_offset = -1L;
		if(limit != null && !limit.trim().isEmpty()) {
			long_limit = Long.parseLong(limit);
		}
		if(offset != null && !offset.trim().isEmpty()) {
			long_offset = Long.parseLong(offset);
		}
		qs1.setLimit(long_limit);
		qs1.setOffSet(long_offset);
		
		return QueryExecutionUtility.flushRsToMap(securityDb, qs1);
	}
	
	/**
	 * Get the list of the database ids that the user has access to
	 * @param user
	 * @param includeGlobal
	 * @param includeDiscoverable
	 * @param includeExistingAccess
	 * @return
	 */
	public static List<String> getUserDatabaseIdList(User user, boolean includeGlobal, boolean includeDiscoverable, boolean includeExistingAccess) {
		String enginePrefix = "ENGINE__";
		String enginePermissionPrefix = "ENGINEPERMISSION__";
		String groupEnginePermissionPrefix = "GROUPENGINEPERMISSION__";
		
		Collection<String> userIds = getUserFiltersQs(user);
		
		SelectQueryStruct qs1 = new SelectQueryStruct();
		// selectors
		qs1.addSelector(new QueryColumnSelector(enginePrefix + "ENGINEID", "database_id"));
		// filters
		OrQueryFilter orFilter = new OrQueryFilter();
		if(includeGlobal) {
			orFilter.addFilter(SimpleQueryFilter.makeColToValFilter(enginePrefix + "GLOBAL", "==", true, PixelDataType.BOOLEAN));
		}
		if(includeDiscoverable) {
			orFilter.addFilter(SimpleQueryFilter.makeColToValFilter(enginePrefix + "DISCOVERABLE", "==", true, PixelDataType.BOOLEAN));
		}
		String existingAccessComparator = "==";
		if(!includeExistingAccess) {
			existingAccessComparator = "!=";
		}
		if(!includeExistingAccess && !includeDiscoverable) {
			throw new IllegalArgumentException("Fitler combinations can result in ids that the user does not have access to. Please adjust your parameters");
		}
		{
			// user access
			SelectQueryStruct qs2 = new SelectQueryStruct();
			qs2.addSelector(new QueryColumnSelector(enginePermissionPrefix + "ENGINEID", "ENGINEID"));
			qs2.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(enginePermissionPrefix + "USERID", "==", userIds));
			orFilter.addFilter(SimpleQueryFilter.makeColToSubQuery(enginePrefix + "ENGINEID", existingAccessComparator, qs2));
		}
		{
			// filter on groups
			OrQueryFilter groupEngineOrFilters = new OrQueryFilter();
			List<AuthProvider> logins = user.getLogins();
			for(AuthProvider login : logins) {
				if(user.getAccessToken(login).getUserGroups().isEmpty()) {
					continue;
				}
				
				AndQueryFilter andFilter = new AndQueryFilter();
				andFilter.addFilter(SimpleQueryFilter.makeColToValFilter(groupEnginePermissionPrefix + "TYPE", "==", user.getAccessToken(login).getUserGroupType()));
				andFilter.addFilter(SimpleQueryFilter.makeColToValFilter(groupEnginePermissionPrefix + "ID", "==", user.getAccessToken(login).getUserGroups()));
				groupEngineOrFilters.addFilter(andFilter);
			}
			
			if (!groupEngineOrFilters.isEmpty()) {
				SelectQueryStruct qs3 = new SelectQueryStruct();
				qs3.addSelector(new QueryColumnSelector(groupEnginePermissionPrefix + "ENGINEID", "ENGINEID"));
				qs3.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.MIN, groupEnginePermissionPrefix + "PERMISSION", "PERMISSION"));
				qs3.addExplicitFilter(groupEngineOrFilters);

				orFilter.addFilter(SimpleQueryFilter.makeColToSubQuery(enginePrefix + "ENGINEID", existingAccessComparator, qs3));
			}
		}
		
		qs1.addExplicitFilter(orFilter);

		return QueryExecutionUtility.flushToListString(securityDb, qs1);
	}
	
    /**
     * Get all the available engine metadata and their counts for given keys
     * @param engineFilters
     * @param metaKey
     * @return
     */
    public static List<Map<String, Object>> getAvailableMetaValues(List<String> engineFilters, List<String> metaKeys) {
        SelectQueryStruct qs = new SelectQueryStruct();
        // selectors
        qs.addSelector(new QueryColumnSelector("ENGINEMETA__METAKEY"));
        qs.addSelector(new QueryColumnSelector("ENGINEMETA__METAVALUE"));
        QueryFunctionSelector fSelector = new QueryFunctionSelector();
        fSelector.setAlias("count");
        fSelector.setFunction(QueryFunctionHelper.COUNT);
        fSelector.addInnerSelector(new QueryColumnSelector("ENGINEMETA__METAVALUE"));
        qs.addSelector(fSelector);
        // filters
        qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEMETA__METAKEY", "==", metaKeys));
        if(engineFilters != null && !engineFilters.isEmpty()) {
            qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEMETA__ENGINEID", "==", engineFilters));
        }
        // group
        qs.addGroupBy(new QueryColumnSelector("ENGINEMETA__METAKEY"));
        qs.addGroupBy(new QueryColumnSelector("ENGINEMETA__METAVALUE"));
        
        return QueryExecutionUtility.flushRsToMap(securityDb, qs);
    }
	
	/**
	 * Get all user database and database ids regardless of it being hidden or not 
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
		
		List<Map<String, Object>> databaseMap = QueryExecutionUtility.flushRsToMap(securityDb, qs2);
		databaseMap.addAll(allGlobalEnginesMap);
		return databaseMap;
	}

	/**
	 * 
	 * @return
	 */
	public static List<Map<String, Object>> getAllDatabaseList() {
		return getAllDatabaseList(null, null, null, null, null);
	}
	
	/**
	 * Get the database information
	 * @param databaseFilter
	 * @return
	 */
	public static List<Map<String, Object>> getAllDatabaseList(String databaseFilter) {
		List<String> filters = null;
		if(databaseFilter != null && !databaseFilter.isEmpty()) {
			filters = new ArrayList<>();
			filters.add(databaseFilter);
		}
		return getAllDatabaseList(filters, null, null, null, null);
	}
	
	/**
	 * Get the database information
	 * @param databaseFilter
	 * @return
	 */
	public static List<Map<String, Object>> getAllDatabaseList(List<String> databaseFilters) {
		return getAllDatabaseList(databaseFilters, null, null, null, null);
	}
	
	/**
	 * Get database information
	 * @param databaseFilters
	 * @param engineMetadataFilter
	 * @param searchTerm
	 * @param limit
	 * @param offset
	 * @return
	 */
	public static List<Map<String, Object>> getAllDatabaseList(List<String> databaseFilters, Map<String, Object> engineMetadataFilter,
			String searchTerm, String limit, String offset) {
		
		boolean hasSearchTerm = searchTerm != null && !(searchTerm=searchTerm.trim()).isEmpty();
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID", "app_id"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINENAME", "app_name"));
		qs.addSelector(new QueryColumnSelector("ENGINE__TYPE", "app_type"));
		qs.addSelector(new QueryColumnSelector("ENGINE__COST", "app_cost"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID", "database_id"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINENAME", "database_name"));
		qs.addSelector(new QueryColumnSelector("ENGINE__TYPE", "database_type"));
		qs.addSelector(new QueryColumnSelector("ENGINE__COST", "database_cost"));
		qs.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.LOWER, "ENGINE__ENGINENAME", "low_database_name"));
		if(databaseFilters != null && !databaseFilters.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINEID", "==", databaseFilters));
		}
		// optional word filter on the engine name
		if(hasSearchTerm) {
			securityDb.getQueryUtil().appendSearchRegexFilter(qs, "ENGINE__ENGINENAME", searchTerm);
		}
		// filtering by enginemeta key-value pairs (i.e. <tag>:value): for each pair, add in-filter against engineids from subquery
		if (engineMetadataFilter!=null && !engineMetadataFilter.isEmpty()) {
			for (String k : engineMetadataFilter.keySet()) {
				SelectQueryStruct subQs = new SelectQueryStruct();
				subQs.addSelector(new QueryColumnSelector("ENGINEMETA__ENGINEID"));
				subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEMETA__METAKEY", "==", k));
				subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEMETA__METAVALUE", "==", engineMetadataFilter.get(k)));
				qs.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery("ENGINE__ENGINEID", "==", subQs));
			}
		}
		qs.addRelation("ENGINE", "ENGINEPERMISSION", "left.outer.join");
		// add the sort
		qs.addOrderBy(new QueryColumnOrderBySelector("low_database_name"));
		
		Long long_limit = -1L;
		Long long_offset = -1L;
		if(limit != null && !limit.trim().isEmpty()) {
			long_limit = Long.parseLong(limit);
		}
		if(offset != null && !offset.trim().isEmpty()) {
			long_offset = Long.parseLong(offset);
		}
		qs.setLimit(long_limit);
		qs.setOffSet(long_offset);
		
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}
	
	/**
	 * Get the list of the database information that the user has access to
	 * @param userId
	 * @return
	 */
	public static List<Map<String, Object>> getUserDatabaseList(User user, String databaseFilter) {
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
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID", "database_id"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINENAME", "database_name"));
		qs.addSelector(new QueryColumnSelector("ENGINE__TYPE", "database_type"));
		qs.addSelector(new QueryColumnSelector("ENGINE__COST", "database_cost"));
		qs.addSelector(new QueryColumnSelector("ENGINE__DISCOVERABLE", "database_discoverable"));
		qs.addSelector(new QueryColumnSelector("ENGINE__GLOBAL", "database_global"));
		QueryFunctionSelector fun = new QueryFunctionSelector();
		fun.setFunction(QueryFunctionHelper.LOWER);
		fun.addInnerSelector(new QueryColumnSelector("ENGINE__ENGINENAME"));
		fun.setAlias("low_database_name");
		qs.addSelector(fun);
		if(databaseFilter != null && !databaseFilter.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINEID", "==", databaseFilter));
		}
		{
			OrQueryFilter orFilter = new OrQueryFilter();
			orFilter.addFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__GLOBAL", "==", true, PixelDataType.BOOLEAN));
			orFilter.addFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__DISCOVERABLE", "==", Arrays.asList(true, null), PixelDataType.BOOLEAN));
			orFilter.addFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", getUserFiltersQs(user)));
			qs.addExplicitFilter(orFilter);
		}
		qs.addRelation("ENGINE", "ENGINEPERMISSION", "left.outer.join");
		qs.addOrderBy(new QueryColumnOrderBySelector("low_database_name"));
		
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}
	
	/**
	 * Get the list of the database information that the user has access to
	 * @param user
	 * @param dbTypeFilter
	 * @return
	 */
	public static List<Map<String, Object>> getUserDatabaseList(User user, List<String> dbTypeFilter) {
		Collection<String> userIds = getUserFiltersQs(user);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID", "app_id"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINENAME", "app_name"));
		qs.addSelector(new QueryColumnSelector("ENGINE__TYPE", "app_type"));
		qs.addSelector(new QueryColumnSelector("ENGINE__COST", "app_cost"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID", "database_id"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINENAME", "database_name"));
		qs.addSelector(new QueryColumnSelector("ENGINE__TYPE", "database_type"));
		qs.addSelector(new QueryColumnSelector("ENGINE__COST", "database_cost"));
		QueryFunctionSelector fun = new QueryFunctionSelector();
		fun.setFunction(QueryFunctionHelper.LOWER);
		fun.addInnerSelector(new QueryColumnSelector("ENGINE__ENGINENAME"));
		fun.setAlias("low_database_name");
		qs.addSelector(fun);
		if(dbTypeFilter != null && !dbTypeFilter.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__TYPE", "==", dbTypeFilter));
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
		qs.addOrderBy(new QueryColumnOrderBySelector("low_database_name"));
		
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}
	
	/**
	 * Get the list of the database information that the user has access to
	 * @param userId
	 * @return
	 */
	public static List<Map<String, Object>> getDiscoverableDatabaseList(String databaseFilter) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID", "database_id"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINENAME", "database_name"));
		qs.addSelector(new QueryColumnSelector("ENGINE__TYPE", "database_type"));
		qs.addSelector(new QueryColumnSelector("ENGINE__COST", "database_cost"));
		QueryFunctionSelector fun = new QueryFunctionSelector();
		fun.setFunction(QueryFunctionHelper.LOWER);
		fun.addInnerSelector(new QueryColumnSelector("ENGINE__ENGINENAME"));
		fun.setAlias("low_database_name");
		qs.addSelector(fun);
		if(databaseFilter != null && !databaseFilter.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINEID", "==", databaseFilter));
		}
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__DISCOVERABLE", "==", true, PixelDataType.BOOLEAN));
		qs.addOrderBy(new QueryColumnOrderBySelector("low_database_name"));
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}
	
	/**
	 * Get the list of the database information that the user does not have access to, but is discoverable
	 * @param user
	 * @param databaseFilters
	 * @param engineMetadataFilter
	 * @param searchTerm
	 * @param limit
	 * @param offset
	 * @return
	 */
	public static List<Map<String, Object>> getUserDiscoverableDatabaseList(User user, 
			 List<String> databaseFilters,
			Map<String, Object> engineMetadataFilter, 
			String searchTerm, String limit, String offset) {
		Collection<String> userIds = getUserFiltersQs(user);
		
		boolean hasSearchTerm = searchTerm != null && !(searchTerm=searchTerm.trim()).isEmpty();
		
		SelectQueryStruct qs1 = new SelectQueryStruct();
		// selectors
		qs1.addSelector(new QueryColumnSelector("ENGINE__ENGINEID", "database_id"));
		qs1.addSelector(new QueryColumnSelector("ENGINE__ENGINENAME", "database_name"));
		qs1.addSelector(new QueryColumnSelector("ENGINE__TYPE", "database_type"));
		qs1.addSelector(new QueryColumnSelector("ENGINE__COST", "database_cost"));
		qs1.addSelector(new QueryColumnSelector("ENGINE__DISCOVERABLE", "database_discoverable"));
		qs1.addSelector(new QueryColumnSelector("ENGINE__GLOBAL", "database_global"));
		qs1.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.LOWER, "ENGINE__ENGINENAME", "low_database_name"));
		// only care about discoverable engines
		qs1.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__DISCOVERABLE", "==", true, PixelDataType.BOOLEAN));
		qs1.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__GLOBAL", "==", false, PixelDataType.BOOLEAN));
		// remove user permission access
		{
			SelectQueryStruct subQsUser = new SelectQueryStruct();
			subQsUser.addSelector(new QueryColumnSelector("ENGINEPERMISSION__ENGINEID"));
			subQsUser.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", userIds));
			qs1.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery("ENGINE__ENGINEID", "!=", subQsUser));
		}
		{
			// remove group permission access
			SelectQueryStruct subQsGroup = new SelectQueryStruct();
			subQsGroup.addSelector(new QueryColumnSelector("GROUPENGINEPERMISSION__ENGINEID"));
			OrQueryFilter orFilter = new OrQueryFilter();
			List<AuthProvider> logins = user.getLogins();
			for(AuthProvider login : logins) {
				if(user.getAccessToken(login).getUserGroups().isEmpty()) {
					continue;
				}
				AndQueryFilter andFilter = new AndQueryFilter();
				andFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPENGINEPERMISSION__TYPE", "==", user.getAccessToken(login).getUserGroupType()));
				andFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPENGINEPERMISSION__ID", "==", user.getAccessToken(login).getUserGroups()));
				orFilter.addFilter(andFilter);
			}
			if (!orFilter.isEmpty()) {
				subQsGroup.addExplicitFilter(orFilter);
				qs1.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery("ENGINE__ENGINEID", "!=", subQsGroup));
			}
		}
		// filters
		if(databaseFilters != null && !databaseFilters.isEmpty()) {
			qs1.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINEID", "==", databaseFilters));
		}
		// optional word filter on the engine name
		if(hasSearchTerm) {
			securityDb.getQueryUtil().appendSearchRegexFilter(qs1, "ENGINE__ENGINENAME", searchTerm);
		}
		// filtering by enginemeta key-value pairs (i.e. <tag>:value): for each pair, add in-filter against engineids from subquery
		if (engineMetadataFilter!=null && !engineMetadataFilter.isEmpty()) {
			for (String k : engineMetadataFilter.keySet()) {
				SelectQueryStruct subQs = new SelectQueryStruct();
				subQs.addSelector(new QueryColumnSelector("ENGINEMETA__ENGINEID"));
				subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEMETA__METAKEY", "==", k));
				subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEMETA__METAVALUE", "==", engineMetadataFilter.get(k)));
				qs1.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery("ENGINE__ENGINEID", "==", subQs));
			}
		}
		
		// add the sort
		qs1.addOrderBy(new QueryColumnOrderBySelector("low_database_name"));
		
		Long long_limit = -1L;
		Long long_offset = -1L;
		if(limit != null && !limit.trim().isEmpty()) {
			long_limit = Long.parseLong(limit);
		}
		if(offset != null && !offset.trim().isEmpty()) {
			long_offset = Long.parseLong(offset);
		}
		qs1.setLimit(long_limit);
		qs1.setOffSet(long_offset);

		return QueryExecutionUtility.flushRsToMap(securityDb, qs1);
	}

	
	/**
	 * Get user databases + global databases 
	 * @param userId
	 * @return
	 */
	public static List<String> getFullUserDatabaseIds(User user) {
//		String userFilters = getUserFilters(user);
//		String query = "SELECT DISTINCT ENGINEID FROM ENGINEPERMISSION WHERE USERID IN " + userFilters;
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__ENGINEID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", getUserFiltersQs(user)));
		List<String> databaseList = QueryExecutionUtility.flushToListString(securityDb, qs);
		databaseList.addAll(SecurityDatabaseUtils.getGlobalDatabaseIds());
		return databaseList.stream().distinct().sorted().collect(Collectors.toList());
	}
	
	/**
	 * Get the visual user databases
	 * @param userId
	 * @return
	 */
	public static List<String> getVisibleUserDatabaseIds(User user) {
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
	 * 
	 * @param metakey
	 * @return
	 */
	public static List<Map<String, Object>> getMetakeyOptions(String metakey) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINEMETAKEYS__METAKEY", "metakey"));
		qs.addSelector(new QueryColumnSelector("ENGINEMETAKEYS__SINGLEMULTI", "single_multi"));
		qs.addSelector(new QueryColumnSelector("ENGINEMETAKEYS__DISPLAYORDER", "display_order"));
		qs.addSelector(new QueryColumnSelector("ENGINEMETAKEYS__DISPLAYOPTIONS", "display_options"));
		if (metakey != null && !metakey.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEMETAKEYS__METAKEY", "==", metakey));
		}
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}
	
	/**
	 * 
	 * @param metaoptions
	 * @return
	 */
	public static boolean updateMetakeyOptions(List<Map<String,Object>> metaoptions) {
		boolean valid = false;
		String[] colNames = new String[]{Constants.METAKEY, Constants.SINGLE_MULTI, Constants.DISPLAY_ORDER, Constants.DISPLAY_OPTIONS};
        PreparedStatement insertPs = null;
        String tableName = "ENGINEMETAKEYS";
        try {
			// first truncate table clean 
			String truncateSql = "DELETE FROM " + tableName + " WHERE 1=1";
			securityDb.removeData(truncateSql);
			insertPs = securityDb.getPreparedStatement(RdbmsQueryBuilder.createInsertPreparedStatementString(tableName, colNames));
			// then insert latest options
			for (int i = 0; i < metaoptions.size(); i++) {
				insertPs.setString(1, (String) metaoptions.get(i).get("metakey"));
				insertPs.setString(2, (String) metaoptions.get(i).get("singlemulti"));
				insertPs.setInt(3, ((Number) metaoptions.get(i).get("order")).intValue());
				insertPs.setString(4, (String) metaoptions.get(i).get("displayoptions"));
				insertPs.addBatch();
			}
			insertPs.executeBatch();
			valid = true;
        } catch (SQLException e) {
        	logger.error(Constants.STACKTRACE, e);
        } finally {
        	if(insertPs != null) {
				try {
					insertPs.close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
				if(securityDb.isConnectionPooling()) {
					try {
						insertPs.getConnection().close();
					} catch (SQLException e) {
						logger.error(Constants.STACKTRACE, e);
					}
				}
			}
        }
		return valid;
	}
	
}
