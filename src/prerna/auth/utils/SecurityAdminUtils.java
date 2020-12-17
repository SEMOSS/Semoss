package prerna.auth.utils;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessPermission;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.ds.util.RdbmsQueryBuilder;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.InsightAdministrator;
import prerna.query.querystruct.AbstractQueryStruct.QUERY_STRUCT_TYPE;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.query.querystruct.update.UpdateQueryStruct;
import prerna.query.querystruct.update.UpdateSqlInterpreter;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.PixelDataType;
import prerna.util.Constants;
import prerna.util.QueryExecutionUtility;
import prerna.util.Utility;

public class SecurityAdminUtils extends AbstractSecurityUtils {

	private static SecurityAdminUtils instance = new SecurityAdminUtils();
	private static final Logger logger = LogManager.getLogger(SecurityAdminUtils.class);

	private SecurityAdminUtils() {
		
	}
	
	public static SecurityAdminUtils getInstance(User user) {
		if(user == null) {
			return null;
		}
		if(userIsAdmin(user)) {
			return instance;
		}
		return null;
	}

	/**
	 * Check if the user is an admin
	 * @param userId	String representing the id of the user to check
	 */
	public static Boolean userIsAdmin(User user) {
//		String userFilters = getUserFilters(user);
//		String query = "SELECT * FROM USER WHERE ADMIN=TRUE AND ID IN " + userFilters + " LIMIT 1;";
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("USER__ID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USER__ID", "==", getUserFiltersQs(user)));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USER__ADMIN", "==", true, PixelDataType.BOOLEAN));
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
	
	/*
	 * all other methods should be on the instance
	 * so that we cannot bypass security easily
	 */
	
	/**
	 * Get all database users
	 * @param User
	 * @return
	 * @throws IllegalArgumentException
	 */
	public List<Map<String, Object>> getAllUsers() throws IllegalArgumentException{
//		String query = "SELECT ID, NAME, USERNAME, EMAIL, TYPE, ADMIN, PUBLISHER FROM USER ORDER BY NAME, TYPE";
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("USER__ID"));
		qs.addSelector(new QueryColumnSelector("USER__NAME"));
		qs.addSelector(new QueryColumnSelector("USER__USERNAME"));
		qs.addSelector(new QueryColumnSelector("USER__EMAIL"));
		qs.addSelector(new QueryColumnSelector("USER__TYPE"));
		qs.addSelector(new QueryColumnSelector("USER__ADMIN"));
		qs.addSelector(new QueryColumnSelector("USER__PUBLISHER"));
		qs.addOrderBy(new QueryColumnOrderBySelector("USER__NAME"));
		qs.addOrderBy(new QueryColumnOrderBySelector("USER__TYPE"));
		return getSimpleQuery(qs);
	}
	
	/**
	 * Get all user databases
	 * @param userId
	 * @return
	 * @throws IllegalArgumentException
	 */
	public List<Map<String, Object>> getAllUserDbs(String userId) throws IllegalArgumentException{
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__USERID", "user_id"));
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__PERMISSION", "app_permission"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID", "app_id"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINENAME", "app_name"));
		qs.addSelector(new QueryColumnSelector("PERMISSION__NAME", "app_permission"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", userId));
		qs.addRelation("ENGINEPERMISSION", "ENGINE", "inner.join");
		qs.addRelation("ENGINEPERMISSION", "PERMISSION", "inner.join");

		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}
	
	/**
	 * Get all user databases
	 * @param userId
	 * @return
	 * @throws IllegalArgumentException
	 */
	public List<Map<String, Object>> getAllUserInsightAccess(String appId, String userId) throws IllegalArgumentException{
		String query = "SELECT DISTINCT " 
				+ "INSIGHT.INSIGHTID AS \"insight_id\", "
				+ "INSIGHT.INSIGHTNAME AS \"insight_name\", " 
				+ "INSIGHT.GLOBAL AS \"insight_public\", " 
				+ "INSIGHT.ENGINEID AS \"app_id\", " 
				+ "SUB_Q.NAME AS \"app_permission\", " 
				+ "SUB_Q.USERID AS \"user_id\" " 
				+ "FROM INSIGHT LEFT OUTER JOIN ( "
					+ "SELECT USERINSIGHTPERMISSION.INSIGHTID, "
						+ "PERMISSION.NAME, "
						+ "USERINSIGHTPERMISSION.USERID "
						+ "FROM USERINSIGHTPERMISSION "
						+ "INNER JOIN PERMISSION on USERINSIGHTPERMISSION.PERMISSION=PERMISSION.ID "
						+ "WHERE USERINSIGHTPERMISSION.ENGINEID = '" + appId + "' AND USERINSIGHTPERMISSION.USERID = '" + userId + "'" 
					+ ") AS SUB_Q ON SUB_Q.INSIGHTID = INSIGHT.INSIGHTID "
				+ "WHERE INSIGHT.ENGINEID = '" + appId + "' ORDER BY INSIGHT.INSIGHTNAME";
		
		HardSelectQueryStruct qs = new HardSelectQueryStruct();
		qs.setQuery(query);
		qs.setQsType(QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY);
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}
	
	/**
	 * Update user information.
	 * @param adminId
	 * @param userInfo
	 * @return
	 * @throws IllegalArgumentException
	 */
	public boolean editUser(Map<String, Object> userInfo) {
		String userId = userInfo.get("id") != null ? userInfo.get("id").toString() : "";
		String username = userInfo.get("username") != null ? userInfo.get("username").toString() : "";
		String email = userInfo.get("email") != null ? userInfo.get("email").toString() : "";
		String password = userInfo.get("password") != null ? userInfo.get("password").toString() : "";
		String name = userInfo.get("name") != null ? userInfo.get("name").toString() : "";
		String newUserId = userInfo.get("newId") != null ? userInfo.get("newId").toString() : "";
		String newUsername = userInfo.get("newUsername") != null ? userInfo.get("newUsername").toString() : "";
		String newEmail = userInfo.get("newEmail") != null ? userInfo.get("newEmail").toString() : "";
		String adminChange = userInfo.get("admin") != null ? userInfo.get("admin").toString() : "";
		boolean admin = false;
		String publisherChange = userInfo.get("publisher") != null ? userInfo.get("publisher").toString() : "";
		boolean publisher = false;

		// always lower case emails
		if(email != null) {
			email = email.toLowerCase();
		}

		// cannot edit a user to match another user when native... would cause some serious issues :/
		boolean isNative = SecurityQueryUtils.isUserType(userId, AuthProvider.NATIVE);
		if(!isNative) {
			throw new IllegalArgumentException("The user is not a NATIVE user");
		}
		if(!SecurityQueryUtils.checkUserExist(username, email)){
			throw new IllegalArgumentException("The user name or email does not exist");
		}
		
		// validate new inputs and insert into selectors and values to use for update
		List<IQuerySelector> selectors = new Vector<>();
		List<Object> values = new Vector<>();
		// check new userID
		if(newUserId != null && !newUserId.isEmpty()) {
			boolean userExists = SecurityQueryUtils.checkUserExist(newUserId);
			if(userExists) {
				throw new IllegalArgumentException("The user already exists");
			}
			selectors.add(new QueryColumnSelector("USER__ID"));
			values.add(newUserId);
		}
	
		String error = "";
		if(newEmail != null && !newEmail.isEmpty()){
			newEmail = newEmail.toLowerCase();
			error = validEmail(newEmail);
			boolean userEmailExists = SecurityQueryUtils.checkUserEmailExist(newEmail);
			if(userEmailExists) {
				throw new IllegalArgumentException("The user email already exists");
			}
			selectors.add(new QueryColumnSelector("USER__EMAIL"));
			values.add(newEmail);
		}
		if(newUsername != null && !newUsername.isEmpty()) {
			boolean usernameExists = SecurityQueryUtils.checkUsernameExist(newUsername);
			if(usernameExists) {
				throw new IllegalArgumentException("The username already exists");
			}
			selectors.add(new QueryColumnSelector("USER__USERNAME"));
			values.add(newUsername);
		}
		if(password != null && !password.isEmpty()){
            error += validPassword(password);
            if(error.isEmpty()){
                String newSalt = SecurityQueryUtils.generateSalt();
    			selectors.add(new QueryColumnSelector("USER__PASSWORD"));
    			values.add(SecurityQueryUtils.hash(password, newSalt));
    			selectors.add(new QueryColumnSelector("USER__SALT"));
    			values.add(newSalt);
            }
        }
		if(name != null && !name.isEmpty()) {
			selectors.add(new QueryColumnSelector("USER__NAME"));
			values.add(name);
		}
		if(adminChange != null && !adminChange.isEmpty()) {
			admin = Boolean.parseBoolean(adminChange);
			selectors.add(new QueryColumnSelector("USER__ADMIN"));
			values.add(admin);
		}
		if(publisherChange != null && !publisherChange.isEmpty()) {
			publisher = Boolean.parseBoolean(publisherChange);
			selectors.add(new QueryColumnSelector("USER__PUBLISHER"));
			values.add(publisher);
		}
		if(error != null && !error.isEmpty()) {
			throw new IllegalArgumentException(error);
		}

		UpdateQueryStruct qs = new UpdateQueryStruct();
		qs.setEngine(securityDb);
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USER__ID", "==", userId));
		qs.setSelectors(selectors);
		qs.setValues(values);
		
		UpdateSqlInterpreter updateInterp = new UpdateSqlInterpreter(qs);
		String updateQ = updateInterp.composeQuery();
		Statement stmt = securityDb.execUpdateAndRetrieveStatement(updateQ, true);
		if(stmt != null){
			securityDb.commit();
			return true;
		}
		
		return false;
    }
	
	/**
	 * Delete a user and all its relationships.
	 * @param userId
	 * @param userDelete
	 */
	public boolean deleteUser(String userToDelete) {
//		List<String> groups = SecurityQueryUtils.getGroupsOwnedByUser(userToDelete);
//		for(String groupId : groups){
//			removeGroup(userToDelete, groupId);
//		}
		String query = "DELETE FROM ENGINEPERMISSION WHERE USERID = '?1'; DELETE FROM USERINSIGHTPERMISSION WHERE USERID = '?1'; DELETE FROM USER WHERE ID = '?1';";
		query = query.replace("?1", RdbmsQueryBuilder.escapeForSQLStatement(userToDelete));
		securityDb.execUpdateAndRetrieveStatement(query, true);
		securityDb.commit();
		return true;
	}
	

	/**
	 * Set the user's publishing rights
	 * @param userId
	 * @param isPublisher
	 */
	public void setUserPublisher(String userId, boolean isPublisher) {
		String query = "UPDATE USER SET PUBLISHER=" + isPublisher + " WHERE ID ='" + RdbmsQueryBuilder.escapeForSQLStatement(userId) + "';";
		try {
			securityDb.insertData(query);
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occured setting this insight global");
		}
	}
	
	///////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////////////

	/*
	 * METHODS FOR APP AUTHORIZATION THAT ARE AT THE ADMIN LEVEL
	 */
	
	/**
	 * Get all databases options
	 * @param usersId
	 * @param isAdmin
	 * @return
	 */
	public List<Map<String, Object>> getAllDatabaseSettings() {
//		String query = "SELECT DISTINCT "
//				+ "ENGINE.ENGINEID as \"app_id\", "
//				+ "ENGINE.ENGINENAME as \"app_name\", "
//				+ "LOWER(ENGINE.ENGINENAME) as \"low_app_name\", "
//				+ "ENGINE.GLOBAL as \"app_global\" "
//				+ "FROM ENGINE "
//				+ "ORDER BY LOWER(ENGINE.ENGINENAME)";
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID", "app_id"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINENAME", "app_name"));
		QueryFunctionSelector fun = new QueryFunctionSelector();
		fun.addInnerSelector(new QueryColumnSelector("ENGINE__ENGINENAME"));
		fun.setFunction(QueryFunctionHelper.LOWER);
		fun.setAlias("low_app_name");
		qs.addSelector(fun);
		qs.addSelector(new QueryColumnSelector("ENGINE__GLOBAL", "app_global"));
		qs.addOrderBy(new QueryColumnOrderBySelector("low_app_name"));
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}
	
	/**
	 * Set if engine should be public or not
	 * @param engineId
	 * @param isPublic
	 */
	public boolean setAppGlobal(String engineId, boolean isPublic) {
		String query = "UPDATE ENGINE SET GLOBAL = " + isPublic + " WHERE ENGINEID ='" + RdbmsQueryBuilder.escapeForSQLStatement(engineId)  + "';";
		securityDb.execUpdateAndRetrieveStatement(query, true);
		securityDb.commit();
		return true;
	}
	
	/**
	 * Get all the users for an app
	 * @param engineId
	 * @return
	 */
	public List<Map<String, Object>> getAppUsers(String engineId) {
		return SecurityQueryUtils.getFullDatabaseOwnersAndEditors(engineId);
	}
	
	/**
	 * 
	 * @param newUserId
	 * @param engineId
	 * @param permission
	 * @return
	 */
	public void addAppUser(String newUserId, String engineId, String permission) {
		// make sure user doesn't already exist for this app
		if(SecurityAppUtils.getUserAppPermission(newUserId, engineId) != null) {
			// that means there is already a value
			throw new IllegalArgumentException("This user already has access to this app. Please edit the existing permission level.");
		}
		
		String query = "INSERT INTO ENGINEPERMISSION (USERID, ENGINEID, PERMISSION, VISIBILITY) VALUES('"
				+ RdbmsQueryBuilder.escapeForSQLStatement(newUserId) + "', '"
				+ RdbmsQueryBuilder.escapeForSQLStatement(engineId) + "', "
				+ AccessPermission.getIdByPermission(permission) + ", "
				+ "TRUE);";

		try {
			securityDb.insertData(query);
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occured adding user permissions for this app");
		}
	}
	
	/** 
	 * give user permission for all the apps
	 * @param userId
	 * @param permission
	 */
	public void grantAllApps(String userId, String permission) {
		// delete all previous permissions for the user
		String query = "DELETE FROM ENGINEPERMISSION WHERE " + "USERID='"
				+ RdbmsQueryBuilder.escapeForSQLStatement(userId) + "';";
		String insertQuery = "INSERT INTO ENGINEPERMISSION (USERID, ENGINEID, VISIBILITY, PERMISSION) VALUES('"
				+ RdbmsQueryBuilder.escapeForSQLStatement(userId) + "', ?, " + "TRUE, "
				+ AccessPermission.getIdByPermission(permission) + ");";
		PreparedStatement ps = null;
		try {
			securityDb.insertData(query);
			ps = securityDb.getPreparedStatement(insertQuery);
			// add new permission for all engines
			List<String> appIds = SecurityQueryUtils.getEngineIds();
			for (String appId : appIds) {
				ps.setString(1, appId);
				ps.addBatch();
			}
			ps.executeBatch();
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occured granting the user permission for all the apps");
		} finally {
			if (ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	/** 
	 * give new users access to an app
	 * @param appId
	 * @param permission
	 */
	public void grantNewUsersAppAccess(String appId, String permission) {
		String query = "INSERT INTO ENGINEPERMISSION (USERID, ENGINEID, PERMISSION, VISIBILITY) VALUES(?, '"
				+ RdbmsQueryBuilder.escapeForSQLStatement(appId) + "', "
				+ AccessPermission.getIdByPermission(permission) + ", " + "TRUE);";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			// get users with no access to app
			List<Map<String, Object>> users = getAppUsersNoCredentials(appId);
			for (Map<String, Object> userMap : users) {
				String userId = (String) userMap.get("id");
				ps.setString(1, userId);
				ps.addBatch();
			}
			ps.executeBatch();
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occured adding user permissions for this app");
		} finally {
			if(ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	/**
	 * Give the user permission for all the insights in an app
	 * @param appId
	 * @param userId
	 * @param permission
	 */
	public void grantAllAppInsights(String appId, String userId, String permission) {
		// delete all previous permissions for the user
		String query = "DELETE FROM USERINSIGHTPERMISSION WHERE " + "USERID='"
				+ RdbmsQueryBuilder.escapeForSQLStatement(userId) + "' AND ENGINEID = '"
				+ RdbmsQueryBuilder.escapeForSQLStatement(appId) + "';";

		String insertQuery = "INSERT INTO USERINSIGHTPERMISSION (USERID, ENGINEID, INSIGHTID, PERMISSION) VALUES('"
				+ RdbmsQueryBuilder.escapeForSQLStatement(userId) + "', '"
				+ RdbmsQueryBuilder.escapeForSQLStatement(appId) + "', ?, "
				+ AccessPermission.getIdByPermission(permission) + ");";

		PreparedStatement ps = null;
		try {
			securityDb.insertData(query);

			ps = securityDb.getPreparedStatement(insertQuery);
			// add new permission for all insights
			List<String> insightIds = getAllInsights(appId);
			for (String x : insightIds) {
				ps.setString(1, x);
				ps.addBatch();
			}
			ps.executeBatch();
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occured granting the user permission for all the apps");
		} finally {
			if (ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	/**
	 * 
	 * @param existingUserId
	 * @param engineId
	 * @param newPermission
	 * @return
	 */
	public void editAppUserPermission(String existingUserId, String engineId, String newPermission) {
		// make sure we are trying to edit a permission that exists
		Integer existingUserPermission = SecurityAppUtils.getUserAppPermission(existingUserId, engineId);
		if(existingUserPermission == null) {
			throw new IllegalArgumentException("Attempting to modify user permission for a user who does not currently have access to the app");
		}
		
		int newPermissionLvl = AccessPermission.getIdByPermission(newPermission);
		
		String query = "UPDATE ENGINEPERMISSION SET PERMISSION=" + newPermissionLvl
				+ " WHERE USERID='" + RdbmsQueryBuilder.escapeForSQLStatement(existingUserId) + "' "
				+ "AND ENGINEID='"	+ RdbmsQueryBuilder.escapeForSQLStatement(engineId) + "';";
		try {
			securityDb.insertData(query);
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occured adding user permissions for this app");
		}
	}
	
	/**
	 * 
	 * @param editedUserId
	 * @param engineId
	 * @return
	 */
	public void removeAppUser(String existingUserId, String engineId) {
		// make sure we are trying to edit a permission that exists
		Integer existingUserPermission = SecurityAppUtils.getUserAppPermission(existingUserId, engineId);
		if(existingUserPermission == null) {
			throw new IllegalArgumentException("Attempting to modify user permission for a user who does not currently have access to the app");
		}
		
		String query = "DELETE FROM ENGINEPERMISSION WHERE "
				+ "USERID='" + RdbmsQueryBuilder.escapeForSQLStatement(existingUserId) + "' "
				+ "AND ENGINEID='"	+ RdbmsQueryBuilder.escapeForSQLStatement(engineId) + "';";
		try {
			securityDb.insertData(query);
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occured adding user permissions for this app");
		}
	}
	
	///////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////////////

	/*
	 * METHODS FOR INSIGHT AUTHORIZATION THAT ARE AT THE ADMIN LEVEL
	 */
	
	/**
	 * 
	 * @param appId
	 * @return
	 */
	public List<Map<String, Object>> getAppInsights(String appId) {
//		String query = "SELECT ENGINEID AS \"app_id\", "
//				+ "INSIGHTID AS \"app_insight_id\", "
//				+ "INSIGHTNAME as \"name\", "
//				+ "GLOBAL as \"insight_global\", "
//				+ "EXECUTIONCOUNT as \"exec_count\", "
//				+ "CREATEDON  as \"created_on\", "
//				+ "LASTMODIFIEDON as \"\", "
//				+ "CACHEABLE as \"cacheable\" "
//				+ "FROM INSIGHT "
//				+ "WHERE ENGINEID='" + appId + "'"
//				;
//
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("INSIGHT__ENGINEID", "app_id"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__INSIGHTID", "app_insight_id"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__INSIGHTNAME", "name"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__GLOBAL", "insight_global"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__EXECUTIONCOUNT", "exec_count"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__CREATEDON", "created_on"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__LASTMODIFIEDON", "last_modified_on"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__CACHEABLE", "cacheable"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHT__ENGINEID", "==", appId));
		qs.addOrderBy(new QueryColumnOrderBySelector("INSIGHT__INSIGHTNAME"));
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}
	
	/**
	 * 
	 * @param appId
	 * @param insightIds
	 * @throws Exception 
	 */
	public void deleteAppInsights(String appId, List<String> insightIds) throws Exception {
		IEngine engine = Utility.getEngine(appId);
		InsightAdministrator admin = new InsightAdministrator(engine.getInsightDatabase());
	
		// delete from insights database
		admin.dropInsight(insightIds);

		// delete from the security database
		String insightFilters = createFilter(insightIds);
		String query = "DELETE FROM INSIGHT WHERE INSIGHTID " + insightFilters + " AND ENGINEID='" + appId + "';";
		query += "DELETE FROM USERINSIGHTPERMISSION  WHERE INSIGHTID " + insightFilters + " AND ENGINEID='" + appId + "'";
		securityDb.insertData(query);
		securityDb.commit();
	}
	
	/**
	 * Retrieve the list of users for a given insight
	 * @param appId
	 * @param insightId
	 * @return
	 * @throws IllegalAccessException
	 */
	public List<Map<String, Object>> getInsightUsers(String appId, String insightId) throws IllegalAccessException {
//		String query = "SELECT USER.ID AS \"id\", "
//				+ "USER.NAME AS \"name\", "
//				+ "PERMISSION.NAME AS \"permission\" "
//				+ "FROM USER "
//				+ "INNER JOIN USERINSIGHTPERMISSION ON (USER.ID = USERINSIGHTPERMISSION.USERID) "
//				+ "INNER JOIN PERMISSION ON (USERINSIGHTPERMISSION.PERMISSION = PERMISSION.ID) "
//				+ "WHERE USERINSIGHTPERMISSION.ENGINEID='" + appId + "'"
//				+ " AND USERINSIGHTPERMISSION.INSIGHTID='" + insightId + "'"
//				;
//		
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("USER__ID", "id"));
		qs.addSelector(new QueryColumnSelector("PERMISSION__NAME", "name"));
		qs.addSelector(new QueryColumnSelector("PERMISSION__ID", "pvalue"));
		qs.addSelector(new QueryColumnSelector("USER__EMAIL", "email"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__ENGINEID", "==", appId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__INSIGHTID", "==", insightId));
		qs.addRelation("USER", "USERINSIGHTPERMISSION", "inner.join");
		qs.addRelation("USERINSIGHTPERMISSION", "PERMISSION", "inner.join");
		qs.addOrderBy(new QueryColumnOrderBySelector("PERMISSION__ID"));
		qs.addOrderBy(new QueryColumnOrderBySelector("USER__ID"));
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}
	
	/**
	 * 
	 * @param newUserId
	 * @param engineId
	 * @param insightId
	 * @param permission
	 * @return
	 */
	public void addInsightUser(String newUserId, String engineId, String insightId, String permission) {
		// make sure user doesn't already exist for this insight
		if(SecurityInsightUtils.getUserInsightPermission(newUserId, engineId, insightId) != null) {
			// that means there is already a value
			throw new IllegalArgumentException("This user already has access to this insight. Please edit the existing permission level.");
		}
		
		String query = "INSERT INTO USERINSIGHTPERMISSION (USERID, ENGINEID, INSIGHTID, PERMISSION) VALUES('"
				+ RdbmsQueryBuilder.escapeForSQLStatement(newUserId) + "', '"
				+ RdbmsQueryBuilder.escapeForSQLStatement(engineId) + "', '"
				+ RdbmsQueryBuilder.escapeForSQLStatement(insightId) + "', "
				+ AccessPermission.getIdByPermission(permission) + ");";

		try {
			securityDb.insertData(query);
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occured adding user permissions for this insight");
		}
	}
	
	/**
	 * Add all users to an insight with permission level
	 * @param engineId
	 * @param insightId
	 * @param permission
	 * @return
	 */
	public void addAllInsightUsers(String engineId, String insightId, String permission) {
		String query = "INSERT INTO USERINSIGHTPERMISSION (USERID, ENGINEID, INSIGHTID, PERMISSION) VALUES(?,'"
				+ RdbmsQueryBuilder.escapeForSQLStatement(engineId) + "', '"
				+ RdbmsQueryBuilder.escapeForSQLStatement(insightId) + "', "
				+ AccessPermission.getIdByPermission(permission) + ");";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			if (engineId != null && permission != null) {
				List<Map<String, Object>> users = getInsightUsersNoCredentials(engineId, insightId);
				for (Map<String, Object> userMap : users) {
					String userId = (String) userMap.get("id");
					ps.setString(1, userId);
					ps.addBatch();
				}
				ps.executeBatch();
				// update existing permissions for users
				updateInsightUserPermissions(engineId, insightId, permission);
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occured adding all users for this insight");
		} finally {
			if (ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	/**
	 * 
	 * @param existingUserId
	 * @param engineId
	 * @param insightId
	 * @param newPermission
	 * @return
	 */
	public void editInsightUserPermission(String existingUserId, String engineId, String insightId, String newPermission) {
		// make sure we are trying to edit a permission that exists
		Integer existingUserPermission = SecurityInsightUtils.getUserInsightPermission(existingUserId, engineId, insightId);
		if(existingUserPermission == null) {
			throw new IllegalArgumentException("Attempting to modify user permission for a user who does not currently have access to the insight");
		}
		
		int newPermissionLvl = AccessPermission.getIdByPermission(newPermission);
		
		String query = "UPDATE USERINSIGHTPERMISSION SET PERMISSION=" + newPermissionLvl
				+ " WHERE USERID='" + RdbmsQueryBuilder.escapeForSQLStatement(existingUserId) + "' "
				+ "AND ENGINEID='"	+ RdbmsQueryBuilder.escapeForSQLStatement(engineId) + "' "
				+ "AND INSIGHTID='" + RdbmsQueryBuilder.escapeForSQLStatement(insightId) + "';";
		try {
			securityDb.insertData(query);
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occured adding user permissions for this insight");
		}
	}
	
	/**
	 * 
	 * @param editedUserId
	 * @param engineId
	 * @param insightId
	 * @return
	 */
	public void removeInsightUser(String existingUserId, String engineId, String insightId) {
		// make sure we are trying to edit a permission that exists
		Integer existingUserPermission = SecurityInsightUtils.getUserInsightPermission(existingUserId, engineId, insightId);
		if(existingUserPermission == null) {
			throw new IllegalArgumentException("Attempting to modify user permission for a user who does not currently have access to the insight");
		}
		
		String query = "DELETE FROM USERINSIGHTPERMISSION WHERE "
				+ "USERID='" + RdbmsQueryBuilder.escapeForSQLStatement(existingUserId) + "' "
				+ "AND ENGINEID='"	+ RdbmsQueryBuilder.escapeForSQLStatement(engineId) + "' "
				+ "AND INSIGHTID='" + RdbmsQueryBuilder.escapeForSQLStatement(insightId) + "';";
		try {
			securityDb.insertData(query);
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occured adding user permissions for this insight");
		}
	}
	
	/**
	 * 	
	 * @param appId
	 * @param insightId
	 * @param isPublic
	 */
	public void setInsightGlobalWithinApp(String appId, String insightId, boolean isPublic) {
		String query = "UPDATE INSIGHT SET GLOBAL=" + isPublic + " WHERE ENGINEID ='" + appId + "' AND INSIGHTID='" + insightId + "';";
		try {
			securityDb.insertData(query);
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occured setting this insight global");
		}
	}
	
	/**
	 * Returns List of users that have no access credentials to a given app
	 * @param appID
	 * @return 
	 */
	public List<Map<String, Object>> getAppUsersNoCredentials(String appId) {
		/*
		 * String Query = 
		 * "SELECT USER.ID, USER.USERNAME, USER.NAME, USER.EMAIL  FROM USER WHERE ID NOT IN 
		 * (SELECT e.USERID FROM ENGINEPERMISSION e WHERE e.ENGINEID = '"+ appID + "' e.PERMISSION IS NOT NULL);"
		 */
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("USER__ID", "id"));
		qs.addSelector(new QueryColumnSelector("USER__USERNAME", "username"));
		qs.addSelector(new QueryColumnSelector("USER__NAME", "name"));
		qs.addSelector(new QueryColumnSelector("USER__EMAIL", "email"));
		//Filter for sub-query
		{
			SelectQueryStruct subQs = new SelectQueryStruct();
			qs.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery("USER__ID", "!=", subQs));
			//Sub-query itself
			subQs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__USERID"));
			subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__ENGINEID","==",appId));
			subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__PERMISSION", "!=", null, PixelDataType.NULL_VALUE));
		}
		
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}
	
	/**
	 * Returns List of users that have no access credentials to a given insight 
	 * @param insightID
	 * @return 
	 */
	public List<Map<String, Object>> getInsightUsersNoCredentials(String appId, String insightId) {
		/*
		 * String Query = 
		 * "SELECT USER.ID, USER.USERNAME, USER.NAME, USER.EMAIL FROM USER WHERE USER.ID NOT IN 
		 * (SELECT u.USERID FROM USERINSIGHTPERMISSION u WHERE u.ENGINEID == '" + appID + "' AND u.INSIGHTID == '"+insightID +"'AND u.PERMISSION IS NOT NULL);"
		 */
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("USER__ID", "id"));
		qs.addSelector(new QueryColumnSelector("USER__USERNAME", "username"));
		qs.addSelector(new QueryColumnSelector("USER__NAME", "name"));
		qs.addSelector(new QueryColumnSelector("USER__EMAIL", "email"));
		//Filter for sub-query
		{
			SelectQueryStruct subQs = new SelectQueryStruct();
			qs.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery("USER__ID", "!=", subQs));
			//Sub-query itself
			subQs.addSelector(new QueryColumnSelector("USERINSIGHTPERMISSION__USERID"));
			subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__ENGINEID", "==", appId));
			subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__INSIGHTID", "==", insightId));
			subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__PERMISSION","!=",null,PixelDataType.NULL_VALUE));
		}
		
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}

	public void updateAppUserPermissions(String appId, String newPermission) {
		int newPermissionLvl = AccessPermission.getIdByPermission(newPermission);
		String query = "UPDATE ENGINEPERMISSION SET PERMISSION=" + newPermissionLvl 
				+ " WHERE ENGINEID='" + RdbmsQueryBuilder.escapeForSQLStatement(appId) + "';";
		try {
			securityDb.insertData(query);
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occured adding user permissions for this app");
		}
		
	}

	/**
	 * Add all users to an app with the same permission
	 * @param appId
	 * @param permission
	 */
	public void addAllAppUsers(String appId, String permission) {
		String query = "INSERT INTO ENGINEPERMISSION (USERID, ENGINEID, PERMISSION, VISIBILITY) VALUES(?, '"
				+ RdbmsQueryBuilder.escapeForSQLStatement(appId) + "', "
				+ AccessPermission.getIdByPermission(permission) + ", " + "TRUE);";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			if (appId != null && permission != null) {
				List<Map<String, Object>> users = getAppUsersNoCredentials(appId);
				for (Map<String, Object> userMap : users) {
					String userId = (String) userMap.get("id");
					ps.setString(1, userId);
					ps.addBatch();
				}
				ps.executeBatch();
				// update existing user permissions
				updateAppUserPermissions(appId, permission);
			}
		} catch (SQLException e1) {
			logger.error(Constants.STACKTRACE, e1);
			throw new IllegalArgumentException("An error occured adding user permissions for this app");
		} finally {
			if(ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	public void updateInsightUserPermissions(String appId, String insightId, String newPermission) {
		String updateQuery = "UPDATE USERINSIGHTPERMISSION SET PERMISSION = '"
				+ AccessPermission.getIdByPermission(newPermission) + "' WHERE ENGINEID ='"
				+ RdbmsQueryBuilder.escapeForSQLStatement(appId) + "' AND INSIGHTID='"
				+ RdbmsQueryBuilder.escapeForSQLStatement(insightId) + "';";
		try {
			securityDb.insertData(updateQuery);
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occured adding user permissions for this app");
		}
	}
	
	private List<String>  getAllInsights(String appId) {
		String query = "SELECT INSIGHTID FROM INSIGHT WHERE ENGINEID='" + RdbmsQueryBuilder.escapeForSQLStatement(appId) + "';";
		HardSelectQueryStruct qs = new HardSelectQueryStruct();
		qs.setQuery(query);
		return QueryExecutionUtility.flushToListString(securityDb, qs);
	}

	public void grantNewUsersInsightAccess(String appId, String insightId, String permission) {
		List<Map<String, Object>> users = getInsightUsersNoCredentials(appId, insightId);
		String insertQuery = "INSERT INTO USERINSIGHTPERMISSION (USERID, ENGINEID, INSIGHTID, PERMISSION) VALUES(?, '"
				+ RdbmsQueryBuilder.escapeForSQLStatement(appId) + "', '"
				+ RdbmsQueryBuilder.escapeForSQLStatement(insightId) + "', "
				+ AccessPermission.getIdByPermission(permission) + ");";

		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(insertQuery);
			for (Map<String, Object> userMap : users) {
				String userId = (String) userMap.get("id");
				ps.setString(1, userId);
				ps.addBatch();
			}
			ps.executeBatch();
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occured granting the user permission for all the apps");
		} finally {
			if (ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}

}
