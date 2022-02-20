package prerna.auth.utils;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessPermission;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.ds.util.RdbmsQueryBuilder;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.InsightAdministrator;
import prerna.project.api.IProject;
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
//		String query = "SELECT * FROM SMSS_USER WHERE ADMIN=TRUE AND ID IN " + userFilters + " LIMIT 1;";
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__ID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__ID", "==", getUserFiltersQs(user)));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__ADMIN", "==", true, PixelDataType.BOOLEAN));
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
//		String query = "SELECT ID, NAME, USERNAME, EMAIL, TYPE, ADMIN, PUBLISHER FROM SMSS_USER ORDER BY NAME, TYPE";
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__ID"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__NAME"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__USERNAME"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__EMAIL"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__TYPE"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__ADMIN"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__PUBLISHER"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__EXPORTER"));
		qs.addOrderBy(new QueryColumnOrderBySelector("SMSS_USER__NAME"));
		qs.addOrderBy(new QueryColumnOrderBySelector("SMSS_USER__TYPE"));
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
	 * Get all user projects
	 * @param userId
	 * @return
	 * @throws IllegalArgumentException
	 */
	public List<Map<String, Object>> getAllUserProjects(String userId) throws IllegalArgumentException{
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__USERID", "user_id"));
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__PERMISSION", "project_permission"));
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTID", "project_id"));
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTNAME", "project_name"));
		qs.addSelector(new QueryColumnSelector("PERMISSION__NAME", "project_permission"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", userId));
		qs.addRelation("PROJECTPERMISSION", "PROJECT", "inner.join");
		qs.addRelation("PROJECTPERMISSION", "PERMISSION", "inner.join");

		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}
	
	//TODO >>> Kunal: update below method	
	/**
	 * Get all user databases
	 * @param userId
	 * @return
	 * @throws IllegalArgumentException
	 */
	public List<Map<String, Object>> getAllUserInsightAccess(String projectId, String userId) throws IllegalArgumentException{
		String query = "SELECT DISTINCT " 
				+ "INSIGHT.INSIGHTID AS \"insight_id\", "
				+ "INSIGHT.INSIGHTNAME AS \"insight_name\", " 
				+ "INSIGHT.GLOBAL AS \"insight_public\", " 
				+ "INSIGHT.PROJECTID AS \"project_id\", " 
				+ "SUB_Q.NAME AS \"insight_permission\", " 
				+ "SUB_Q.USERID AS \"user_id\" " 
				+ "FROM INSIGHT LEFT OUTER JOIN ( "
					+ "SELECT USERINSIGHTPERMISSION.INSIGHTID, "
						+ "PERMISSION.NAME, "
						+ "USERINSIGHTPERMISSION.USERID "
						+ "FROM USERINSIGHTPERMISSION "
						+ "INNER JOIN PERMISSION on USERINSIGHTPERMISSION.PERMISSION=PERMISSION.ID "
						+ "WHERE USERINSIGHTPERMISSION.PROJECTID = '" + projectId + "' AND USERINSIGHTPERMISSION.USERID = '" + userId + "'" 
					+ ") AS SUB_Q ON SUB_Q.INSIGHTID = INSIGHT.INSIGHTID "
				+ "WHERE INSIGHT.PROJECTID = '" + projectId + "' ORDER BY INSIGHT.INSIGHTNAME";
		
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
		// input fields
		String userId = userInfo.get("id") != null ? userInfo.get("id").toString() : "";
		if(userId == null || userId.isEmpty()) {
			throw new NullPointerException("Must provide a unique and non-empty user id");
		}
		String password = userInfo.get("password") != null ? userInfo.get("password").toString() : "";
		String name = userInfo.get("name") != null ? userInfo.get("name").toString() : "";
		String type = userInfo.get("type") != null ? userInfo.get("type").toString() : "";
		// modified fields
		String newUserId = (String) userInfo.get("newId");
		if(newUserId != null && newUserId.trim().isEmpty()) {
			newUserId = null;
		}
		String newUsername = (String) userInfo.get("newUsername");
		if(newUsername != null && newUsername.trim().isEmpty()) {
			newUsername = null;
		}
		String newEmail = (String) userInfo.get("newEmail");
		// always lower case emails
		if(newEmail != null) {
			newEmail = newEmail.toLowerCase();
		}
		Boolean adminChange = null;
		if(userInfo.containsKey("admin")) {
			if(userInfo.get("admin") instanceof Number) {
				adminChange = ((Number) userInfo.get("admin")).intValue() == 1;
			} else {
				adminChange = Boolean.parseBoolean( userInfo.get("admin") + "");
			}
		}

		Boolean publisherChange = null;
		if(userInfo.containsKey("publisher")) {
			if(userInfo.get("publisher") instanceof Number) {
				publisherChange = ((Number) userInfo.get("publisher")).intValue() == 1;
			} else {
				publisherChange = Boolean.parseBoolean( userInfo.get("publisher") + "");
			}
		}
		
		Boolean exporterChange = Boolean.TRUE;
		if(userInfo.containsKey("exporter")) {
			if(userInfo.get("exporter") instanceof Number) {
				exporterChange = ((Number) userInfo.get("exporter")).intValue() == 1;
			} else {
				exporterChange = Boolean.parseBoolean( userInfo.get("exporter") + "");
			}
		}

		String newSalt = null;
		String newHashPass = null;
		
		// validate new inputs and insert into selectors and values to use for update
		List<IQuerySelector> selectors = new Vector<>();
		List<Object> values = new Vector<>();
		
		// cannot edit a user to match another user when native... would cause some serious issues :/
		// so we will check if you are switching to a native
		boolean isNative = false;
		if(type != null && !type.isEmpty()) {
			isNative = type.equalsIgnoreCase("NATIVE");
		} else {
			isNative = SecurityQueryUtils.isUserType(userId, AuthProvider.NATIVE);
		}
		if(isNative) {
			// username and id must match for native
			// so they should be updated together and have the same value
			if( !( (newUsername != null && newUserId != null) || (newUsername == null && newUserId == null) ) ){
				throw new IllegalArgumentException("For native users, the id and the username must be updated together and have the same value");
			}
			if(newUserId != null && newUserId.isEmpty()) {
				throw new IllegalArgumentException("For native users, the id and the username must be updated together and have the same value");
			}
			if(newUserId != null && !newUserId.equalsIgnoreCase(newUsername)) {
				throw new IllegalArgumentException("For native users, the id and the username must be updated together and have the same value");
			}
		}
		// if we are updating the user id
		// make sure the new id does not exist
		if(newUserId != null) {
			if(SecurityQueryUtils.checkUserExist(newUserId)){
				throw new IllegalArgumentException("The new user id already exists. Please enter a unique user id.");
			}
		}
		
		// check new userID
		if(newUserId != null) {
			selectors.add(new QueryColumnSelector("SMSS_USER__ID"));
			values.add(newUserId);
		}
	
		String error = "";
		if(newEmail != null && !newEmail.isEmpty()){
			try {
				validEmail(newEmail);
			} catch(Exception e) {
				logger.error(Constants.STACKTRACE, e);
				error += e.getMessage();
			}
			boolean userEmailExists = SecurityQueryUtils.checkUserEmailExist(newEmail);
			if(userEmailExists) {
				throw new IllegalArgumentException("The user email already exists");
			}
			selectors.add(new QueryColumnSelector("SMSS_USER__EMAIL"));
			values.add(newEmail);
		}
		if(newUsername != null && !newUsername.isEmpty()) {
			boolean usernameExists = SecurityQueryUtils.checkUsernameExist(newUsername);
			if(usernameExists) {
				throw new IllegalArgumentException("The username already exists");
			}
			selectors.add(new QueryColumnSelector("SMSS_USER__USERNAME"));
			values.add(newUsername);
		}
		if(password != null && !password.isEmpty()){
            try {
				validPassword(userId, AuthProvider.NATIVE, password);
			} catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
				error += e.getMessage();			
			}
            if(error.isEmpty()){
                newSalt = SecurityQueryUtils.generateSalt();
    			selectors.add(new QueryColumnSelector("SMSS_USER__PASSWORD"));
    			newHashPass = SecurityQueryUtils.hash(password, newSalt); 
    			values.add(newHashPass);
    			selectors.add(new QueryColumnSelector("SMSS_USER__SALT"));
    			values.add(newSalt);
            }
        }
		if(name != null && !name.isEmpty()) {
			selectors.add(new QueryColumnSelector("SMSS_USER__NAME"));
			values.add(name);
		}
		if(type != null && !type.isEmpty()) {
			selectors.add(new QueryColumnSelector("SMSS_USER__TYPE"));
			values.add(type);
		}
		if(adminChange != null) {
			selectors.add(new QueryColumnSelector("SMSS_USER__ADMIN"));
			values.add(adminChange);
		}
		if(publisherChange != null) {
			selectors.add(new QueryColumnSelector("SMSS_USER__PUBLISHER"));
			values.add(publisherChange);
		}
		if(exporterChange != null) {
			selectors.add(new QueryColumnSelector("SMSS_USER__EXPORTER"));
			values.add(exporterChange);
		}
		
		if(error != null && !error.isEmpty()) {
			throw new IllegalArgumentException(error);
		}

		UpdateQueryStruct qs = new UpdateQueryStruct();
		qs.setEngine(securityDb);
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__ID", "==", userId));
		qs.setSelectors(selectors);
		qs.setValues(values);
		
		UpdateSqlInterpreter updateInterp = new UpdateSqlInterpreter(qs);
		String updateQ = updateInterp.composeQuery();
		Statement stmt = securityDb.execUpdateAndRetrieveStatement(updateQ, true);
		if(stmt != null){
			securityDb.commit();
			if(isNative) {
				if(newUserId != null && !userId.equals(newUserId)) {
					// need to update the password history
					String updateQuery = "UPDATE PASSWORD_HISTORY SET USERID=? WHERE USERID=? and TYPE=?";
					PreparedStatement ps = null;
					try {
						ps = securityDb.getPreparedStatement(updateQuery);
						int parameterIndex = 1;
						ps.setString(parameterIndex++, newUserId);
						ps.setString(parameterIndex++, userId);
						ps.setString(parameterIndex++, type);
						ps.execute();
						if(!ps.getConnection().getAutoCommit()) {
							ps.getConnection().commit();
						}
					} catch (SQLException e) {
						e.printStackTrace();
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
				if(newHashPass != null && newSalt != null) {
					Calendar cal = Calendar.getInstance(TimeZone.getTimeZone(Utility.getApplicationTimeZoneId()));
					java.sql.Timestamp timestamp = java.sql.Timestamp.valueOf(LocalDateTime.now());
					try {
						SecurityNativeUserUtils.storeUserPassword(userId, type, newHashPass, newSalt, timestamp, cal);
					} catch (Exception e) {
						logger.error(Constants.STACKTRACE, e);
					}
				}
				return true;
			}
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
		String query = "DELETE FROM ENGINEPERMISSION WHERE USERID = '?1'; "
				+ "DELETE FROM USERINSIGHTPERMISSION WHERE USERID = '?1'; "
				+ "DELETE FROM SMSS_USER WHERE ID = '?1';";
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
		String query = "UPDATE SMSS_USER SET PUBLISHER=" + isPublisher + " WHERE ID ='" + RdbmsQueryBuilder.escapeForSQLStatement(userId) + "';";
		try {
			securityDb.insertData(query);
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occured setting this user as a publisher");
		}
	}
	
	/**
	 * Set the user's exporting rights
	 * @param userId
	 * @param isExporter
	 */
	public void setUserExporter(String userId, boolean isExporter) {
		String query = "UPDATE SMSS_USER SET EXPORTER=" + isExporter + " WHERE ID ='" + RdbmsQueryBuilder.escapeForSQLStatement(userId) + "';";
		try {
			securityDb.insertData(query);
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occured setting this user as an exporter");
		}
	}
	
	///////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////////////

	/*
	 * METHODS FOR DATABASE AUTHORIZATION THAT ARE AT THE ADMIN LEVEL
	 */
	
	/**
	 * Get all databases options
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
	 * Get all project options
	 * @return
	 */
	public List<Map<String, Object>> getAllProjectSettings() {
//		String query = "SELECT DISTINCT "
//				+ "ENGINE.ENGINEID as \"app_id\", "
//				+ "ENGINE.ENGINENAME as \"app_name\", "
//				+ "LOWER(ENGINE.ENGINENAME) as \"low_app_name\", "
//				+ "ENGINE.GLOBAL as \"app_global\" "
//				+ "FROM ENGINE "
//				+ "ORDER BY LOWER(ENGINE.ENGINENAME)";
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTID", "project_id"));
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTNAME", "project_name"));
		QueryFunctionSelector fun = new QueryFunctionSelector();
		fun.addInnerSelector(new QueryColumnSelector("PROJECT__PROJECTNAME"));
		fun.setFunction(QueryFunctionHelper.LOWER);
		fun.setAlias("low_project_name");
		qs.addSelector(fun);
		qs.addSelector(new QueryColumnSelector("PROJECT__GLOBAL", "project_global"));
		qs.addOrderBy(new QueryColumnOrderBySelector("low_project_name"));
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}
	
	/**
	 * Set if database should be public or not
	 * @param databaseId
	 * @param isPublic
	 */
	public boolean setDatabaseGlobal(String databaseId, boolean isPublic) {
		String query = "UPDATE ENGINE SET GLOBAL = " + isPublic + " WHERE ENGINEID ='" + RdbmsQueryBuilder.escapeForSQLStatement(databaseId)  + "';";
		securityDb.execUpdateAndRetrieveStatement(query, true);
		securityDb.commit();
		return true;
	}
	
	/**
	 * Set if project should be public or not
	 * @param projectId
	 * @param isPublic
	 */
	public boolean setProjectGlobal(String projectId, boolean isPublic) {
		String query = "UPDATE PROJECT SET GLOBAL = " + isPublic + " WHERE PROJECTID ='" + RdbmsQueryBuilder.escapeForSQLStatement(projectId)  + "';";
		securityDb.execUpdateAndRetrieveStatement(query, true);
		securityDb.commit();
		return true;
	}
	
	/**
	 * Get all the users for a databases
	 * @param databaseId
	 * @return
	 */
	public List<Map<String, Object>> getAppUsers(String databaseId) {
		return SecurityQueryUtils.getFullDatabaseOwnersAndEditors(databaseId);
	}
	
	
	/**
	 * Get all the users for a project
	 * @param projectId
	 * @return
	 */
	public List<Map<String, Object>> getProjectUsers(String projectId) {
		return SecurityQueryUtils.getFullProjectOwnersAndEditors(projectId);
	}
	
	/**
	 * 
	 * @param newUserId
	 * @param databaseId
	 * @param permission
	 * @return
	 */
	public void addDatabaseUser(String newUserId, String databaseId, String permission) {
		// make sure user doesn't already exist for this database
		if(SecurityUserDatabaseUtils.getUserDatabasePermission(newUserId, databaseId) != null) {
			// that means there is already a value
			throw new IllegalArgumentException("This user already has access to this database. Please edit the existing permission level.");
		}
		
		String query = "INSERT INTO ENGINEPERMISSION (USERID, ENGINEID, PERMISSION, VISIBILITY) VALUES('"
				+ RdbmsQueryBuilder.escapeForSQLStatement(newUserId) + "', '"
				+ RdbmsQueryBuilder.escapeForSQLStatement(databaseId) + "', "
				+ AccessPermission.getIdByPermission(permission) + ", "
				+ "TRUE);";

		try {
			securityDb.insertData(query);
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occured adding user permissions for this database");
		}
	}
	
	/**
	 * 
	 * @param newUserId
	 * @param projectId
	 * @param permission
	 * @return
	 */
	public void addProjectUser(String newUserId, String projectId, String permission) {
		// make sure user doesn't already exist for this project
		if(SecurityUserProjectUtils.getUserProjectPermission(newUserId, projectId) != null) {
			// that means there is already a value
			throw new IllegalArgumentException("This user already has access to this project. Please edit the existing permission level.");
		}
		
		String query = "INSERT INTO PROJECTPERMISSION (USERID, PROJECTID, PERMISSION, VISIBILITY) VALUES('"
				+ RdbmsQueryBuilder.escapeForSQLStatement(newUserId) + "', '"
				+ RdbmsQueryBuilder.escapeForSQLStatement(projectId) + "', "
				+ AccessPermission.getIdByPermission(permission) + ", "
				+ "TRUE);";

		try {
			securityDb.insertData(query);
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occured adding user permissions for this project");
		}
	}
	
	/**
	 * Return the databases the user has explicit access to
	 * @param singleUserId
	 * @return
	 */
	public List<String> getProjectsUserHasExplicitAccess(String singleUserId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__PROJECTID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", singleUserId));
		return QueryExecutionUtility.flushToListString(securityDb, qs);
	}
	
	/**
	 * Return the databases the user has explicit access to
	 * @param singleUserId
	 * @return
	 */
	public Map<String, Boolean> getProjectsAndVisibilityUserHasExplicitAccess(String singleUserId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__PROJECTID"));
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__VISIBILITY"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", singleUserId));
		Map<String, Boolean> values = new HashMap<>();
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			while(wrapper.hasNext()) {
				Object[] row = wrapper.next().getValues();
				values.put((String) row[0], (Boolean) row[1]);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
		}
		
		return values;
	}
	
	/** 
	 * Give user permission for all the projects
	 * @param userId		String - 	The user id we are providing permissions to
	 * @param permission	String - 	The permission level for the access
	 * @param isAddNew 		boolean - 	If false, modifying existing project permissions to the new permission level
	 * 									If true, adding new projects with the permission level specified
	 */
	public void grantAllProjects(String userId, String permission, boolean isAddNew) {
		if(isAddNew) {
			List<String> currentProjectAccess = getProjectsUserHasExplicitAccess(userId);
			List<String> projectIds = SecurityProjectUtils.getAllProjectIds();
			String insertQuery = "INSERT INTO PROJECTPERMISSION (USERID, PROJECTID, VISIBILITY, PERMISSION) VALUES(?,?,?,?)";
			int permissionLevel = AccessPermission.getIdByPermission(permission);
			boolean visible = true;
			PreparedStatement ps = null;

			try {
				ps = securityDb.getPreparedStatement(insertQuery);
				// add new permission for projects
				for (String projectId : projectIds) {
					if(currentProjectAccess.contains(projectId)) {
						// only add for new projects, not existing projects
						continue;
					}
					int parameterIndex = 1;
					ps.setString(parameterIndex++, userId);
					ps.setString(parameterIndex++, projectId);
					ps.setBoolean(parameterIndex++, visible);
					ps.setInt(parameterIndex++, permissionLevel);
					ps.addBatch();
				}
				ps.executeBatch();
			} catch (SQLException e) {
				logger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("An error occured granting the user permission for all the projects");
			} finally {
				if (ps != null) {
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
		} else {
			// first grab the projects and visibility
			Map<String, Boolean> currentProjectToVisibilityMap = getProjectsAndVisibilityUserHasExplicitAccess(userId);
			
			// we will remove all the current permissions
			// and then re-add the ones they used to have but with the new level
			
			// delete first
			{
				String deleteQuery = "DELETE FROM PROJECTPERMISSION WHERE USERID=?";
				PreparedStatement ps = null;
				try {
					ps = securityDb.getPreparedStatement(deleteQuery);
					ps.setString(1, userId);
					ps.execute();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
					throw new IllegalArgumentException("An error occured granting the user permission for all the projects");
				} finally {
					if (ps != null) {
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
			// now add
			{
				// now we insert the values
				String insertQuery = "INSERT INTO PROJECTPERMISSION (USERID, PROJECTID, VISIBILITY, PERMISSION) VALUES(?,?,?,?)";
				int permissionLevel = AccessPermission.getIdByPermission(permission);
				PreparedStatement ps = null;

				try {
					ps = securityDb.getPreparedStatement(insertQuery);
					// add new permission for all projects
					for (String projectId : currentProjectToVisibilityMap.keySet()) {
						boolean visible = currentProjectToVisibilityMap.get(projectId);
						int parameterIndex = 1;
						ps.setString(parameterIndex++, userId);
						ps.setString(parameterIndex++, projectId);
						ps.setBoolean(parameterIndex++, visible);
						ps.setInt(parameterIndex++, permissionLevel);
						ps.addBatch();
					}
					ps.executeBatch();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
					throw new IllegalArgumentException("An error occured granting the user permission for all the projects");
				} finally {
					if (ps != null) {
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
		}
	}
	
	/**
	 * Return the databases the user has explicit access to
	 * @param singleUserId
	 * @return
	 */
	public static List<String> getDatabasesUserHasExplicitAccess(String singleUserId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__ENGINEID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", singleUserId));
		return QueryExecutionUtility.flushToListString(securityDb, qs);
	}
	
	/**
	 * Return the databases the user has explicit access to
	 * @param singleUserId
	 * @return
	 */
	public Map<String, Boolean> getDatabasesAndVisibilityUserHasExplicitAccess(String singleUserId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__ENGINEID"));
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__VISIBILITY"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", singleUserId));
		Map<String, Boolean> values = new HashMap<>();
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			while(wrapper.hasNext()) {
				Object[] row = wrapper.next().getValues();
				values.put((String) row[0], (Boolean) row[1]);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
		}
		
		return values;
	}
	
	/** 
	 * give user permission for all the databases
	 * @param userId		String - 	The user id we are providing permissions to
	 * @param permission	String - 	The permission level for the access
	 * @param isAddNew 		boolean - 	If false, modifying existing project permissions to the new permission level
	 * 									If true, adding new projects with the permission level specified
	 */
	public void grantAllDatabases(String userId, String permission, boolean isAddNew) {
		if(isAddNew) {
			List<String> currentDatabaseAccess = getDatabasesUserHasExplicitAccess(userId);
			List<String> databaseIds = SecurityDatabaseUtils.getAllDatabaseIds();
			String insertQuery = "INSERT INTO ENGINEPERMISSION (USERID, ENGINEID, VISIBILITY, PERMISSION) VALUES(?,?,?,?)";
			int permissionLevel = AccessPermission.getIdByPermission(permission);
			boolean visible = true;
			PreparedStatement ps = null;

			try {
				ps = securityDb.getPreparedStatement(insertQuery);
				// add new permission for databases
				for (String databaseId : databaseIds) {
					if(currentDatabaseAccess.contains(databaseId)) {
						// only add for new databases, not existing databases
						continue;
					}
					int parameterIndex = 1;
					ps.setString(parameterIndex++, userId);
					ps.setString(parameterIndex++, databaseId);
					ps.setBoolean(parameterIndex++, visible);
					ps.setInt(parameterIndex++, permissionLevel);
					ps.addBatch();
				}
				ps.executeBatch();
			} catch (SQLException e) {
				logger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("An error occured granting the user permission for all the databases");
			} finally {
				if (ps != null) {
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
		} else {
			// first grab the databases and visibility
			Map<String, Boolean> currentDatabaseToVisibilityMap = getDatabasesAndVisibilityUserHasExplicitAccess(userId);
			
			// we will remove all the current permissions
			// and then re-add the ones they used to have but with the new level
			
			// delete first
			{
				String deleteQuery = "DELETE FROM ENGINEPERMISSION WHERE USERID=?";
				PreparedStatement ps = null;
				try {
					ps = securityDb.getPreparedStatement(deleteQuery);
					ps.setString(1, userId);
					ps.execute();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
					throw new IllegalArgumentException("An error occured granting the user permission for all the databases");
				} finally {
					if (ps != null) {
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
			// now add
			{
				// now we insert the values
				String insertQuery = "INSERT INTO ENGINEPERMISSION (USERID, ENGINEID, VISIBILITY, PERMISSION) VALUES(?,?,?,?)";
				int permissionLevel = AccessPermission.getIdByPermission(permission);
				PreparedStatement ps = null;

				try {
					ps = securityDb.getPreparedStatement(insertQuery);
					// add new permission for all projects
					for (String databaseId : currentDatabaseToVisibilityMap.keySet()) {
						boolean visible = currentDatabaseToVisibilityMap.get(databaseId);
						int parameterIndex = 1;
						ps.setString(parameterIndex++, userId);
						ps.setString(parameterIndex++, databaseId);
						ps.setBoolean(parameterIndex++, visible);
						ps.setInt(parameterIndex++, permissionLevel);
						ps.addBatch();
					}
					ps.executeBatch();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
					throw new IllegalArgumentException("An error occured granting the user permission for all the databases");
				} finally {
					if (ps != null) {
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
		}
	}
	
	
	/** 
	 * give new users access to a database
	 * @param databaseId
	 * @param permission
	 */
	public void grantNewUsersDatabaseAccess(String databaseId, String permission) {
		String query = "INSERT INTO ENGINEPERMISSION (USERID, ENGINEID, PERMISSION, VISIBILITY) VALUES(?, '"
				+ RdbmsQueryBuilder.escapeForSQLStatement(databaseId) + "', "
				+ AccessPermission.getIdByPermission(permission) + ", " + "TRUE);";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			// get users with no access to app
			List<Map<String, Object>> users = getDatabaseUsersNoCredentials(databaseId);
			for (Map<String, Object> userMap : users) {
				String userId = (String) userMap.get("id");
				ps.setString(1, userId);
				ps.addBatch();
			}
			ps.executeBatch();
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occured adding user permissions for this database");
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
	
	public void grantNewUsersProjectAccess(String projectId, String permission) {
		String query = "INSERT INTO PROJECTPERMISSION (USERID, PROJECTID, PERMISSION, VISIBILITY) VALUES(?, '"
				+ RdbmsQueryBuilder.escapeForSQLStatement(projectId) + "', "
				+ AccessPermission.getIdByPermission(permission) + ", " + "TRUE);";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			// get users with no access to project
			List<Map<String, Object>> users = getProjectUsersNoCredentials(projectId);
			for (Map<String, Object> userMap : users) {
				String userId = (String) userMap.get("id");
				ps.setString(1, userId);
				ps.addBatch();
			}
			ps.executeBatch();
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occured adding user permissions for this project");
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
	
	/**
	 * Give the user permission for all the insights in a project
	 * @param projectId
	 * @param userId
	 * @param permission
	 */
	public void grantAllProjectInsights(String projectId, String userId, String permission) {
		// delete all previous permissions for the user
		String query = "DELETE FROM USERINSIGHTPERMISSION WHERE USERID='"
				+ RdbmsQueryBuilder.escapeForSQLStatement(userId) + "' AND PROJECTID = '"
				+ RdbmsQueryBuilder.escapeForSQLStatement(projectId) + "';";

		String insertQuery = "INSERT INTO USERINSIGHTPERMISSION (USERID, PROJECTID, INSIGHTID, PERMISSION) VALUES('"
				+ RdbmsQueryBuilder.escapeForSQLStatement(userId) + "', '"
				+ RdbmsQueryBuilder.escapeForSQLStatement(projectId) + "', ?, "
				+ AccessPermission.getIdByPermission(permission) + ");";

		PreparedStatement ps = null;
		try {
			securityDb.insertData(query);

			ps = securityDb.getPreparedStatement(insertQuery);
			// add new permission for all insights
			List<String> insightIds = getAllInsights(projectId);
			for (String x : insightIds) {
				ps.setString(1, x);
				ps.addBatch();
			}
			ps.executeBatch();
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occured granting the user permission for all the projects");
		} finally {
			if (ps != null) {
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
	/**
	 * 
	 * @param existingUserId
	 * @param databaseId
	 * @param newPermission
	 * @return
	 */
	public void editDatabaseUserPermission(String existingUserId, String databaseId, String newPermission) {
		// make sure we are trying to edit a permission that exists
		Integer existingUserPermission = SecurityUserDatabaseUtils.getUserDatabasePermission(existingUserId, databaseId);
		if(existingUserPermission == null) {
			throw new IllegalArgumentException("Attempting to modify user permission for a user who does not currently have access to the database");
		}
		
		int newPermissionLvl = AccessPermission.getIdByPermission(newPermission);
		
		String query = "UPDATE ENGINEPERMISSION SET PERMISSION=" + newPermissionLvl
				+ " WHERE USERID='" + RdbmsQueryBuilder.escapeForSQLStatement(existingUserId) + "' "
				+ "AND ENGINEID='"	+ RdbmsQueryBuilder.escapeForSQLStatement(databaseId) + "';";
		try {
			securityDb.insertData(query);
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occured adding user permissions for this database");
		}
	}
	
	
	/**
	 * 
	 * @param existingUserId
	 * @param projectId
	 * @param newPermission
	 * @return
	 */
	public void editProjectUserPermission(String existingUserId, String projectId, String newPermission) {
		// make sure we are trying to edit a permission that exists
		Integer existingUserPermission = SecurityUserProjectUtils.getUserProjectPermission(existingUserId, projectId);
		if(existingUserPermission == null) {
			throw new IllegalArgumentException("Attempting to modify user permission for a user who does not currently have access to the project");
		}
		
		int newPermissionLvl = AccessPermission.getIdByPermission(newPermission);
		
		String query = "UPDATE PROJECTPERMISSION SET PERMISSION=" + newPermissionLvl
				+ " WHERE USERID='" + RdbmsQueryBuilder.escapeForSQLStatement(existingUserId) + "' "
				+ "AND PROJECTID='"	+ RdbmsQueryBuilder.escapeForSQLStatement(projectId) + "';";
		try {
			securityDb.insertData(query);
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occured adding user permissions for this project");
		}
	}
	/**
	 * 
	 * @param editedUserId
	 * @param databaseId
	 * @return
	 */
	public void removeDatabaseUser(String existingUserId, String databaseId) {
		// make sure we are trying to edit a permission that exists
		Integer existingUserPermission = SecurityUserDatabaseUtils.getUserDatabasePermission(existingUserId, databaseId);
		if(existingUserPermission == null) {
			throw new IllegalArgumentException("Attempting to modify user permission for a user who does not currently have access to the database");
		}
		
		String query = "DELETE FROM ENGINEPERMISSION WHERE "
				+ "USERID='" + RdbmsQueryBuilder.escapeForSQLStatement(existingUserId) + "' "
				+ "AND ENGINEID='"	+ RdbmsQueryBuilder.escapeForSQLStatement(databaseId) + "';";
		try {
			securityDb.insertData(query);
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occured adding user permissions for this database");
		}
	}
	
	
	/**
	 * 
	 * @param editedUserId
	 * @param projectId
	 * @return
	 */
	public void removeProjectUser(String existingUserId, String projectId) {
		// make sure we are trying to edit a permission that exists
		Integer existingUserPermission = SecurityUserProjectUtils.getUserProjectPermission(existingUserId, projectId);
		if(existingUserPermission == null) {
			throw new IllegalArgumentException("Attempting to modify user permission for a user who does not currently have access to the project");
		}
		
		String query = "DELETE FROM PROJECTPERMISSION WHERE "
				+ "USERID='" + RdbmsQueryBuilder.escapeForSQLStatement(existingUserId) + "' "
				+ "AND PROJECTID='"	+ RdbmsQueryBuilder.escapeForSQLStatement(projectId) + "';";
		try {
			securityDb.insertData(query);
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occured adding user permissions for this project");
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
	public List<Map<String, Object>> getProjectInsights(String projectId) {

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("INSIGHT__PROJECTID", "project_id"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__INSIGHTID", "project_insight_id"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__INSIGHTNAME", "name"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__GLOBAL", "insight_global"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__EXECUTIONCOUNT", "exec_count"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__CREATEDON", "created_on"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__LASTMODIFIEDON", "last_modified_on"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__CACHEABLE", "cacheable"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHT__PROJECTID", "==", projectId));
		qs.addOrderBy(new QueryColumnOrderBySelector("INSIGHT__INSIGHTNAME"));
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}
	
//	/**
//	 * @deprecated
//	 * @param appId
//	 * @param insightIds
//	 * @throws Exception 
//	 */
//	public void deleteAppInsights(String appId, List<String> insightIds) throws Exception {
//		IEngine engine = Utility.getEngine(appId);
//		InsightAdministrator admin = new InsightAdministrator(engine.getInsightDatabase());
//	
//		// delete from insights database
//		admin.dropInsight(insightIds);
//
//		// delete from the security database
//		String insightFilters = createFilter(insightIds);
//		String query = "DELETE FROM INSIGHT WHERE INSIGHTID " + insightFilters + " AND ENGINEID='" + appId + "';";
//		query += "DELETE FROM USERINSIGHTPERMISSION  WHERE INSIGHTID " + insightFilters + " AND ENGINEID='" + appId + "'";
//		securityDb.insertData(query);
//		securityDb.commit();
//	}
	
	/**
	 * @param projectId
	 * @param insightIds
	 * @throws Exception 
	 */
	public void deleteProjectInsights(String projectId, List<String> insightIds) throws Exception {
		IProject project =  Utility.getProject(projectId);
		InsightAdministrator admin = new InsightAdministrator(project.getInsightDatabase());
	
		// delete from insights database
		admin.dropInsight(insightIds);

		// delete from the security database
		String insightFilters = createFilter(insightIds);
		String query = "DELETE FROM INSIGHT WHERE INSIGHTID " + insightFilters + " AND PROJECTID='" + projectId + "';";
		query += "DELETE FROM USERINSIGHTPERMISSION  WHERE INSIGHTID " + insightFilters + " AND PROJECTID='" + projectId + "'";
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
	public List<Map<String, Object>> getInsightUsers(String projectId, String insightId) throws IllegalAccessException {
//		String query = "SELECT SMSS_USER.ID AS \"id\", "
//				+ "SMSS_USER.NAME AS \"name\", "
//				+ "PERMISSION.NAME AS \"permission\" "
//				+ "FROM SMSS_USER "
//				+ "INNER JOIN USERINSIGHTPERMISSION ON (USER.ID = USERINSIGHTPERMISSION.USERID) "
//				+ "INNER JOIN PERMISSION ON (USERINSIGHTPERMISSION.PERMISSION = PERMISSION.ID) "
//				+ "WHERE USERINSIGHTPERMISSION.ENGINEID='" + appId + "'"
//				+ " AND USERINSIGHTPERMISSION.INSIGHTID='" + insightId + "'"
//				;
//		
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__ID", "id"));
		qs.addSelector(new QueryColumnSelector("PERMISSION__NAME", "name"));
		qs.addSelector(new QueryColumnSelector("PERMISSION__ID", "pvalue"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__EMAIL", "email"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__INSIGHTID", "==", insightId));
		qs.addRelation("SMSS_USER", "USERINSIGHTPERMISSION", "inner.join");
		qs.addRelation("USERINSIGHTPERMISSION", "PERMISSION", "inner.join");
		qs.addOrderBy(new QueryColumnOrderBySelector("PERMISSION__ID"));
		qs.addOrderBy(new QueryColumnOrderBySelector("SMSS_USER__ID"));
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}
	
	/**
	 * 
	 * @param newUserId
	 * @param projectId
	 * @param insightId
	 * @param permission
	 * @return
	 */
	public void addInsightUser(String newUserId, String projectId, String insightId, String permission) {
		// make sure user doesn't already exist for this insight
		if(SecurityInsightUtils.getUserInsightPermission(newUserId, projectId, insightId) != null) {
			// that means there is already a value
			throw new IllegalArgumentException("This user already has access to this insight. Please edit the existing permission level.");
		}
		
		String query = "INSERT INTO USERINSIGHTPERMISSION (USERID, PROJECTID, INSIGHTID, PERMISSION) VALUES('"
				+ RdbmsQueryBuilder.escapeForSQLStatement(newUserId) + "', '"
				+ RdbmsQueryBuilder.escapeForSQLStatement(projectId) + "', '"
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
	 * @param projectId
	 * @param insightId
	 * @param permission
	 * @return
	 */
	public void addAllInsightUsers(String projectId, String insightId, String permission) {
		String query = "INSERT INTO USERINSIGHTPERMISSION (USERID, PROJECTID, INSIGHTID, PERMISSION) VALUES(?,'"
				+ RdbmsQueryBuilder.escapeForSQLStatement(projectId) + "', '"
				+ RdbmsQueryBuilder.escapeForSQLStatement(insightId) + "', "
				+ AccessPermission.getIdByPermission(permission) + ");";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			if (projectId != null && permission != null) {
				List<Map<String, Object>> users = getInsightUsersNoCredentials(projectId, insightId);
				for (Map<String, Object> userMap : users) {
					String userId = (String) userMap.get("id");
					ps.setString(1, userId);
					ps.addBatch();
				}
				ps.executeBatch();
				// update existing permissions for users
				updateInsightUserPermissions(projectId, insightId, permission);
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occured adding all users for this insight");
		} finally {
			if (ps != null) {
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
	
	/**
	 * 
	 * @param existingUserId
	 * @param projectId
	 * @param insightId
	 * @param newPermission
	 * @return
	 */
	public void editInsightUserPermission(String existingUserId, String projectId, String insightId, String newPermission) {
		// make sure we are trying to edit a permission that exists
		Integer existingUserPermission = SecurityInsightUtils.getUserInsightPermission(existingUserId, projectId, insightId);
		if(existingUserPermission == null) {
			throw new IllegalArgumentException("Attempting to modify user permission for a user who does not currently have access to the insight");
		}
		
		int newPermissionLvl = AccessPermission.getIdByPermission(newPermission);
		
		String query = "UPDATE USERINSIGHTPERMISSION SET PERMISSION=" + newPermissionLvl
				+ " WHERE USERID='" + RdbmsQueryBuilder.escapeForSQLStatement(existingUserId) + "' "
				+ "AND PROJECTID='"	+ RdbmsQueryBuilder.escapeForSQLStatement(projectId) + "' "
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
	 * @param projectId
	 * @param insightId
	 * @return
	 */
	public void removeInsightUser(String existingUserId, String projectId, String insightId) {
		// make sure we are trying to edit a permission that exists
		Integer existingUserPermission = SecurityInsightUtils.getUserInsightPermission(existingUserId, projectId, insightId);
		if(existingUserPermission == null) {
			throw new IllegalArgumentException("Attempting to modify user permission for a user who does not currently have access to the insight");
		}
		
		String query = "DELETE FROM USERINSIGHTPERMISSION WHERE "
				+ "USERID='" + RdbmsQueryBuilder.escapeForSQLStatement(existingUserId) + "' "
				+ "AND PROJECTID='"	+ RdbmsQueryBuilder.escapeForSQLStatement(projectId) + "' "
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
	 * @param projectId
	 * @param isPublic
	 */
	public void setInsightGlobalWithinProject(String projectId, String insightId, boolean isPublic) {
		String query = "UPDATE INSIGHT SET GLOBAL=" + isPublic + " WHERE PROJECTID ='" + projectId + "' AND INSIGHTID='" + insightId + "';";
		try {
			securityDb.insertData(query);
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occured setting this insight global");
		}
	}
	
	/**
	 * Returns List of users that have no access credentials to a given database
	 * @param appID
	 * @return 
	 */
	public List<Map<String, Object>> getDatabaseUsersNoCredentials(String databaseId) {
		/*
		 * String Query = 
		 * "SELECT USER.ID, USER.USERNAME, USER.NAME, USER.EMAIL  FROM USER WHERE ID NOT IN 
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
	 * Returns List of users that have no access credentials to a given project
	 * @param appID
	 * @return 
	 */
	public List<Map<String, Object>> getProjectUsersNoCredentials(String projectId) {
		/*
		 * String Query = 
		 * "SELECT USER.ID, USER.USERNAME, USER.NAME, USER.EMAIL  FROM USER WHERE ID NOT IN 
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
			subQs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__USERID"));
			subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__PROJECTID","==",projectId));
			subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__PERMISSION", "!=", null, PixelDataType.NULL_VALUE));
		}
		
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}
	
	
	/**
	 * Returns List of users that have no access credentials to a given insight 
	 * @param insightID
	 * @param projectId
	 * @return 
	 */
	public List<Map<String, Object>> getInsightUsersNoCredentials(String projectId, String insightId) {
		/*
		 * String Query = 
		 * "SELECT USER.ID, USER.USERNAME, USER.NAME, USER.EMAIL FROM USER WHERE USER.ID NOT IN 
		 * (SELECT u.USERID FROM USERINSIGHTPERMISSION u WHERE u.ENGINEID == '" + appID + "' AND u.INSIGHTID == '"+insightID +"'AND u.PERMISSION IS NOT NULL);"
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
			subQs.addSelector(new QueryColumnSelector("USERINSIGHTPERMISSION__USERID"));
			subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__PROJECTID", "==", projectId));
			subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__INSIGHTID", "==", insightId));
			subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__PERMISSION","!=", null, PixelDataType.NULL_VALUE));
		}
		
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}

	public void updateDatabaseUserPermissions(String databaseId, String newPermission) {
		int newPermissionLvl = AccessPermission.getIdByPermission(newPermission);
		String query = "UPDATE ENGINEPERMISSION SET PERMISSION=" + newPermissionLvl 
				+ " WHERE ENGINEID='" + RdbmsQueryBuilder.escapeForSQLStatement(databaseId) + "';";
		try {
			securityDb.insertData(query);
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occured adding user permissions for this database");
		}
		
	}
	
	public void updateProjectUserPermissions(String projectId, String newPermission) {
		int newPermissionLvl = AccessPermission.getIdByPermission(newPermission);
		String query = "UPDATE PROJECTPERMISSION SET PERMISSION=" + newPermissionLvl 
				+ " WHERE PROJECTID='" + RdbmsQueryBuilder.escapeForSQLStatement(projectId) + "';";
		try {
			securityDb.insertData(query);
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occured adding user permissions for this project");
		}
		
	}

	/**
	 * Add all users to an database with the same permission
	 * @param databaseId
	 * @param permission
	 */
	public void addAllDatabaseUsers(String databaseId, String permission) {
		String query = "INSERT INTO ENGINEPERMISSION (USERID, ENGINEID, PERMISSION, VISIBILITY) VALUES(?, '"
				+ RdbmsQueryBuilder.escapeForSQLStatement(databaseId) + "', "
				+ AccessPermission.getIdByPermission(permission) + ", " + "TRUE);";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			if (databaseId != null && permission != null) {
				List<Map<String, Object>> users = getDatabaseUsersNoCredentials(databaseId);
				for (Map<String, Object> userMap : users) {
					String userId = (String) userMap.get("id");
					ps.setString(1, userId);
					ps.addBatch();
				}
				ps.executeBatch();
				// update existing user permissions
				updateDatabaseUserPermissions(databaseId, permission);
			}
		} catch (SQLException e1) {
			logger.error(Constants.STACKTRACE, e1);
			throw new IllegalArgumentException("An error occured adding user permissions for this database");
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
	
	
	/**
	 * Add all users to an project with the same permission
	 * @param projectId
	 * @param permission
	 */
	public void addAllProjectUsers(String projectId, String permission) {
		String query = "INSERT INTO PROJECTPERMISSION (USERID, PROJECTID, PERMISSION, VISIBILITY) VALUES(?, '"
				+ RdbmsQueryBuilder.escapeForSQLStatement(projectId) + "', "
				+ AccessPermission.getIdByPermission(permission) + ", " + "TRUE);";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			if (projectId != null && permission != null) {
				List<Map<String, Object>> users = getProjectUsersNoCredentials(projectId);
				for (Map<String, Object> userMap : users) {
					String userId = (String) userMap.get("id");
					ps.setString(1, userId);
					ps.addBatch();
				}
				ps.executeBatch();
				// update existing user permissions
				updateProjectUserPermissions(projectId, permission);
			}
		} catch (SQLException e1) {
			logger.error(Constants.STACKTRACE, e1);
			throw new IllegalArgumentException("An error occured adding user permissions for this project");
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
	
	public void updateInsightUserPermissions(String projectId, String insightId, String newPermission) {
		String updateQuery = "UPDATE USERINSIGHTPERMISSION SET PERMISSION = '"
				+ AccessPermission.getIdByPermission(newPermission) + "' WHERE PROJECTID ='"
				+ RdbmsQueryBuilder.escapeForSQLStatement(projectId) + "' AND INSIGHTID='"
				+ RdbmsQueryBuilder.escapeForSQLStatement(insightId) + "';";
		try {
			securityDb.insertData(updateQuery);
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occured adding user permissions for this project");
		}
	}
	
	private List<String>  getAllInsights(String appId) {
		String query = "SELECT INSIGHTID FROM INSIGHT WHERE PROJECTID='" + RdbmsQueryBuilder.escapeForSQLStatement(appId) + "';";
		HardSelectQueryStruct qs = new HardSelectQueryStruct();
		qs.setQuery(query);
		return QueryExecutionUtility.flushToListString(securityDb, qs);
	}

	public void grantNewUsersInsightAccess(String projectId, String insightId, String permission) {
		List<Map<String, Object>> users = getInsightUsersNoCredentials(projectId, insightId);
		String insertQuery = "INSERT INTO USERINSIGHTPERMISSION (USERID, PROJECTID, INSIGHTID, PERMISSION) VALUES(?, '"
				+ RdbmsQueryBuilder.escapeForSQLStatement(projectId) + "', '"
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
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occured granting the user permission for all the projects");
		} finally {
			if (ps != null) {
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
	
	/**
	 * Lock accounts
	 * @param numDaysSinceLastLogin
	 */
	public int lockAccounts(int numDaysSinceLastLogin) {
		int numUpdated = 0;
		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone(Utility.getApplicationTimeZoneId()));
		
		LocalDateTime dateToFilter = LocalDateTime.now();
		dateToFilter = dateToFilter.minusDays(numDaysSinceLastLogin);
		
		String query = "UPDATE SMSS_USER SET LOCKED=? WHERE LASTLOGIN<=?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			int parameterIndex = 1;
			ps.setBoolean(parameterIndex++, true);
			ps.setTimestamp(parameterIndex++, java.sql.Timestamp.valueOf(dateToFilter), cal);
			numUpdated = ps.executeUpdate();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occured granting the user permission for all the projects");
		} finally {
			if (ps != null) {
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
		
		logger.info("Number of accounts locked = " + numUpdated);
		return numUpdated;
	}
	
	/**
	 * Lock accounts
	 * @param numDaysSinceLastLogin
	 */
	public int setLockAccountsAndRecalculate(int numDaysSinceLastLogin) {
		int numUpdated = 0;
		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone(Utility.getApplicationTimeZoneId()));
		
		LocalDateTime dateToFilter = LocalDateTime.now();
		dateToFilter = dateToFilter.minusDays(numDaysSinceLastLogin);
		java.sql.Timestamp sqlTimestamp = java.sql.Timestamp.valueOf(dateToFilter);
		
		String[] queries = new String[] {
				"UPDATE SMSS_USER SET LOCKED=? WHERE LASTLOGIN<=?",
				"UPDATE SMSS_USER SET LOCKED=? WHERE LASTLOGIN>?"
		};
		boolean [] queryUpdateBool = new boolean[] {true, false};
		
		for(int i = 0; i < queries.length; i++) {
			String query = queries[i];
			boolean updateBool = queryUpdateBool[i];
			PreparedStatement ps = null;
			try {
				ps = securityDb.getPreparedStatement(query);
				int parameterIndex = 1;
				ps.setBoolean(parameterIndex++, updateBool);
				ps.setTimestamp(parameterIndex++, sqlTimestamp, cal);
				numUpdated = ps.executeUpdate();
				if(!ps.getConnection().getAutoCommit()) {
					ps.getConnection().commit();
				}
			} catch (SQLException e) {
				logger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("An error occured granting the user permission for all the projects");
			} finally {
				if (ps != null) {
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
		
		logger.info("Number of accounts locked = " + numUpdated);
		return numUpdated;
	}

}
