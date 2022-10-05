package prerna.auth.utils;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Calendar;
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
import prerna.auth.AuthProvider;
import prerna.auth.PasswordRequirements;
import prerna.auth.User;
import prerna.date.SemossDate;
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
	
	/**
	 * See if the user is an admin
	 * @param userId
	 * @param type
	 * @return
	 */
	public boolean userIsAdmin(String userId, String type) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__ID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__ID", "==", userId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__TYPE", "==", type));
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
	
	public boolean otherAdminsExist(String userId, String type) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__ID"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__TYPE"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__ADMIN", "==", true, PixelDataType.BOOLEAN));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			while(wrapper.hasNext()) {
				Object[] row = wrapper.next().getValues();
				if( (row[0] + "").equals(userId)
						&& (row[1] + "").equals(type) ) {
					continue;
				} else {
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
	
	/*
	 * all other methods should be on the instance
	 * so that we cannot bypass security easily
	 */
	
	/**
	 * Get all database users
	 * @param offset 
	 * @param limit 
	 * @return
	 * @throws IllegalArgumentException
	 */
	public List<Map<String, Object>> getAllUsers(long limit, long offset) throws IllegalArgumentException{
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
		if(limit > 0) {
			qs.setLimit(limit);
		}
		if(offset > 0) {
			qs.setOffSet(offset);
		}
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
				validEmail(newEmail, true);
			} catch(Exception e) {
				logger.error(Constants.STACKTRACE, e);
				error += e.getMessage();
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
		try {
			securityDb.insertData(updateQ);
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
			return false;
		}
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
		}
		return true;
    }
	
	/**
	 * Delete a user and all its relationships.
	 * @param userId
	 * @param type 
	 * @param userDelete
	 */
	public boolean deleteUser(String userToDelete, String type) {
		//TODO: need to start binding on userId + type
		//TODO: need to start binding on userId + type
		//TODO: need to start binding on userId + type
		//TODO: need to start binding on userId + type
		//TODO: need to start binding on userId + type
		//TODO: need to start binding on userId + type
		//TODO: need to start binding on userId + type
		String[] deleteQueries = new String[] {
				"DELETE FROM ENGINEPERMISSION WHERE USERID=?",
				"DELETE FROM USERINSIGHTPERMISSION WHERE USERID=?",
				"DELETE FROM SMSS_USER WHERE ID=?"
		};
		for(String query : deleteQueries) {
			PreparedStatement ps = null;
			try {
				ps = securityDb.getPreparedStatement(query);
				int parameterIndex = 1;
				ps.setString(parameterIndex++, userToDelete);
				ps.execute();
				if(!ps.getConnection().getAutoCommit()) {
					ps.getConnection().commit();
				}
			} catch (SQLException e) {
				logger.error(Constants.STACKTRACE, e);
			} finally {
				if(ps != null) {
					try {
						ps.close();
					} catch (SQLException e) {
						logger.error(Constants.STACKTRACE, e);
					}
				}
				if(securityDb.isConnectionPooling()) {
					try {
						if(ps != null) {
						ps.getConnection().close();
						}
					} catch (SQLException e) {
						logger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}
		return true;
	}
	

	/**
	 * Set the user's publishing rights
	 * @param userId
	 * @param isPublisher
	 */
	public void setUserPublisher(String userId, boolean isPublisher) {
		String query = "UPDATE SMSS_USER SET PUBLISHER=? WHERE ID=?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			int parameterIndex = 1;
			ps.setBoolean(parameterIndex++, isPublisher);
			ps.setString(parameterIndex++, userId);
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occured setting this user as a publisher");
		} finally {
			if(ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
			if(securityDb.isConnectionPooling()) {
				try {
					if(ps != null) {
					ps.getConnection().close();
					}
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}
	
	/**
	 * Set the user's exporting rights
	 * @param userId
	 * @param isExporter
	 */
	public void setUserExporter(String userId, boolean isExporter) {
		String query = "UPDATE SMSS_USER SET EXPORTER=? WHERE ID=?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			int parameterIndex = 1;
			ps.setBoolean(parameterIndex++, isExporter);
			ps.setString(parameterIndex++, userId);
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occured setting this user as an exporter");
		} finally {
			if(ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
			if(securityDb.isConnectionPooling()) {
				try {
					if(ps != null) {
					ps.getConnection().close();
					}
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}
	
	/**
	 * Set the user locked/unlocked
	 * @param userId
	 * @param isExporter
	 */
	public void setUserLock(String userId, String type, boolean isLocked) {
		String query = null;
		if(isLocked) {
			query = "UPDATE SMSS_USER SET LOCKED=? WHERE ID=? AND TYPE=?";
		} else {
			query = "UPDATE SMSS_USER SET LOCKED=?, LASTLOGIN=? WHERE ID=? AND TYPE=?";
		}
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			int parameterIndex = 1;
			ps.setBoolean(parameterIndex++, isLocked);
			if(!isLocked) {
				// we reset the counter so lastlogin will be today
				Calendar cal = Calendar.getInstance(TimeZone.getTimeZone(Utility.getApplicationTimeZoneId()));
				java.sql.Timestamp timestamp = java.sql.Timestamp.valueOf(LocalDateTime.now());
				ps.setTimestamp(parameterIndex++, timestamp, cal);
			}
			ps.setString(parameterIndex++, userId);
			ps.setString(parameterIndex++, type);
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occured setting this user as locked/unlocked");
		} finally {
			if(ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
			if(securityDb.isConnectionPooling()) {
				try {
					if(ps != null) {
					ps.getConnection().close();
					}
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
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
		return getAllDatabaseSettings(null);
	}
	
	public List<Map<String, Object>> getAllDatabaseSettings(String databaseFilter) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID", "app_id"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINENAME", "app_name"));
		qs.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.LOWER, "ENGINE__ENGINENAME", "low_app_name"));
		qs.addSelector(new QueryColumnSelector("ENGINE__GLOBAL", "app_global"));
		if(databaseFilter != null && !databaseFilter.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINEID", "==", databaseFilter));
		}
		qs.addOrderBy(new QueryColumnOrderBySelector("low_app_name"));
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}
	
	/**
	 * Get all project options
	 * @return
	 */
	public List<Map<String, Object>> getAllProjectSettings() {
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
	
	public List<Map<String, Object>> getAllProjectSettings(String projectFilter) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTID", "project_id"));
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTNAME", "project_name"));
		qs.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.LOWER, "PROJECT__PROJECTNAME", "low_project_name"));
		qs.addSelector(new QueryColumnSelector("PROJECT__GLOBAL", "project_global"));
		if(projectFilter != null && !projectFilter.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__PROJECTID", "==", projectFilter));
		}
		qs.addOrderBy(new QueryColumnOrderBySelector("low_project_name"));
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}

	
	/**
	 * Set if database should be public or not
	 * @param databaseId
	 * @param global
	 */
	public boolean setDatabaseGlobal(String databaseId, boolean global) {
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
	public boolean setDatabaseDiscoverable(String databaseId, boolean discoverable) {
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
	 * Set if project should be public or not
	 * @param projectId
	 * @param isPublic
	 */
	public boolean setProjectGlobal(String projectId, boolean global) {
		String updateQ = "UPDATE PROJECT SET GLOBAL=? WHERE PROJECTID=?";
		PreparedStatement updatePs = null;
		try {
			updatePs = securityDb.getPreparedStatement(updateQ);
			updatePs.setBoolean(1, global);
			updatePs.setString(2, projectId);
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
	 * DEPRECIATED DO NOT USE
	 * Get all the users for a databases
	 * @param databaseId
	 * @return
	 */
	@Deprecated
	public List<Map<String, Object>> getAppUsers(String databaseId) {
		return SecurityQueryUtils.getFullDatabaseOwnersAndEditors(databaseId);
	}

	/**
	 * Get all the users for a databases
	 * @param databaseId
	 * @param userId
	 * @param permission
	 * @param limit
	 * @param offset
	 * @return
	 */
	public List<Map<String, Object>> getDatabaseUsers(String databaseId, String userId, String permission, long limit, long offset) {
		return SecurityQueryUtils.getFullDatabaseOwnersAndEditorsParams(databaseId, userId, permission, limit, offset);
	}
	
	
	/**
	 * Get all the users for a project
	 * @param projectId
	 * @return
	 */
	public List<Map<String, Object>> getProjectUsers(String projectId, String userId, String permission, long limit, long offset) {
		return SecurityQueryUtils.getFullProjectOwnersAndEditorsParams(projectId, userId, permission, limit, offset);
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
				+ AccessPermissionEnum.getIdByPermission(permission) + ", "
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
	 * @param databaseId
	 * @param permission
	 * @return
	 */
	public void addDatabaseUserPermissions(String databaseId, List<Map<String,String>> permission) {
		// first, check to make sure these users do not already have permissions to database
		// get list of userids from permission list map
		List<String> userIds = permission.stream().map(map -> map.get("userid")).collect(Collectors.toList());
		// this returns a list of existing permissions
		Map<String, Integer> existingUserPermission = SecurityUserDatabaseUtils.getUserDatabasePermissions(userIds, databaseId);
		if (!existingUserPermission.isEmpty()) {
			throw new IllegalArgumentException("The following users already have access to this database. Please edit the existing permission level: "+String.join(",", existingUserPermission.keySet()));
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
	}
	
	/**
	 * 
	 * @param newUserId
	 * @param projectId
	 * @param permission
	 * @return
	 */
	public void addProjectUserPermissions(String projectId, List<Map<String,String>> permission) {
		// first, check to make sure these users do not already have permissions to project
		// get list of userids from permission list map
		List<String> userIds = permission.stream().map(map -> map.get("userid")).collect(Collectors.toList());
		// this returns a list of existing permissions
		Map<String, Integer> existingUserPermission = SecurityProjectUtils.getUserProjectPermissions(userIds, projectId);
		if (!existingUserPermission.isEmpty()) {
			throw new IllegalArgumentException("The following users already have access to this project. Please edit the existing permission level: "+String.join(",", existingUserPermission.keySet()));
		}
		
		// insert new user permissions in bulk
		String insertQ = "INSERT INTO PROJECTPERMISSION (USERID, PROJECTID, PERMISSION, VISIBILITY) VALUES(?,?,?,?)";
		PreparedStatement insertPs = null;
		try {
			insertPs = securityDb.getPreparedStatement(insertQ);
			for(int i=0; i<permission.size(); i++) {
				int parameterIndex = 1;
				insertPs.setString(parameterIndex++, permission.get(i).get("userid"));
				insertPs.setString(parameterIndex++, projectId);
				insertPs.setInt(parameterIndex++, AccessPermissionEnum.getIdByPermission(permission.get(i).get("permission")));
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
				+ AccessPermissionEnum.getIdByPermission(permission) + ", "
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
			int permissionLevel = AccessPermissionEnum.getIdByPermission(permission);
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
				int permissionLevel = AccessPermissionEnum.getIdByPermission(permission);
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
			int permissionLevel = AccessPermissionEnum.getIdByPermission(permission);
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
				int permissionLevel = AccessPermissionEnum.getIdByPermission(permission);
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
				+ AccessPermissionEnum.getIdByPermission(permission) + ", " + "TRUE);";
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
				+ AccessPermissionEnum.getIdByPermission(permission) + ", " + "TRUE);";
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
				+ AccessPermissionEnum.getIdByPermission(permission) + ");";

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
		
		int newPermissionLvl = AccessPermissionEnum.getIdByPermission(newPermission);
		
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
	 * @param user
	 * @param existingUserId
	 * @param databaseId
	 * @param newPermission
	 * @return
	 * @throws IllegalAccessException 
	 */
	public static void editDatabaseUserPermissions(String databaseId, List<Map<String, String>> requests) throws IllegalAccessException {

		// get userid of all requests
		List<String> existingUserIds = new ArrayList<String>();
	    for(Map<String,String> i:requests){
	    	existingUserIds.add(i.get("userid"));
	    }
			    
		// get user permissions to edit
		Map<String, Integer> existingUserPermission = SecurityUserDatabaseUtils.getUserDatabasePermissions(existingUserIds, databaseId);
		
		// make sure all users to edit currently has access to database
		Set<String> toRemoveUserIds = new HashSet<String>(existingUserIds);
		toRemoveUserIds.removeAll(existingUserPermission.keySet());
		if (!toRemoveUserIds.isEmpty()) {
			throw new IllegalArgumentException("Attempting to modify user permission for the following users who do not currently have access to the database: "+String.join(",", toRemoveUserIds));
		}
		
		// update user permissions in bulk
		String insertQ = "UPDATE ENGINEPERMISSION SET PERMISSION = ? WHERE USERID = ? AND ENGINEID = ?";
		PreparedStatement insertPs = null;
		try {
			insertPs = securityDb.getPreparedStatement(insertQ);
			for(int i=0; i<requests.size(); i++) {
				int parameterIndex = 1;
				//SET
				insertPs.setInt(parameterIndex++, AccessPermissionEnum.getIdByPermission(requests.get(i).get("permission")));
				//WHERE
				insertPs.setString(parameterIndex++, requests.get(i).get("userid"));
				insertPs.setString(parameterIndex++, databaseId);
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
		
		int newPermissionLvl = AccessPermissionEnum.getIdByPermission(newPermission);
		
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
	 * @param user
	 * @param existingUserId
	 * @param projectId
	 * @param newPermission
	 * @return
	 * @throws IllegalAccessException 
	 */
	public static void editProjectUserPermissions(String projectId, List<Map<String, String>> requests) throws IllegalAccessException {

		// get userid of all requests
		List<String> existingUserIds = new ArrayList<String>();
	    for(Map<String,String> i:requests){
	    	existingUserIds.add(i.get("userid"));
	    }
			    
		// get user permissions to edit
		Map<String, Integer> existingUserPermission = SecurityProjectUtils.getUserProjectPermissions(existingUserIds, projectId);
		
		// make sure all users to edit currently has access to database
		Set<String> toRemoveUserIds = new HashSet<String>(existingUserIds);
		toRemoveUserIds.removeAll(existingUserPermission.keySet());
		if (!toRemoveUserIds.isEmpty()) {
			throw new IllegalArgumentException("Attempting to modify user permission for the following users who do not currently have access to the project: "+String.join(",", toRemoveUserIds));
		}
		
		// update user permissions in bulk
		String insertQ = "UPDATE PROJECTPERMISSION SET PERMISSION = ? WHERE USERID = ? AND PROJECTID = ?";
		PreparedStatement insertPs = null;
		try {
			insertPs = securityDb.getPreparedStatement(insertQ);
			for(int i=0; i<requests.size(); i++) {
				int parameterIndex = 1;
				//SET
				insertPs.setInt(parameterIndex++, AccessPermissionEnum.getIdByPermission(requests.get(i).get("permission")));
				//WHERE
				insertPs.setString(parameterIndex++, requests.get(i).get("userid"));
				insertPs.setString(parameterIndex++, projectId);
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
	 * @param databaseId
	 * @return
	 */
	public void removeDatabaseUsers(List<String> existingUserIds, String databaseId) {
		Map<String, Integer> existingUserPermission = SecurityUserDatabaseUtils.getUserDatabasePermissions(existingUserIds, databaseId);
		
		// make sure these users all exist and have access
		Set<String> toRemoveUserIds = new HashSet<String>(existingUserIds);
		toRemoveUserIds.removeAll(existingUserPermission.keySet());
		if (!toRemoveUserIds.isEmpty()) {
			throw new IllegalArgumentException("Attempting to modify user permission for the following users who do not currently have access to the database: "+String.join(",", toRemoveUserIds));
		}

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
	 * 
	 * @param editedUserId
	 * @param projectId
	 * @return
	 */
	public void removeProjectUsers(List<String> existingUserIds, String projectId) {
		Map<String, Integer> existingUserPermission = SecurityProjectUtils.getUserProjectPermissions(existingUserIds, projectId);
		
		// make sure these users all exist and have access
		Set<String> toRemoveUserIds = new HashSet<String>(existingUserIds);
		toRemoveUserIds.removeAll(existingUserPermission.keySet());
		if (!toRemoveUserIds.isEmpty()) {
			throw new IllegalArgumentException("Attempting to modify user permission for the following users who do not currently have access to the project: "+String.join(",", toRemoveUserIds));
		}

		String deleteQ = "DELETE FROM PROJECTPERMISSION WHERE USERID=? AND PROJECTID=?";
		PreparedStatement deletePs = null;
		try {
			deletePs = securityDb.getPreparedStatement(deleteQ);
			for(int i=0; i<existingUserIds.size(); i++) {
				int parameterIndex = 1;
				deletePs.setString(parameterIndex++, existingUserIds.get(i));
				deletePs.setString(parameterIndex++, projectId);
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
				+ AccessPermissionEnum.getIdByPermission(permission) + ");";

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
				+ AccessPermissionEnum.getIdByPermission(permission) + ");";
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
		
		int newPermissionLvl = AccessPermissionEnum.getIdByPermission(newPermission);
		
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
		int newPermissionLvl = AccessPermissionEnum.getIdByPermission(newPermission);
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
		int newPermissionLvl = AccessPermissionEnum.getIdByPermission(newPermission);
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
				+ AccessPermissionEnum.getIdByPermission(permission) + ", " + "TRUE);";
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
				+ AccessPermissionEnum.getIdByPermission(permission) + ", " + "TRUE);";
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
				+ AccessPermissionEnum.getIdByPermission(newPermission) + "' WHERE PROJECTID ='"
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
				+ AccessPermissionEnum.getIdByPermission(permission) + ");";

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
	public List<Object[]> getUserEmailsGettingLocked() {
		// if we never lock - nothing to worry about
		int daysToLock = -1;
		int daysToLockEmail = 14;
		try {
			PasswordRequirements passReqInstance = PasswordRequirements.getInstance();
			daysToLock = passReqInstance.getDaysToLock();
			daysToLockEmail = passReqInstance.getDaysToLockEmail();
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		}
		if(daysToLock < 0) {
			return new ArrayList<>();
		}
		int daysSinceLastLoginToSendEmail = (daysToLock - daysToLockEmail);
		if(daysSinceLastLoginToSendEmail < 0) {
			logger.warn("Days to Lock is less than the Days To Lock Email Warning. Would result in constant emails. Returning empty set until configured properly");
			return new ArrayList<>();
		}
		
		LocalDateTime now = LocalDateTime.now();
		
		List<Object[]> emailsToSend = new ArrayList<>();
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__EMAIL"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__LASTLOGIN"));
		List<Boolean> values = new ArrayList<>();
		values.add(null);
		values.add(false);
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__LOCKED", "==", values, PixelDataType.BOOLEAN));
		
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			while(wrapper.hasNext()) {
				Object[] row = wrapper.next().getValues();
				String email = (String) row[0];
				if(email != null) {
					SemossDate lastLogin = null;
					if(row[1] != null) {
						Object potentialDateValue = row[1];
						if(potentialDateValue instanceof SemossDate) {
							lastLogin = (SemossDate) potentialDateValue;
						} else if(potentialDateValue instanceof String) {
							lastLogin = SemossDate.genTimeStampDateObj(potentialDateValue + "");
						}
					}
					
					long daysSinceLastLogin = Duration.between(lastLogin.getLocalDateTime(), now).toDays();
					if(daysSinceLastLogin >= daysSinceLastLoginToSendEmail) {
						emailsToSend.add(new Object[] {email, daysSinceLastLogin});
					}
				}
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
		}
		
		/*
		 * Sadly, the below does work with sqlite since it is a dumb db 
		 * and doesn't store dates properly as one would expect
		 */
		
//		AbstractSqlQueryUtil queryUtil = securityDb.getQueryUtil();
//		String dateDiff = queryUtil.getDateDiffFunctionSyntax("day", "SMSS_USER.LASTLOGIN", queryUtil.getCurrentTimestamp());
//
//		String query = "SELECT DISTINCT SMSS_USER.EMAIL, (" + dateDiff + ") as DAYS_SINCE_LASTLOGIN FROM SMSS_USER WHERE "
//				+ "(LOCKED IS NULL OR LOCKED='false') AND (" + dateDiff + ") > " + (daysToLock - daysToLockEmail); 
//		
//		IRawSelectWrapper wrapper = null;
//		try {
//			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
//			while(wrapper.hasNext()) {
//				Object[] row = wrapper.next().getValues();
//				if(row[0] != null) {
//					emailsToSend.add(row);
//				}
//			}
//		} catch (Exception e) {
//			logger.error(Constants.STACKTRACE, e);
//		} finally {
//			if(wrapper != null) {
//				wrapper.cleanUp();
//			}
//		}
		
		return emailsToSend;
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
	
	/**
	 * Return the number of admins
	 * @return
	 */
	public int getNumAdmins() {
		SelectQueryStruct qs = new SelectQueryStruct();
		QueryFunctionSelector fun = new QueryFunctionSelector();
		fun.setFunction(QueryFunctionHelper.COUNT);
		fun.addInnerSelector(new QueryColumnSelector("SMSS_USER__ID"));
		qs.addSelector(fun);
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__ADMIN", "==", true, PixelDataType.BOOLEAN));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if(wrapper.hasNext()) {
				return ((Number) wrapper.next().getValues()[0]).intValue();
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
		}
		
		return 0;
	}
	
	/**
	 * Approving user access requests and giving user access in permissions
	 * @param userId
	 * @param userType
	 * @param databaseId
	 * @param requests
	 */
	public static void approveDatabaseUserAccessRequests(String userId, String userType, String databaseId, List<Map<String, Object>> requests) {
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
				insertPs.setInt(parameterIndex++, AccessPermissionEnum.getIdByPermission((String) requests.get(i).get("permission")));
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
		String updateQ = "UPDATE DATABASEACCESSREQUEST SET PERMISSION = ?, APPROVER_USERID = ?, APPROVER_TYPE = ?, APPROVER_DECISION = ?, APPROVER_TIMESTAMP = ? WHERE ID = ? AND ENGINEID = ?";
		PreparedStatement updatePs = null;
		try {
			Calendar cal = Calendar.getInstance(TimeZone.getTimeZone(Utility.getApplicationTimeZoneId()));
			java.sql.Timestamp timestamp = java.sql.Timestamp.valueOf(LocalDateTime.now());
			updatePs = securityDb.getPreparedStatement(updateQ);
			for(int i=0; i<requests.size(); i++) {
				int index = 1;
				//set
				updatePs.setInt(index++, AccessPermissionEnum.getIdByPermission((String) requests.get(i).get("permission")));
				updatePs.setString(index++, userId);
				updatePs.setString(index++, userType);
				updatePs.setString(index++, "APPROVED");
				updatePs.setTimestamp(index++, timestamp, cal);
				//where
				updatePs.setString(index++, (String) requests.get(i).get("requestid"));
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
	public static void denyDatabaseUserAccessRequests(String userId, String userType, String databaseId, List<String> RequestIdList) {
		// bulk update to databaseaccessrequest table
		String updateQ = "UPDATE DATABASEACCESSREQUEST SET APPROVER_USERID = ?, APPROVER_TYPE = ?, APPROVER_DECISION = ?, APPROVER_TIMESTAMP = ? WHERE ID = ? AND ENGINEID = ?";
		PreparedStatement updatePs = null;
		try {
			Calendar cal = Calendar.getInstance(TimeZone.getTimeZone(Utility.getApplicationTimeZoneId()));
			java.sql.Timestamp timestamp = java.sql.Timestamp.valueOf(LocalDateTime.now());
			updatePs = securityDb.getPreparedStatement(updateQ);
			for(int i=0; i<RequestIdList.size(); i++) {
				int index = 1;
				//set
				updatePs.setString(index++, userId);
				updatePs.setString(index++, userType);
				updatePs.setString(index++, "DENIED");
				updatePs.setTimestamp(index++, timestamp, cal);
				//where
				updatePs.setString(index++, RequestIdList.get(i));
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
	 * Approving user access requests and giving user access in permissions
	 * @param userId
	 * @param userType
	 * @param projectId
	 * @param requests
	 */
	public static void approveProjectUserAccessRequests(String userId, String userType, String projectId, List<Map<String, Object>> requests) {
		// bulk delete
		String deleteQ = "DELETE FROM PROJECTPERMISSION WHERE USERID=? AND PROJECTID=?";
		PreparedStatement deletePs = null;
		try {
			deletePs = securityDb.getPreparedStatement(deleteQ);
			for(int i=0; i<requests.size(); i++) {
				int parameterIndex = 1;
				deletePs.setString(parameterIndex++, (String) requests.get(i).get("userid"));
				deletePs.setString(parameterIndex++, projectId);
				deletePs.addBatch();
			}
			deletePs.executeBatch();
			if(!deletePs.getConnection().getAutoCommit()) {
				deletePs.getConnection().commit();
			}
		} catch(Exception e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred while deleting projectpermission with detailed message = " + e.getMessage());
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
		String insertQ = "INSERT INTO PROJECTPERMISSION (USERID, PROJECTID, PERMISSION, VISIBILITY) VALUES(?,?,?,?)";
		PreparedStatement insertPs = null;
		try {
			insertPs = securityDb.getPreparedStatement(insertQ);
			for(int i=0; i<requests.size(); i++) {
				int parameterIndex = 1;
				insertPs.setString(parameterIndex++, (String) requests.get(i).get("userid"));
				insertPs.setString(parameterIndex++, projectId);
				insertPs.setInt(parameterIndex++, AccessPermissionEnum.getIdByPermission((String) requests.get(i).get("permission")));
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

		// now we do the new bulk update to projectaccessrequest table
		String updateQ = "UPDATE PROJECTACCESSREQUEST SET PERMISSION = ?, APPROVER_USERID = ?, APPROVER_TYPE = ?, APPROVER_DECISION = ?, APPROVER_TIMESTAMP = ? WHERE ID = ?";
		PreparedStatement updatePs = null;
		try {
			Calendar cal = Calendar.getInstance(TimeZone.getTimeZone(Utility.getApplicationTimeZoneId()));
			java.sql.Timestamp timestamp = java.sql.Timestamp.valueOf(LocalDateTime.now());
			updatePs = securityDb.getPreparedStatement(updateQ);
			for(int i=0; i<requests.size(); i++) {
				int index = 1;
				//set
				updatePs.setInt(index++, AccessPermissionEnum.getIdByPermission((String) requests.get(i).get("permission")));
				updatePs.setString(index++, userId);
				updatePs.setString(index++, userType);
				updatePs.setString(index++, "APPROVED");
				updatePs.setTimestamp(index++, timestamp, cal);
				//where
				updatePs.setString(index++, (String) requests.get(i).get("requestid"));
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
	 * Denying user access requests to project
	 * @param userId
	 * @param userType
	 * @param projectId
	 * @param requests
	 */
	public static void denyProjectUserAccessRequests(String userId, String userType, String projectId, List<String> RequestIdList) {
		// bulk update to projectaccessrequest table
		String updateQ = "UPDATE PROJECTACCESSREQUEST SET APPROVER_USERID = ?, APPROVER_TYPE = ?, APPROVER_DECISION = ?, APPROVER_TIMESTAMP = ? WHERE ID = ?";
		PreparedStatement updatePs = null;
		try {
			Calendar cal = Calendar.getInstance(TimeZone.getTimeZone(Utility.getApplicationTimeZoneId()));
			java.sql.Timestamp timestamp = java.sql.Timestamp.valueOf(LocalDateTime.now());
			updatePs = securityDb.getPreparedStatement(updateQ);
			for(int i=0; i<RequestIdList.size(); i++) {
				int index = 1;
				//set
				updatePs.setString(index++, userId);
				updatePs.setString(index++, userType);
				updatePs.setString(index++, "DENIED");
				updatePs.setTimestamp(index++, timestamp, cal);
				//where
				updatePs.setString(index++, RequestIdList.get(i));
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

}
