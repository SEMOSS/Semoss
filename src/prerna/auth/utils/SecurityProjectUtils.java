package prerna.auth.utils;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
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

import prerna.auth.AccessPermissionEnum;
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
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.query.querystruct.selectors.QueryIfSelector;
import prerna.query.querystruct.update.UpdateQueryStruct;
import prerna.query.querystruct.update.UpdateSqlInterpreter;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.PixelDataType;
import prerna.util.Constants;
import prerna.util.QueryExecutionUtility;
import prerna.util.sql.AbstractSqlQueryUtil;

public class SecurityProjectUtils extends AbstractSecurityUtils {

	private static final Logger logger = LogManager.getLogger(SecurityProjectUtils.class);

	/**
	 * Try to reconcile and get the engine id
	 * @return
	 */
	public static String testUserProjectIdForAlias(User user, String potentialId) {
		List<String> ids = new Vector<String>();
		
//		String userFilters = getUserFilters(user);
//		String query = "SELECT DISTINCT PROJECTPERMISSION.PROJECTID "
//				+ "FROM PROJECTPERMISSION INNER JOIN PROJECT ON PROJECT.PROJECTID=PROJECTPERMISSION.PROJECTID "
//				+ "WHERE PROJECT.PROJECTNAME='" + potentialId + "' AND PROJECTPERMISSION.USERID IN " + userFilters;
//		
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__PROJECTID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__PROJECTNAME", "==", potentialId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", getUserFiltersQs(user)));
		qs.addRelation("PROJECT", "PROJECTPERMISSION", "inner.join");

		ids = QueryExecutionUtility.flushToListString(securityDb, qs);
		if(ids.isEmpty()) {
//			query = "SELECT DISTINCT PROJECT.PROJECTID FROM PROJECT WHERE PROJECT.PROJECTNAME='" + potentialId + "' AND PROJECT.GLOBAL=TRUE";

			qs = new SelectQueryStruct();
			qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTID"));
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__PROJECTNAME", "==", potentialId));
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__GLOBAL", "==", true, PixelDataType.BOOLEAN));
			
			ids = QueryExecutionUtility.flushToListString(securityDb, qs);
		}
		
		if(ids.size() == 1) {
			potentialId = ids.get(0);
		} else if(ids.size() > 1) {
			throw new IllegalArgumentException("There are 2 projects with the name " + potentialId + ". Please pass in the correct id to know which source you want to load from");
		}
		
		return potentialId;
	}
	
	/**
	 * Get the engine alias for a id
	 * @return
	 */
	public static String getProjectAliasForId(String id) {
//		String query = "SELECT PROJECTNAME FROM PROJECT WHERE PROJECTID='" + id + "'";
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTNAME"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__PROJECTID", "==", id));
		List<String> results = QueryExecutionUtility.flushToListString(securityDb, qs);
		if(results.isEmpty()) {
			return null;
		}
		return results.get(0);
	}
	
	/**
	 * Get user databases + global databases 
	 * @param userId
	 * @return
	 */
	public static List<String> getFullUserProjectIds(User user) {
		List<String> databaseList = SecurityUserProjectUtils.getFullUserProjectIds(user);
		databaseList.addAll(getGlobalProjectIds());
		return databaseList.stream().distinct().sorted().collect(Collectors.toList());
	}
	
	/**
	 * Get global databases
	 * @return
	 */
	public static Set<String> getGlobalProjectIds() {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__GLOBAL", "==", true, PixelDataType.BOOLEAN));
		return QueryExecutionUtility.flushToSetString(securityDb, qs, false);
	}

	/**
	 * Get what permission the user has for a given app
	 * @param userId
	 * @param projectId
	 * @param insightId
	 * @return
	 */
	public static String getActualUserProjectPermission(User user, String projectId) {
		return SecurityUserProjectUtils.getActualUserProjectPermission(user, projectId);
	}
	
	/**
	 * Get a list of the project ids
	 * @return
	 */
	public static List<String> getAllProjectIds() {
//		String query = "SELECT DISTINCT ENGINEID FROM ENGINE";
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTID"));
		return QueryExecutionUtility.flushToListString(securityDb, qs);
	}

	/**
	 * Get the project permissions for a specific user
	 * @param singleUserId
	 * @param projectId
	 * @return
	 */
	public static Integer getUserProjectPermission(String singleUserId, String projectId) {
		return SecurityUserProjectUtils.getUserProjectPermission(singleUserId, projectId);
	}

	/**
	 * See if specific project is global
	 * @return
	 */
	public static boolean projectIsGlobal(String projectId) {
		//		String query = "SELECT ENGINEID FROM ENGINE WHERE GLOBAL=TRUE and ENGINEID='" + engineId + "'";
		//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__GLOBAL", "==", true, PixelDataType.BOOLEAN));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__PROJECTID", "==", projectId));
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
	 * Determine if the user is the owner of a project
	 * @param userFilters
	 * @param engineId
	 * @return
	 */
	public static boolean userIsOwner(User user, String projectId) {
		return SecurityUserProjectUtils.userIsOwner(getUserFiltersQs(user), projectId)
				|| SecurityGroupProjectUtils.userGroupIsOwner(user, projectId);
	}

	/**
	 * Determine if a user can view a project
	 * @param user
	 * @param projectId
	 * @return
	 */
	public static boolean userCanViewProject(User user, String projectId) {
		return SecurityUserProjectUtils.userCanViewProject(user, projectId)
				|| SecurityGroupProjectUtils.userGroupCanViewProject(user, projectId);
	}

	/**
	 * Determine if the user can modify the database
	 * @param projectId
	 * @param userId
	 * @return
	 */
	public static boolean userCanEditProject(User user, String projectId) {
		return SecurityUserProjectUtils.userCanEditProject(user, projectId)
				|| SecurityGroupProjectUtils.userGroupCanEditProject(user, projectId);
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
	 * Retrieve the list of users for a given project
	 * @param user
	 * @param engineId
	 * @param insightId
	 * @return
	 * @throws IllegalAccessException
	 */
	public static List<Map<String, Object>> getProjectUsers(User user, String projectId) throws IllegalAccessException {
		if(!userCanViewProject(user, projectId)) {
			throw new IllegalArgumentException("The user does not have access to view this project");
		}

		//		String query = "SELECT SMSS_USER.ID AS \"id\", "
		//				+ "SMSS_USER.NAME AS \"name\", "
		//				+ "PERMISSION.NAME AS \"permission\" "
		//				+ "FROM SMSS_USER "
		//				+ "INNER JOIN ENGINEPERMISSION ON (USER.ID = ENGINEPERMISSION.USERID) "
		//				+ "INNER JOIN PERMISSION ON (ENGINEPERMISSION.PERMISSION = PERMISSION.ID) "
		//				+ "WHERE ENGINEPERMISSION.ENGINEID='" + appId + "';"
		//				;
		//		
		//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__ID", "id"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__NAME", "name"));
		qs.addSelector(new QueryColumnSelector("PERMISSION__NAME", "permission"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__PROJECTID", "==", projectId));
		qs.addRelation("SMSS_USER", "PROJECTPERMISSION", "inner.join");
		qs.addRelation("PROJECTPERMISSION", "PERMISSION", "inner.join");
		qs.addOrderBy(new QueryColumnOrderBySelector("SMSS_USER__ID"));
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}

	/**
	 * 
	 * @param user
	 * @param newUserId
	 * @param projectId
	 * @param permission
	 * @return
	 * @throws IllegalAccessException 
	 */
	public static void addProjectUser(User user, String newUserId, String projectId, String permission) throws IllegalAccessException {
		// make sure user can edit the app
		int userPermissionLvl = getMaxUserProjectPermission(user, projectId);
		if(!AccessPermissionEnum.isEditor(userPermissionLvl)) {
			throw new IllegalAccessException("Insufficient privileges to modify this project's permissions.");
		}
				
		// make sure user doesn't already exist for this insight
		if(getUserProjectPermission(newUserId, projectId) != null) {
			// that means there is already a value
			throw new IllegalArgumentException("This user already has access to this project. Please edit the existing permission level.");
		}
		
		// if i am not an owner
		// then i need to check if i can edit this users permission
		if(!AccessPermissionEnum.isOwner(userPermissionLvl)) {
			int newPermissionLvl = AccessPermissionEnum.getIdByPermission(permission);

			// cannot give some owner permission if i am just an editor
			if(AccessPermissionEnum.OWNER.getId() == newPermissionLvl) {
				throw new IllegalAccessException("Cannot give owner level access to this project since you are not currently an owner.");
			}
		}

		String query = "INSERT INTO PROJECTPERMISSION (USERID, PROJECTID, VISIBILITY, PERMISSION) VALUES('"
				+ RdbmsQueryBuilder.escapeForSQLStatement(newUserId) + "', '"
				+ RdbmsQueryBuilder.escapeForSQLStatement(projectId) + "', "
				+ "TRUE, "
				+ AccessPermissionEnum.getIdByPermission(permission) + ");";

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
	public static void editProjectUserPermission(User user, String existingUserId, String projectId, String newPermission) throws IllegalAccessException {
		// make sure user can edit the app
		int userPermissionLvl = getMaxUserProjectPermission(user, projectId);
		if(!AccessPermissionEnum.isEditor(userPermissionLvl)) {
			throw new IllegalAccessException("Insufficient privileges to modify this project's permissions.");
		}

		// make sure we are trying to edit a permission that exists
		Integer existingUserPermission = getUserProjectPermission(existingUserId, projectId);
		if(existingUserPermission == null) {
			throw new IllegalArgumentException("Attempting to modify project permission for a user who does not currently have access to the project");
		}

		int newPermissionLvl = AccessPermissionEnum.getIdByPermission(newPermission);

		// if i am not an owner
		// then i need to check if i can edit this users permission
		if(!AccessPermissionEnum.isOwner(userPermissionLvl)) {
			// not an owner, check if trying to edit an owner or an editor/reader
			// get the current permission
			if(AccessPermissionEnum.OWNER.getId() == existingUserPermission) {
				throw new IllegalAccessException("The user doesn't have the high enough permissions to modify this users project permission.");
			}

			// also, cannot give some owner permission if i am just an editor
			if(AccessPermissionEnum.OWNER.getId() == newPermissionLvl) {
				throw new IllegalAccessException("Cannot give owner level access to this project since you are not currently an owner.");
			}
		}

		String query = "UPDATE PROJECTPERMISSION SET PERMISSION=" + newPermissionLvl
				+ " WHERE USERID='" + RdbmsQueryBuilder.escapeForSQLStatement(existingUserId) + "' "
				+ "AND PROJECTID='"	+ RdbmsQueryBuilder.escapeForSQLStatement(projectId) + "';";
		try {
			securityDb.insertData(query);
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occured updating the user permissions for this project");
		}
	}


	/**
	 * 
	 * @param user
	 * @param editedUserId
	 * @param projectId
	 * @return
	 * @throws IllegalAccessException 
	 */
	public static void removeProjectUser(User user, String existingUserId, String projectId) throws IllegalAccessException {
		// make sure user can edit the app
		int userPermissionLvl = getMaxUserProjectPermission(user, projectId);
		if(!AccessPermissionEnum.isEditor(userPermissionLvl)) {
			throw new IllegalAccessException("Insufficient privileges to modify this project's permissions.");
		}

		// make sure we are trying to edit a permission that exists
		Integer existingUserPermission = getUserProjectPermission(existingUserId, projectId);
		if(existingUserPermission == null) {
			throw new IllegalArgumentException("Attempting to modify user permission for a user who does not currently have access to the project");
		}

		// if i am not an ownerId
		// then i need to check if i can remove this users permission
		if(!AccessPermissionEnum.isOwner(userPermissionLvl)) {
			// not an owner, check if trying to edit an owner or an editor/reader
			// get the current permission
			if(AccessPermissionEnum.OWNER.getId() == existingUserPermission) {
				throw new IllegalAccessException("The user doesn't have the high enough permissions to modify this users project permission.");
			}
		}

		String query = "DELETE FROM PROJECTPERMISSION WHERE USERID='" 
				+ RdbmsQueryBuilder.escapeForSQLStatement(existingUserId) + "' "
				+ "AND PROJECTID='"	+ RdbmsQueryBuilder.escapeForSQLStatement(projectId) + "';";
		try {
			securityDb.insertData(query);
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occured removing the user permissions for this project");
		}

		// need to also delete all insight permissions for this app
		query = "DELETE FROM USERINSIGHTPERMISSION WHERE USERID='" 
				+ RdbmsQueryBuilder.escapeForSQLStatement(existingUserId) + "' "
				+ "AND PROJECTID='"	+ RdbmsQueryBuilder.escapeForSQLStatement(projectId) + "';";
		try {
			securityDb.insertData(query);
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occured removing the user permissions for the insights of this project");
		}
	}


	/**
	 * Set if the project is public to all users on this instance
	 * @param user
	 * @param projectId
	 * @param global
	 * @return
	 * @throws IllegalAccessException 
	 */
	public static boolean setProjectGlobal(User user, String projectId, boolean global) throws IllegalAccessException {
		if(!SecurityUserProjectUtils.userIsOwner(user, projectId)) {
			throw new IllegalAccessException("The user doesn't have the permission to set this project as global. Only the owner or an admin can perform this action.");
		}
		
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
	 * update the project name
	 * @param user
	 * @param projectId
	 * @param isPublic
	 * @return
	 */
	public static boolean setProjectName(User user, String projectId, String newProjectName) {
		if(!SecurityUserProjectUtils.userIsOwner(user, projectId)) {
			throw new IllegalArgumentException("The user doesn't have the permission to change the project name. Only the owner or an admin can perform this action.");
		}
		newProjectName = RdbmsQueryBuilder.escapeForSQLStatement(newProjectName);
		projectId = RdbmsQueryBuilder.escapeForSQLStatement(projectId);
		String query = "UPDATE PROJECT SET PROJECTNAME = '" + newProjectName + "' WHERE PROJECTID ='" + projectId + "';";
		securityDb.execUpdateAndRetrieveStatement(query, true);
		securityDb.commit();
		return true;
	}


	/*
	 * Project Metadata
	 */

	/**
	 * Update the project metadata
	 * Will delete existing values and then perform a bulk insert
	 * @param projectId
	 * @param insightId
	 * @param tags
	 */
	public static void updateProjectMetadata(String projectId, Map<String, Object> metadata) {
		// first do a delete
		String deleteQ = "DELETE FROM PROJECTMETA WHERE METAKEY=? AND PROJECTID=?";
		PreparedStatement deletePs = null;
		try {
			deletePs = securityDb.getPreparedStatement(deleteQ);
			for(String field : metadata.keySet()) {
				int parameterIndex = 1;
				deletePs.setString(parameterIndex++, field);
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
		
		// now we do the new insert with the order of the tags
		String query = securityDb.getQueryUtil().createInsertPreparedStatementString("PROJECTMETA", new String[]{"PROJECTID", "METAKEY", "METAVALUE", "METAORDER"});
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
					
					ps.setString(parameterIndex++, projectId);
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
//	 * Update the project description
//	 * Will perform an insert if the description doesn't currently exist
//	 * @param projectId
//	 * @param insideId
//	 */
//	public static void updateProjectDescription(String projectId, String description) {
//		// try to do an update
//		// if nothing is updated
//		// do an insert
//		projectId = RdbmsQueryBuilder.escapeForSQLStatement(projectId);
//		String query = "UPDATE PROJECTMETA SET METAVALUE='" 
//				+ AbstractSqlQueryUtil.escapeForSQLStatement(description) + "' "
//				+ "WHERE METAKEY='description' AND PROJECTID='" + projectId + "'";
//		Statement stmt = null;
//		try {
//			stmt = securityDb.execUpdateAndRetrieveStatement(query, false);
//			if(stmt.getUpdateCount() <= 0) {
//				// need to perform an insert
//				query = securityDb.getQueryUtil().insertIntoTable("PROJECTMETA", 
//						new String[]{"PROJECTID", "METAKEY", "METAVALUE", "METAORDER"}, 
//						new String[]{"varchar(255)", "varchar(255)", "clob", "int"}, 
//						new Object[]{projectId, "description", description, 0});
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
//
//	/**
//	 * Update the project tags
//	 * Will delete existing values and then perform a bulk insert
//	 * @param projectId
//	 * @param insightId
//	 * @param tags
//	 */
//	public static void updateProjectTags(String projectId, List<String> tags) {
//		// first do a delete
//		String query = "DELETE FROM PROJECTMETA WHERE METAKEY='tag' AND PROJECTID='" + projectId + "'";
//		try {
//			securityDb.insertData(query);
//			securityDb.commit();
//		} catch (SQLException e) {
//			logger.error(Constants.STACKTRACE, e);
//		}
//
//		// now we do the new insert with the order of the tags
//		query = securityDb.getQueryUtil().createInsertPreparedStatementString("PROJECTMETA", 
//				new String[]{"PROJECTID", "METAKEY", "METAVALUE", "METAORDER"});
//		PreparedStatement ps = null;
//		try {
//			ps = securityDb.getPreparedStatement(query);
//			for(int i = 0; i < tags.size(); i++) {
//				String tag = tags.get(i);
//				ps.setString(1, projectId);
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
	 * Get the wrapper for additional project metadata
	 * @param projectId
	 * @param metaKeys
	 * @return
	 * @throws Exception 
	 */
	public static IRawSelectWrapper getProjectMetadataWrapper(Collection<String> projectId, List<String> metaKeys) throws Exception {
		SelectQueryStruct qs = new SelectQueryStruct();
		// selectors
		qs.addSelector(new QueryColumnSelector("PROJECTMETA__PROJECTID"));
		qs.addSelector(new QueryColumnSelector("PROJECTMETA__METAKEY"));
		qs.addSelector(new QueryColumnSelector("PROJECTMETA__METAVALUE"));
		qs.addSelector(new QueryColumnSelector("PROJECTMETA__METAORDER"));
		// filters
		if(projectId != null && !projectId.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTMETA__PROJECTID", "==", projectId));
		}
		if(metaKeys != null && !metaKeys.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTMETA__METAKEY", "==", metaKeys));
		}
		// order
		qs.addSelector(new QueryColumnSelector("PROJECTMETA__METAORDER"));
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
		return wrapper;
	}

	
	/**
	 * Get the metadata for a specific project
	 * @param projectId
	 * @return
	 */
	public static Map<String, Object> getAggregateProjectMetadata(String projectId) {
		Map<String, Object> retMap = new HashMap<String, Object>();

		List<String> projectIds = new ArrayList<>();
		projectIds.add(projectId);

		IRawSelectWrapper wrapper = null;
		try {
			wrapper = getProjectMetadataWrapper(projectIds, null);
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
	
	
	/*
	 * Copying permissions
	 */
	
	/**
	 * Copy the project permissions from one project to another
	 * @param sourceProjectId
	 * @param targetProjectId
	 * @throws SQLException
	 */
	public static void copyProjectPermissions(String sourceProjectId, String targetProjectId) throws Exception {
		String insertTargetAppPermissionSql = "INSERT INTO PROJECTPERMISSION (PROJECTID, USERID, PERMISSION, VISIBILITY) VALUES (?, ?, ?, ?)";
		PreparedStatement insertTargetAppPermissionStatement = securityDb.getPreparedStatement(insertTargetAppPermissionSql);
		
		// grab the permissions, filtered on the source engine id
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__PROJECTID"));
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__USERID"));
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__PERMISSION"));
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__VISIBILITY"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__PROJECTID", "==", sourceProjectId));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			while(wrapper.hasNext()) {
				Object[] row = wrapper.next().getValues();
				// now loop through all the permissions
				// but with the target engine id instead of the source engine id
				insertTargetAppPermissionStatement.setString(1, targetProjectId);
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
		String deleteTargetAppPermissionsSql = "DELETE FROM PROJECTPERMISSION WHERE PROJECTID = '" + AbstractSqlQueryUtil.escapeForSQLStatement(targetProjectId) + "'";
		securityDb.removeData(deleteTargetAppPermissionsSql);
		// execute the query
		insertTargetAppPermissionStatement.executeBatch();
	}
	

	/**
	 * Returns List of users that have no access credentials to a given App.
	 * @param appID
	 * @return 
	 */
	public static List<Map<String, Object>> getProjectUsersNoCredentials(User user, String projectId) throws IllegalAccessException {
		/*
		 * Security check to make sure that the user can view the application provided. 
		 */
		if(!userCanViewProject(user, projectId)) {
			throw new IllegalArgumentException("The user does not have access to view this project");
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
			subQs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__USERID"));
			subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__PROJECTID","==",projectId));
			subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__PERMISSION", "!=", null, PixelDataType.NULL_VALUE));
		}
		
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}
	
	/**
	 * Get the list of the engine information that the user has access to
	 * @param user
	 * @param favoritesOnly
	 * @param projectMetadataFilter
	 * @return
	 */
	public static List<Map<String, Object>> getUserProjectList(User user, boolean favoritesOnly, 
			Map<String,Object> projectMetadataFilter, String limit, String offset) {
		Collection<String> userIds = getUserFiltersQs(user);
		
		
		String groupProjectPermission = "GROUPPROJECTPERMISSION__";
		String projectPrefix = "PROJECT__";
		
		SelectQueryStruct qs1 = new SelectQueryStruct();
		// selectors
		qs1.addSelector(new QueryColumnSelector("PROJECT__PROJECTID", "project_id"));
		qs1.addSelector(new QueryColumnSelector("PROJECT__PROJECTNAME", "project_name"));
		qs1.addSelector(new QueryColumnSelector("PROJECT__TYPE", "project_type"));
		qs1.addSelector(new QueryColumnSelector("PROJECT__COST", "project_cost"));
		qs1.addSelector(new QueryColumnSelector("PROJECT__GLOBAL", "project_global"));
		qs1.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.LOWER, "PROJECT__PROJECTNAME", "low_project_name"));
		qs1.addSelector(new QueryColumnSelector("USER_PERMISSIONS__FAVORITE", "project_favorite"));
		qs1.addSelector(new QueryColumnSelector("USER_PERMISSIONS__PERMISSION", "user_permission"));
		qs1.addSelector(new QueryColumnSelector("GROUP_PERMISSIONS__PERMISSION", "group_permission"));
		
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
			qs2.addSelector(new QueryColumnSelector("PROJECTPERMISSION__PROJECTID", "PROJECTID"));
			qs2.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.MAX, "PROJECTPERMISSION__FAVORITE", "FAVORITE"));
			qs2.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.MIN, "PROJECTPERMISSION__PERMISSION", "PERMISSION"));
			qs2.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.MAX, "PROJECTPERMISSION__VISIBILITY", "VISIBILITY"));
			qs2.addGroupBy(new QueryColumnSelector("PROJECTPERMISSION__PROJECTID", "PROJECTID"));
			qs2.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", userIds));
			IRelation subQuery = new SubqueryRelationship(qs2, "USER_PERMISSIONS", "left.outer.join", new String[] {"USER_PERMISSIONS__PROJECTID", "PROJECT__PROJECTID", "="});
			qs1.addRelation(subQuery);
		}
		
		// add a join to get the group permission level
		{
			SelectQueryStruct qs3 = new SelectQueryStruct();
			qs3.addSelector(new QueryColumnSelector(groupProjectPermission + "PROJECTID", "PROJECTID"));
			qs3.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.MIN, groupProjectPermission + "PERMISSION", "PERMISSION"));
			qs3.addGroupBy(new QueryColumnSelector(groupProjectPermission + "PROJECTID", "PROJECTID"));
			
			// filter on groups
			OrQueryFilter groupProjectOrFilters = new OrQueryFilter();
			List<AuthProvider> logins = user.getLogins();
			for(AuthProvider login : logins) {
				if(user.getAccessToken(login).getUserGroups().isEmpty()) {
					continue;
				}
				
				AndQueryFilter andFilter = new AndQueryFilter();
				andFilter.addFilter(SimpleQueryFilter.makeColToValFilter(groupProjectPermission + "TYPE", "==", user.getAccessToken(login).getUserGroupType()));
				andFilter.addFilter(SimpleQueryFilter.makeColToValFilter(groupProjectPermission + "ID", "==", user.getAccessToken(login).getUserGroups()));
				groupProjectOrFilters.addFilter(andFilter);
			}
			
			if (!groupProjectOrFilters.isEmpty()) {
				qs3.addExplicitFilter(groupProjectOrFilters);
			} else {
				AndQueryFilter andFilter1 = new AndQueryFilter();
				andFilter1.addFilter(SimpleQueryFilter.makeColToValFilter(groupProjectPermission + "TYPE", "==", null));
				andFilter1.addFilter(SimpleQueryFilter.makeColToValFilter(groupProjectPermission + "ID", "==", null));
				qs3.addExplicitFilter(andFilter1);
			}
			
			IRelation subQuery = new SubqueryRelationship(qs3, "GROUP_PERMISSIONS", "left.outer.join", new String[] {"GROUP_PERMISSIONS__PROJECTID", "PROJECT__PROJECTID", "="});
			qs1.addRelation(subQuery);
		}
		
		// filters
		OrQueryFilter orFilter = new OrQueryFilter();
		{
			orFilter.addFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__GLOBAL", "==", true, PixelDataType.BOOLEAN));
			orFilter.addFilter(SimpleQueryFilter.makeColToValFilter("USER_PERMISSIONS__PERMISSION", "!=", null, PixelDataType.CONST_INT));
			orFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUP_PERMISSIONS__PERMISSION", "!=", null, PixelDataType.CONST_INT));
			qs1.addExplicitFilter(orFilter);
		}
		// only show those that are visible
		qs1.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USER_PERMISSIONS__VISIBILITY", "==", Arrays.asList(new Object[] {true, null}), PixelDataType.BOOLEAN));
		// favorites only
		if(favoritesOnly) {
			qs1.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USER_PERMISSIONS__FAVORITE", "==", true, PixelDataType.BOOLEAN));
		}
		// filtering by projectmeta key-value pairs (i.e. <tag>:value): for each pair, add in-filter against projectids from subquery
		if (projectMetadataFilter!=null && !projectMetadataFilter.isEmpty()) {
			for (String k : projectMetadataFilter.keySet()) {
				SelectQueryStruct subQs = new SelectQueryStruct();
				subQs.addSelector(new QueryColumnSelector("PROJECTMETA__PROJECTID"));
				subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTMETA__METAKEY", "==", k));
				subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTMETA__METAVALUE", "==", projectMetadataFilter.get(k)));
				qs1.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery("PROJECT__PROJECTID", "==", subQs));
			}
		}
		
		{
			// first lets make sure we have any groups
			OrQueryFilter groupProjectOrFilters = new OrQueryFilter();
			List<AuthProvider> logins = user.getLogins();
			for(AuthProvider login : logins) {
				if(user.getAccessToken(login).getUserGroups().isEmpty()) {
					continue;
				}
				AndQueryFilter andFilter = new AndQueryFilter();
				andFilter.addFilter(SimpleQueryFilter.makeColToValFilter(groupProjectPermission + "TYPE", "==", user.getAccessToken(login).getUserGroupType()));
				andFilter.addFilter(SimpleQueryFilter.makeColToValFilter(groupProjectPermission + "ID", "==", user.getAccessToken(login).getUserGroups()));
				groupProjectOrFilters.addFilter(andFilter);
			}
			// 4.a does the group have explicit access
			if(!groupProjectOrFilters.isEmpty()) {
				SelectQueryStruct subQs = new SelectQueryStruct();
				// store first and fill in sub query after
				orFilter.addFilter(SimpleQueryFilter.makeColToSubQuery(projectPrefix + "PROJECTID", "==", subQs));
				
				// we need to have the insight filters
				subQs.addSelector(new QueryColumnSelector(groupProjectPermission + "PROJECTID"));
				subQs.addExplicitFilter(groupProjectOrFilters);
			}
		}
		
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
	 * Get all user engines and engine Ids regardless of it being hidden or not 
	 * @param userId
	 * @return
	 */
	public static List<Map<String, Object>> getAllUserProjectList(User user) {	
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTID", "project_id"));
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTNAME", "project_name"));
		qs.addSelector(new QueryColumnSelector("PROJECT__TYPE", "project_type"));
		qs.addSelector(new QueryColumnSelector("PROJECT__COST", "project_cost"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__GLOBAL", "==", true, PixelDataType.BOOLEAN));
		List<Map<String, Object>> allGlobalEnginesMap = QueryExecutionUtility.flushRsToMap(securityDb, qs);

		SelectQueryStruct qs2 = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTID", "project_id"));
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTNAME", "project_name"));
		qs.addSelector(new QueryColumnSelector("PROJECT__TYPE", "project_type"));
		qs.addSelector(new QueryColumnSelector("PROJECT__COST", "project_cost"));
		qs2.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", getUserFiltersQs(user)));
		qs.addRelation("PROJECT", "PROJECTPERMISSION", "left.outer.join");
		
		List<Map<String, Object>> engineMap = QueryExecutionUtility.flushRsToMap(securityDb, qs2);
		engineMap.addAll(allGlobalEnginesMap);
		return engineMap;
	}
	
	/**
	 * Get the list of the project information that the user has access to
	 * @param userId
	 * @return
	 */
	public static List<Map<String, Object>> getUserProjectList(User user, String projectFilter) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTID", "project_id"));
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTNAME", "project_name"));
		qs.addSelector(new QueryColumnSelector("PROJECT__TYPE", "project_type"));
		qs.addSelector(new QueryColumnSelector("PROJECT__COST", "project_cost"));
		QueryFunctionSelector fun = new QueryFunctionSelector();
		fun.setFunction(QueryFunctionHelper.LOWER);
		fun.addInnerSelector(new QueryColumnSelector("PROJECT__PROJECTNAME"));
		fun.setAlias("low_project_name");
		qs.addSelector(fun);
		if(projectFilter != null && !projectFilter.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__PROJECTID", "==", projectFilter));
		}
		{
			OrQueryFilter orFilter = new OrQueryFilter();
			orFilter.addFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__GLOBAL", "==", true, PixelDataType.BOOLEAN));
			orFilter.addFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", getUserFiltersQs(user)));
			qs.addExplicitFilter(orFilter);
		}
		qs.addRelation("PROJECT", "PROJECTPERMISSION", "left.outer.join");
		qs.addOrderBy(new QueryColumnOrderBySelector("low_project_name"));
		
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}
	
	/**
	 * Get the list of the projects with an optional filter
	 * @param userId
	 * @return
	 */
	public static List<Map<String, Object>> getAllProjectList(String projectFilter, String limit, String offset) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTID", "project_id"));
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTNAME", "project_name"));
		qs.addSelector(new QueryColumnSelector("PROJECT__TYPE","project_type"));
		qs.addSelector(new QueryColumnSelector("PROJECT__COST", "project_cost"));
		QueryFunctionSelector fun = new QueryFunctionSelector();
		fun.setFunction(QueryFunctionHelper.LOWER);
		fun.addInnerSelector(new QueryColumnSelector("PROJECT__PROJECTNAME"));
		fun.setAlias("low_project_name");
		qs.addSelector(fun);
		if(projectFilter != null && !projectFilter.isEmpty()) { 
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__PROJECTID", "==", projectFilter));
		}
		qs.addOrderBy(new QueryColumnOrderBySelector("low_project_name"));
		
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
	 * Change the user visibility (show/hide) for a project. Without removing its permissions.
	 * @param user
	 * @param projectId
	 * @param visibility
	 * @throws SQLException 
	 * @throws IllegalAccessException 
	 */
	public static void setProjectVisibility(User user, String projectId, boolean visibility) throws SQLException, IllegalAccessException {
		if(!userCanViewProject(user, projectId)) {
			throw new IllegalAccessException("The user doesn't have the permission to modify his visibility of this project.");
		}
		Collection<String> userIdFilters = getUserFiltersQs(user);
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__PROJECTID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", userIdFilters));

		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if(wrapper.hasNext()){
				UpdateQueryStruct uqs = new UpdateQueryStruct();
				uqs.setEngine(securityDb);
				uqs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__PROJECTID", "==", projectId));
				uqs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", userIdFilters));

				List<IQuerySelector> selectors = new Vector<>();
				selectors.add(new QueryColumnSelector("PROJECTPERMISSION__VISIBILITY"));
				List<Object> values = new Vector<>();
				values.add(visibility);
				uqs.setSelectors(selectors);
				uqs.setValues(values);
				
				UpdateSqlInterpreter updateInterp = new UpdateSqlInterpreter(uqs);
				String updateQuery = updateInterp.composeQuery();
				securityDb.insertData(updateQuery);
				
			} else {
				// need to insert
				PreparedStatement ps = securityDb.getPreparedStatement("INSERT INTO PROJECTPERMISSION "
						+ "(USERID, PROJECTID, VISIBILITY, FAVORITE, PERMISSION) VALUES (?,?,?,?,?)");
				if(ps == null) {
					throw new IllegalArgumentException("Error generating prepared statement to set project visibility");
				}
				try {
					// we will set the permission to read only
					for(AuthProvider loginType : user.getLogins()) {
						String userId = user.getAccessToken(loginType).getId();
						int parameterIndex = 1;
						ps.setString(parameterIndex++, userId);
						ps.setString(parameterIndex++, projectId);
						ps.setBoolean(parameterIndex++, visibility);
						// default favorite as false
						ps.setBoolean(parameterIndex++, false);
						ps.setInt(parameterIndex++, 3);
	
						ps.addBatch();
					}
					ps.executeBatch();
				} catch(Exception e) {
					logger.error(Constants.STACKTRACE, e);
					throw e;
				} finally {
					if(ps != null) {
						ps.close();
					}
				}
			}
			
			securityDb.commit();
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
		}
	}
	
	/**
	 * Change the user favorite (is favorite / not favorite) for a project. Without removing its permissions.
	 * @param user
	 * @param projectId
	 * @param visibility
	 * @throws SQLException 
	 * @throws IllegalAccessException 
	 */
	public static void setProjectFavorite(User user, String projectId, boolean isFavorite) throws SQLException, IllegalAccessException {
		if(!userCanViewProject(user, projectId)) {
			throw new IllegalAccessException("The user doesn't have the permission to modify his visibility of this project.");
		}
		Collection<String> userIdFilters = getUserFiltersQs(user);
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__PROJECTID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", userIdFilters));

		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if(wrapper.hasNext()){
				UpdateQueryStruct uqs = new UpdateQueryStruct();
				uqs.setEngine(securityDb);
				uqs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__PROJECTID", "==", projectId));
				uqs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", userIdFilters));

				List<IQuerySelector> selectors = new Vector<>();
				selectors.add(new QueryColumnSelector("PROJECTPERMISSION__FAVORITE"));
				List<Object> values = new Vector<>();
				values.add(isFavorite);
				uqs.setSelectors(selectors);
				uqs.setValues(values);
				
				UpdateSqlInterpreter updateInterp = new UpdateSqlInterpreter(uqs);
				String updateQuery = updateInterp.composeQuery();
				securityDb.insertData(updateQuery);
				
			} else {
				// need to insert
				PreparedStatement ps = securityDb.getPreparedStatement("INSERT INTO PROJECTPERMISSION "
						+ "(USERID, PROJECTID, VISIBILITY, FAVORITE, PERMISSION) VALUES (?,?,?,?,?)");
				if(ps == null) {
					throw new IllegalArgumentException("Error generating prepared statement to set project visibility");
				}
				try {
					// we will set the permission to read only
					for(AuthProvider loginType : user.getLogins()) {
						String userId = user.getAccessToken(loginType).getId();
						int parameterIndex = 1;
						ps.setString(parameterIndex++, userId);
						ps.setString(parameterIndex++, projectId);
						// default visibility as true
						ps.setBoolean(parameterIndex++, true);
						ps.setBoolean(parameterIndex++, isFavorite);
						ps.setInt(parameterIndex++, 3);
	
						ps.addBatch();
					}
					ps.executeBatch();
				} catch(Exception e) {
					logger.error(Constants.STACKTRACE, e);
					throw e;
				} finally {
					if(ps != null) {
						ps.close();
					}
				}
			}
			
			// commit regardless of insert or update
			securityDb.commit();
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
		}
	}

	/**
	 * Get all projects for setting options that the user has access to
	 * @param usersId
	 * @return
	 */
	public static List<Map<String, Object>> getAllUserProjectSettings(User user) {
		return getAllUserProjectSettings(user, null);
	}
	
	/**
	 * Get project settings - if projectFilter passed will filter to that project otherwise returns all
	 * @param user
	 * @param projectFilter
	 * @return
	 */
	public static List<Map<String, Object>> getAllUserProjectSettings(User user, String projectFilter) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTID", "project_id"));
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTNAME", "project_name"));
		qs.addSelector(new QueryColumnSelector("PROJECT__GLOBAL", "project_global"));
		qs.addSelector(QueryFunctionSelector.makeCol2ValCoalesceSelector("PROJECTPERMISSION__VISIBILITY", true, "project_visibility"));
		qs.addSelector(QueryFunctionSelector.makeCol2ValCoalesceSelector("PERMISSION__NAME", "READ_ONLY", "project_permission"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", getUserFiltersQs(user)));
		if(projectFilter != null && !projectFilter.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__PROJECTID", "==", projectFilter));
		}
		qs.addRelation("PROJECT", "PROJECTPERMISSION", "inner.join");
		qs.addRelation("PROJECTPERMISSION", "PERMISSION", "left.outer.join");
		
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
		
		// we dont need to run 2nd query if we are filtering to one db and already have it
		if(projectFilter != null && !projectFilter.isEmpty() && !result.isEmpty()) {
			qs = new SelectQueryStruct();
			qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTID", "project_id"));
			qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTNAME", "project_name"));
			qs.addSelector(QueryFunctionSelector.makeCol2ValCoalesceSelector("PROJECTPERMISSION__VISIBILITY", true, "project_visibility"));
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__GLOBAL", "==", true, PixelDataType.BOOLEAN));
			// since some rdbms do not allow "not in ()" - we will only add if necessary
			if(!engineIdsIncluded.isEmpty()) {
				qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__PROJECTID", "!=", new Vector<String>(engineIdsIncluded)));
			}
			if(projectFilter != null && !projectFilter.isEmpty()) {
				qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__PROJECTID", "==", projectFilter));
			}
			qs.addRelation("PROJECT", "PROJECTPERMISSION", "left.outer.join");
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
					map.put("project_global", true);
					map.put("project_permission", "READ_ONLY");
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
					String appName1 = o1.get("project_name").toString().toLowerCase();
					String appName2 = o2.get("project_name").toString().toLowerCase();
					return appName1.compareTo(appName2);
				}
			
			});
		}
		
		return result;
	}

///////////////////////////////////////////////
///////////////////////////////////////////////
/////////////////PROJECTS//////////////////////

	/**
	 * Return the projects the user has explicit access to
	 * 
	 * @param singleUserId
	 * @return
	 */
	public static Set<String> getProjectsUserHasExplicitAccess(User user) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTID"));
		OrQueryFilter orFilter = new OrQueryFilter();
		orFilter.addFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__GLOBAL", "==", true, PixelDataType.BOOLEAN));
		orFilter.addFilter(
				SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", getUserFiltersQs(user)));
		qs.addExplicitFilter(orFilter);
		qs.addRelation("PROJECT", "PROJECTPERMISSION", "left.outer.join");
		return QueryExecutionUtility.flushToSetString(securityDb, qs, false);
	}

	public static List<Map<String, Object>> getProjectInfo(Collection dbFilter) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTID"));
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTNAME"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__PROJECTID", "==", dbFilter));
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}

	/**
	 * Get the list of projects the user does not have access to but can request
	 * 
	 * @param allUserProjects
	 * @throws Exception
	 */
	public static List<Map<String, Object>> getUserRequestableProjects(Collection<String> allUserProjects) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTID"));
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTNAME"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__PROJECTID", "!=", allUserProjects));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__DISCOVERABLE", "==", true, PixelDataType.BOOLEAN));
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}
	
	/**
	 * Determine if a user can request a project
	 * @param projectId
	 * @return
	 */
	public static boolean canRequestProject(String projectId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__DISCOVERABLE", "==", true, PixelDataType.BOOLEAN));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__PROJECTID", "==", projectId));
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
	 * Retrieve the project owner
	 * @param user
	 * @param projectId
	 * @param insightId
	 * @return
	 * @throws IllegalAccessException
	 */
	public static List<String> getProjectOwners(String projectId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__EMAIL", "email"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PERMISSION__ID", "==", AccessPermissionEnum.OWNER.getId()));
		qs.addRelation("SMSS_USER", "PROJECTPERMISSION", "inner.join");
		qs.addRelation("PROJECTPERMISSION", "PERMISSION", "inner.join");
		qs.addOrderBy(new QueryColumnOrderBySelector("SMSS_USER__ID"));
		return QueryExecutionUtility.flushToListString(securityDb, qs);
	}
	
	/**
	 * 
	 * @param metakey
	 * @return
	 */
	public static List<Map<String, Object>> getMetakeyOptions( String metakey) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECTMETAKEYS__METAKEY"));
		qs.addSelector(new QueryColumnSelector("PROJECTMETAKEYS__SINGLEMULTI"));
		qs.addSelector(new QueryColumnSelector("PROJECTMETAKEYS__DISPLAYORDER"));
		qs.addSelector(new QueryColumnSelector("PROJECTMETAKEYS__DISPLAYOPTIONS"));
		if (metakey != null && !metakey.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTMETAKEYS__METAKEY", "==", metakey));
		}
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}
	
	/**
	 * 
	 * @param metaoptions
	 * @return
	 */
	public static boolean updateMetakeyOptions( List<Map<String,Object>> metaoptions) {
		boolean valid = false;
		String[] colNames = new String[]{Constants.METAKEY, Constants.SINGLE_MULTI, Constants.DISPLAY_ORDER, Constants.DISPLAY_OPTIONS};
        PreparedStatement insertPs = null;
        String tableName = "PROJECTMETAKEYS";
        try {
			// first truncate table clean 
			String truncateSql = "TRUNCATE TABLE " + tableName;
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
