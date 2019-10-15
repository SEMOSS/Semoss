package prerna.auth.utils;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.auth.AccessPermission;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.ds.util.RdbmsQueryBuilder;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.FunctionQueryFilter;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryConstantSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.PixelDataType;
import prerna.util.sql.AbstractSqlQueryUtil;

public class SecurityInsightUtils extends AbstractSecurityUtils {

	/**
	 * Get what permission the user has for a given insight
	 * @param userId
	 * @param engineId
	 * @param insightId
	 * @return
	 */
	public static String getActualUserInsightPermission(User user, String engineId, String insightId) {
		Collection<String> userIds = getUserFiltersQs(user);

		// if user is owner
		// they can do whatever they want
		if(SecurityAppUtils.userIsOwner(userIds, engineId)) {
			return AccessPermission.OWNER.getPermission();
		}
		
//		// query the database
//		String query = "SELECT DISTINCT USERINSIGHTPERMISSION.PERMISSION FROM USERINSIGHTPERMISSION  "
//				+ "WHERE ENGINEID='" + engineId + "' AND INSIGHTID='" + insightId + "' AND USERID IN " + userFilters;
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("USERINSIGHTPERMISSION__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__ENGINEID", "==", engineId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__INSIGHTID", "==", insightId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__USERID", "==", userIds));
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
		try {
			if(wrapper.hasNext()) {
				Object val = wrapper.next().getValues()[0];
				if(val != null && val instanceof Number) {
					return AccessPermission.getPermissionValueById( ((Number) val).intValue() );
				}
			}
		} finally {
			wrapper.cleanUp();
		}
		
		if(SecurityQueryUtils.insightIsGlobal(engineId, insightId)) {
			return AccessPermission.READ_ONLY.getPermission();
		}
		
		return null;
	}
	
	static Integer getUserInsightPermission(String singleUserId, String engineId, String insightId) {
//		String query = "SELECT DISTINCT USERINSIGHTPERMISSION.PERMISSION FROM USERINSIGHTPERMISSION  "
//				+ "WHERE ENGINEID='" + engineId + "' AND INSIGHTID='" + insightId + "' AND USERID='" + singleUserId + "'";
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("USERINSIGHTPERMISSION__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__ENGINEID", "==", engineId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__INSIGHTID", "==", insightId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__USERID", "==", singleUserId));
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
		try {
			if(wrapper.hasNext()) {
				Object val = wrapper.next().getValues()[0];
				if(val != null && val instanceof Number) {
					return ((Number) val).intValue();
				}
			}
		} finally {
			wrapper.cleanUp();
		}
		
		return null;
	}
	
	/**
	 * Determine if the user can edit the insight
	 * User must be database owner OR be given explicit permissions on the insight
	 * @param userId
	 * @param engineId
	 * @param insightId
	 * @return
	 */
	public static boolean userCanViewInsight(User user, String engineId, String insightId) {
		Collection<String> userIds = getUserFiltersQs(user);

		if(SecurityQueryUtils.insightIsGlobal(engineId, insightId)) {
			return true;
		}
		
		// if user is owner
		// they can do whatever they want
		if(SecurityAppUtils.userIsOwner(userIds, engineId)) {
			return true;
		}
		
		// else query the database
//		String query = "SELECT DISTINCT USERINSIGHTPERMISSION.PERMISSION FROM USERINSIGHTPERMISSION  "
//				+ "WHERE ENGINEID='" + engineId + "' AND INSIGHTID='" + insightId + "' AND USERID IN " + userFilters;
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("USERINSIGHTPERMISSION__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__ENGINEID", "==", engineId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__INSIGHTID", "==", insightId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__USERID", "==", userIds));
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
		try {
			if(wrapper.hasNext()) {
				// do not care if owner/edit/read
				return true;
			}
		} finally {
			wrapper.cleanUp();
		}
		
		return false;
	}
	
	/**
	 * Determine if the user can edit the insight
	 * User must be database owner OR be given explicit permissions on the insight
	 * @param userId
	 * @param engineId
	 * @param insightId
	 * @return
	 */
	public static boolean userCanEditInsight(User user, String engineId, String insightId) {
		Collection<String> userIds = getUserFiltersQs(user);

		// if user is owner
		// they can do whatever they want
		if(SecurityAppUtils.userIsOwner(userIds, engineId)) {
			return true;
		}
		
		// else query the database
//		String query = "SELECT DISTINCT USERINSIGHTPERMISSION.PERMISSION FROM USERINSIGHTPERMISSION "
//				+ "WHERE ENGINEID='" + engineId + "' AND INSIGHTID='" + insightId + "' AND USERID IN " + userFilters;
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("USERINSIGHTPERMISSION__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__ENGINEID", "==", engineId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__INSIGHTID", "==", insightId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__USERID", "==", userIds));
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
		try {
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
		} finally {
			wrapper.cleanUp();
		}		
		return false;
	}
	
	/**
	 * Determine if the user is an owner of an insight
	 * User must be database owner OR be given explicit permissions on the insight
	 * @param userId
	 * @param engineId
	 * @param insightId
	 * @return
	 */
	public static boolean userIsInsightOwner(User user, String engineId, String insightId) {
		Collection<String> userIds = getUserFiltersQs(user);

		// if user is owner of app
		// they can do whatever they want
		if(SecurityAppUtils.userIsOwner(userIds, engineId)) {
			return true;
		}
		
		// else query the database
//		String query = "SELECT DISTINCT USERINSIGHTPERMISSION.PERMISSION FROM USERINSIGHTPERMISSION "
//				+ "WHERE ENGINEID='" + engineId + "' AND INSIGHTID='" + insightId + "' AND USERID IN " + userFilters;
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("USERINSIGHTPERMISSION__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__ENGINEID", "==", engineId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__INSIGHTID", "==", insightId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__USERID", "==", userIds));
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
		try {
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
		} finally {
			wrapper.cleanUp();
		}		
		return false;
	}
	
	/**
	 * Determine if the user can edit the insight
	 * User must be database owner OR be given explicit permissions on the insight
	 * @param userId
	 * @param engineId
	 * @param insightId
	 * @return
	 */
	static int getMaxUserInsightPermission(User user, String engineId, String insightId) {
		Collection<String> userIds = getUserFiltersQs(user);

		// if user is owner of the app
		// they can do whatever they want
		if(SecurityAppUtils.userIsOwner(userIds, engineId)) {
			// owner of engine is owner of all the insights
			return AccessPermission.OWNER.getId();
		}
		
		// else query the database
//		String query = "SELECT DISTINCT USERINSIGHTPERMISSION.PERMISSION FROM USERINSIGHTPERMISSION "
//				+ "WHERE ENGINEID='" + engineId + "' AND INSIGHTID='" + insightId + "' AND USERID IN " + userFilters + " ORDER BY PERMISSION";
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("USERINSIGHTPERMISSION__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__ENGINEID", "==", engineId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__INSIGHTID", "==", insightId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__USERID", "==", userIds));
		qs.addOrderBy(new QueryColumnOrderBySelector("USERINSIGHTPERMISSION__PERMISSION"));
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
		try {
			while(wrapper.hasNext()) {
				Object val = wrapper.next().getValues()[0];
				if(val == null) {
					return AccessPermission.READ_ONLY.getId();
				}
				int permission = ((Number) val).intValue();
				return permission;
			}
		} finally {
			wrapper.cleanUp();
		}		
		return AccessPermission.READ_ONLY.getId();
	}

	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	
	/*
	 * Modify insight details
	 */
	
	public static void setInsightGlobalWithinApp(User user, String appId, String insightId, boolean isPublic) {
		if(!userIsInsightOwner(user, appId, insightId)) {
			throw new IllegalArgumentException("The user doesn't have the permission to set this database as global. Only the owner or an admin can perform this action.");
		}
		
		String query = "UPDATE INSIGHT SET GLOBAL=" + isPublic + " WHERE ENGINEID ='" + appId + "' AND INSIGHTID='" + insightId + "';";
		try {
			securityDb.insertData(query);
		} catch (SQLException e) {
			e.printStackTrace();
			throw new IllegalArgumentException("An error occured setting this insight global");
		}
	}
	
	
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	
	/*
	 * Query for insight users
	 */
	
	/**
	 * Retrieve the list of users for a given insight
	 * @param user
	 * @param appId
	 * @param insightId
	 * @return
	 * @throws IllegalAccessException
	 */
	public static List<Map<String, Object>> getInsightUsers(User user, String appId, String insightId) throws IllegalAccessException {
		if(!userCanViewInsight(user, appId, insightId)) {
			throw new IllegalArgumentException("The user does not have access to view this insight");
		}
		
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
		qs.addSelector(new QueryColumnSelector("USER__NAME", "name"));
		qs.addSelector(new QueryColumnSelector("PERMISSION__NAME", "permission"));
		qs.addRelation("USER", "USERINSIGHTPERMISSION", "inner.join");
		qs.addRelation("USERINSIGHTPERMISSION", "PERMISSION", "inner.join");
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__ENGINEID", "==", appId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__INSIGHTID", "==", insightId));
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
		return flushRsToMap(wrapper);
	}
	
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////

	/*
	 * Adding Insight
	 */

	/**
	 * 
	 * @param engineId
	 * @param insightId
	 * @param insightName
	 * @param global
	 * @param layout
	 */
	public static void addInsight(String engineId, String insightId, String insightName, boolean global, String layout) {
		LocalDateTime now = LocalDateTime.now();
		String nowString = java.sql.Timestamp.valueOf(now).toString();
		String insightQuery = "INSERT INTO INSIGHT (ENGINEID, INSIGHTID, INSIGHTNAME, GLOBAL, EXECUTIONCOUNT, CREATEDON, LASTMODIFIEDON, LAYOUT, CACHEABLE) "
				+ "VALUES ('" + engineId + "', '" + insightId + "', '" + RdbmsQueryBuilder.escapeForSQLStatement(insightName) + "', " 
				+ global + " ," + 0 + " ,'" + nowString + "' ,'" + nowString + "','" + layout + "', true)";
		try {
			securityDb.insertData(insightQuery);
			securityDb.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 
	 * @param user
	 * @param engineId
	 * @param insightId
	 */
	public static void addUserInsightCreator(User user, String engineId, String insightId) {
		List<AuthProvider> logins = user.getLogins();
		StringBuilder insightInsert = new StringBuilder();
		for(AuthProvider login : logins) {
			insightInsert.append("INSERT INTO USERINSIGHTPERMISSION (USERID, ENGINEID, INSIGHTID, PERMISSION) "
					+ "VALUES ('" + user.getAccessToken(login).getId() + "', '" + engineId + "', '" + insightId + "', " + 1 + ");");
		}
		
		try {
			securityDb.insertData(insightInsert.toString());
			securityDb.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	
	// TODO >>>timb: push app here on create/update
	/**
	 * 
 	 * @param engineId
	 * @param insightId
	 * @param insightName
	 * @param global
	 * @param exCount
	 * @param lastModified
	 * @param layout
	 */
	public static void updateInsight(String engineId, String insightId, String insightName, boolean global, String layout) {
		LocalDateTime now = LocalDateTime.now();
		String nowString = java.sql.Timestamp.valueOf(now).toString();
		String query = "UPDATE INSIGHT SET INSIGHTNAME='" + insightName + "', GLOBAL=" + global + ", LASTMODIFIEDON='" + nowString 
				+ "', LAYOUT='" + layout + "'  WHERE INSIGHTID = '" + insightId + "' AND ENGINEID='" + engineId + "'"; 
		try {
			securityDb.insertData(query);
			securityDb.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Update the insight name
	 * @param engineId
	 * @param insightId
	 * @param insightName
	 */
	public static void updateInsightName(String engineId, String insightId, String insightName) {
		LocalDateTime now = LocalDateTime.now();
		String nowString = java.sql.Timestamp.valueOf(now).toString();
		String query = "UPDATE INSIGHT SET INSIGHTNAME='" + insightName + "', LASTMODIFIEDON='" + nowString + "' "
				+ "WHERE INSIGHTID = '" + insightId + "' AND ENGINEID='" + engineId + "'"; 
		try {
			securityDb.insertData(query);
			securityDb.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Update if an insight is cacheable
	 * @param appId
	 * @param existingId
	 * @param cache
	 */
	public static void updateInsightCache(String engineId, String insightId, boolean cache) {
		LocalDateTime now = LocalDateTime.now();
		String nowString = java.sql.Timestamp.valueOf(now).toString();
		String query = "UPDATE INSIGHT SET CACHEABLE=" + cache + ", LASTMODIFIEDON='" + nowString + "' "
				+ "WHERE INSIGHTID = '" + insightId + "' AND ENGINEID='" + engineId + "'"; 
		try {
			securityDb.insertData(query);
			securityDb.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Update the insight description
	 * Will perform an insert if the description doesn't currently exist
	 * @param engineId
	 * @param insideId
	 * @param description
	 */
	public static void updateInsightDescription(String engineId, String insightId, String description) {
		// try to do an update
		// if nothing is updated
		// do an insert
		String query = "UPDATE INSIGHTMETA SET METAVALUE='" 
				+ AbstractSqlQueryUtil.escapeForSQLStatement(description) + "' "
				+ "WHERE METAKEY='description' AND INSIGHTID='" + insightId + "' AND ENGINEID='" + engineId + "'";
		Statement stmt = null;
		try {
			stmt = securityDb.execUpdateAndRetrieveStatement(query, false);
			if(stmt.getUpdateCount() == 0) {
				// need to perform an insert
				query = securityDb.getQueryUtil().insertIntoTable("INSIGHTMETA", 
						new String[]{"ENGINEID", "INSIGHTID", "METAKEY", "METAVALUE", "METAORDER"}, 
						new String[]{"varchar(255)", "varchar(255)", "varchar(255)", "clob", "int"}, 
						new Object[]{engineId, insightId, "description", description, 0});
				securityDb.insertData(query);
			}
		} catch(SQLException e) {
			e.printStackTrace();
		} finally {
			if(stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	/**
	 * Update the insight tags for the insight
	 * Will delete existing values and then perform a bulk insert
	 * @param engineId
	 * @param insightId
	 * @param tags
	 */
	public static void updateInsightTags(String engineId, String insightId, List<String> tags) {
		// first do a delete
		String query = "DELETE FROM INSIGHTMETA WHERE METAKEY='tag' AND INSIGHTID='" + insightId + "' AND ENGINEID='" + engineId + "'";
		try {
			securityDb.insertData(query);
			securityDb.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		// now we do the new insert with the order of the tags
		query = securityDb.getQueryUtil().createInsertPreparedStatementString("INSIGHTMETA", 
				new String[]{"ENGINEID", "INSIGHTID", "METAKEY", "METAVALUE", "METAORDER"});
		PreparedStatement ps = securityDb.bulkInsertPreparedStatement(query);
		try {
			for(int i = 0; i < tags.size(); i++) {
				String tag = tags.get(i);
				ps.setString(1, engineId);
				ps.setString(2, insightId);
				ps.setString(3, "tag");
				ps.setString(4, tag);
				ps.setInt(5, i);
				ps.addBatch();;
			}
			
			ps.executeBatch();
		} catch(Exception e) {
			e.printStackTrace();
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
	 * Update the insight tags for the insight
	 * Will delete existing values and then perform a bulk insert
	 * @param engineId
	 * @param insightId
	 * @param tags
	 */
	public static void updateInsightTags(String engineId, String insightId, String[] tags) {
		// first do a delete
		String query = "DELETE FROM INSIGHTMETA WHERE METAKEY='tag' AND INSIGHTID='" + insightId + "' AND ENGINEID='" + engineId + "'";
		try {
			securityDb.insertData(query);
			securityDb.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		// now we do the new insert with the order of the tags
		query = securityDb.getQueryUtil().createInsertPreparedStatementString("INSIGHTMETA", 
				new String[]{"ENGINEID", "INSIGHTID", "METAKEY", "METAVALUE", "METAORDER"});
		PreparedStatement ps = securityDb.bulkInsertPreparedStatement(query);
		try {
			for(int i = 0; i < tags.length; i++) {
				String tag = tags[i];
				ps.setString(1, engineId);
				ps.setString(2, insightId);
				ps.setString(3, "tag");
				ps.setString(4, tag);
				ps.setInt(5, i);
				ps.addBatch();;
			}
			
			ps.executeBatch();
		} catch(Exception e) {
			e.printStackTrace();
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
	 * 
	 * @param engineId
	 * @param insightId
	 */
	public static void deleteInsight(String engineId, String insightId) {
		String query = "DELETE FROM INSIGHT WHERE INSIGHTID ='" + insightId + "' AND ENGINEID='" + engineId + "'";
		try {
			securityDb.insertData(query);
			securityDb.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		}		
		query = "DELETE FROM USERINSIGHTPERMISSION WHERE INSIGHTID ='" + insightId + "' AND ENGINEID='" + engineId + "'";
		try {
			securityDb.insertData(query);
			securityDb.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		query = "DELETE FROM INSIGHTMETA WHERE INSIGHTID ='" + insightId + "' AND ENGINEID='" + engineId + "'";
		try {
			securityDb.insertData(query);
			securityDb.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 
	 * @param engineId
	 * @param insightId
	 */
	public static void deleteInsight(String engineId, String... insightId) {
		String insightFilter = createFilter(insightId);
		String query = "DELETE FROM INSIGHT WHERE INSIGHTID " + insightFilter + " AND ENGINEID='" + engineId + "'";
		try {
			securityDb.insertData(query);
			securityDb.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		query = "DELETE FROM USERINSIGHTPERMISSION WHERE INSIGHTID " + insightFilter + " AND ENGINEID='" + engineId + "'";
		try {
			securityDb.insertData(query);
			securityDb.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Update the total execution count
	 * @param engineId
	 * @param insightId
	 */
	public static void updateExecutionCount(String engineId, String insightId) {
		String updateQuery = "UPDATE INSIGHT SET EXECUTIONCOUNT = EXECUTIONCOUNT + 1 "
				+ "WHERE ENGINEID='" + engineId + "' AND INSIGHTID='" + insightId + "'";
		try {
			securityDb.insertData(updateQuery);
			securityDb.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////

	/**
	 * 
	 * @param user
	 * @param newUserId
	 * @param engineId
	 * @param insightId
	 * @param permission
	 * @return
	 */
	public static void addInsightUser(User user, String newUserId, String engineId, String insightId, String permission) {
		if(!userCanEditInsight(user, engineId, insightId)) {
			throw new IllegalArgumentException("Insufficient privileges to modify this insight's permissions.");
		}
		
		// make sure user doesn't already exist for this insight
		if(getUserInsightPermission(newUserId, engineId, insightId) != null) {
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
			e.printStackTrace();
			throw new IllegalArgumentException("An error occured adding user permissions for this insight");
		}
	}
	
	/**
	 * 
	 * @param user
	 * @param existingUserId
	 * @param engineId
	 * @param insightId
	 * @param newPermission
	 * @return
	 */
	public static void editInsightUserPermission(User user, String existingUserId, String engineId, String insightId, String newPermission) {
		// make sure user can edit the insight
		int userPermissionLvl = getMaxUserInsightPermission(user, engineId, insightId);
		if(!AccessPermission.isEditor(userPermissionLvl)) {
			throw new IllegalArgumentException("Insufficient privileges to modify this insight's permissions.");
		}
		
		// make sure we are trying to edit a permission that exists
		Integer existingUserPermission = getUserInsightPermission(existingUserId, engineId, insightId);
		if(existingUserPermission == null) {
			throw new IllegalArgumentException("Attempting to modify user permission for a user who does not currently have access to the insight");
		}
		
		int newPermissionLvl = AccessPermission.getIdByPermission(newPermission);
		
		// if i am not an owner
		// then i need to check if i can edit this users permission
		if(!AccessPermission.isOwner(userPermissionLvl)) {
			// not an owner, check if trying to edit an owner or an editor/reader
			// get the current permission
			if(AccessPermission.OWNER.getId() == existingUserPermission) {
				throw new IllegalArgumentException("The user doesn't have the high enough permissions to modify this users insight permission.");
			}
			
			// also, cannot give some owner permission if i am just an editor
			if(AccessPermission.OWNER.getId() == newPermissionLvl) {
				throw new IllegalArgumentException("Cannot give owner level access to this insight since you are not currently an owner.");
			}
		}
		
		String query = "UPDATE USERINSIGHTPERMISSION SET PERMISSION=" + newPermissionLvl
				+ " WHERE USERID='" + RdbmsQueryBuilder.escapeForSQLStatement(existingUserId) + "' "
				+ "AND ENGINEID='"	+ RdbmsQueryBuilder.escapeForSQLStatement(engineId) + "' "
				+ "AND INSIGHTID='" + RdbmsQueryBuilder.escapeForSQLStatement(insightId) + "';";
		try {
			securityDb.insertData(query);
		} catch (SQLException e) {
			e.printStackTrace();
			throw new IllegalArgumentException("An error occured updating the user permissions for this insight");
		}
	}
	
	/**
	 * 
	 * @param user
	 * @param editedUserId
	 * @param engineId
	 * @param insightId
	 * @return
	 */
	public static void removeInsightUser(User user, String existingUserId, String engineId, String insightId) {
		// make sure user can edit the insight
		int userPermissionLvl = getMaxUserInsightPermission(user, engineId, insightId);
		if(!AccessPermission.isEditor(userPermissionLvl)) {
			throw new IllegalArgumentException("Insufficient privileges to modify this insight's permissions.");
		}
		
		// make sure we are trying to edit a permission that exists
		Integer existingUserPermission = getUserInsightPermission(existingUserId, engineId, insightId);
		if(existingUserPermission == null) {
			throw new IllegalArgumentException("Attempting to modify user permission for a user who does not currently have access to the insight");
		}
		
		// if i am not an owner
		// then i need to check if i can remove this users permission
		if(!AccessPermission.isOwner(userPermissionLvl)) {
			// not an owner, check if trying to edit an owner or an editor/reader
			// get the current permission
			if(AccessPermission.OWNER.getId() == existingUserPermission) {
				throw new IllegalArgumentException("The user doesn't have the high enough permissions to modify this users insight permission.");
			}
		}
		
		String query = "DELETE FROM USERINSIGHTPERMISSION WHERE "
				+ "USERID='" + RdbmsQueryBuilder.escapeForSQLStatement(existingUserId) + "' "
				+ "AND ENGINEID='"	+ RdbmsQueryBuilder.escapeForSQLStatement(engineId) + "' "
				+ "AND INSIGHTID='" + RdbmsQueryBuilder.escapeForSQLStatement(insightId) + "';";
		try {
			securityDb.insertData(query);
		} catch (SQLException e) {
			e.printStackTrace();
			throw new IllegalArgumentException("An error occured removing the user permissions for this insight");
		}
	}
	
	
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	
	/*
	 * Querying for insight lists
	 */
	
	/**
	 * User has access to specific insights within a database
	 * User can access if:
	 * 	1) Is Owner, Editer, or Reader of insight
	 * 	2) Insight is global
	 * 	3) Is Owner of database
	 * 
	 * @param engineId
	 * @param userId
	 * @param searchTerm
	 * @param limit
	 * @param offset
	 * @return
	 */
	public static List<Map<String, Object>> searchUserInsights(User user, List<String> engineFilter, String searchTerm, List<String> tags, String limit, String offset) {
//		String userFilters = getUserFilters(user);
//
//		String query = "SELECT DISTINCT "
//				+ "INSIGHT.ENGINEID AS \"app_id\", "
//				+ "ENGINE.ENGINENAME AS \"app_name\", "
//				+ "INSIGHT.INSIGHTID as \"app_insight_id\", "
//				+ "INSIGHT.INSIGHTNAME as \"name\", "
//				+ "INSIGHT.LAYOUT as \"layout\", "
//				+ "INSIGHT.CREATEDON as \"created_on\", "
//				+ "INSIGHT.LASTMODIFIEDON as \"last_modified_on\", "
//				+ "INSIGHT.CACHEABLE as \"cacheable\", "
//				+ "INSIGHT.GLOBAL as \"insight_global\", "
//				+ "LOWER(INSIGHT.INSIGHTNAME) as \"low_name\" "
//				+ "FROM INSIGHT "
//				+ "INNER JOIN ENGINE ON ENGINE.ENGINEID=INSIGHT.ENGINEID "
//				+ "LEFT JOIN ENGINEPERMISSION ON ENGINE.ENGINEID=ENGINEPERMISSION.ENGINEID "
//				+ "LEFT JOIN USERINSIGHTPERMISSION ON USERINSIGHTPERMISSION.INSIGHTID=INSIGHT.INSIGHTID "
//				+ "WHERE "
//				+ "INSIGHT.ENGINEID " + createFilter(engineFilter)+ " "
//				+ " AND (USERINSIGHTPERMISSION.USERID IN " + userFilters + " OR INSIGHT.GLOBAL=TRUE OR "
//						+ "(ENGINEPERMISSION.PERMISSION=1 AND ENGINEPERMISSION.USERID IN " + userFilters + ") ) "
//				+ ( (searchTerm != null && !searchTerm.trim().isEmpty()) ? "AND REGEXP_LIKE(INSIGHT.INSIGHTNAME, '"+ RdbmsQueryBuilder.escapeForSQLStatement(escapeRegexCharacters(searchTerm)) + "', 'i')" : "")
//				+ "ORDER BY LOWER(INSIGHT.INSIGHTNAME), \"last_modified_on\" DESC "
//				+ ( (limit != null && !limit.trim().isEmpty()) ? "LIMIT " + limit + " " : "")
//				+ ( (offset != null && !offset.trim().isEmpty()) ? "OFFSET " + offset + " ": "")
//				;
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
//		
//		// if no engine filters
//		
//		String userFilters = getUserFilters(user);
//
//		String query = "SELECT DISTINCT "
//				+ "INSIGHT.ENGINEID AS \"app_id\", "
//				+ "ENGINE.ENGINENAME AS \"app_name\", "
//				+ "INSIGHT.INSIGHTID as \"app_insight_id\", "
//				+ "INSIGHT.INSIGHTNAME as \"name\", "
//				+ "INSIGHT.EXECUTIONCOUNT as \"view_count\", "
//				+ "INSIGHT.LAYOUT as \"layout\", "
//				+ "INSIGHT.CREATEDON as \"created_on\", "
//				+ "INSIGHT.LASTMODIFIEDON as \"last_modified_on\", "
//				+ "INSIGHT.CACHEABLE as \"cacheable\", "
//				+ "INSIGHT.GLOBAL as \"insight_global\", "
//				+ "LOWER(INSIGHT.INSIGHTNAME) AS \"low_name\" "
//				+ "FROM INSIGHT "
//				+ "INNER JOIN ENGINE ON ENGINE.ENGINEID=INSIGHT.ENGINEID "
//				+ "LEFT JOIN ENGINEPERMISSION ON ENGINE.ENGINEID=ENGINEPERMISSION.ENGINEID "
//				+ "LEFT JOIN USER ON ENGINEPERMISSION.USERID=USER.ID "
//				+ "LEFT JOIN USERINSIGHTPERMISSION ON USER.ID=USERINSIGHTPERMISSION.USERID "
//				+ "WHERE "
//				// engine is visible to me
//				+ "( ENGINE.GLOBAL=TRUE "
//				+ "OR ENGINEPERMISSION.USERID IN " + userFilters + " ) "
//				+ "AND ENGINE.ENGINEID NOT IN (SELECT ENGINEID FROM ENGINEPERMISSION WHERE VISIBILITY=FALSE AND USERID IN " + userFilters + " "
//				// have access to insight
//				+ "AND (USERINSIGHTPERMISSION.USERID IN " + userFilters + " OR INSIGHT.GLOBAL=TRUE OR "
//						// if i own this, i dont care what permissions you want to give me + i want to see this engine
//						+ "(ENGINEPERMISSION.PERMISSION=1 AND ENGINEPERMISSION.USERID IN " + userFilters + " AND ENGINEPERMISSION.VISIBILITY=TRUE) )) "
//				// and match what i search
//				+ ( (searchTerm != null && !searchTerm.trim().isEmpty()) ? "AND REGEXP_LIKE(INSIGHT.INSIGHTNAME, '"+ RdbmsQueryBuilder.escapeForSQLStatement(RdbmsQueryBuilder.escapeRegexCharacters(searchTerm)) + "', 'i') " : "")
//				+ "ORDER BY LOWER(INSIGHT.INSIGHTNAME), \"last_modified_on\" DESC "
//				+ ( (limit != null && !limit.trim().isEmpty()) ? "LIMIT " + limit + " " : "")
//				+ ( (offset != null && !offset.trim().isEmpty()) ? "OFFSET " + offset + " ": "")
//				;
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		boolean hasEngineFilters = engineFilter != null && !engineFilter.isEmpty();
		
		Collection<String> userIds = getUserFiltersQs(user);
		SelectQueryStruct qs = new SelectQueryStruct();
		// selectors
		qs.addSelector(new QueryColumnSelector("INSIGHT__ENGINEID", "app_id"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINENAME", "app_name"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__INSIGHTID", "app_insight_id"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__INSIGHTNAME", "name"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__EXECUTIONCOUNT", "view_count"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__LAYOUT", "layout"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__CREATEDON", "created_on"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__LASTMODIFIEDON", "last_modified_on"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__CACHEABLE", "cacheable"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__GLOBAL", "insight_global"));
		QueryFunctionSelector fun = new QueryFunctionSelector();
		fun.setFunction(QueryFunctionHelper.LOWER);
		fun.addInnerSelector(new QueryColumnSelector("INSIGHT__INSIGHTNAME"));
		fun.setAlias("low_name");
		qs.addSelector(fun);
		
		// filters
		// if we have an engine filter
		// i'm assuming you want these even if visibility is false
		if(hasEngineFilters) {
			// will filter to the list of engines
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHT__ENGINEID", "==", engineFilter));
			// make sure you have access to each of these insights
			// 1) you have access based on user insight permission table -- or
			// 2) the insight is global -- or 
			// 3) you are the owner of this engine (defined by the embedded and)
			OrQueryFilter orFilter = new OrQueryFilter();
			{
				orFilter.addFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__USERID", "==", userIds));
				orFilter.addFilter(SimpleQueryFilter.makeColToValFilter("INSIGHT__GLOBAL", "==", true, PixelDataType.BOOLEAN));
				AndQueryFilter embedAndFilter = new AndQueryFilter();
				embedAndFilter.addFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__PERMISSION", "==", 1, PixelDataType.CONST_INT));
				embedAndFilter.addFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", userIds));
				orFilter.addFilter(embedAndFilter);
			}
			qs.addExplicitFilter(orFilter);
		} else {
			// search across all engines
			// so guessing you only want those you have visible to you
			// 1) the engine is global -- or
			// 2) you have access to it
			
			OrQueryFilter firstOrFilter = new OrQueryFilter();
			{
				firstOrFilter.addFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__GLOBAL", "==", true, PixelDataType.BOOLEAN));
				firstOrFilter.addFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", userIds));
			}
			qs.addExplicitFilter(firstOrFilter);

			// subquery time
			// remove those engines you have visibility as false
			{
				SelectQueryStruct subQs = new SelectQueryStruct();
				// store first and fill in sub query after
				qs.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery("ENGINE__ENGINEID", "!=", subQs));
				
				// fill in the sub query with the single return + filters
				subQs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__ENGINEID"));
				subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__VISIBILITY", "==", false, PixelDataType.BOOLEAN));
				subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", userIds));
			}
			
			OrQueryFilter secondOrFilter = new OrQueryFilter();
			{
				secondOrFilter.addFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__USERID", "==", userIds));
				secondOrFilter.addFilter(SimpleQueryFilter.makeColToValFilter("INSIGHT__GLOBAL", "==", true, PixelDataType.BOOLEAN));
				AndQueryFilter embedAndFilter = new AndQueryFilter();
				embedAndFilter.addFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__PERMISSION", "==", 1, PixelDataType.CONST_INT));
				embedAndFilter.addFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", userIds));
				embedAndFilter.addFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__VISIBILITY", "==", true, PixelDataType.BOOLEAN));
				secondOrFilter.addFilter(embedAndFilter);
			}
			qs.addExplicitFilter(secondOrFilter);
		}
		// add the search term filter
		if(searchTerm != null && !searchTerm.trim().isEmpty()) {
			FunctionQueryFilter filter = new FunctionQueryFilter();
			QueryFunctionSelector regexFunction = new QueryFunctionSelector();
			regexFunction.setFunction(QueryFunctionHelper.REGEXP_LIKE);
			regexFunction.addInnerSelector(new QueryColumnSelector("INSIGHT__INSIGHTNAME"));
			regexFunction.addInnerSelector(new QueryConstantSelector(RdbmsQueryBuilder.escapeForSQLStatement(RdbmsQueryBuilder.escapeRegexCharacters(searchTerm))));
			regexFunction.addInnerSelector(new QueryConstantSelector("i"));
			filter.setFunctionSelector(regexFunction);
			qs.addExplicitFilter(filter);
		}
		// if we have tag filters
		boolean tagFiltering = tags != null && !tags.isEmpty();
		if(tagFiltering) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHTMETA__METAKEY", "==", "tag"));
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHTMETA__METAVALUE", "==", tags));
		}
		
		// joins
		qs.addRelation("ENGINE", "INSIGHT", "inner.join");
		if(tagFiltering) {
			qs.addRelation("INSIGHT__INSIGHTID", "INSIGHTMETA__INSIGHTID", "inner.join");
			qs.addRelation("INSIGHT__ENGINEID", "INSIGHTMETA__ENGINEID", "inner.join");
		}
		qs.addRelation("ENGINE", "ENGINEPERMISSION", "left.outer.join");
		qs.addRelation("INSIGHT", "USERINSIGHTPERMISSION", "left.outer.join");
		// sort
		qs.addOrderBy(new QueryColumnOrderBySelector("low_name"));
		// limit 
		if(limit != null && !limit.trim().isEmpty()) {
			qs.setLimit(Long.parseLong(limit));
		}
		// offset
		if(offset != null && !offset.trim().isEmpty()) {
			qs.setOffSet(Long.parseLong(offset));
		}
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
		return flushRsToMap(wrapper);
	}
	
	/**
	 * Search through all insights with an optional filter on engines and an optional search term
	 * @param engineFilter
	 * @param searchTerm
	 * @param limit
	 * @param offset
	 * @return
	 */
	public static List<Map<String, Object>> searchInsights(List<String> engineFilter, String searchTerm, List<String> tags, String limit, String offset) {
//		String query = "SELECT DISTINCT "
//				+ "INSIGHT.ENGINEID AS \"app_id\", "
//				+ "ENGINE.ENGINENAME AS \"app_name\", "
//				+ "INSIGHT.INSIGHTID as \"app_insight_id\", "
//				+ "INSIGHT.INSIGHTNAME as \"name\", "
//				+ "INSIGHT.LAYOUT as \"layout\", "
//				+ "INSIGHT.CREATEDON as \"created_on\", "
//				+ "INSIGHT.LASTMODIFIEDON as \"last_modified_on\", "
//				+ "INSIGHT.CACHEABLE as \"cacheable\", "
//				+ "INSIGHT.GLOBAL as \"insight_global\", "
//				+ "LOWER(INSIGHT.INSIGHTNAME) as \"low_name\" "
//				+ "FROM INSIGHT "
//				+ "INNER JOIN ENGINE ON ENGINE.ENGINEID=INSIGHT.ENGINEID "
//				+ "WHERE "
//				+ "INSIGHT.ENGINEID " + createFilter(engineFilter) + " "
//				+ ( (searchTerm != null && !searchTerm.trim().isEmpty()) ? "AND REGEXP_LIKE(INSIGHT.INSIGHTNAME, '"+ RdbmsQueryBuilder.escapeForSQLStatement(RdbmsQueryBuilder.escapeRegexCharacters(searchTerm)) + "', 'i')" : "")
//				+ "ORDER BY LOWER(INSIGHT.INSIGHTNAME), \"last_modified_on\" DESC "
//				+ ( (limit != null && !limit.trim().isEmpty()) ? "LIMIT " + limit + " " : "")
//				+ ( (offset != null && !offset.trim().isEmpty()) ? "OFFSET " + offset + " ": "")
//				;
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		// selectors
		qs.addSelector(new QueryColumnSelector("INSIGHT__ENGINEID", "app_id"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINENAME", "app_name"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__INSIGHTID", "app_insight_id"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__INSIGHTNAME", "name"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__EXECUTIONCOUNT", "view_count"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__LAYOUT", "layout"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__CREATEDON", "created_on"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__LASTMODIFIEDON", "last_modified_on"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__CACHEABLE", "cacheable"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__GLOBAL", "insight_global"));
		QueryFunctionSelector fun = new QueryFunctionSelector();
		fun.setFunction(QueryFunctionHelper.LOWER);
		fun.addInnerSelector(new QueryColumnSelector("INSIGHT__INSIGHTNAME"));
		fun.setAlias("low_name");
		qs.addSelector(fun);
		// filters
		if(engineFilter != null && !engineFilter.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHT__ENGINEID", "==", engineFilter));
		}
		if(searchTerm != null && !searchTerm.trim().isEmpty()) {
			FunctionQueryFilter filter = new FunctionQueryFilter();
			QueryFunctionSelector regexFunction = new QueryFunctionSelector();
			regexFunction.setFunction(QueryFunctionHelper.REGEXP_LIKE);
			regexFunction.addInnerSelector(new QueryColumnSelector("INSIGHT__INSIGHTNAME"));
			regexFunction.addInnerSelector(new QueryConstantSelector(RdbmsQueryBuilder.escapeForSQLStatement(RdbmsQueryBuilder.escapeRegexCharacters(searchTerm))));
			regexFunction.addInnerSelector(new QueryConstantSelector("i"));
			filter.setFunctionSelector(regexFunction);
			qs.addExplicitFilter(filter);
		}
		// if we have tag filters
		boolean tagFiltering = tags != null && !tags.isEmpty();
		if(tagFiltering) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHTMETA__METAKEY", "==", "tag"));
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHTMETA__METAVALUE", "==", tags));
		}
		// joins
		qs.addRelation("ENGINE", "INSIGHT", "inner.join");
		if(tagFiltering) {
			qs.addRelation("INSIGHT__INSIGHTID", "INSIGHTMETA__INSIGHTID", "inner.join");
			qs.addRelation("INSIGHT__ENGINEID", "INSIGHTMETA__ENGINEID", "inner.join");
		}
		// sort
		qs.addOrderBy(new QueryColumnOrderBySelector("low_app_name"));
		// limit 
		if(limit != null && !limit.trim().isEmpty()) {
			qs.setLimit(Long.parseLong(limit));
		}
		// offset
		if(offset != null && !offset.trim().isEmpty()) {
			qs.setOffSet(Long.parseLong(offset));
		}
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
		return flushRsToMap(wrapper);
	}
	
	/**
	 * Get the wrapper for additional insight metadata
	 * @param engineId
	 * @param insightIds
	 * @param metaKeys
	 * @return
	 */
	public static IRawSelectWrapper getInsightMetadataWrapper(String engineId, Collection<String> insightIds, List<String> metaKeys) {
		SelectQueryStruct qs = new SelectQueryStruct();
		// selectors
		qs.addSelector(new QueryColumnSelector("INSIGHTMETA__ENGINEID"));
		qs.addSelector(new QueryColumnSelector("INSIGHTMETA__INSIGHTID"));
		qs.addSelector(new QueryColumnSelector("INSIGHTMETA__METAKEY"));
		qs.addSelector(new QueryColumnSelector("INSIGHTMETA__METAVALUE"));
		qs.addSelector(new QueryColumnSelector("INSIGHTMETA__METAORDER"));
		// filters
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHTMETA__ENGINEID", "==", engineId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHTMETA__INSIGHTID", "==", insightIds));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHTMETA__METAKEY", "==", metaKeys));
		// order
		qs.addSelector(new QueryColumnSelector("INSIGHTMETA__METAORDER"));
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
		return wrapper;
	}
	
	/**
	 * Get the insight metadata for a specific insight
	 * @param engineId
	 * @param insightId
	 * @param metaKeys
	 * @return
	 */
	public static Map<String, Object> getSpecificInsightMetadata(String engineId, String insightId, List<String> metaKeys) {
		SelectQueryStruct qs = new SelectQueryStruct();
		// selectors
		qs.addSelector(new QueryColumnSelector("INSIGHTMETA__ENGINEID"));
		qs.addSelector(new QueryColumnSelector("INSIGHTMETA__INSIGHTID"));
		qs.addSelector(new QueryColumnSelector("INSIGHTMETA__METAKEY"));
		qs.addSelector(new QueryColumnSelector("INSIGHTMETA__METAVALUE"));
		qs.addSelector(new QueryColumnSelector("INSIGHTMETA__METAORDER"));
		// filters
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHTMETA__ENGINEID", "==", engineId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHTMETA__INSIGHTID", "==", insightId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHTMETA__METAKEY", "==", metaKeys));
		// order
		qs.addSelector(new QueryColumnSelector("INSIGHTMETA__METAORDER"));
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);

		Map<String, Object> retMap = new HashMap<String, Object>();
		while(wrapper.hasNext()) {
			Object[] data = wrapper.next().getValues();
			String metaKey = data[2].toString();
			String metaValue = AbstractSqlQueryUtil.flushClobToString((java.sql.Clob) data[3]);

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

		return retMap;
	}

	/**
	 * Get all the available tags and their count
	 * @param engineFilters
	 * @return
	 */
	public static List<Map<String, Object>> getAvailableInsightTagsAndCounts(List<String> engineFilters) {
		SelectQueryStruct qs = new SelectQueryStruct();
		// selectors
		qs.addSelector(new QueryColumnSelector("INSIGHTMETA__METAVALUE", "tag"));
		QueryFunctionSelector fSelector = new QueryFunctionSelector();
		fSelector.setAlias("count");
		fSelector.setFunction(QueryFunctionHelper.COUNT);
		fSelector.addInnerSelector(new QueryColumnSelector("INSIGHTMETA__METAVALUE"));
		qs.addSelector(fSelector);
		// filters
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHTMETA__METAKEY", "==", "tag"));
		if(engineFilters != null && !engineFilters.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHTMETA__ENGINEID", "==", engineFilters));
		}
		// group
		qs.addGroupBy(new QueryColumnSelector("INSIGHTMETA__METAVALUE", "tag"));
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
		return flushRsToMap(wrapper);
	}
	
	//////////////////////////////////////////////////////////////////
	
	/*
	 * For autocompletion of user searching
	 */
	
	/**
	 * User will see specific insight predictions for their searches
	 * User can see records if:
	 * 	1) Is Owner, Editer, or Reader of insight
	 * 	2) Insight is global
	 * 	3) Is Owner of database
	 * 
	 * @param userId
	 * @param searchTerm
	 * @param limit
	 * @param offset
	 * @return
	 */
	public static List<String> predictUserInsightSearch(User user, String searchTerm, String limit, String offset) {
//		String userFilters = getUserFilters(user);
//
//		String query = "SELECT DISTINCT "
//				+ "INSIGHT.INSIGHTNAME as \"name\", "
//				+ "LOWER(INSIGHT.INSIGHTNAME) as \"low_name\" "
//				+ "FROM INSIGHT "
//				+ "LEFT JOIN ENGINEPERMISSION ON INSIGHT.ENGINEID=ENGINEPERMISSION.ENGINEID "
//				+ "LEFT JOIN USERINSIGHTPERMISSION ON USERINSIGHTPERMISSION.ENGINEID=INSIGHT.ENGINEID "
//				+ "WHERE "
//				+ "(USERINSIGHTPERMISSION.USERID IN " + userFilters + " OR INSIGHT.GLOBAL=TRUE OR "
//						+ "(ENGINEPERMISSION.PERMISSION=1 AND ENGINEPERMISSION.USERID IN " + userFilters + ") ) "
//				+ ( (searchTerm != null && !searchTerm.trim().isEmpty()) ? "AND REGEXP_LIKE(INSIGHT.INSIGHTNAME, '"+ RdbmsQueryBuilder.escapeForSQLStatement(RdbmsQueryBuilder.escapeRegexCharacters(searchTerm)) + "', 'i')" : "")
//				+ "ORDER BY LOWER(INSIGHT.INSIGHTNAME) "
//				+ ( (limit != null && !limit.trim().isEmpty()) ? "LIMIT " + limit + " " : "")
//				+ ( (offset != null && !offset.trim().isEmpty()) ? "OFFSET " + offset + " ": "")
//				;
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		Collection<String> userIds = getUserFiltersQs(user);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("INSIGHT__INSIGHTNAME", "name"));
		QueryFunctionSelector fun = new QueryFunctionSelector();
		fun.setFunction(QueryFunctionHelper.LOWER);
		fun.addInnerSelector(new QueryColumnSelector("INSIGHT__INSIGHTNAME"));
		fun.setAlias("low_name");
		qs.addSelector(fun);
		OrQueryFilter orFilters = new OrQueryFilter();
		{
			// i have access to the insight
			orFilters.addFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__USERID", "==", userIds));
			// or, the insight is global
			orFilters.addFilter(SimpleQueryFilter.makeColToValFilter("INSIGHT__GLOBAL", "==", true, PixelDataType.BOOLEAN));
			// or, i'm the app owner ( you can't hide your stuff from me O_O )
			AndQueryFilter andFilter = new AndQueryFilter();
			{
				andFilter.addFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__PERMISSION", "==", 1, PixelDataType.CONST_INT));
				andFilter.addFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", userIds));
			}
			orFilters.addFilter(andFilter);
		}
		qs.addExplicitFilter(orFilters);
		if(searchTerm != null && !searchTerm.trim().isEmpty()) {
			FunctionQueryFilter filter = new FunctionQueryFilter();
			QueryFunctionSelector regexFunction = new QueryFunctionSelector();
			regexFunction.setFunction(QueryFunctionHelper.REGEXP_LIKE);
			regexFunction.addInnerSelector(new QueryColumnSelector("INSIGHT__INSIGHTNAME"));
			regexFunction.addInnerSelector(new QueryConstantSelector(RdbmsQueryBuilder.escapeForSQLStatement(RdbmsQueryBuilder.escapeRegexCharacters(searchTerm))));
			regexFunction.addInnerSelector(new QueryConstantSelector("i"));
			filter.setFunctionSelector(regexFunction);
			qs.addExplicitFilter(filter);
		}
		// sort
		qs.addOrderBy(new QueryColumnOrderBySelector("low_name"));
		// limit 
		if(limit != null && !limit.trim().isEmpty()) {
			qs.setLimit(Long.parseLong(limit));
		}
		// offset
		if(offset != null && !offset.trim().isEmpty()) {
			qs.setOffSet(Long.parseLong(offset));
		}
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
		return flushToListString(wrapper);
	}
	
	public static List<String> predictInsightSearch(String searchTerm, String limit, String offset) {
//		String query = "SELECT DISTINCT "
//				+ "INSIGHT.INSIGHTNAME as \"name\", "
//				+ "LOWER(INSIGHT.INSIGHTNAME) as \"low_name\" "
//				+ "FROM INSIGHT "
//				+ ( (searchTerm != null && !searchTerm.trim().isEmpty()) ? "WHERE REGEXP_LIKE(INSIGHT.INSIGHTNAME, '"+ RdbmsQueryBuilder.escapeForSQLStatement(RdbmsQueryBuilder.escapeRegexCharacters(searchTerm)) + "', 'i')" : "")
//				+ "ORDER BY LOWER(INSIGHT.INSIGHTNAME) "
//				+ ( (limit != null && !limit.trim().isEmpty()) ? "LIMIT " + limit + " " : "")
//				+ ( (offset != null && !offset.trim().isEmpty()) ? "OFFSET " + offset + " ": "")
//				;
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("INSIGHT__INSIGHTNAME", "name"));
		QueryFunctionSelector fun = new QueryFunctionSelector();
		fun.setFunction(QueryFunctionHelper.LOWER);
		fun.addInnerSelector(new QueryColumnSelector("INSIGHT__INSIGHTNAME"));
		fun.setAlias("low_name");
		qs.addSelector(fun);
		if(searchTerm != null && !searchTerm.trim().isEmpty()) {
			FunctionQueryFilter filter = new FunctionQueryFilter();
			QueryFunctionSelector regexFunction = new QueryFunctionSelector();
			regexFunction.setFunction(QueryFunctionHelper.REGEXP_LIKE);
			regexFunction.addInnerSelector(new QueryColumnSelector("INSIGHT__INSIGHTNAME"));
			regexFunction.addInnerSelector(new QueryConstantSelector(RdbmsQueryBuilder.escapeForSQLStatement(RdbmsQueryBuilder.escapeRegexCharacters(searchTerm))));
			regexFunction.addInnerSelector(new QueryConstantSelector("i"));
			filter.setFunctionSelector(regexFunction);
			qs.addExplicitFilter(filter);
		}
		// sort
		qs.addOrderBy(new QueryColumnOrderBySelector("low_name"));
		// limit 
		if(limit != null && !limit.trim().isEmpty()) {
			qs.setLimit(Long.parseLong(limit));
		}
		// offset
		if(offset != null && !offset.trim().isEmpty()) {
			qs.setOffSet(Long.parseLong(offset));
		}
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
		return flushToListString(wrapper);
	}
	
}
