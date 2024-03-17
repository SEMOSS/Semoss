package prerna.auth.utils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;

import com.google.gson.Gson;

import prerna.auth.AccessPermissionEnum;
import prerna.auth.User;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.PixelDataType;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.QueryExecutionUtility;
import prerna.util.Utility;

public class AdminSecurityGroupUtils extends AbstractSecurityUtils {

	private static AdminSecurityGroupUtils instance = new AdminSecurityGroupUtils();

	private static final Logger classLogger = LogManager.getLogger(AdminSecurityGroupUtils.class);

	private AdminSecurityGroupUtils() {

	}

	public static AdminSecurityGroupUtils getInstance(User user) {
		if (user == null) {
			return null;
		}
		if (SecurityAdminUtils.userIsAdmin(user)) {
			return instance;
		}
		return null;
	}

	/**
	 * Filter a collection of typed groups to those that are in the SMSS_GROUP table
	 * 
	 * @param groupIds
	 * @param groupType
	 * @return
	 * @throws Exception
	 */
	public static Set<String> getMatchingGroupsByType(Collection<String> groupIds, String groupType) throws Exception {
		Set<String> results = new HashSet<>();

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_GROUP__ID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_GROUP__TYPE", "==", groupType));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_GROUP__ID", "==", groupIds));

		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			while (wrapper.hasNext()) {
				Object val = wrapper.next().getValues()[0];
				if (val != null) {
					results.add(val.toString());
				}
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
		} finally {
			if (wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}

		return results;
	}

	/**
	 * Add a group with description
	 * 
	 * @param groupId
	 * @param groupType
	 * @param description
	 * @throws Exception
	 */
	public void addGroup(User user, String groupId, String groupType, String description, boolean isCustomGroup)
			throws Exception {
		Connection conn = null;
		try {
			conn = securityDb.makeConnection();

			// need to ensure that the group is unique...
			if (groupExists(groupId, groupType)) {
				throw new IllegalArgumentException("Group already exists");
			}

			Pair<String, String> userDetails = User.getPrimaryUserIdAndTypePair(user);

			Gson gson = new Gson();
			String query = "INSERT INTO SMSS_GROUP (ID, TYPE, DESCRIPTION, IS_CUSTOM_GROUP, DATEADDED, USERID, USERIDTYPE) "
					+ "VALUES (?,?,?,?,?,?,?)";
			try (PreparedStatement ps = conn.prepareStatement(query)) {
				int parameterIndex = 1;
				ps.setString(parameterIndex++, groupId);
				// handle null type for custom groups
				if(groupType == null || groupType.isEmpty()) {
					ps.setNull(parameterIndex++, java.sql.Types.VARCHAR);
				} else {
					ps.setString(parameterIndex++, groupType);
				}
				securityDb.getQueryUtil().handleInsertionOfClob(conn, ps, description, parameterIndex++, gson);
				ps.setBoolean(parameterIndex++, isCustomGroup);
				ps.setTimestamp(parameterIndex++, Utility.getCurrentSqlTimestampUTC());
				ps.setString(parameterIndex++, userDetails.getValue0());
				ps.setString(parameterIndex++, userDetails.getValue1());
				ps.execute();
				if (!conn.getAutoCommit()) {
					conn.commit();
				}
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
		} finally {
			if (securityDb.isConnectionPooling() && conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}

	/**
	 * Delete a new group and its references across the tables
	 * 
	 * @param groupId
	 * @param groupType
	 * @throws Exception
	 */
	public void deleteGroupAndPropagate(String groupId, String groupType) throws Exception {
		if (!groupExists(groupId, groupType)) {
			throw new IllegalArgumentException("Group " + groupId + " does not exist");
		}

		String[] queries = null;
		if(groupType == null || groupType.isEmpty()) {
			queries = new String[] { 
					"DELETE FROM GROUPENGINEPERMISSION WHERE ID=? AND TYPE IS NULL",
					"DELETE FROM GROUPPROJECTPERMISSION WHERE ID=? AND TYPE IS NULL",
					"DELETE FROM GROUPINSIGHTPERMISSION WHERE ID=? AND TYPE IS NULL",
					"DELETE FROM SMSS_GROUP WHERE ID=? AND TYPE IS NULL",
					"DELETE FROM CUSTOMGROUPASSIGNMENT WHERE GROUPID=?"
				};
		} else {
			queries = new String[] { 
					"DELETE FROM GROUPENGINEPERMISSION WHERE ID=? AND TYPE=?",
					"DELETE FROM GROUPPROJECTPERMISSION WHERE ID=? AND TYPE=?",
					"DELETE FROM GROUPINSIGHTPERMISSION WHERE ID=? AND TYPE=?",
					"DELETE FROM SMSS_GROUP WHERE ID=? AND TYPE=?"
				};
		}

		Connection conn = null;
		try {
			conn = securityDb.makeConnection();
			
			try {
				for (String query : queries) {
					try (PreparedStatement ps = conn.prepareStatement(query)) {
						int parameterIndex = 1;
						ps.setString(parameterIndex++, groupId);
						// if group type is not null
						if(groupType != null) {
							ps.setString(parameterIndex++, groupType);
						}
						ps.execute();
					}
				}
				
				// commit
				if (!conn.getAutoCommit()) {
					conn.commit();
				}
			} catch (SQLException e) {
				if (!conn.getAutoCommit()) {
					conn.rollback();
				}
				throw e;
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
		} finally {
			if (securityDb.isConnectionPooling() && conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}

	/**
	 * Edit an existing group across all the tables
	 * 
	 * @param curGroupId
	 * @param curGroupType
	 * @param newGroupId
	 * @param newGroupType
	 * @param newDescription
	 * @param newIsCustomGroup
	 * @throws Exception
	 */
	public void editGroupAndPropagate(User user, String curGroupId, String curGroupType, String newGroupId,
			String newGroupType, String newDescription, boolean newIsCustomGroup) throws Exception {
		if (!groupExists(curGroupId, curGroupType)) {
			throw new IllegalArgumentException("Group " + curGroupId + " does not exist");
		}
		String groupQuery = null;
		String[] propagateQueries = null;
		
		if(curGroupType == null ) {
			groupQuery = "UPDATE SMSS_GROUP SET ID=?, TYPE=?, DESCRIPTION=?, IS_CUSTOM_GROUP=?, DATEADDED=?, USERID=?, USERIDTYPE=? WHERE ID=? AND TYPE IS NULL";
			propagateQueries = new String[] {
					"UPDATE GROUPENGINEPERMISSION SET ID=?, TYPE=? WHERE ID=? AND TYPE IS NULL",
					"UPDATE GROUPPROJECTPERMISSION SET ID=?, TYPE=? WHERE ID=? AND TYPE IS NULL",
					"UPDATE GROUPINSIGHTPERMISSION SET ID=?, TYPE=? WHERE ID=? AND TYPE IS NULL", 
				};	
		} else {
			groupQuery = "UPDATE SMSS_GROUP SET ID=?, TYPE=?, DESCRIPTION=?, IS_CUSTOM_GROUP=?, DATEADDED=?, USERID=?, USERIDTYPE=? WHERE ID=? AND TYPE=?";
			propagateQueries = new String[] {
					"UPDATE GROUPENGINEPERMISSION SET ID=?, TYPE=? WHERE ID=? AND TYPE=?",
					"UPDATE GROUPPROJECTPERMISSION SET ID=?, TYPE=? WHERE ID=? AND TYPE=?",
					"UPDATE GROUPINSIGHTPERMISSION SET ID=?, TYPE=? WHERE ID=? AND TYPE=?", 
				};	
		}
		
		Connection conn = null;
		try {
			conn = securityDb.makeConnection();

			Pair<String, String> userDetails = User.getPrimaryUserIdAndTypePair(user);

			Gson gson = new Gson();
			try {
				// group edit
				try (PreparedStatement ps = conn.prepareStatement(groupQuery)) {
					int parameterIndex = 1;
					ps.setString(parameterIndex++, newGroupId);
					// handle null type for custom groups
					if(newGroupType == null || newGroupType.isEmpty()) {
						ps.setNull(parameterIndex++, java.sql.Types.VARCHAR);
					} else {
						ps.setString(parameterIndex++, newGroupType);
					}
					securityDb.getQueryUtil().handleInsertionOfClob(conn, ps, newDescription, parameterIndex++, gson);
					ps.setBoolean(parameterIndex++, newIsCustomGroup);
					ps.setTimestamp(parameterIndex++, Utility.getCurrentSqlTimestampUTC());
					ps.setString(parameterIndex++, userDetails.getValue0());
					ps.setString(parameterIndex++, userDetails.getValue1());
					// where
					ps.setString(parameterIndex++, curGroupId);
					// do we have a current group type that is not null
					if(curGroupType != null) {
						ps.setString(parameterIndex++, curGroupType);
					}
					ps.execute();
				}

				// propagation
				for (String query : propagateQueries) {
					try (PreparedStatement ps = conn.prepareStatement(query)) {
						int parameterIndex = 1;
						ps.setString(parameterIndex++, newGroupId);
						// handle null type for custom groups
						if(newGroupType == null || newGroupType.isEmpty()) {
							ps.setNull(parameterIndex++, java.sql.Types.VARCHAR);
						} else {
							ps.setString(parameterIndex++, newGroupType);
						}
						ps.setString(parameterIndex++, curGroupId);
						// do we have a current group type that is not null
						if(curGroupType != null) {
							ps.setString(parameterIndex++, curGroupType);
						}
						ps.execute();
					}
				}
				if (!conn.getAutoCommit()) {
					conn.commit();
				}
			} catch (SQLException e) {
				if (!conn.getAutoCommit()) {
					conn.rollback();
				}
				throw e;
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
		} finally {
			if (securityDb.isConnectionPooling() && conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}

	/**
	 * 
	 * @param groupId
	 * @param userId
	 * @param userType
	 * @throws Exception 
	 */
	public void addUserToGroup(User user, String groupId, String userId, String userType, String endDate) throws Exception {
		if (!groupExists(groupId, null)) {
			throw new IllegalArgumentException("Group " + groupId + " does not exist");
		}

		if (!isCustomGroup(groupId)) {
			throw new IllegalArgumentException("Can only add/remove users for custom groups");
		}

		if (userInCustomGroup(groupId, userId, userType)) {
			throw new IllegalArgumentException("User " + userId + " already has access to group " + groupId);
		}
		
		if(!userExists(userId, userType)) {
			throw new IllegalArgumentException("User " + userId + " doesn't exist");
		}

		Pair<String, String> userDetails = User.getPrimaryUserIdAndTypePair(user);

		Timestamp verifiedEndDate = null;
		if (endDate != null) {
			verifiedEndDate = AbstractSecurityUtils.calculateEndDate(endDate);
		}

		Connection conn = null;
		try {
			conn = securityDb.makeConnection();
			String query = "INSERT INTO CUSTOMGROUPASSIGNMENT (GROUPID, USERID, TYPE, "
					+ "DATEADDED, ENDDATE, PERMISSIONGRANTEDBY, PERMISSIONGRANTEDBYTYPE) " + "VALUES (?,?,?,?,?,?,?)";
			try (PreparedStatement ps = conn.prepareStatement(query)) {
				int parameterIndex = 1;
				ps.setString(parameterIndex++, groupId);
				ps.setString(parameterIndex++, userId);
				ps.setString(parameterIndex++, userType);
				ps.setTimestamp(parameterIndex++, Utility.getCurrentSqlTimestampUTC());
				if (verifiedEndDate == null) {
					ps.setNull(parameterIndex++, java.sql.Types.TIMESTAMP);
				} else {
					ps.setTimestamp(parameterIndex++, verifiedEndDate);
				}
				ps.setString(parameterIndex++, userDetails.getValue0());
				ps.setString(parameterIndex++, userDetails.getValue1());
				ps.execute();
				if (!conn.getAutoCommit()) {
					conn.commit();
				}
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
		} finally {
			if (securityDb.isConnectionPooling() && conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}

	/**
	 * 
	 * @param groupId
	 * @param userId
	 * @param userType
	 * @throws Exception 
	 */
	public void removeUserFromGroup(String groupId, String userId, String userType) throws Exception {
		if (!groupExists(groupId, null)) {
			throw new IllegalArgumentException("Group " + groupId + " does not exist");
		}

		if (!isCustomGroup(groupId)) {
			throw new IllegalArgumentException("Can only add/remove users for custom groups");
		}

		if (!userInCustomGroup(groupId, userId, userType)) {
			throw new IllegalArgumentException("User " + userId + " does not have access to group " + groupId);
		}

		Connection conn = null;
		try {
			conn = securityDb.makeConnection();
			String query = "DELETE FROM CUSTOMGROUPASSIGNMENT WHERE GROUPID=? AND USERID=? AND TYPE=?";
			try (PreparedStatement ps = conn.prepareStatement(query)) {
				int parameterIndex = 1;
				ps.setString(parameterIndex++, groupId);
				ps.setString(parameterIndex++, userId);
				ps.setString(parameterIndex++, userType);
				ps.execute();
				if (!conn.getAutoCommit()) {
					conn.commit();
				}
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
		} finally {
			if (securityDb.isConnectionPooling() && conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}

	/**
	 * Get all groups
	 * 
	 * @return
	 */
	public List<Map<String, Object>> getGroups(String searchTerm, long limit, long offset) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_GROUP__ID"));
		qs.addSelector(new QueryColumnSelector("SMSS_GROUP__TYPE"));
		qs.addSelector(new QueryColumnSelector("SMSS_GROUP__DESCRIPTION"));
		qs.addSelector(new QueryColumnSelector("SMSS_GROUP__IS_CUSTOM_GROUP"));
		qs.addSelector(new QueryColumnSelector("SMSS_GROUP__USERID"));
		qs.addSelector(new QueryColumnSelector("SMSS_GROUP__USERIDTYPE"));
		qs.addSelector(new QueryColumnSelector("SMSS_GROUP__DATEADDED"));
		qs.addOrderBy(new QueryColumnOrderBySelector("SMSS_GROUP__TYPE"));
		qs.addOrderBy(new QueryColumnOrderBySelector("SMSS_GROUP__ID"));
		if (searchTerm != null && !(searchTerm = searchTerm.trim()).isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_GROUP__ID", "?like", searchTerm));
		}
		if (limit > 0) {
			qs.setLimit(limit);
		}
		if (offset > 0) {
			qs.setOffSet(offset);
		}
		return getSimpleQuery(qs);
	}

	/**
	 * This is only valid for members assigned to custom group assignments
	 * 
	 * @return
	 */
	public List<Map<String, Object>> getGroupMembers(String groupId, String searchTerm, long limit, long offset) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("CUSTOMGROUPASSIGNMENT__GROUPID"));
		qs.addSelector(new QueryColumnSelector("CUSTOMGROUPASSIGNMENT__USERID"));
		qs.addSelector(new QueryColumnSelector("CUSTOMGROUPASSIGNMENT__TYPE"));
		qs.addSelector(new QueryColumnSelector("CUSTOMGROUPASSIGNMENT__DATEADDED"));
		qs.addSelector(new QueryColumnSelector("CUSTOMGROUPASSIGNMENT__ENDDATE"));
		qs.addSelector(new QueryColumnSelector("CUSTOMGROUPASSIGNMENT__PERMISSIONGRANTEDBY"));
		qs.addSelector(new QueryColumnSelector("CUSTOMGROUPASSIGNMENT__PERMISSIONGRANTEDBYTYPE"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__NAME"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__USERNAME"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__EMAIL"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__ADMIN"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__PUBLISHER"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__EXPORTER"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__PHONE"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__PHONEEXTENSION"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__COUNTRYCODE"));
		qs.addOrderBy(new QueryColumnOrderBySelector("SMSS_USER__NAME"));
		qs.addOrderBy(new QueryColumnOrderBySelector("SMSS_USER__TYPE"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("CUSTOMGROUPASSIGNMENT__GROUPID", "==", groupId));
		qs.addRelation("CUSTOMGROUPASSIGNMENT__USERID", "SMSS_USER__ID", "inner.join");
		qs.addRelation("CUSTOMGROUPASSIGNMENT__TYPE", "SMSS_USER__TYPE", "inner.join");
		if (searchTerm != null && !(searchTerm = searchTerm.trim()).isEmpty()) {
			OrQueryFilter or = new OrQueryFilter();
			or.addFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__ID", "?like", searchTerm));
			or.addFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__NAME", "?like", searchTerm));
			or.addFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__USERNAME", "?like", searchTerm));
			or.addFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__EMAIL", "?like", searchTerm));
			qs.addExplicitFilter(or);
		}
		if (limit > 0) {
			qs.setLimit(limit);
		}
		if (offset > 0) {
			qs.setOffSet(offset);
		}
		return getSimpleQuery(qs);
	}
	
	/**
	 * This is only valid for members assigned to custom group assignments
	 * 
	 * @return
	 */
	public Long getNumMembersInGroup(String groupId, String searchTerm) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.COUNT, "CUSTOMGROUPASSIGNMENT__USERID", "numUsers"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("CUSTOMGROUPASSIGNMENT__GROUPID", "==", groupId));
		if (searchTerm != null && !(searchTerm = searchTerm.trim()).isEmpty()) {
			qs.addRelation("CUSTOMGROUPASSIGNMENT__USERID", "SMSS_USER__ID", "inner.join");
			qs.addRelation("CUSTOMGROUPASSIGNMENT__TYPE", "SMSS_USER__TYPE", "inner.join");
			OrQueryFilter or = new OrQueryFilter();
			or.addFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__ID", "?like", searchTerm));
			or.addFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__NAME", "?like", searchTerm));
			or.addFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__USERNAME", "?like", searchTerm));
			or.addFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__EMAIL", "?like", searchTerm));
			qs.addExplicitFilter(or);
		}
		return QueryExecutionUtility.flushToLong(securityDb, qs);
	}

	/**
	 * This is only valid for members assigned to custom group assignments
	 * 
	 * @return
	 */
	public List<Map<String, Object>> getNonGroupMembers(String groupId, String searchTerm, long limit, long offset) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__ID"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__TYPE"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__NAME"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__USERNAME"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__EMAIL"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__ADMIN"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__PUBLISHER"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__EXPORTER"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__PHONE"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__PHONEEXTENSION"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__COUNTRYCODE"));
		{
			SelectQueryStruct exisitngMembersQs = new SelectQueryStruct();
			exisitngMembersQs.addSelector(QueryFunctionSelector.makeConcat2ColumnsFunction(
					"CUSTOMGROUPASSIGNMENT__USERID", "CUSTOMGROUPASSIGNMENT__TYPE", "UUID"));
			qs.addExplicitFilter(SimpleQueryFilter.makeQuerySelectorToSubQuery(
					QueryFunctionSelector.makeConcat2ColumnsFunction("SMSS_USER__ID", "SMSS_USER__TYPE", "UUID"), "!=",
					exisitngMembersQs));
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("CUSTOMGROUPASSIGNMENT__GROUPID", "==", groupId));
		}
		if (searchTerm != null && !(searchTerm = searchTerm.trim()).isEmpty()) {
			OrQueryFilter or = new OrQueryFilter();
			or.addFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__ID", "?like", searchTerm));
			or.addFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__NAME", "?like", searchTerm));
			or.addFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__USERNAME", "?like", searchTerm));
			or.addFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__EMAIL", "?like", searchTerm));
			qs.addExplicitFilter(or);
		}
		if (limit > 0) {
			qs.setLimit(limit);
		}
		if (offset > 0) {
			qs.setOffSet(offset);
		}
		return getSimpleQuery(qs);
	}
	
	/**
	 * This is only valid for members assigned to custom group assignments
	 * 
	 * @return
	 */
	public Long getNumNonMembersInGroup(String groupId, String searchTerm) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.COUNT, "SMSS_USER__ID", "numUsers"));
		{
			SelectQueryStruct exisitngMembersQs = new SelectQueryStruct();
			exisitngMembersQs.addSelector(QueryFunctionSelector.makeConcat2ColumnsFunction(
					"CUSTOMGROUPASSIGNMENT__USERID", "CUSTOMGROUPASSIGNMENT__TYPE", "UUID"));
			qs.addExplicitFilter(SimpleQueryFilter.makeQuerySelectorToSubQuery(
					QueryFunctionSelector.makeConcat2ColumnsFunction("SMSS_USER__ID", "SMSS_USER__TYPE", "UUID"), "!=",
					exisitngMembersQs));
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("CUSTOMGROUPASSIGNMENT__GROUPID", "==", groupId));
		}
		if (searchTerm != null && !(searchTerm = searchTerm.trim()).isEmpty()) {
			OrQueryFilter or = new OrQueryFilter();
			or.addFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__ID", "?like", searchTerm));
			or.addFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__NAME", "?like", searchTerm));
			or.addFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__USERNAME", "?like", searchTerm));
			or.addFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__EMAIL", "?like", searchTerm));
			qs.addExplicitFilter(or);
		}
		return QueryExecutionUtility.flushToLong(securityDb, qs);
	}

	/**
	 * 
	 * @param user
	 * @param groupId
	 * @param groupType
	 * @param projectId
	 * @param permission
	 * @param endDate
	 */
	public void addGroupProjectPermission(User user, String groupId, String groupType, String projectId, int permission, String endDate) {
		int curPermission = groupProjectPermission(groupId, groupType, projectId);
		if(curPermission!= -1) {
			throw new IllegalArgumentException("Group " + groupId + " already has access to project " + projectId + " with permission = " + AccessPermissionEnum.getPermissionValueById(curPermission));
		}
		Pair<String, String> userDetails = User.getPrimaryUserIdAndTypePair(user);
		
		Timestamp startDate = Utility.getCurrentSqlTimestampUTC();
		Timestamp verifiedEndDate = null;
		if (endDate != null) {
			verifiedEndDate = AbstractSecurityUtils.calculateEndDate(endDate);
		}
		
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement("INSERT INTO GROUPPROJECTPERMISSION (ID, TYPE, PROJECTID, PERMISSION, DATEADDED, ENDDATE, PERMISSIONGRANTEDBY, PERMISSIONGRANTEDBYTYPE) VALUES(?,?,?,?,?,?,?,?)");
			int parameterIndex = 1;
			ps.setString(parameterIndex++, groupId);
			ps.setString(parameterIndex++, groupType);
			ps.setString(parameterIndex++, projectId);
			ps.setInt(parameterIndex++, permission);
			ps.setTimestamp(parameterIndex++, startDate);
			if(verifiedEndDate == null) {
				ps.setNull(parameterIndex++, java.sql.Types.TIMESTAMP);
			} else {
				ps.setTimestamp(parameterIndex++, verifiedEndDate);
			}
			ps.setString(parameterIndex++, userDetails.getValue0());
			ps.setString(parameterIndex++, userDetails.getValue1());
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error occurred adding the group permission");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}
	
	/**
	 * 
	 * @param user
	 * @param groupId
	 * @param groupType
	 * @param projectId
	 * @param permission
	 * @param endDate
	 */
	public void editGroupProjectPermission(User user, String groupId, String groupType, String projectId, int permission, String endDate) {
		int curPermission = groupProjectPermission(groupId, groupType, projectId);
		if(curPermission == -1) {
			throw new IllegalArgumentException("Group " + groupId + " does not currently have access to project " + projectId + " to edit");
		}
		if(curPermission == permission) {
			throw new IllegalArgumentException("Group " + groupId + " already has permission level " 
					+ AccessPermissionEnum.getPermissionValueById(curPermission) + " to project " + projectId);
		}
		
		Pair<String, String> userDetails = User.getPrimaryUserIdAndTypePair(user);
		
		Timestamp startDate = Utility.getCurrentSqlTimestampUTC();
		Timestamp verifiedEndDate = null;
		if (endDate != null) {
			verifiedEndDate = AbstractSecurityUtils.calculateEndDate(endDate);
		}
		
		String updateQuery = null;
		if(groupType == null || groupType.isEmpty()) {
			updateQuery = "UPDATE GROUPPROJECTPERMISSION SET PERMISSION=?, DATEADDED=?, ENDDATE=?, PERMISSIONGRANTEDBY=?, PERMISSIONGRANTEDBYTYPE=? WHERE ID=? AND PROJECTID=? AND TYPE IS NULL";
		} else {
			updateQuery = "UPDATE GROUPPROJECTPERMISSION SET PERMISSION=?, DATEADDED=?, ENDDATE=?, PERMISSIONGRANTEDBY=?, PERMISSIONGRANTEDBYTYPE=? WHERE ID=? AND PROJECTID=? AND TYPE=?";
		}
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(updateQuery);
			int parameterIndex = 1;
			ps.setInt(parameterIndex++, permission);
			ps.setTimestamp(parameterIndex++, startDate);
			if(verifiedEndDate == null) {
				ps.setNull(parameterIndex++, java.sql.Types.TIMESTAMP);
			} else {
				ps.setTimestamp(parameterIndex++, verifiedEndDate);
			}
			ps.setString(parameterIndex++, userDetails.getValue0());
			ps.setString(parameterIndex++, userDetails.getValue1());
			ps.setString(parameterIndex++, groupId);
			ps.setString(parameterIndex++, projectId);
			if(groupType != null) {
				ps.setString(parameterIndex++, groupType);
			}
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error occurred editing the group permission");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}
	
	/**
	 * 
	 * @param user
	 * @param groupId
	 * @param groupType
	 * @param projectId
	 */
	public void removeGroupProjectPermission(User user, String groupId, String groupType, String projectId) {
		int curPermission = groupProjectPermission(groupId, groupType, projectId);
		if(curPermission == -1) {
			throw new IllegalArgumentException("Group " + groupId + " does not currently have access to project " + projectId + " to remove");
		}
		
		String deleteQuery = null;
		if(groupType == null || groupType.isEmpty()) {
			deleteQuery = "DELETE FROM GROUPPROJECTPERMISSION WHERE ID=? AND PROJECTID=? AND TYPE IS NULL";
		} else {
			deleteQuery = "DELETE FROM GROUPPROJECTPERMISSION WHERE ID=? AND PROJECTID=? AND TYPE=?";
		}
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(deleteQuery);
			int parameterIndex = 1;
			ps.setString(parameterIndex++, groupId);
			ps.setString(parameterIndex++, projectId);
			if(groupType != null) {
				ps.setString(parameterIndex++, groupType);
			}
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error occurred deleting the group permission");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}
	
	/**
	 * 
	 * @param groupId
	 * @param searchTerm
	 * @param limit
	 * @param offset
	 * @param onlyApps
	 * @return
	 */
	public List<Map<String, Object>> getProjectsForGroup(String groupId, String groupType, String searchTerm, long limit, long offset, boolean onlyApps) {
		boolean hasSearchTerm = searchTerm != null && !(searchTerm=searchTerm.trim()).isEmpty();

		String groupProjectPermission = "GROUPPROJECTPERMISSION__";
		String projectPrefix = "PROJECT__";
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(groupProjectPermission+"ID")); // this is the group id
		qs.addSelector(new QueryColumnSelector(groupProjectPermission+"TYPE")); // this is the group type
		qs.addSelector(new QueryColumnSelector(groupProjectPermission+"PROJECTID"));
		qs.addSelector(new QueryColumnSelector(groupProjectPermission+"PERMISSION"));
		qs.addSelector(new QueryColumnSelector(groupProjectPermission+"ENDDATE"));
		// filter for the group being specified
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(groupProjectPermission+"ID", "==", groupId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(groupProjectPermission+"TYPE", "==", groupType));
		// project selectors
		qs.addSelector(new QueryColumnSelector(projectPrefix+"PROJECTID", "project_id"));
		qs.addSelector(new QueryColumnSelector(projectPrefix+"PROJECTNAME", "project_name"));
		qs.addSelector(new QueryColumnSelector(projectPrefix+"TYPE", "project_type"));
		qs.addSelector(new QueryColumnSelector(projectPrefix+"COST", "project_cost"));
		qs.addSelector(new QueryColumnSelector(projectPrefix+"GLOBAL", "project_global"));
		qs.addSelector(new QueryColumnSelector(projectPrefix+"DISCOVERABLE", "project_discoverable"));
		qs.addSelector(new QueryColumnSelector(projectPrefix+"CATALOGNAME", "project_catalog_name"));
		qs.addSelector(new QueryColumnSelector(projectPrefix+"CREATEDBY", "project_created_by"));
		qs.addSelector(new QueryColumnSelector(projectPrefix+"CREATEDBYTYPE", "project_created_by_type"));
		qs.addSelector(new QueryColumnSelector(projectPrefix+"DATECREATED", "project_date_created"));
		// dont forget reactors/portal information
		qs.addSelector(new QueryColumnSelector(projectPrefix+"HASPORTAL", "project_has_portal"));
		qs.addSelector(new QueryColumnSelector(projectPrefix+"PORTALNAME", "project_portal_name"));
		qs.addSelector(new QueryColumnSelector(projectPrefix+"PORTALPUBLISHED", "project_portal_published_date"));
		qs.addSelector(new QueryColumnSelector(projectPrefix+"PORTALPUBLISHEDUSER", "project_published_user"));
		qs.addSelector(new QueryColumnSelector(projectPrefix+"PORTALPUBLISHEDTYPE", "project_published_user_type"));
		qs.addSelector(new QueryColumnSelector(projectPrefix+"REACTORSCOMPILED", "project_reactors_compiled_date"));
		qs.addSelector(new QueryColumnSelector(projectPrefix+"REACTORSCOMPILEDUSER", "project_reactors_compiled_user"));
		qs.addSelector(new QueryColumnSelector(projectPrefix+"REACTORSCOMPILEDTYPE", "project_reactors_compiled_user_type"));
		// back to the others
		qs.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.LOWER, projectPrefix+"PROJECTNAME", "low_project_name"));
		
		if(hasSearchTerm) {
			OrQueryFilter searchFilter = new OrQueryFilter();
			searchFilter.addFilter(securityDb.getQueryUtil().getSearchRegexFilter(projectPrefix+"PROJECTID", searchTerm));
			searchFilter.addFilter(securityDb.getQueryUtil().getSearchRegexFilter(projectPrefix+"PROJECTNAME", searchTerm));
			qs.addExplicitFilter(searchFilter);
		}
		
		if(onlyApps) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(projectPrefix+"HASPORTAL", "==", true, PixelDataType.BOOLEAN));
		}
		
		// join
		qs.addRelation(groupProjectPermission+"PROJECTID", projectPrefix+"PROJECTID", "inner.join");
		
		// add the sort
		qs.addOrderBy(new QueryColumnOrderBySelector("low_project_name"));
		
		if (limit > 0) {
			qs.setLimit(limit);
		}
		if (offset > 0) {
			qs.setOffSet(offset);
		}
		return getSimpleQuery(qs);
	}
	
	
	
	
	
	
	
	
	
	
	
	/**
	 * 
	 * @param user
	 * @param groupId
	 * @param groupType
	 * @param engineId
	 * @param permission
	 * @param endDate
	 */
	public void addGroupEnginePermission(User user, String groupId, String groupType, String engineId, int permission, String endDate) {
		int curPermission = groupEnginePermission(groupId, groupType, engineId);
		if(curPermission!= -1) {
			throw new IllegalArgumentException("Group " + groupId + " already has access to engine " + engineId + " with permission = " + AccessPermissionEnum.getPermissionValueById(curPermission));
		}
		
		Pair<String, String> userDetails = User.getPrimaryUserIdAndTypePair(user);
		
		Timestamp startDate = Utility.getCurrentSqlTimestampUTC();
		Timestamp verifiedEndDate = null;
		if (endDate != null) {
			verifiedEndDate = AbstractSecurityUtils.calculateEndDate(endDate);
		}
		
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement("INSERT INTO GROUPENGINEPERMISSION (ID, TYPE, ENGINEID, PERMISSION, DATEADDED, ENDDATE, PERMISSIONGRANTEDBY, PERMISSIONGRANTEDBYTYPE) VALUES(?,?,?,?,?,?,?,?)");
			int parameterIndex = 1;
			ps.setString(parameterIndex++, groupId);
			ps.setString(parameterIndex++, groupType);
			ps.setString(parameterIndex++, engineId);
			ps.setInt(parameterIndex++, permission);
			ps.setTimestamp(parameterIndex++, startDate);
			if(verifiedEndDate == null) {
				ps.setNull(parameterIndex++, java.sql.Types.TIMESTAMP);
			} else {
				ps.setTimestamp(parameterIndex++, verifiedEndDate);
			}
			ps.setString(parameterIndex++, userDetails.getValue0());
			ps.setString(parameterIndex++, userDetails.getValue1());
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error occurred adding the group permission");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}
	
	/**
	 * 
	 * @param user
	 * @param groupId
	 * @param groupType
	 * @param projectId
	 * @param permission
	 * @param endDate
	 */
	public void editGroupEnginePermission(User user, String groupId, String groupType, String engineId, int permission, String endDate) {
		int curPermission = groupEnginePermission(groupId, groupType, engineId);
		if(curPermission == -1) {
			throw new IllegalArgumentException("Group " + groupId + " does not currently have access to engine " + engineId + " to edit");
		}
		if(curPermission == permission) {
			throw new IllegalArgumentException("Group " + groupId + " already has permission level " 
					+ AccessPermissionEnum.getPermissionValueById(curPermission) + " to engine " + engineId);
		}
		
		Pair<String, String> userDetails = User.getPrimaryUserIdAndTypePair(user);
		
		Timestamp startDate = Utility.getCurrentSqlTimestampUTC();
		Timestamp verifiedEndDate = null;
		if (endDate != null) {
			verifiedEndDate = AbstractSecurityUtils.calculateEndDate(endDate);
		}
		
		String updateQuery = null;
		if(groupType == null || groupType.isEmpty()) {
			updateQuery = "UPDATE GROUPENGINEPERMISSION SET PERMISSION=?, DATEADDED=?, ENDDATE=?, PERMISSIONGRANTEDBY=?, PERMISSIONGRANTEDBYTYPE=? WHERE ID=? AND ENGINEID=? AND TYPE IS NULL";
		} else {
			updateQuery = "UPDATE GROUPENGINEPERMISSION SET PERMISSION=?, DATEADDED=?, ENDDATE=?, PERMISSIONGRANTEDBY=?, PERMISSIONGRANTEDBYTYPE=? WHERE ID=? AND ENGINEID=? AND TYPE=?";
		}
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(updateQuery);
			int parameterIndex = 1;
			ps.setInt(parameterIndex++, permission);
			ps.setTimestamp(parameterIndex++, startDate);
			if(verifiedEndDate == null) {
				ps.setNull(parameterIndex++, java.sql.Types.TIMESTAMP);
			} else {
				ps.setTimestamp(parameterIndex++, verifiedEndDate);
			}
			ps.setString(parameterIndex++, userDetails.getValue0());
			ps.setString(parameterIndex++, userDetails.getValue1());
			ps.setString(parameterIndex++, groupId);
			ps.setString(parameterIndex++, engineId);
			if(groupType != null) {
				ps.setString(parameterIndex++, groupType);
			}
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error occurred editing the group permission");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}
	
	/**
	 * 
	 * @param user
	 * @param groupId
	 * @param groupType
	 * @param engineId
	 */
	public void removeGroupEnginePermission(User user, String groupId, String groupType, String engineId) {
		int curPermission = groupEnginePermission(groupId, groupType, engineId);
		if(curPermission == -1) {
			throw new IllegalArgumentException("Group " + groupId + " does not currently have access to engine " + engineId + " to remove");
		}
		
		String deleteQuery = null;
		if(groupType == null || groupType.isEmpty()) {
			deleteQuery = "DELETE FROM GROUPENGINEPERMISSION WHERE ID=? AND ENGINEID=? AND TYPE IS NULL";
		} else {
			deleteQuery = "DELETE FROM GROUPENGINEPERMISSION WHERE ID=? AND ENGINEID=? AND TYPE=?";
		}
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(deleteQuery);
			int parameterIndex = 1;
			ps.setString(parameterIndex++, groupId);
			ps.setString(parameterIndex++, engineId);
			if(groupType != null) {
				ps.setString(parameterIndex++, groupType);
			}
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error occurred deleting the group permission");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}
	
	/**
	 * 
	 * @param groupId
	 * @param searchTerm
	 * @param limit
	 * @param offset
	 * @param onlyApps
	 * @return
	 */
	public List<Map<String, Object>> getEnginesForGroup(String groupId, String groupType, String searchTerm, long limit, long offset) {
		boolean hasSearchTerm = searchTerm != null && !(searchTerm=searchTerm.trim()).isEmpty();

		String groupEnginePermission = "GROUPENGINEPERMISSION__";
		String enginePrefix = "ENGINE__";
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(groupEnginePermission+"ID")); // this is the group id
		qs.addSelector(new QueryColumnSelector(groupEnginePermission+"TYPE")); // this is the group type
		qs.addSelector(new QueryColumnSelector(groupEnginePermission+"ENGINEID"));
		qs.addSelector(new QueryColumnSelector(groupEnginePermission+"PERMISSION"));
		qs.addSelector(new QueryColumnSelector(groupEnginePermission+"ENDDATE"));
		// filter for the group being specified
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(groupEnginePermission+"ID", "==", groupId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(groupEnginePermission+"TYPE", "==", groupType));
		// engine selectors
		qs.addSelector(new QueryColumnSelector(enginePrefix+"ENGINEID", "engine_id"));
		qs.addSelector(new QueryColumnSelector(enginePrefix+"ENGINENAME", "engine_name"));
		qs.addSelector(new QueryColumnSelector(enginePrefix+"ENGINETYPE", "engine_type"));
		qs.addSelector(new QueryColumnSelector(enginePrefix+"ENGINESUBTYPE", "engine_subtype"));
		qs.addSelector(new QueryColumnSelector(enginePrefix+"COST", "engine_cost"));
		qs.addSelector(new QueryColumnSelector(enginePrefix+"DISCOVERABLE", "engine_discoverable"));
		qs.addSelector(new QueryColumnSelector(enginePrefix+"GLOBAL", "engine_global"));
		qs.addSelector(new QueryColumnSelector(enginePrefix+"CREATEDBY", "engine_created_by"));
		qs.addSelector(new QueryColumnSelector(enginePrefix+"CREATEDBYTYPE", "engine_created_by_type"));
		qs.addSelector(new QueryColumnSelector(enginePrefix+"DATECREATED", "engine_date_created"));
		// back to the others
		qs.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.LOWER, enginePrefix+"ENGINENAME", "low_engine_name"));
		
		if(hasSearchTerm) {
			OrQueryFilter searchFilter = new OrQueryFilter();
			searchFilter.addFilter(securityDb.getQueryUtil().getSearchRegexFilter(enginePrefix+"ENGINEID", searchTerm));
			searchFilter.addFilter(securityDb.getQueryUtil().getSearchRegexFilter(enginePrefix+"ENGINENAME", searchTerm));
			qs.addExplicitFilter(searchFilter);
		}
		
		// join
		qs.addRelation(groupEnginePermission+"ENGINEID", enginePrefix+"ENGINEID", "inner.join");
		
		// add the sort
		qs.addOrderBy(new QueryColumnOrderBySelector("low_engine_name"));
		
		if (limit > 0) {
			qs.setLimit(limit);
		}
		if (offset > 0) {
			qs.setOffSet(offset);
		}
		return getSimpleQuery(qs);
	}
	
	
	
	
	/////////////////////////////////////////////////////
	
	/*
	 * Useful methods as utility
	 */
	
	
	/**
	 * 
	 * @param groupId
	 * @param userId
	 * @param userType
	 * @return
	 */
	public boolean userInCustomGroup(String groupId, String userId, String userType) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("CUSTOMGROUPASSIGNMENT__GROUPID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("CUSTOMGROUPASSIGNMENT__GROUPID", "==", groupId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("CUSTOMGROUPASSIGNMENT__USERID", "==", userId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("CUSTOMGROUPASSIGNMENT__TYPE", "==", userType));

		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if (wrapper.hasNext()) {
				return true;
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if (wrapper != null) {
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
	 * 
	 * @param groupId
	 * @param groupType
	 * @return
	 */
	public boolean groupExists(String groupId, String groupType) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_GROUP__ID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_GROUP__ID", "==", groupId));
		if (groupType == null || (groupType = groupType.trim()).isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_GROUP__IS_CUSTOM_GROUP", "==", true,
					PixelDataType.BOOLEAN));
		} else {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_GROUP__TYPE", "==", groupType));
		}
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if (wrapper.hasNext()) {
				return true;
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if (wrapper != null) {
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
	 * 
	 * @param groupId
	 * @return
	 * @throws Exception 
	 */
	public boolean isCustomGroup(String groupId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_GROUP__ID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_GROUP__ID", "==", groupId));
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("SMSS_GROUP__IS_CUSTOM_GROUP", "==", true, PixelDataType.BOOLEAN));

		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if (wrapper.hasNext()) {
				return true;
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if (wrapper != null) {
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
	 * 
	 * @param userId
	 * @param userType
	 * @return
	 */
	public boolean userExists(String userId, String userType) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__ID"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__TYPE"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__ID", "==", userId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__TYPE", "==", userType));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if (wrapper.hasNext()) {
				return true;
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if (wrapper != null) {
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
	 * 
	 * @param groupId
	 * @param groupType
	 * @param projectId
	 * @return
	 */
	public int groupProjectPermission(String groupId, String groupType, String projectId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("GROUPPROJECTPERMISSION__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__ID", "==", groupId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__TYPE", "==", groupType));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__PROJECTID", "==", projectId));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if (wrapper.hasNext()) {
				return ((Number)wrapper.next().getValues()[0]).intValue();
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if (wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}

		return -1;
	}
	
	/**
	 * 
	 * @param groupId
	 * @param groupType
	 * @param engineId
	 * @return
	 */
	public int groupEnginePermission(String groupId, String groupType, String engineId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("GROUPENGINEPERMISSION__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPENGINEPERMISSION__ID", "==", groupId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPENGINEPERMISSION__TYPE", "==", groupType));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPENGINEPERMISSION__ENGINEID", "==", engineId));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if (wrapper.hasNext()) {
				return ((Number)wrapper.next().getValues()[0]).intValue();
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if (wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}

		return -1;
	}
	
}
