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
				ps.setString(parameterIndex++, groupType);
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

		String[] queries = new String[] { "DELETE FROM GROUPENGINEPERMISSION WHERE ID=? AND TYPE=?",
				"DELETE FROM GROUPPROJECTPERMISSION WHERE ID=? AND TYPE=?",
				"DELETE FROM GROUPINSIGHTPERMISSION WHERE ID=? AND TYPE=?",
				"DELETE FROM SMSS_GROUP WHERE ID= ? AND TYPE = ?", };

		Connection conn = null;
		try {
			conn = securityDb.makeConnection();
			try {
				for (String query : queries) {
					try (PreparedStatement ps = conn.prepareStatement(query)) {
						int parameterIndex = 1;
						ps.setString(parameterIndex++, groupId);
						ps.setString(parameterIndex++, groupType);
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
		String groupQuery = "UPDATE SMSS_GROUP SET ID=?, TYPE=?, DESCRIPTION=?, IS_CUSTOM_GROUP=?, DATEADDED=?, USERID=?, USERIDTYPE=? WHERE ID=? AND TYPE=?";
		String[] propagateQueries = new String[] {
				"UPDATE GROUPENGINEPERMISSION SET ID=?, TYPE=? WHERE ID=? AND TYPE=?",
				"UPDATE GROUPPROJECTPERMISSION SET ID=?, TYPE=? WHERE ID=? AND TYPE=?",
				"UPDATE GROUPINSIGHTPERMISSION SET ID=?, TYPE=? WHERE ID=? AND TYPE=?", };

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
					ps.setString(parameterIndex++, newGroupType);
					securityDb.getQueryUtil().handleInsertionOfClob(conn, ps, newDescription, parameterIndex++, gson);
					ps.setBoolean(parameterIndex++, newIsCustomGroup);
					ps.setTimestamp(parameterIndex++, Utility.getCurrentSqlTimestampUTC());
					ps.setString(parameterIndex++, userDetails.getValue0());
					ps.setString(parameterIndex++, userDetails.getValue1());
					// where
					ps.setString(parameterIndex++, curGroupId);
					ps.setString(parameterIndex++, curGroupType);
					ps.execute();
				}

				// propagation
				for (String query : propagateQueries) {
					try (PreparedStatement ps = conn.prepareStatement(query)) {
						int parameterIndex = 1;
						ps.setString(parameterIndex++, newGroupId);
						ps.setString(parameterIndex++, newGroupType);
						ps.setString(parameterIndex++, curGroupId);
						ps.setString(parameterIndex++, curGroupType);
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

}
