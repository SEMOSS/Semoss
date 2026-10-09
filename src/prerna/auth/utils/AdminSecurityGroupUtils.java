/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.auth.utils;

import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;

import prerna.auth.AccessPermissionEnum;
import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.engine.api.IRDBMSEngine;
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
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;
import prerna.util.ValueUtils;

public class AdminSecurityGroupUtils extends AbstractSecurityUtils {

	private static AdminSecurityGroupUtils instance = new AdminSecurityGroupUtils();

	private static final Logger classLogger = LogManager.getLogger(AdminSecurityGroupUtils.class);

	/**
	 * Moves a custom group's members and managers to a new id: new id, then old id
	 */
	private static final String[] CUSTOM_GROUP_ROW_RENAMES = {
			"UPDATE CUSTOMGROUPASSIGNMENT SET GROUPID=? WHERE GROUPID=?",
			"UPDATE GROUPMANAGERS SET GROUPID=? WHERE GROUPID=?" };

	/** Deletes the members and managers kept under a custom group id */
	private static final String[] CUSTOM_GROUP_ROW_DELETES = { "DELETE FROM CUSTOMGROUPASSIGNMENT WHERE GROUPID=?",
			"DELETE FROM GROUPMANAGERS WHERE GROUPID=?" };

	private AdminSecurityGroupUtils() {

	}

	/**
	 * The group utilities without an admin check, for callers in this package that
	 * check the user's rights themselves, such as {@link SecurityGroupManagerUtils}
	 * for a group's managers.
	 * 
	 * @return the group utilities
	 */
	static AdminSecurityGroupUtils getUncheckedInstance() {
		return instance;
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
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		Set<String> results = new HashSet<>();

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_GROUP__ID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_GROUP__TYPE", "==", groupType));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_GROUP__ID", "==", groupIds));

		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			while (wrapper.hasNext()) {
				Object val = wrapper.next().getValues()[0];
				if (val != null) {
					results.add(val.toString());
				}
			}
		} catch (Exception e) {
			classLogger.error("Unable to retrieve matching groups by type.", e);
			throw e;
		}

		return results;
	}

	/**
	 * Group ids are SQL escaped each time an endpoint reads them, so an id stored
	 * with a single quote could never be found again.
	 *
	 * @param groupId the id a group is being given
	 * @throws IllegalArgumentException when the id is blank or has a single quote
	 */
	static void validateGroupId(String groupId) {
		ValueUtils.requireNonBlank(groupId, "The group id cannot be null or empty");
		if (groupId.contains("'")) {
			throw new IllegalArgumentException("Group names cannot contain an apostrophe (')");
		}
	}

	/**
	 * Add a group with description
	 * 
	 * @param groupId
	 * @param groupType
	 * @param description
	 * @throws Exception
	 */
	public void addGroup(User user, String groupId, String groupType, String description) throws Exception {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		try {
			validateGroupId(groupId);
			if (groupExists(groupId, groupType)) {
				throw new IllegalArgumentException("Group " + groupId + " with type " + groupType + " already exists");
			}
			QueryExecutionUtility.write(securityDb, conn -> {
				Pair<String, String> userDetails = User.getPrimaryUserIdAndTypePair(user);

				String query = "INSERT INTO SMSS_GROUP (ID, TYPE, DESCRIPTION, DATEADDED, USERID, USERIDTYPE) "
						+ "VALUES (?,?,?,?,?,?)";
				QueryExecutionUtility.executeUpdate(conn, query, ps -> {
					int parameterIndex = 1;
					ps.setString(parameterIndex++, groupId);
					ps.setString(parameterIndex++, groupType);
					securityDb.getQueryUtil().setNullableLargeText(ps, parameterIndex++, description);
					ps.setTimestamp(parameterIndex++, Utility.getCurrentSqlTimestampUTC());
					ps.setString(parameterIndex++, userDetails.getValue0());
					ps.setString(parameterIndex++, userDetails.getValue1());
				});

				// the admin who creates a custom group is its first manager. Members and
				// managers left under the id by an earlier group are not carried over.
				if (SecurityGroupManagerUtils.CUSTOM_GROUP_TYPE.equals(groupType)) {
					for (String deleteQuery : CUSTOM_GROUP_ROW_DELETES) {
						QueryExecutionUtility.executeUpdate(conn, deleteQuery, ps -> ps.setString(1, groupId));
					}
					QueryExecutionUtility.executeUpdate(conn, SecurityGroupManagerUtils.INSERT_MANAGER_QUERY, ps -> {
						int parameterIndex = 1;
						ps.setString(parameterIndex++, groupId);
						ps.setString(parameterIndex++, userDetails.getValue0());
						ps.setString(parameterIndex++, userDetails.getValue1());
						ps.setTimestamp(parameterIndex++, Utility.getCurrentSqlTimestampUTC());
						ps.setString(parameterIndex++, userDetails.getValue0());
						ps.setString(parameterIndex++, userDetails.getValue1());
					});
				}
				return null;
			});
		} catch (Exception e) {
			classLogger.error("Unable to add group.", e);
			throw e;
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
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		if (!groupExists(groupId, groupType)) {
			throw new IllegalArgumentException("Group " + groupId + " does not exist");
		}
		String[] queries;

		if ("CUSTOM".equals(groupType)) {
			queries = new String[] { "DELETE FROM GROUPENGINEPERMISSION WHERE ID=? AND TYPE=?",
					"DELETE FROM GROUPPROJECTPERMISSION WHERE ID=? AND TYPE=?",
					"DELETE FROM GROUPINSIGHTPERMISSION WHERE ID=? AND TYPE=?",
					"DELETE FROM SMSS_GROUP WHERE ID=? AND TYPE=?", "DELETE FROM CUSTOMGROUPASSIGNMENT WHERE GROUPID=?",
					"DELETE FROM GROUPMANAGERS WHERE GROUPID=?" };
		} else {
			queries = new String[] { "DELETE FROM GROUPENGINEPERMISSION WHERE ID=? AND TYPE=?",
					"DELETE FROM GROUPPROJECTPERMISSION WHERE ID=? AND TYPE=?",
					"DELETE FROM GROUPINSIGHTPERMISSION WHERE ID=? AND TYPE=?",
					"DELETE FROM SMSS_GROUP WHERE ID=? AND TYPE=?" };
		}

		try {
			QueryExecutionUtility.write(securityDb, conn -> {

				for (String query : queries) {
					try (PreparedStatement ps = conn.prepareStatement(query)) {
						int parameterIndex = 1;
						// the custom group tables key the group by its id alone
						if (query.endsWith("WHERE GROUPID=?")) {
							ps.setString(parameterIndex++, groupId);
							ps.execute();
						} else {
							ps.setString(parameterIndex++, groupId);
							ps.setString(parameterIndex++, groupType);
							ps.execute();
						}
					}
				}
				return null;
			});
		} catch (Exception e) {
			classLogger.error("Unable to delete the group and clean up related permissions.", e);
			throw e;
		}
	}

	/**
	 * Edit an existing group across all the tables
	 * 
	 * @param curGroupId
	 * @param curGroupType
	 * @param newGroupId
	 * @param newDescription
	 * @throws Exception
	 */
	public void editGroupDetailsAndPropagate(User user, String curGroupId, String curGroupType, String newGroupId,
			String newDescription) throws Exception {

		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();

		if (!groupExists(curGroupId, curGroupType)) {
			throw new IllegalArgumentException("Group " + curGroupId + " does not exist");
		}
		if (!curGroupId.equals(newGroupId)) {
			validateGroupId(newGroupId);
			if (groupExists(newGroupId, curGroupType)) {
				throw new IllegalArgumentException(
						"Group " + newGroupId + " of type " + curGroupType + " already exist");
			}
		}

		String groupQuery = "UPDATE SMSS_GROUP SET ID=?, DESCRIPTION=? WHERE ID=? AND TYPE=?";
		String[] propagateQueries = new String[] { "UPDATE GROUPENGINEPERMISSION SET ID=? WHERE ID=? AND TYPE=?",
				"UPDATE GROUPPROJECTPERMISSION SET ID=? WHERE ID=? AND TYPE=?",
				"UPDATE GROUPINSIGHTPERMISSION SET ID=? WHERE ID=? AND TYPE=?" };

		try {
			QueryExecutionUtility.write(securityDb, conn -> {

				QueryExecutionUtility.executeUpdate(conn, groupQuery, ps -> {
					int parameterIndex = 1;
					ps.setString(parameterIndex++, newGroupId);
					securityDb.getQueryUtil().setNullableLargeText(ps, parameterIndex++, newDescription);
					// where
					ps.setString(parameterIndex++, curGroupId);
					ps.setString(parameterIndex++, curGroupType);
				});

				// the members and managers of a custom group, which are keyed by the id
				// alone, so a group of another type with the same id must not move them
				if (SecurityGroupManagerUtils.CUSTOM_GROUP_TYPE.equals(curGroupType)) {
					// rows already under the new id belong to no group, so they are not
					// carried into it
					if (!newGroupId.equals(curGroupId)) {
						for (String query : CUSTOM_GROUP_ROW_DELETES) {
							QueryExecutionUtility.executeUpdate(conn, query, ps -> ps.setString(1, newGroupId));
						}
					}
					for (String query : CUSTOM_GROUP_ROW_RENAMES) {
						QueryExecutionUtility.executeUpdate(conn, query, ps -> {
							ps.setString(1, newGroupId);
							// where
							ps.setString(2, curGroupId);
						});
					}
				}

				// propagation
				for (String query : propagateQueries) {
					QueryExecutionUtility.executeUpdate(conn, query, ps -> {
						int parameterIndex = 1;
						ps.setString(parameterIndex++, newGroupId);
						// where
						ps.setString(parameterIndex++, curGroupId);
						ps.setString(parameterIndex++, curGroupType);
					});
				}
				return null;
			});
		} catch (Exception e) {
			classLogger.error("Unable to delete the group and clean up related permissions.", e);
			throw e;
		}
	}

	/**
	 * 
	 * @param groupId
	 * @param userId
	 * @param userType
	 * @throws Exception
	 */
	public void addUserToGroup(User user, String groupId, String userId, String userType, String endDate)
			throws Exception {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		if (!groupExists(groupId, "CUSTOM")) {
			throw new IllegalArgumentException("Group " + groupId + " does not exist");
		}

		if (userInCustomGroup(groupId, userId, userType)) {
			throw new IllegalArgumentException("User " + userId + " already has access to group " + groupId);
		}

		if (!userExists(userId, userType)) {
			throw new IllegalArgumentException("User " + userId + " does not exist");
		}

		Pair<String, String> userDetails = User.getPrimaryUserIdAndTypePair(user);

		Timestamp verifiedEndDate = endDate == null ? null : AbstractSecurityUtils.calculateEndDate(endDate);

		try {
			QueryExecutionUtility.write(securityDb, conn -> {
				String query = "INSERT INTO CUSTOMGROUPASSIGNMENT (GROUPID, USERID, TYPE, "
						+ "DATEADDED, ENDDATE, PERMISSIONGRANTEDBY, PERMISSIONGRANTEDBYTYPE) "
						+ "VALUES (?,?,?,?,?,?,?)";
				QueryExecutionUtility.executeUpdate(conn, query, ps -> {
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
				});
				return null;
			});
		} catch (Exception e) {
			classLogger.error("Unable to delete the group and clean up related permissions.", e);
			throw e;
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
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		if (!groupExists(groupId, "CUSTOM")) {
			throw new IllegalArgumentException("Group " + groupId + " does not exist");
		}

		if (!userInCustomGroup(groupId, userId, userType)) {
			throw new IllegalArgumentException("User " + userId + " does not have access to group " + groupId);
		}

		try {
			QueryExecutionUtility.write(securityDb, conn -> {
				String query = "DELETE FROM CUSTOMGROUPASSIGNMENT WHERE GROUPID=? AND USERID=? AND TYPE=?";
				QueryExecutionUtility.executeUpdate(conn, query, ps -> {
					int parameterIndex = 1;
					ps.setString(parameterIndex++, groupId);
					ps.setString(parameterIndex++, userId);
					ps.setString(parameterIndex++, userType);
				});
				return null;
			});
		} catch (Exception e) {
			classLogger.error("Unable to remove user from group.", e);
			throw e;
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
		qs.addSelector(new QueryColumnSelector("SMSS_GROUP__USERID", "CREATED_BY_USERID"));
		qs.addSelector(new QueryColumnSelector("SMSS_GROUP__USERIDTYPE", "CREATED_BY_USERIDTYPE"));
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
		List<Map<String, Object>> groups = getSimpleQuery(qs);
		addMemberCounts(groups);
		return groups;
	}

	/**
	 * Adds {@code member_count}, how many members a custom group has, to each
	 * custom group in a group list. Groups of other types take their members from
	 * the login, so they get no count.
	 *
	 * @param groups rows with {@code id} and {@code type}, as the group lists
	 *               return them
	 */
	public static void addMemberCounts(List<Map<String, Object>> groups) {
		List<String> customIds = new ArrayList<>();
		for (Map<String, Object> group : groups) {
			if (SecurityGroupManagerUtils.CUSTOM_GROUP_TYPE.equals(group.get("type")) && group.get("id") != null) {
				customIds.add(group.get("id").toString());
			}
		}
		if (customIds.isEmpty()) {
			return;
		}

		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("CUSTOMGROUPASSIGNMENT__GROUPID", "GROUPID"));
		qs.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.COUNT,
				"CUSTOMGROUPASSIGNMENT__USERID", "MEMBERS"));
		qs.addGroupBy(new QueryColumnSelector("CUSTOMGROUPASSIGNMENT__GROUPID", "GROUPID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("CUSTOMGROUPASSIGNMENT__GROUPID", "==", customIds));
		// count the members the member list shows, which leaves out deleted users
		qs.addRelation("CUSTOMGROUPASSIGNMENT__USERID", "SMSS_USER__ID", "inner.join");
		qs.addRelation("CUSTOMGROUPASSIGNMENT__TYPE", "SMSS_USER__TYPE", "inner.join");

		Map<String, Long> counts = new HashMap<>();
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			while (wrapper.hasNext()) {
				Object[] values = wrapper.next().getValues();
				if (values[0] != null && values[1] instanceof Number) {
					counts.put(values[0].toString(), ((Number) values[1]).longValue());
				}
			}
		} catch (Exception e) {
			classLogger.error("Unable to count the members of the custom groups.", e);
			return;
		}
		for (Map<String, Object> group : groups) {
			if (SecurityGroupManagerUtils.CUSTOM_GROUP_TYPE.equals(group.get("type")) && group.get("id") != null) {
				group.put("member_count", counts.getOrDefault(group.get("id").toString(), 0L));
			}
		}
	}

	/**
	 * Get specific group details
	 * 
	 * @return
	 */
	public Map<String, Object> getGroupDetails(String groupId, String groupType) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_GROUP__ID"));
		qs.addSelector(new QueryColumnSelector("SMSS_GROUP__TYPE"));
		qs.addSelector(new QueryColumnSelector("SMSS_GROUP__DESCRIPTION"));
		qs.addSelector(new QueryColumnSelector("SMSS_GROUP__USERID", "CREATED_BY_USERID"));
		qs.addSelector(new QueryColumnSelector("SMSS_GROUP__USERIDTYPE", "CREATED_BY_USERIDTYPE"));
		qs.addSelector(new QueryColumnSelector("SMSS_GROUP__DATEADDED"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_GROUP__ID", "==", groupId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_GROUP__TYPE", "==", groupType));

		List<Map<String, Object>> group = getSimpleQuery(qs);
		if (group != null && !group.isEmpty()) {
			return group.get(0);
		}

		throw new IllegalArgumentException("Group " + groupId + " does not exist");
	}

	/**
	 * Get number of groups
	 * 
	 * @return
	 */
	public Long getNumGroups(String searchTerm) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(
				QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.COUNT, "SMSS_GROUP__ID", "numGroups"));
		if (searchTerm != null && !(searchTerm = searchTerm.trim()).isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_GROUP__ID", "?like", searchTerm));
		}
		return QueryExecutionUtility.flushToLong(securityDb, qs);
	}

	/**
	 * The members of a custom group.
	 *
	 * @param groupId    the custom group
	 * @param searchTerm text matched against the member's id, name, username and
	 *                   email, or null
	 * @param limit      page size, or 0 or less for all
	 * @param offset     rows to skip
	 * @return the members, with the keys {@code userid}, {@code type},
	 *         {@code dateadded}, {@code name}, {@code username} and {@code email}
	 */
	public List<Map<String, Object>> getGroupMembers(String groupId, String searchTerm, long limit, long offset) {
		if (!groupExists(groupId, "CUSTOM")) {
			throw new IllegalArgumentException("Group " + groupId + " with type custom does not exist");
		}

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("CUSTOMGROUPASSIGNMENT__USERID"));
		qs.addSelector(new QueryColumnSelector("CUSTOMGROUPASSIGNMENT__TYPE"));
		qs.addSelector(new QueryColumnSelector("CUSTOMGROUPASSIGNMENT__DATEADDED"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__NAME"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__USERNAME"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__EMAIL"));
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
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		if (!groupExists(groupId, "CUSTOM")) {
			throw new IllegalArgumentException("Group " + groupId + " with type custom does not exist");
		}

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.COUNT,
				"CUSTOMGROUPASSIGNMENT__USERID", "numUsers"));
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
	 * The users who are not members of a custom group.
	 *
	 * @param groupId    the custom group
	 * @param searchTerm text matched against the user's id, name, username and
	 *                   email, or null
	 * @param limit      page size, or 0 or less for all
	 * @param offset     rows to skip
	 * @return the users, with the keys {@code id}, {@code type}, {@code name},
	 *         {@code username} and {@code email}
	 */
	public List<Map<String, Object>> getNonGroupMembers(String groupId, String searchTerm, long limit, long offset) {
		if (!groupExists(groupId, "CUSTOM")) {
			throw new IllegalArgumentException("Group " + groupId + " with type custom does not exist");
		}

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__ID"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__TYPE"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__NAME"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__USERNAME"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__EMAIL"));
		{
			SelectQueryStruct exisitngMembersQs = new SelectQueryStruct();
			exisitngMembersQs.addSelector(QueryFunctionSelector.makeConcat2ColumnsFunction(
					"CUSTOMGROUPASSIGNMENT__USERID", "CUSTOMGROUPASSIGNMENT__TYPE", "UUID"));
			exisitngMembersQs.addExplicitFilter(
					SimpleQueryFilter.makeColToValFilter("CUSTOMGROUPASSIGNMENT__GROUPID", "==", groupId));

			// add the subqs to the main qs
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

	/**
	 * 
	 * @param user
	 * @param groupId
	 * @param groupType
	 * @param projectId
	 * @param permission
	 * @param endDat
	 */
	public void addGroupProjectPermission(User user, String groupId, String groupType, String projectId, int permission,
			String endDate) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		if (!groupExists(groupId, groupType)) {
			throw new IllegalArgumentException("Group " + groupId + " with type " + groupType + " does not exist");
		}

		int curPermission = groupProjectPermission(groupId, groupType, projectId);
		if (curPermission != -1) {
			throw new IllegalArgumentException("Group " + groupId + " already has access to project " + projectId
					+ " with permission = " + AccessPermissionEnum.getPermissionValueById(curPermission));
		}
		Pair<String, String> userDetails = User.getPrimaryUserIdAndTypePair(user);

		Timestamp startDate = Utility.getCurrentSqlTimestampUTC();
		Timestamp verifiedEndDate = endDate == null ? null : AbstractSecurityUtils.calculateEndDate(endDate);

		try {
			QueryExecutionUtility.executeUpdate(securityDb,
					"INSERT INTO GROUPPROJECTPERMISSION (ID, TYPE, PROJECTID, PERMISSION, DATEADDED, ENDDATE, PERMISSIONGRANTEDBY, PERMISSIONGRANTEDBYTYPE) VALUES(?,?,?,?,?,?,?,?)",
					ps -> {
						int parameterIndex = 1;
						ps.setString(parameterIndex++, groupId);
						ps.setString(parameterIndex++, groupType);
						ps.setString(parameterIndex++, projectId);
						ps.setInt(parameterIndex++, permission);
						ps.setTimestamp(parameterIndex++, startDate);
						if (verifiedEndDate == null) {
							ps.setNull(parameterIndex++, java.sql.Types.TIMESTAMP);
						} else {
							ps.setTimestamp(parameterIndex++, verifiedEndDate);
						}
						ps.setString(parameterIndex++, userDetails.getValue0());
						ps.setString(parameterIndex++, userDetails.getValue1());
					});
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Unable to retrieve the number of users who are not in the group.", e);
			throw new IllegalArgumentException("Error occurred adding the group permission");
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
	public void editGroupProjectPermission(User user, String groupId, String groupType, String projectId,
			int permission, String endDate) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		int curPermission = groupProjectPermission(groupId, groupType, projectId);
		if (curPermission == -1) {
			throw new IllegalArgumentException(
					"Group " + groupId + " does not currently have access to project " + projectId + " to edit");
		}
		if (curPermission == permission) {
			throw new IllegalArgumentException("Group " + groupId + " already has permission level "
					+ AccessPermissionEnum.getPermissionValueById(curPermission) + " to project " + projectId);
		}
		Pair<String, String> userDetails = User.getPrimaryUserIdAndTypePair(user);

		Timestamp startDate = Utility.getCurrentSqlTimestampUTC();
		Timestamp verifiedEndDate = endDate == null ? null : AbstractSecurityUtils.calculateEndDate(endDate);

		String updateQuery;
		updateQuery = "UPDATE GROUPPROJECTPERMISSION SET PERMISSION=?, DATEADDED=?, ENDDATE=?, PERMISSIONGRANTEDBY=?, PERMISSIONGRANTEDBYTYPE=? WHERE ID=? AND PROJECTID=? AND TYPE=?";
		try {
			QueryExecutionUtility.executeUpdate(securityDb, updateQuery, ps -> {
				int parameterIndex = 1;
				ps.setInt(parameterIndex++, permission);
				ps.setTimestamp(parameterIndex++, startDate);
				if (verifiedEndDate == null) {
					ps.setNull(parameterIndex++, java.sql.Types.TIMESTAMP);
				} else {
					ps.setTimestamp(parameterIndex++, verifiedEndDate);
				}
				ps.setString(parameterIndex++, userDetails.getValue0());
				ps.setString(parameterIndex++, userDetails.getValue1());
				ps.setString(parameterIndex++, groupId);
				ps.setString(parameterIndex++, projectId);
				ps.setString(parameterIndex++, groupType);
			});
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Unable to retrieve the number of users who are not in the group.", e);
			throw new IllegalArgumentException("Error occurred editing the group permission");
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
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		int curPermission = groupProjectPermission(groupId, groupType, projectId);
		if (curPermission == -1) {
			throw new IllegalArgumentException(
					"Group " + groupId + " does not currently have access to project " + projectId + " to remove");
		}
		String deleteQuery;
		deleteQuery = "DELETE FROM GROUPPROJECTPERMISSION WHERE ID=? AND PROJECTID=? AND TYPE=?";
		try {
			QueryExecutionUtility.executeUpdate(securityDb, deleteQuery, ps -> {
				int parameterIndex = 1;
				ps.setString(parameterIndex++, groupId);
				ps.setString(parameterIndex++, projectId);
				ps.setString(parameterIndex++, groupType);
			});
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Unable to remove group project permission.", e);
			throw new IllegalArgumentException("Error occurred deleting the group permission");
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
	public List<Map<String, Object>> getProjectsForGroup(String groupId, String groupType, String searchTerm,
			long limit, long offset, boolean onlyApps) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		if (!groupExists(groupId, groupType)) {
			throw new IllegalArgumentException("Group " + groupId + " with type " + groupType + " does not exist");
		}
		boolean hasSearchTerm = searchTerm != null && !(searchTerm = searchTerm.trim()).isEmpty();

		String groupProjectPermission = "GROUPPROJECTPERMISSION__";
		String projectPrefix = "PROJECT__";

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(groupProjectPermission + "ID")); // this is the group id
		qs.addSelector(new QueryColumnSelector(groupProjectPermission + "TYPE")); // this is the group type
		qs.addSelector(new QueryColumnSelector(groupProjectPermission + "PROJECTID"));
		qs.addSelector(new QueryColumnSelector(groupProjectPermission + "PERMISSION"));
		qs.addSelector(new QueryColumnSelector(groupProjectPermission + "ENDDATE"));
		// filter for the group being specified
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(groupProjectPermission + "ID", "==", groupId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(groupProjectPermission + "TYPE", "==", groupType));
		// project selectors
		qs.addSelector(new QueryColumnSelector(projectPrefix + "PROJECTID", "project_id"));
		qs.addSelector(new QueryColumnSelector(projectPrefix + "PROJECTNAME", "project_name"));
		qs.addSelector(new QueryColumnSelector(projectPrefix + "TYPE", "project_type"));
		qs.addSelector(new QueryColumnSelector(projectPrefix + "COST", "project_cost"));
		qs.addSelector(new QueryColumnSelector(projectPrefix + "GLOBAL", "project_global"));
		qs.addSelector(new QueryColumnSelector(projectPrefix + "DISCOVERABLE", "project_discoverable"));
		qs.addSelector(new QueryColumnSelector(projectPrefix + "CATALOGNAME", "project_catalog_name"));
		qs.addSelector(new QueryColumnSelector(projectPrefix + "CREATEDBY", "project_created_by"));
		qs.addSelector(new QueryColumnSelector(projectPrefix + "CREATEDBYTYPE", "project_created_by_type"));
		qs.addSelector(new QueryColumnSelector(projectPrefix + "DATECREATED", "project_date_created"));
		qs.addSelector(new QueryColumnSelector(projectPrefix + "DATELASTEDITED", "project_date_last_edited"));
		// dont forget reactors/portal information
		qs.addSelector(new QueryColumnSelector(projectPrefix + "PORTALPUBLISHED", "project_portal_published_date"));
		qs.addSelector(new QueryColumnSelector(projectPrefix + "PORTALPUBLISHEDUSER", "project_published_user"));
		qs.addSelector(new QueryColumnSelector(projectPrefix + "PORTALPUBLISHEDTYPE", "project_published_user_type"));
		qs.addSelector(new QueryColumnSelector(projectPrefix + "REACTORSCOMPILED", "project_reactors_compiled_date"));
		qs.addSelector(
				new QueryColumnSelector(projectPrefix + "REACTORSCOMPILEDUSER", "project_reactors_compiled_user"));
		qs.addSelector(
				new QueryColumnSelector(projectPrefix + "REACTORSCOMPILEDTYPE", "project_reactors_compiled_user_type"));
		// back to the others
		qs.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.LOWER,
				projectPrefix + "PROJECTNAME", "low_project_name"));

		if (hasSearchTerm) {
			OrQueryFilter searchFilter = new OrQueryFilter();
			searchFilter
					.addFilter(securityDb.getQueryUtil().getSearchRegexFilter(projectPrefix + "PROJECTID", searchTerm));
			searchFilter.addFilter(
					securityDb.getQueryUtil().getSearchRegexFilter(projectPrefix + "PROJECTNAME", searchTerm));
			qs.addExplicitFilter(searchFilter);
		}

		if (onlyApps) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(projectPrefix + "HASPORTAL", "==", true,
					PixelDataType.BOOLEAN));
		}

		// join
		qs.addRelation(groupProjectPermission + "PROJECTID", projectPrefix + "PROJECTID", "inner.join");

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
	 * @param groupId
	 * @param searchTerm
	 * @return
	 */
	public Long getNumProjectsForGroup(String groupId, String groupType, String searchTerm, boolean onlyApps) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		if (!groupExists(groupId, groupType)) {
			throw new IllegalArgumentException("Group " + groupId + " with type " + groupType + " does not exist");
		}

		String groupProjectPermission = "GROUPPROJECTPERMISSION__";
		String projectPrefix = "PROJECT__";

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.COUNT,
				groupProjectPermission + "PROJECTID", "numProjects"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(groupProjectPermission + "ID", "==", groupId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(groupProjectPermission + "TYPE", "==", groupType));

		boolean hasSearchTerm = searchTerm != null && !(searchTerm = searchTerm.trim()).isEmpty();

		if (hasSearchTerm || onlyApps) {
			qs.addRelation(groupProjectPermission + "PROJECTID", projectPrefix + "PROJECTID", "inner.join");
		}

		if (hasSearchTerm) {
			OrQueryFilter searchFilter = new OrQueryFilter();
			searchFilter
					.addFilter(securityDb.getQueryUtil().getSearchRegexFilter(projectPrefix + "PROJECTID", searchTerm));
			searchFilter.addFilter(
					securityDb.getQueryUtil().getSearchRegexFilter(projectPrefix + "PROJECTNAME", searchTerm));
			qs.addExplicitFilter(searchFilter);
		}

		if (onlyApps) {
			qs.addRelation(groupProjectPermission + "PROJECTID", projectPrefix + "PROJECTID", "inner.join");
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(projectPrefix + "HASPORTAL", "==", true,
					PixelDataType.BOOLEAN));
		}

		return QueryExecutionUtility.flushToLong(securityDb, qs);
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
	public List<Map<String, Object>> getAvailableProjectsForGroup(String groupId, String groupType, String searchTerm,
			long limit, long offset, boolean onlyApps) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		if (!groupExists(groupId, groupType)) {
			throw new IllegalArgumentException("Group " + groupId + " with type " + groupType + " does not exist");
		}

		boolean hasSearchTerm = searchTerm != null && !(searchTerm = searchTerm.trim()).isEmpty();

		String groupProjectPermission = "GROUPPROJECTPERMISSION__";
		String projectPrefix = "PROJECT__";

		SelectQueryStruct qs = new SelectQueryStruct();
		// project selectors
		qs.addSelector(new QueryColumnSelector(projectPrefix + "PROJECTID", "project_id"));
		qs.addSelector(new QueryColumnSelector(projectPrefix + "PROJECTNAME", "project_name"));
		qs.addSelector(new QueryColumnSelector(projectPrefix + "TYPE", "project_type"));
		qs.addSelector(new QueryColumnSelector(projectPrefix + "COST", "project_cost"));
		qs.addSelector(new QueryColumnSelector(projectPrefix + "GLOBAL", "project_global"));
		qs.addSelector(new QueryColumnSelector(projectPrefix + "DISCOVERABLE", "project_discoverable"));
		qs.addSelector(new QueryColumnSelector(projectPrefix + "CATALOGNAME", "project_catalog_name"));
		qs.addSelector(new QueryColumnSelector(projectPrefix + "CREATEDBY", "project_created_by"));
		qs.addSelector(new QueryColumnSelector(projectPrefix + "CREATEDBYTYPE", "project_created_by_type"));
		qs.addSelector(new QueryColumnSelector(projectPrefix + "DATECREATED", "project_date_created"));
		qs.addSelector(new QueryColumnSelector(projectPrefix + "PORTALPUBLISHED", "project_portal_published_date"));
		qs.addSelector(new QueryColumnSelector(projectPrefix + "PORTALPUBLISHEDUSER", "project_published_user"));
		qs.addSelector(new QueryColumnSelector(projectPrefix + "PORTALPUBLISHEDTYPE", "project_published_user_type"));
		qs.addSelector(new QueryColumnSelector(projectPrefix + "REACTORSCOMPILED", "project_reactors_compiled_date"));
		qs.addSelector(
				new QueryColumnSelector(projectPrefix + "REACTORSCOMPILEDUSER", "project_reactors_compiled_user"));
		qs.addSelector(
				new QueryColumnSelector(projectPrefix + "REACTORSCOMPILEDTYPE", "project_reactors_compiled_user_type"));
		// back to the others
		qs.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.LOWER,
				projectPrefix + "PROJECTNAME", "low_project_name"));

		if (hasSearchTerm) {
			OrQueryFilter searchFilter = new OrQueryFilter();
			searchFilter
					.addFilter(securityDb.getQueryUtil().getSearchRegexFilter(projectPrefix + "PROJECTID", searchTerm));
			searchFilter.addFilter(
					securityDb.getQueryUtil().getSearchRegexFilter(projectPrefix + "PROJECTNAME", searchTerm));
			qs.addExplicitFilter(searchFilter);
		}

		// filter out projects that are already added
		{
			SelectQueryStruct subQs = new SelectQueryStruct();
			subQs.addSelector(new QueryColumnSelector(groupProjectPermission + "PROJECTID")); // this is the group id
			// filter for the group being specified
			subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(groupProjectPermission + "ID", "==", groupId));
			subQs.addExplicitFilter(
					SimpleQueryFilter.makeColToValFilter(groupProjectPermission + "TYPE", "==", groupType));
			// filter out from engine list
			qs.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery(projectPrefix + "PROJECTID", "!=", subQs));
		}

		if (onlyApps) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(projectPrefix + "HASPORTAL", "==", true,
					PixelDataType.BOOLEAN));
		}

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
	public void addGroupEnginePermission(User user, String groupId, String groupType, String engineId, int permission,
			String endDate) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		if (!groupExists(groupId, groupType)) {
			throw new IllegalArgumentException("Group " + groupId + " with type " + groupType + " does not exist");
		}

		int curPermission = groupEnginePermission(groupId, groupType, engineId);
		if (curPermission != -1) {
			throw new IllegalArgumentException("Group " + groupId + " already has access to engine " + engineId
					+ " with permission = " + AccessPermissionEnum.getPermissionValueById(curPermission));
		}

		Pair<String, String> userDetails = User.getPrimaryUserIdAndTypePair(user);

		Timestamp startDate = Utility.getCurrentSqlTimestampUTC();
		Timestamp verifiedEndDate = endDate == null ? null : AbstractSecurityUtils.calculateEndDate(endDate);

		try {
			QueryExecutionUtility.executeUpdate(securityDb,
					"INSERT INTO GROUPENGINEPERMISSION (ID, TYPE, ENGINEID, PERMISSION, DATEADDED, ENDDATE, PERMISSIONGRANTEDBY, PERMISSIONGRANTEDBYTYPE) VALUES(?,?,?,?,?,?,?,?)",
					ps -> {
						int parameterIndex = 1;
						ps.setString(parameterIndex++, groupId);
						ps.setString(parameterIndex++, groupType);
						ps.setString(parameterIndex++, engineId);
						ps.setInt(parameterIndex++, permission);
						ps.setTimestamp(parameterIndex++, startDate);
						if (verifiedEndDate == null) {
							ps.setNull(parameterIndex++, java.sql.Types.TIMESTAMP);
						} else {
							ps.setTimestamp(parameterIndex++, verifiedEndDate);
						}
						ps.setString(parameterIndex++, userDetails.getValue0());
						ps.setString(parameterIndex++, userDetails.getValue1());
					});
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Unable to retrieve the number of projects available for the group.", e);
			throw new IllegalArgumentException("Error occurred adding the group permission");
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
	public void editGroupEnginePermission(User user, String groupId, String groupType, String engineId, int permission,
			String endDate) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		int curPermission = groupEnginePermission(groupId, groupType, engineId);
		if (curPermission == -1) {
			throw new IllegalArgumentException(
					"Group " + groupId + " does not currently have access to engine " + engineId + " to edit");
		}
		if (curPermission == permission) {
			throw new IllegalArgumentException("Group " + groupId + " already has permission level "
					+ AccessPermissionEnum.getPermissionValueById(curPermission) + " to engine " + engineId);
		}
		Pair<String, String> userDetails = User.getPrimaryUserIdAndTypePair(user);

		Timestamp startDate = Utility.getCurrentSqlTimestampUTC();
		Timestamp verifiedEndDate = endDate == null ? null : AbstractSecurityUtils.calculateEndDate(endDate);

		String updateQuery;
		updateQuery = "UPDATE GROUPENGINEPERMISSION SET PERMISSION=?, DATEADDED=?, ENDDATE=?, PERMISSIONGRANTEDBY=?, PERMISSIONGRANTEDBYTYPE=? WHERE ID=? AND ENGINEID=? AND TYPE=?";
		try {
			QueryExecutionUtility.executeUpdate(securityDb, updateQuery, ps -> {
				int parameterIndex = 1;
				ps.setInt(parameterIndex++, permission);
				ps.setTimestamp(parameterIndex++, startDate);
				if (verifiedEndDate == null) {
					ps.setNull(parameterIndex++, java.sql.Types.TIMESTAMP);
				} else {
					ps.setTimestamp(parameterIndex++, verifiedEndDate);
				}
				ps.setString(parameterIndex++, userDetails.getValue0());
				ps.setString(parameterIndex++, userDetails.getValue1());
				ps.setString(parameterIndex++, groupId);
				ps.setString(parameterIndex++, engineId);
				ps.setString(parameterIndex++, groupType);
			});
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Unable to retrieve the number of projects available for the group.", e);
			throw new IllegalArgumentException("Error occurred editing the group permission");
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
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		int curPermission = groupEnginePermission(groupId, groupType, engineId);
		if (curPermission == -1) {
			throw new IllegalArgumentException(
					"Group " + groupId + " does not currently have access to engine " + engineId + " to remove");
		}
		String deleteQuery;
		deleteQuery = "DELETE FROM GROUPENGINEPERMISSION WHERE ID=? AND ENGINEID=? AND TYPE=?";
		try {
			QueryExecutionUtility.executeUpdate(securityDb, deleteQuery, ps -> {
				int parameterIndex = 1;
				ps.setString(parameterIndex++, groupId);
				ps.setString(parameterIndex++, engineId);
				ps.setString(parameterIndex++, groupType);
			});
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Unable to remove group engine permission.", e);
			throw new IllegalArgumentException("Error occurred deleting the group permission");
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
	public List<Map<String, Object>> getEnginesForGroup(String groupId, String groupType, String searchTerm, long limit,
			long offset) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		if (!groupExists(groupId, groupType)) {
			throw new IllegalArgumentException("Group " + groupId + " with type " + groupType + " does not exist");
		}

		boolean hasSearchTerm = searchTerm != null && !(searchTerm = searchTerm.trim()).isEmpty();

		String groupEnginePermission = "GROUPENGINEPERMISSION__";
		String enginePrefix = "ENGINE__";

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(groupEnginePermission + "ID")); // this is the group id
		qs.addSelector(new QueryColumnSelector(groupEnginePermission + "TYPE")); // this is the group type
		qs.addSelector(new QueryColumnSelector(groupEnginePermission + "ENGINEID"));
		qs.addSelector(new QueryColumnSelector(groupEnginePermission + "PERMISSION"));
		qs.addSelector(new QueryColumnSelector(groupEnginePermission + "ENDDATE"));
		// filter for the group being specified
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(groupEnginePermission + "ID", "==", groupId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(groupEnginePermission + "TYPE", "==", groupType));
		// engine selectors
		qs.addSelector(new QueryColumnSelector(enginePrefix + "ENGINEID", "engine_id"));
		qs.addSelector(new QueryColumnSelector(enginePrefix + "ENGINENAME", "engine_name"));
		qs.addSelector(new QueryColumnSelector(enginePrefix + "ENGINETYPE", "engine_type"));
		qs.addSelector(new QueryColumnSelector(enginePrefix + "ENGINESUBTYPE", "engine_subtype"));
		qs.addSelector(new QueryColumnSelector(enginePrefix + "COST", "engine_cost"));
		qs.addSelector(new QueryColumnSelector(enginePrefix + "DISCOVERABLE", "engine_discoverable"));
		qs.addSelector(new QueryColumnSelector(enginePrefix + "GLOBAL", "engine_global"));
		qs.addSelector(new QueryColumnSelector(enginePrefix + "CREATEDBY", "engine_created_by"));
		qs.addSelector(new QueryColumnSelector(enginePrefix + "CREATEDBYTYPE", "engine_created_by_type"));
		qs.addSelector(new QueryColumnSelector(enginePrefix + "DATECREATED", "engine_date_created"));
		// back to the others
		qs.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.LOWER,
				enginePrefix + "ENGINENAME", "low_engine_name"));

		if (hasSearchTerm) {
			OrQueryFilter searchFilter = new OrQueryFilter();
			searchFilter
					.addFilter(securityDb.getQueryUtil().getSearchRegexFilter(enginePrefix + "ENGINEID", searchTerm));
			searchFilter
					.addFilter(securityDb.getQueryUtil().getSearchRegexFilter(enginePrefix + "ENGINENAME", searchTerm));
			qs.addExplicitFilter(searchFilter);
		}

		// join
		qs.addRelation(groupEnginePermission + "ENGINEID", enginePrefix + "ENGINEID", "inner.join");

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

	/**
	 * 
	 * @param groupId
	 * @param searchTerm
	 * @return
	 */
	public Long getNumEnginesForGroup(String groupId, String groupType, String searchTerm) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		if (!groupExists(groupId, groupType)) {
			throw new IllegalArgumentException("Group " + groupId + " with type " + groupType + " does not exist");
		}

		String groupEnginePermission = "GROUPENGINEPERMISSION__";
		String enginePrefix = "ENGINE__";

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.COUNT,
				groupEnginePermission + "ENGINEID", "numEngines"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(groupEnginePermission + "ID", "==", groupId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(groupEnginePermission + "TYPE", "==", groupType));

		if (searchTerm != null && !(searchTerm = searchTerm.trim()).isEmpty()) {
			qs.addRelation(groupEnginePermission + "ENGINEID", enginePrefix + "ENGINEID", "inner.join");

			OrQueryFilter searchFilter = new OrQueryFilter();
			searchFilter
					.addFilter(securityDb.getQueryUtil().getSearchRegexFilter(enginePrefix + "ENGINEID", searchTerm));
			searchFilter
					.addFilter(securityDb.getQueryUtil().getSearchRegexFilter(enginePrefix + "ENGINENAME", searchTerm));
			qs.addExplicitFilter(searchFilter);
		}
		return QueryExecutionUtility.flushToLong(securityDb, qs);
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
	public List<Map<String, Object>> getAvailableEnginesForGroup(String groupId, String groupType, String searchTerm,
			long limit, long offset) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		if (!groupExists(groupId, groupType)) {
			throw new IllegalArgumentException("Group " + groupId + " with type " + groupType + " does not exist");
		}

		boolean hasSearchTerm = searchTerm != null && !(searchTerm = searchTerm.trim()).isEmpty();

		String groupEnginePermission = "GROUPENGINEPERMISSION__";
		String enginePrefix = "ENGINE__";

		SelectQueryStruct qs = new SelectQueryStruct();
		// engine selectors
		qs.addSelector(new QueryColumnSelector(enginePrefix + "ENGINEID", "engine_id"));
		qs.addSelector(new QueryColumnSelector(enginePrefix + "ENGINENAME", "engine_name"));
		qs.addSelector(new QueryColumnSelector(enginePrefix + "ENGINETYPE", "engine_type"));
		qs.addSelector(new QueryColumnSelector(enginePrefix + "ENGINESUBTYPE", "engine_subtype"));
		qs.addSelector(new QueryColumnSelector(enginePrefix + "COST", "engine_cost"));
		qs.addSelector(new QueryColumnSelector(enginePrefix + "DISCOVERABLE", "engine_discoverable"));
		qs.addSelector(new QueryColumnSelector(enginePrefix + "GLOBAL", "engine_global"));
		qs.addSelector(new QueryColumnSelector(enginePrefix + "CREATEDBY", "engine_created_by"));
		qs.addSelector(new QueryColumnSelector(enginePrefix + "CREATEDBYTYPE", "engine_created_by_type"));
		qs.addSelector(new QueryColumnSelector(enginePrefix + "DATECREATED", "engine_date_created"));
		// back to the others
		qs.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.LOWER,
				enginePrefix + "ENGINENAME", "low_engine_name"));

		if (hasSearchTerm) {
			OrQueryFilter searchFilter = new OrQueryFilter();
			searchFilter
					.addFilter(securityDb.getQueryUtil().getSearchRegexFilter(enginePrefix + "ENGINEID", searchTerm));
			searchFilter
					.addFilter(securityDb.getQueryUtil().getSearchRegexFilter(enginePrefix + "ENGINENAME", searchTerm));
			qs.addExplicitFilter(searchFilter);
		}

		// filter out engines that are already added
		{
			SelectQueryStruct subQs = new SelectQueryStruct();
			subQs.addSelector(new QueryColumnSelector(groupEnginePermission + "ENGINEID")); // this is the group id
			// filter for the group being specified
			subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(groupEnginePermission + "ID", "==", groupId));
			subQs.addExplicitFilter(
					SimpleQueryFilter.makeColToValFilter(groupEnginePermission + "TYPE", "==", groupType));
			// filter out from engine list
			qs.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery(enginePrefix + "ENGINEID", "!=", subQs));
		}

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

	/**
	 * 
	 * @param accessToken
	 * @return
	 */
	public static List<String> getUserCustomGroups(AccessToken accessToken) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		List<String> groups = new ArrayList<>();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("CUSTOMGROUPASSIGNMENT__GROUPID"));
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("CUSTOMGROUPASSIGNMENT__TYPE", "==", accessToken.getProvider()));
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("CUSTOMGROUPASSIGNMENT__USERID", "==", accessToken.getId()));
		qs.addExplicitFilter(getUnexpiredFilter("CUSTOMGROUPASSIGNMENT__ENDDATE"));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			while (wrapper.hasNext()) {
				Object[] values = wrapper.next().getValues();
				if (values != null) {
					groups.add((String) values[0]);
				}
			}
		} catch (Exception e) {
			classLogger.error("Unable to retrieve user custom groups.", e);
		}
		return groups;
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
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("CUSTOMGROUPASSIGNMENT__GROUPID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("CUSTOMGROUPASSIGNMENT__GROUPID", "==", groupId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("CUSTOMGROUPASSIGNMENT__USERID", "==", userId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("CUSTOMGROUPASSIGNMENT__TYPE", "==", userType));

		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (wrapper.hasNext()) {
				return true;
			}
		} catch (Exception e) {
			classLogger.error("Unable to verify whether the user belongs to the custom group.", e);
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
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_GROUP__ID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_GROUP__ID", "==", groupId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_GROUP__TYPE", "==", groupType));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (wrapper.hasNext()) {
				return true;
			}
		} catch (Exception e) {
			classLogger.error("Unable to verify whether the group exists.", e);
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
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_GROUP__ID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_GROUP__ID", "==", groupId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_GROUP__TYPE", "==", "CUSTOM"));

		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (wrapper.hasNext()) {
				return true;
			}
		} catch (Exception e) {
			classLogger.error("Unable to verify whether the group is custom.", e);
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
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__ID"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__TYPE"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__ID", "==", userId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__TYPE", "==", userType));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (wrapper.hasNext()) {
				return true;
			}
		} catch (Exception e) {
			classLogger.error("Unable to verify whether the user exists.", e);
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
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("GROUPPROJECTPERMISSION__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__ID", "==", groupId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__TYPE", "==", groupType));
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("GROUPPROJECTPERMISSION__PROJECTID", "==", projectId));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (wrapper.hasNext()) {
				return ((Number) wrapper.next().getValues()[0]).intValue();
			}
		} catch (Exception e) {
			classLogger.error("Unable to retrieve the group permission for the project.", e);
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
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("GROUPENGINEPERMISSION__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPENGINEPERMISSION__ID", "==", groupId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPENGINEPERMISSION__TYPE", "==", groupType));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GROUPENGINEPERMISSION__ENGINEID", "==", engineId));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (wrapper.hasNext()) {
				return ((Number) wrapper.next().getValues()[0]).intValue();
			}
		} catch (Exception e) {
			classLogger.error("Unable to retrieve the group permission for the engine.", e);
		}

		return -1;
	}

}
