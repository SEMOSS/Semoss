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
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessPermissionEnum;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.date.SemossDate;
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
import prerna.util.ConnectionUtils;
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;

class SecurityUserInsightUtils extends AbstractSecurityUtils {

	private static final Logger classLogger = LogManager.getLogger(SecurityUserInsightUtils.class);

	/**
	 * Get what permission the user has for a given insight
	 * 
	 * @param userId
	 * @param projectId
	 * @param insightId
	 * @return
	 */
	public static String getActualUserInsightPermission(User user, String projectId, String insightId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		Collection<String> userIds = getUserFiltersQs(user);

		// if user is owner
		// they can do whatever they want
		if (SecurityUserProjectUtils.userIsOwner(userIds, projectId)) {
			return AccessPermissionEnum.OWNER.getPermission();
		}

//		// query the database
//		String query = "SELECT DISTINCT USERINSIGHTPERMISSION.PERMISSION FROM USERINSIGHTPERMISSION  "
//				+ "WHERE ENGINEID='" + engineId + "' AND INSIGHTID='" + insightId + "' AND USERID IN " + userFilters;
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("USERINSIGHTPERMISSION__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__INSIGHTID", "==", insightId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__USERID", "==", userIds));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (wrapper.hasNext()) {
				Object val = wrapper.next().getValues()[0];
				if (val != null && val instanceof Number) {
					return AccessPermissionEnum.getPermissionValueById(((Number) val).intValue());
				}
			}
		} catch (Exception e) {
			classLogger.error("Unable to retrieve the user's effective insight permission.", e);
		}

		if (SecurityInsightUtils.insightIsGlobal(projectId, insightId)) {
			return AccessPermissionEnum.READ_ONLY.getPermission();
		}

		return null;
	}

	/**
	 * Determine if the user can edit the insight User must be database owner OR be
	 * given explicit permissions on the insight
	 * 
	 * @param userId
	 * @param projectId
	 * @param insightId
	 * @return
	 */
	public static boolean userCanViewInsight(User user, String projectId, String insightId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		// Check to see if permission has expired
		try {
			boolean isExpired = insightPermissionIsExpired(User.getSingleLogginName(user), projectId, insightId);
			// If permission is expired remove permission
			if (isExpired) {
				SecurityInsightUtils.removeExpiredInsightUser(User.getSingleLogginName(user), projectId, insightId);
			}
		} catch (Exception e) {
			classLogger.error("Unable to verify whether the user can view insight.", e);
		}

		Collection<String> userIds = getUserFiltersQs(user);
		// else query the database
//		String query = "SELECT DISTINCT USERINSIGHTPERMISSION.PERMISSION FROM USERINSIGHTPERMISSION  "
//				+ "WHERE ENGINEID='" + engineId + "' AND INSIGHTID='" + insightId + "' AND USERID IN " + userFilters;
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("USERINSIGHTPERMISSION__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__INSIGHTID", "==", insightId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__USERID", "==", userIds));

		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (wrapper.hasNext()) {
				// do not care if owner/edit/read
				return true;
			}
		} catch (Exception e) {
			classLogger.error("Unable to verify whether the user can view insight.", e);
		}

		return false;
	}

	/**
	 * Determine if the user can edit the insight User must be database owner OR be
	 * given explicit permissions on the insight
	 * 
	 * @param userId
	 * @param projectId
	 * @param insightId
	 * @return
	 */
	public static boolean userCanEditInsight(User user, String projectId, String insightId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		Collection<String> userIds = getUserFiltersQs(user);

		// Check to see if permission has expired
		try {
			boolean isExpired = insightPermissionIsExpired(User.getSingleLogginName(user), projectId, insightId);
			// If permission is expired remove permission
			if (isExpired) {
				SecurityInsightUtils.removeExpiredInsightUser(User.getSingleLogginName(user), projectId, insightId);
			}
		} catch (Exception e) {
			classLogger.error("Unable to verify whether the user can edit insight.", e);
		}
		// else query the database
//		String query = "SELECT DISTINCT USERINSIGHTPERMISSION.PERMISSION FROM USERINSIGHTPERMISSION "
//				+ "WHERE ENGINEID='" + engineId + "' AND INSIGHTID='" + insightId + "' AND USERID IN " + userFilters;
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("USERINSIGHTPERMISSION__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__INSIGHTID", "==", insightId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__USERID", "==", userIds));

		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			while (wrapper.hasNext()) {
				Object val = wrapper.next().getValues()[0];
				if (val == null) {
					return false;
				}
				int permission = ((Number) val).intValue();
				if (AccessPermissionEnum.isEditor(permission)) {
					return true;
				}
			}
		} catch (Exception e) {
			classLogger.error("Unable to verify whether the user can edit insight.", e);
		}
		return false;
	}

	/**
	 * Determine if the user is an owner of an insight User must be database owner
	 * OR be given explicit permissions on the insight
	 * 
	 * @param userId
	 * @param projectId
	 * @param insightId
	 * @return
	 */
	public static boolean userIsInsightOwner(User user, String projectId, String insightId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		// Check to see if permission has expired
		try {
			boolean isExpired = insightPermissionIsExpired(User.getSingleLogginName(user), projectId, insightId);
			// If permission is expired remove permission
			if (isExpired) {
				SecurityInsightUtils.removeExpiredInsightUser(User.getSingleLogginName(user), projectId, insightId);
			}
		} catch (Exception e) {
			classLogger.error("Unable to verify whether the user owns the insight.", e);
		}

		Collection<String> userIds = getUserFiltersQs(user);
		// else query the database
//		String query = "SELECT DISTINCT USERINSIGHTPERMISSION.PERMISSION FROM USERINSIGHTPERMISSION "
//				+ "WHERE ENGINEID='" + engineId + "' AND INSIGHTID='" + insightId + "' AND USERID IN " + userFilters;
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("USERINSIGHTPERMISSION__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__INSIGHTID", "==", insightId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__USERID", "==", userIds));

		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			while (wrapper.hasNext()) {
				Object val = wrapper.next().getValues()[0];
				if (val == null) {
					return false;
				}
				int permission = ((Number) val).intValue();
				if (AccessPermissionEnum.isOwner(permission)) {
					return true;
				}
			}
		} catch (Exception e) {
			classLogger.error("Unable to verify whether the user owns the insight.", e);
		}
		return false;
	}

	/**
	 * Determine if the user can edit the insight User must be database owner OR be
	 * given explicit permissions on the insight
	 * 
	 * @param userId
	 * @param projectId
	 * @param insightId
	 * @return
	 */
	static int getMaxUserInsightPermission(User user, String projectId, String insightId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		Collection<String> userIds = getUserFiltersQs(user);

		// if user is owner of the app
		// they can do whatever they want
		if (SecurityUserProjectUtils.userIsOwner(userIds, projectId)) {
			// owner of project is owner of all the insights
			return AccessPermissionEnum.OWNER.getId();
		}

		// else query the database
//		String query = "SELECT DISTINCT USERINSIGHTPERMISSION.PERMISSION FROM USERINSIGHTPERMISSION "
//				+ "WHERE ENGINEID='" + engineId + "' AND INSIGHTID='" + insightId + "' AND USERID IN " + userFilters + " ORDER BY PERMISSION";
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("USERINSIGHTPERMISSION__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__INSIGHTID", "==", insightId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__USERID", "==", userIds));
		qs.addOrderBy(new QueryColumnOrderBySelector("USERINSIGHTPERMISSION__PERMISSION"));

		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			while (wrapper.hasNext()) {
				Object val = wrapper.next().getValues()[0];
				if (val == null) {
					return AccessPermissionEnum.READ_ONLY.getId();
				}
				int permission = ((Number) val).intValue();
				return permission;
			}
		} catch (Exception e) {
			classLogger.error("Unable to retrieve the user's highest insight permission.", e);
		}
		return AccessPermissionEnum.READ_ONLY.getId();
	}

	/**
	 * Change the user favorite (is favorite / not favorite) for a database. Without
	 * removing its permissions.
	 * 
	 * @param user
	 * @param projectId
	 * @param visibility
	 * @throws SQLException
	 * @throws IllegalAccessException
	 */
	public static void setInsightFavorite(User user, String projectId, String insightId, boolean isFavorite)
			throws SQLException, IllegalAccessException {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		// must have ability to edit the project
		if (!SecurityProjectUtils.projectIsGlobal(projectId)
				&& !SecurityUserProjectUtils.userCanEditProject(user, projectId)
				&& !SecurityUserInsightUtils.userCanViewInsight(user, projectId, insightId)) {
			throw new IllegalAccessException("The user doesn't have the permission to modify this insight");
		}
		Collection<String> userIdFilters = getUserFiltersQs(user);
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("USERINSIGHTPERMISSION__PROJECTID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__INSIGHTID", "==", insightId));
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__USERID", "==", userIdFilters));

		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (wrapper.hasNext()) {
				PreparedStatement ps = securityDb.getPreparedStatement(
						"UPDATE USERINSIGHTPERMISSION SET FAVORITE=? WHERE PROJECTID=? AND INSIGHTID=? AND USERID=?");
				if (ps == null) {
					throw new IllegalArgumentException(
							"Error generating prepared statement to set user insight favorite");
				}
				try {
					// we will set the permission to read only
					for (AuthProvider loginType : user.getLogins()) {
						String userId = user.getAccessToken(loginType).getId();
						int parameterIndex = 1;
						ps.setBoolean(parameterIndex++, isFavorite);
						ps.setString(parameterIndex++, projectId);
						ps.setString(parameterIndex++, insightId);
						ps.setString(parameterIndex++, userId);
						ps.addBatch();
					}
					ps.executeBatch();
					if (!ps.getConnection().getAutoCommit()) {
						ps.getConnection().commit();
					}
				} catch (Exception e) {
					classLogger.error("Unable to retrieve the user's highest insight permission.", e);
					throw e;
				} finally {
					ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
				}
			} else {
				// need to insert
				PreparedStatement ps = securityDb.getPreparedStatement("INSERT INTO USERINSIGHTPERMISSION "
						+ "(USERID, PROJECTID, INSIGHTID, FAVORITE, PERMISSION) VALUES (?,?,?,?,?)");
				if (ps == null) {
					throw new IllegalArgumentException("Error generating prepared statement to set app visibility");
				}
				try {
					// we will set the permission to read only
					for (AuthProvider loginType : user.getLogins()) {
						String userId = user.getAccessToken(loginType).getId();
						int parameterIndex = 1;
						ps.setString(parameterIndex++, userId);
						ps.setString(parameterIndex++, projectId);
						ps.setString(parameterIndex++, insightId);
						ps.setBoolean(parameterIndex++, isFavorite);
						ps.setInt(parameterIndex++, 3);

						ps.addBatch();
					}
					ps.executeBatch();
					// commit the insertion
					ps.getConnection().commit();
				} catch (Exception e) {
					classLogger.error("Unable to retrieve the user's highest insight permission.", e);
					throw e;
				} finally {
					ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
				}
			}
		} catch (Exception e) {
			classLogger.error("Unable to retrieve the user's highest insight permission.", e);
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
	 * 
	 * @param user
	 * @param appId
	 * @param insightId
	 * @return
	 * @throws IllegalAccessException
	 */
	public static List<Map<String, Object>> getInsightUsers(User user, String projectId, String insightId,
			String searchTerm, String permission, long limit, long offset) throws IllegalAccessException {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		if (!SecurityInsightUtils.userCanViewInsight(user, projectId, insightId)) {
			throw new IllegalAccessException("The user does not have access to view this insight");
		}
		boolean hasSearchId = searchTerm != null && !(searchTerm = searchTerm.trim()).isEmpty();
		boolean hasPermission = permission != null && !(permission = permission.trim()).isEmpty();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__ID", "id"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__NAME", "name"));
		qs.addSelector(new QueryColumnSelector("PERMISSION__NAME", "permission"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__EMAIL", "email"));
		if (hasSearchId) {
			OrQueryFilter or = new OrQueryFilter();
			or.addFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__USERID", "?like", searchTerm));
			or.addFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__NAME", "?like", searchTerm));
			or.addFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__USERNAME", "?like", searchTerm));
			or.addFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__EMAIL", "?like", searchTerm));
			qs.addExplicitFilter(or);
		}
		if (hasPermission) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__PERMISSION", "==",
					AccessPermissionEnum.getIdByPermission(permission)));
		}
		qs.addRelation("SMSS_USER", "USERINSIGHTPERMISSION", "inner.join");
		qs.addRelation("USERINSIGHTPERMISSION", "PERMISSION", "inner.join");
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__INSIGHTID", "==", insightId));
		qs.addOrderBy(new QueryColumnOrderBySelector("PERMISSION__ID"));
		qs.addOrderBy(new QueryColumnOrderBySelector("SMSS_USER__ID"));
		if (limit > 0) {
			qs.setLimit(limit);
		}
		if (offset > 0) {
			qs.setOffSet(offset);
		}
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}

	/**
	 * 
	 * @param user
	 * @param projectId
	 * @param insightId
	 * @param userId
	 * @param permission
	 * @return
	 * @throws IllegalAccessException
	 */
	public static long getInsightUsersCount(User user, String projectId, String insightId, String userId,
			String permission) throws IllegalAccessException {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		if (!SecurityInsightUtils.userCanViewInsight(user, projectId, insightId)) {
			throw new IllegalAccessException("The user does not have access to view this insight");
		}
		boolean hasUserId = userId != null && !(userId = userId.trim()).isEmpty();
		boolean hasPermission = permission != null && !(permission = permission.trim()).isEmpty();
		SelectQueryStruct qs = new SelectQueryStruct();
		QueryFunctionSelector fSelector = new QueryFunctionSelector();
		fSelector.setAlias("count");
		fSelector.setFunction(QueryFunctionHelper.COUNT);
		fSelector.addInnerSelector(new QueryColumnSelector("SMSS_USER__ID"));
		qs.addSelector(fSelector);
		if (hasUserId) {
			qs.addExplicitFilter(
					SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__USERID", "?like", userId));
		}
		if (hasPermission) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__PERMISSION", "==",
					AccessPermissionEnum.getIdByPermission(permission)));
		}
		qs.addRelation("SMSS_USER", "USERINSIGHTPERMISSION", "inner.join");
		qs.addRelation("USERINSIGHTPERMISSION", "PERMISSION", "inner.join");
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__INSIGHTID", "==", insightId));
		return QueryExecutionUtility.flushToLong(securityDb, qs);
	}

	/**
	 * 
	 * @param projectId
	 * @param insightId
	 */
	public static void deleteInsight(String projectId, String insightId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String[] deleteQueries = new String[] { "DELETE FROM INSIGHT WHERE INSIGHTID =? AND PROJECTID=?",
				"DELETE FROM USERINSIGHTPERMISSION WHERE INSIGHTID =? AND PROJECTID=?",
				"DELETE FROM INSIGHTMETA WHERE INSIGHTID =? AND PROJECTID=?",
				"DELETE FROM INSIGHTFRAMES WHERE INSIGHTID =? AND PROJECTID=?", };

		for (String dQuery : deleteQueries) {
			PreparedStatement ps = null;
			try {
				ps = securityDb.getPreparedStatement(dQuery);
				int parameterIndex = 1;
				ps.setString(parameterIndex++, insightId);
				ps.setString(parameterIndex++, projectId);
				ps.execute();
				if (!ps.getConnection().getAutoCommit()) {
					ps.getConnection().commit();
				}
			} catch (SQLException e) {
				classLogger.error("Unable to delete insight.", e);
			} finally {
				if (ps != null) {
					try {
						ps.close();
					} catch (SQLException e) {
						classLogger.error("Unable to delete insight.", e);
					}
				}
				if (securityDb.isConnectionPooling()) {
					try {
						if (ps != null) {
							ps.getConnection().close();
						}
					} catch (SQLException e) {
						classLogger.error("Unable to delete insight.", e);
					}
				}
			}

		}
	}

	/**
	 * 
	 * @param projectId
	 * @param insightId
	 */
	public static void deleteInsight(String projectId, String... insightId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String insightFilter = createFilter(insightId);
		String query = "DELETE FROM INSIGHT WHERE INSIGHTID " + insightFilter + " AND PROJECTID='" + projectId + "'";
		try {
			securityDb.insertData(query);
			securityDb.commit();
		} catch (Exception e) {
			classLogger.error("Unable to delete insight.", e);
		}
		query = "DELETE FROM USERINSIGHTPERMISSION WHERE INSIGHTID " + insightFilter + " AND PROJECTID='" + projectId
				+ "'";
		try {
			securityDb.insertData(query);
			securityDb.commit();
		} catch (Exception e) {
			classLogger.error("Unable to delete insight.", e);
		}
	}

	/**
	 * Check if permission to insight has expired
	 * 
	 * @param engineId
	 * @param userId
	 */
	public static boolean insightPermissionIsExpired(String userId, String projectId, String insightId)
			throws Exception {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		LocalDateTime currentTime = LocalDateTime.now();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("USERINSIGHTPERMISSION__ENDDATE"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__USERID", "==", userId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__INSIGHTID", "==", insightId));

		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (wrapper.hasNext()) {
				SemossDate endDate = (SemossDate) wrapper.next().getValues()[0];
				if (endDate == null) {
					return false;
				}
				LocalDateTime formattedEndDate = endDate.getLocalDateTime();
				return formattedEndDate.isBefore(currentTime);
			} else {
				return false;
			}
		} catch (Exception e) {
			classLogger.error("Unable to delete insight.", e);
			throw e;
		}
	}
}
