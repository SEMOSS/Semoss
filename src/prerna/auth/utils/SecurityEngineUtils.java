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

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;

import com.google.gson.Gson;

import prerna.auth.AccessPermissionEnum;
import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.SmssUtilities;
import prerna.notifications.NotificationDbUtils;
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
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.usertracking.UserAuditTrailUtils;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.EmailUtility;
import prerna.util.NotificationConstants;
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

public class SecurityEngineUtils extends AbstractSecurityUtils {

	private static final Logger classLogger = LogManager.getLogger(SecurityEngineUtils.class);

	private static String getAuditEngineTypeName(String engineId) {
		IEngine.CATALOG_TYPE engineType = getEngineType(engineId);
		return engineType == null ? "ENGINE" : engineType.name();
	}

	/**
	 * Add an entire database into the security db
	 * 
	 * @param engineId
	 * 
	 *                 PLEASE DEFINE GLOBAL
	 *                 {@link #addEngine(String, boolean, User)}
	 */
	@Deprecated
	public static void addEngine(String engineId, User user) {
		if (ignoreDatabase(engineId)) {
			// dont add local master or security db to security db
			return;
		}
		// default engine is not global
		addEngine(engineId, false, user);
	}

	/**
	 * Add an entire database into the security db
	 * 
	 * @param engineId
	 */
	public static void addEngine(String engineId, boolean global, User user) {
		if (ignoreDatabase(engineId)) {
			// dont add local master or security db to security db
			return;
		}
		String smssFile = DIHelper.getInstance().getEngineProperty(engineId + "_" + Constants.STORE) + "";
		Properties prop = Utility.loadProperties(smssFile);

		String engineName = prop.getProperty(Constants.ENGINE_ALIAS);
		if (engineName == null) {
			engineName = engineId;
		}

		String displayName = prop.getProperty(Constants.ENGINE_DISPLAY_NAME);
		if (displayName == null || displayName.trim().isEmpty()) {
			displayName = engineName;
		}

		Object[] typeAndCost = getEngineTypeAndSubTypeAndCost(prop);
		IEngine.CATALOG_TYPE engineType = (IEngine.CATALOG_TYPE) typeAndCost[0];
		boolean engineExists = containsEngineId(engineId);
		if (engineExists) {
			updateEngineTypeAndSubType(engineId, engineType, (String) typeAndCost[1]);
			classLogger.info("Security database already contains engine of type {} with unique id = {}", typeAndCost[0],
					Utility.cleanLogString(SmssUtilities.getUniqueName(prop)));
		} else {
			addEngine(engineId, engineName, displayName, engineType, (String) typeAndCost[1],
					(String) typeAndCost[2], global, user);
		}

		if (engineType == IEngine.CATALOG_TYPE.MODEL) {
			SecurityModelMetadataUtils.upsertModelMetadata(engineId, prop);
		}

		// TODO: need to see when we should be updating the database metadata
		// if(engineExists) {
		// // update database properties anyway ... in case global was shifted for
		// example
		// updateDatabase(databaseId, databaseName, typeAndCost[0], typeAndCost[1],
		// global);
		// }

		classLogger.info("Finished adding engine = {}", Utility.cleanLogString(engineId));
	}

	/**
	 * Utility method to get the engine type, subtype, and cost This returns
	 * ENGINETYPE as the enum IEngine.CATALOG_TYPE and not the String format it is
	 * stored in
	 * 
	 * @param prop
	 * @return
	 */
	public static Object[] getEngineTypeAndSubTypeAndCost(Properties smssProp) {
		IEngine.CATALOG_TYPE engineType = null;
		String engineSubType = null;
		String engineCost = "$";

		String rawType = smssProp.get(Constants.ENGINE_TYPE).toString();
		try {
			IEngine emptyClass = (IEngine) Class.forName(rawType).getConstructor().newInstance();
			engineType = emptyClass.getCatalogType();
			engineSubType = emptyClass.getCatalogSubType(smssProp);
		} catch (Exception e) {
			classLogger.warn("Unknown class name = {}", rawType);
		}

		return new Object[] { engineType, engineSubType, engineCost };
	}

	/**
	 * This returns ENGINETYPE as the enum IEngine.CATALOG_TYPE and not the String
	 * format it is stored in
	 * 
	 * @param engineId
	 * @return
	 */
	public static IEngine.CATALOG_TYPE getEngineType(String engineId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINETYPE"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINEID", "==", engineId));
		List<Object[]> results = QueryExecutionUtility.flushRsToListOfObjArray(securityDb, qs);
		if (results == null || results.isEmpty()) {
			throw new IllegalArgumentException("Could not find engine with id " + engineId);
		}
		Object[] result = results.get(0);
		return IEngine.CATALOG_TYPE.valueOf(result[0] + "");
	}

	/**
	 * This returns ENGINETYPE as the enum IEngine.CATALOG_TYPE and not the String
	 * format it is stored in
	 * 
	 * @param engineId
	 * @return
	 */
	public static Object[] getEngineTypeAndSubtype(String engineId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINETYPE"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINESUBTYPE"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINEID", "==", engineId));
		List<Object[]> results = QueryExecutionUtility.flushRsToListOfObjArray(securityDb, qs);
		if (results == null || results.isEmpty()) {
			throw new IllegalArgumentException("Could not find engine with id " + engineId);
		}
		Object[] result = results.get(0);
		result[0] = IEngine.CATALOG_TYPE.valueOf(result[0] + "");
		return results.get(0);
	}

	/**
	 * 
	 * @param engineId
	 * @param engineName
	 * @param engineType
	 * @param engineSubType
	 * @param engineCost
	 * @param global
	 * @param user
	 */
	public static void addEngine(String engineId, String engineName, IEngine.CATALOG_TYPE engineType,
			String engineSubType, String engineCost, boolean global, User user) {
		addEngine(engineId, engineName, engineName, engineType, engineSubType, engineCost, global, user);
	}

	public static void addEngine(String engineId, String engineName, String engineDisplayName,
			IEngine.CATALOG_TYPE engineType, String engineSubType, String engineCost, boolean global, User user) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String query = "INSERT INTO ENGINE (ENGINEID, ENGINENAME, ENGINETYPE, ENGINESUBTYPE, COST, GLOBAL, DISCOVERABLE, CREATEDBY, CREATEDBYTYPE, DATECREATED, ENGINEDISPLAYNAME) "
				+ "VALUES (?,?,?,?,?,?,?,?,?,?,?)";

		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			int parameterIndex = 1;
			ps.setString(parameterIndex++, engineId);
			if (engineName == null) {
				ps.setNull(parameterIndex++, java.sql.Types.VARCHAR);
			} else {
				ps.setString(parameterIndex++, engineName);
			}
			ps.setString(parameterIndex++, engineType.toString());
			if (engineSubType == null) {
				ps.setNull(parameterIndex++, java.sql.Types.VARCHAR);
			} else {
				ps.setString(parameterIndex++, engineSubType);
			}
			if (engineCost == null) {
				ps.setNull(parameterIndex++, java.sql.Types.VARCHAR);
			} else {
				ps.setString(parameterIndex++, engineCost);
			}
			ps.setBoolean(parameterIndex++, global);
			ps.setBoolean(parameterIndex++, false);
			if (user != null) {
				AuthProvider ap = user.getPrimaryLogin();
				AccessToken token = user.getAccessToken(ap);
				ps.setString(parameterIndex++, token.getId());
				ps.setString(parameterIndex++, ap.toString());
			} else {
				ps.setNull(parameterIndex++, java.sql.Types.VARCHAR);
				ps.setNull(parameterIndex++, java.sql.Types.VARCHAR);
			}
			ps.setTimestamp(parameterIndex++, Utility.getCurrentSqlTimestampUTC());
			if (engineDisplayName == null || engineDisplayName.trim().isEmpty()) {
				ps.setString(parameterIndex++, engineName);
			} else {
				ps.setString(parameterIndex++, engineDisplayName);
			}
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error("Failed to create the engine record in the security database", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	public static void updateEngineTypeAndSubType(String engineId, IEngine.CATALOG_TYPE engineType,
			String engineSubType) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String query = "UPDATE ENGINE SET ENGINETYPE=?, ENGINESUBTYPE=? WHERE ENGINEID=?";

		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			int parameterIndex = 1;
			ps.setString(parameterIndex++, engineType.toString());
			ps.setString(parameterIndex++, engineSubType);
			ps.setString(parameterIndex++, engineId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error("Failed to update engine type and subtype", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	public static void addEngineOwner(String engineId, String userId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		// make sure user doesn't already exist for this database
		if (getUserEnginePermission(userId, engineId) != null) {
			// that means there is already a value
			throw new IllegalArgumentException("This user already has access to this engine");
		}

		String query = "INSERT INTO ENGINEPERMISSION (USERID, PERMISSION, ENGINEID, VISIBILITY, DATEADDED) VALUES (?,?,?,?,?)";

		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			int parameterIndex = 1;
			ps.setString(parameterIndex++, userId);
			ps.setInt(parameterIndex++, AccessPermissionEnum.OWNER.getId());
			ps.setString(parameterIndex++, engineId);
			ps.setBoolean(parameterIndex++, true);
			ps.setTimestamp(parameterIndex++, Utility.getCurrentSqlTimestampUTC());
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error("Failed to add engine owner", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * Get the database alias for a id
	 * 
	 * @return
	 */
	public static String getEngineAliasForId(String id) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		// String query = "SELECT ENGINENAME FROM ENGINE WHERE ENGINEID='" + id + "'";
		// IRawSelectWrapper wrapper =
		// WrapperManager.getInstance().getRawWrapper(securityDb, query);

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
	 * Get the database alias for a id
	 * 
	 * @return
	 */
	public static Map<Object, Object> getEngineAliasForIds(Collection<String> ids) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINENAME"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINEID", "==", ids));
		return QueryExecutionUtility.flushRsToKeyValueMap(securityDb, qs);
	}

	/**
	 * Get the display name for an engine id. Falls back to canonical name
	 * (ENGINENAME) if display name is null or blank.
	 *
	 * @param id
	 * @return
	 */
	public static String getEngineDisplayNameForId(String id) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEDISPLAYNAME"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINENAME"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINEID", "==", id));
		List<Object[]> results = QueryExecutionUtility.flushRsToListOfObjArray(securityDb, qs);
		if (results.isEmpty()) {
			return null;
		}
		Object[] row = results.get(0);
		String displayName = row[0] != null ? row[0].toString() : null;
		String canonicalName = row[1] != null ? row[1].toString() : null;
		if (displayName == null || displayName.trim().isEmpty()) {
			return canonicalName;
		}
		return displayName;
	}

	/**
	 * Set the display name for an engine. Only the engine owner can perform this
	 * action.
	 *
	 * @param user
	 * @param engineId
	 * @param newDisplayName
	 * @return
	 * @throws IllegalAccessException
	 */
	public static boolean setEngineDisplayName(User user, String engineId, String newDisplayName)
			throws IllegalAccessException {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		if (!SecurityUserEngineUtils.userIsOwner(user, engineId)) {
			throw new IllegalAccessException(
					"The user doesn't have the permission to change the engine display name. Only the owner can perform this action.");
		}
		if (newDisplayName == null || newDisplayName.trim().isEmpty()) {
			throw new IllegalArgumentException("Display name cannot be null or blank.");
		}

		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement("UPDATE ENGINE SET ENGINEDISPLAYNAME=? WHERE ENGINEID=?");
			int parameterIndex = 1;
			ps.setString(parameterIndex++, newDisplayName);
			ps.setString(parameterIndex++, engineId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to update engine display name", e);
			throw new IllegalArgumentException("An error occurred updating the engine display name");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}

		UserAuditTrailUtils.recordEngineLifecycle(user, "ENGINE_UPDATE", getAuditEngineTypeName(engineId), engineId,
				newDisplayName, Map.of("field", "displayName", "displayName", newDisplayName));
		return true;
	}

	/**
	 * Get what permission the user has for a given engine
	 * 
	 * @param userId
	 * @param engineId
	 * @return
	 */
	public static String getActualUserEnginePermission(User user, String engineId) {
		String userPermission = SecurityUserEngineUtils.getActualUserEnginePermission(user, engineId);
		List<String> groupUserPermissions = SecurityUserEngineUtils.getActualGroupUserEnginePermission(user, engineId);
		return SecurityUserEngineUtils.getHighestEnginePermission(userPermission, groupUserPermissions);
	}

	/**
	 * 
	 * @return
	 */
	public static List<String> getAllEngineIds() {
		return getAllEngineIds(null);
	}

	/**
	 * Get a list of the database ids
	 * 
	 * @return
	 */
	public static List<String> getAllEngineIds(List<String> engineTypes) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		// String query = "SELECT DISTINCT ENGINEID FROM ENGINE";
		// IRawSelectWrapper wrapper =
		// WrapperManager.getInstance().getRawWrapper(securityDb, query);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID"));
		if (engineTypes != null && !engineTypes.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINETYPE", "==", engineTypes));
		}
		return QueryExecutionUtility.flushToListString(securityDb, qs);
	}

	/**
	 * Get markdown for a given engine
	 * 
	 * @param user
	 * @param engineId
	 * @return
	 */
	public static String getEngineMarkdown(User user, String engineId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINEMETA__METAVALUE"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEMETA__METAKEY", "==", Constants.MARKDOWN));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEMETA__ENGINEID", "==", engineId));
		{
			SelectQueryStruct qs1 = new SelectQueryStruct();
			qs1.addSelector(new QueryColumnSelector("ENGINE__ENGINEID"));
			{
				OrQueryFilter orFilter = new OrQueryFilter();
				orFilter.addFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__DISCOVERABLE", "==",
						Arrays.asList(true, null), PixelDataType.BOOLEAN));
				orFilter.addFilter(
						SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", getUserFiltersQs(user)));
				qs1.addExplicitFilter(orFilter);
			}
			qs1.addRelation("ENGINE", "ENGINEPERMISSION", "join");
			IRelation subQuery = new SubqueryRelationship(qs1, "ENGINE", "join",
					new String[] { "ENGINE__ENGINEID", "ENGINEMETA__ENGINEID", "=" });
			qs.addRelation(subQuery);
		}
		return QueryExecutionUtility.flushToString(securityDb, qs);
	}

	/**
	 * Get the engine permissions for a specific user
	 * 
	 * @param singleUserId
	 * @param engineId
	 * @return
	 */
	public static Integer getUserEnginePermission(String singleUserId, String engineId) {
		return SecurityUserEngineUtils.getUserEnginePermission(singleUserId, engineId);
	}

	/**
	 * Get the request pending database permission for a specific user
	 * 
	 * @param singleUserId
	 * @param databaseId
	 * @return
	 */
	public static Integer getUserAccessRequestEnginePermission(String userId, String databaseId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINEACCESSREQUEST__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEACCESSREQUEST__REQUEST_USERID", "==", userId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEACCESSREQUEST__ENGINEID", "==", databaseId));
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("ENGINEACCESSREQUEST__APPROVER_DECISION", "==", null));
		return QueryExecutionUtility.flushToInteger(securityDb, qs);
	}

	/**
	 * Approving user access requests and giving user access in permissions
	 * 
	 * @param userId
	 * @param userType
	 * @param engineId
	 * @param requests
	 */
	public static void approveEngineUserAccessRequests(User user, String engineId, List<Map<String, String>> requests,
			String endDate) throws IllegalAccessException {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		// make sure user has right permission level to approve access requests
		int userPermissionLvl = getMaxUserEnginePermission(user, engineId);
		if (!AccessPermissionEnum.isEditor(userPermissionLvl)) {
			throw new IllegalAccessException("Insufficient privileges to modify this engine's permissions.");
		}

		// get user permissions of all requests
		List<String> permissions = new ArrayList<String>();
		for (Map<String, String> i : requests) {
			permissions.add(i.get("permission"));
		}

		// if user is not an owner, check to make sure they cannot grant owner access
		if (!AccessPermissionEnum.isEditor(userPermissionLvl)) {
			throw new IllegalArgumentException("You cannot grant user access to others.");
		} else {
			if (!AccessPermissionEnum.isOwner(userPermissionLvl) && permissions.contains("OWNER")) {
				throw new IllegalArgumentException("As a non-owner, you cannot grant owner access.");
			}
		}

		Timestamp startDate = Utility.getCurrentSqlTimestampUTC();
		Timestamp verifiedEndDate = null;
		if (endDate != null) {
			verifiedEndDate = AbstractSecurityUtils.calculateEndDate(endDate);
		}

		// bulk delete
		String deleteQ = "DELETE FROM ENGINEPERMISSION WHERE USERID=? AND ENGINEID=?";
		PreparedStatement deletePs = null;
		try {
			deletePs = securityDb.getPreparedStatement(deleteQ);
			for (int i = 0; i < requests.size(); i++) {
				int parameterIndex = 1;
				deletePs.setString(parameterIndex++, requests.get(i).get("userid"));
				deletePs.setString(parameterIndex++, engineId);
				deletePs.addBatch();
			}
			deletePs.executeBatch();
			if (!deletePs.getConnection().getAutoCommit()) {
				deletePs.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to approve engine user access requests", e);
			throw new IllegalArgumentException(
					"An error occurred while deleting enginepermission with detailed message = " + e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, deletePs);
		}
		// insert new user permissions in bulk
		String insertQ = "INSERT INTO ENGINEPERMISSION (USERID, ENGINEID, PERMISSION, VISIBILITY, PERMISSIONGRANTEDBY, PERMISSIONGRANTEDBYTYPE, DATEADDED, ENDDATE) VALUES(?,?,?,?,?,?,?,?)";
		PreparedStatement insertPs = null;
		Pair<String, String> userDetails = User.getPrimaryUserIdAndTypePair(user);

		try {
			insertPs = securityDb.getPreparedStatement(insertQ);
			for (int i = 0; i < requests.size(); i++) {
				int parameterIndex = 1;
				insertPs.setString(parameterIndex++, requests.get(i).get("userid"));
				insertPs.setString(parameterIndex++, engineId);
				insertPs.setInt(parameterIndex++,
						AccessPermissionEnum.getIdByPermission(requests.get(i).get("permission")));
				insertPs.setBoolean(parameterIndex++, true);
				insertPs.setString(parameterIndex++, userDetails.getValue0());
				insertPs.setString(parameterIndex++, userDetails.getValue1());
				insertPs.setTimestamp(parameterIndex++, startDate);
				insertPs.setTimestamp(parameterIndex++, verifiedEndDate);
				insertPs.addBatch();
			}
			insertPs.executeBatch();
			if (!insertPs.getConnection().getAutoCommit()) {
				insertPs.getConnection().commit();
			}
			for (Map<String, String> request : requests) {
				UserAuditTrailUtils.recordPermissionAdd(user, "ENGINE", engineId, null, null, engineId, null,
						request.get("userid"), request.get("type"), request.get("permission"), request);
				UserAuditTrailUtils.recordAccessRequestDecision(user, "ACCESS_REQUEST_APPROVE", "ENGINE", engineId,
						null, null, engineId, null, request.get("requestid"), request.get("userid"),
						request.get("type"), request.get("permission"), null);
			}
		} catch (Exception e) {
			classLogger.error("Failed to approve engine user access requests", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, insertPs);
		}

		// now we do the new bulk update to engineaccessrequest table
		String updateQ = "UPDATE ENGINEACCESSREQUEST SET PERMISSION = ?, APPROVER_USERID = ?, APPROVER_TYPE = ?, APPROVER_DECISION = ?, APPROVER_TIMESTAMP = ? WHERE ID = ? AND ENGINEID = ?";
		PreparedStatement updatePs = null;
		try {
			java.sql.Timestamp timestamp = Utility.getCurrentSqlTimestampUTC();
			updatePs = securityDb.getPreparedStatement(updateQ);
			AccessToken token = user.getAccessToken(user.getPrimaryLogin());
			String userId = token.getId();
			String userType = token.getProvider().toString();
			for (int i = 0; i < requests.size(); i++) {
				int index = 1;
				// set
				updatePs.setInt(index++, AccessPermissionEnum.getIdByPermission(requests.get(i).get("permission")));
				updatePs.setString(index++, userId);
				updatePs.setString(index++, userType);
				updatePs.setString(index++, "APPROVED");
				updatePs.setTimestamp(index++, timestamp);
				// where
				updatePs.setString(index++, requests.get(i).get("requestid"));
				updatePs.setString(index++, engineId);
				updatePs.addBatch();
			}
			updatePs.executeBatch();
			if (!updatePs.getConnection().getAutoCommit()) {
				updatePs.getConnection().commit();
			}

			// Adding Notification
			if (Utility.isNotificationDatabaseEnabled()) {
				String engineType = String.valueOf(getEngineType(engineId)).toLowerCase();
				for (int i = 0; i < requests.size(); i++) {
					NotificationDbUtils.createNotification(user, requests.get(i).get("userid"),
							requests.get(i).get("type"), engineId, NotificationConstants.Type.REQUEST_APPROVAL,
							engineType, NotificationConstants.Priority.MEDIUM, null, requests.get(i).get("permission"));
					// Adding email notification
					EmailUtility.sendAccessRequestApprovalEmailNotification(user, requests.get(i).get("userid"),
							engineId, requests.get(i).get("permission"), EmailUtility.RESOURCE_TYPE.ENGINE);
				}
			}
		} catch (Exception e) {
			classLogger.error("Failed to approve engine user access requests", e);
			throw new IllegalArgumentException(
					"An error occurred while updating user access request detailed message = " + e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, updatePs);
		}
	}

	/**
	 * Denying user access requests to engine
	 * 
	 * @param userId
	 * @param userType
	 * @param engineId
	 * @param requests
	 */
	public static void denyEngineUserAccessRequests(User user, String engineId, List<String> requestIds)
			throws IllegalAccessException {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		// make sure user has right permission level to approve acces requests
		int userPermissionLvl = getMaxUserEnginePermission(user, engineId);
		if (!AccessPermissionEnum.isEditor(userPermissionLvl)) {
			throw new IllegalAccessException("Insufficient privileges to modify this engine's permissions.");
		}

		// only database owners can deny user access requests
		if (!AccessPermissionEnum.isOwner(userPermissionLvl)) {
			throw new IllegalArgumentException("Insufficient privileges to deny user access requests.");
		}

		// bulk update to engineaccessrequest table
		String updateQ = "UPDATE ENGINEACCESSREQUEST SET APPROVER_USERID = ?, APPROVER_TYPE = ?, APPROVER_DECISION = ?, APPROVER_TIMESTAMP = ? WHERE ID = ? AND ENGINEID = ?";
		PreparedStatement ps = null;
		try {
			java.sql.Timestamp timestamp = Utility.getCurrentSqlTimestampUTC();
			ps = securityDb.getPreparedStatement(updateQ);
			AccessToken token = user.getAccessToken(user.getPrimaryLogin());
			String userId = token.getId();
			String userType = token.getProvider().toString();
			for (int i = 0; i < requestIds.size(); i++) {
				int index = 1;
				// set
				ps.setString(index++, userId);
				ps.setString(index++, userType);
				ps.setString(index++, "DENIED");
				ps.setTimestamp(index++, timestamp);
				// where
				ps.setString(index++, requestIds.get(i));
				ps.setString(index++, engineId);
				ps.addBatch();
			}
			ps.executeBatch();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
			for (String requestId : requestIds) {
				UserAuditTrailUtils.recordAccessRequestDecision(user, "ACCESS_REQUEST_REJECT", "ENGINE", engineId,
						null, null, engineId, null, requestId, null, null, null, null);
			}

			// Adding Notification
			if (Utility.isNotificationDatabaseEnabled()) {
				String engineType = String.valueOf(getEngineType(engineId)).toLowerCase();
				for (int i = 0; i < requestIds.size(); i++) {
					String requestId = requestIds.get(i);
					List<Map<String, Object>> deniedUserDetails = getUserDetailsFromEngineAccessRequest(requestId);
					String permission = AccessPermissionEnum
							.getPermissionValueById((Integer) deniedUserDetails.get(i).get("permission"));
					NotificationDbUtils.createNotification(user, (String) deniedUserDetails.get(i).get("userId"),
							(String) deniedUserDetails.get(i).get("type"), engineId,
							NotificationConstants.Type.REQUEST_DENIAL, engineType,
							NotificationConstants.Priority.MEDIUM, null, permission);
				}
			}

		} catch (Exception e) {
			classLogger.error("Failed to deny engine user access requests", e);
			throw new IllegalArgumentException(
					"An error occurred while updating user access request detailed message = " + e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * Get the request pending engine permission for a specific user
	 * 
	 * @param singleUserId
	 * @param engineId
	 * @return
	 */
	public static List<Map<String, Object>> getUserAccessRequestsByEngine(String engineId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINEACCESSREQUEST__ID"));
		qs.addSelector(new QueryColumnSelector("ENGINEACCESSREQUEST__REQUEST_USERID"));
		qs.addSelector(new QueryColumnSelector("ENGINEACCESSREQUEST__REQUEST_TYPE"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__NAME"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__EMAIL"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__USERNAME"));
		qs.addSelector(new QueryColumnSelector("ENGINEACCESSREQUEST__REQUEST_TIMESTAMP"));
		qs.addSelector(new QueryColumnSelector("ENGINEACCESSREQUEST__ENGINEID"));
		qs.addSelector(new QueryColumnSelector("ENGINEACCESSREQUEST__PERMISSION"));
		qs.addSelector(new QueryColumnSelector("ENGINEACCESSREQUEST__APPROVER_USERID"));
		qs.addSelector(new QueryColumnSelector("ENGINEACCESSREQUEST__APPROVER_TYPE"));
		qs.addSelector(new QueryColumnSelector("ENGINEACCESSREQUEST__APPROVER_DECISION"));
		qs.addSelector(new QueryColumnSelector("ENGINEACCESSREQUEST__APPROVER_TIMESTAMP"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEACCESSREQUEST__ENGINEID", "==", engineId));
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("ENGINEACCESSREQUEST__APPROVER_DECISION", "==", "NEW_REQUEST"));
		qs.addRelation("ENGINEACCESSREQUEST__REQUEST_USERID", "SMSS_USER__ID", "inner.join");
		qs.addRelation("ENGINEACCESSREQUEST__REQUEST_TYPE", "SMSS_USER__TYPE", "inner.join");
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}

	/**
	 * Validate if the given engineId exists in the database.
	 * 
	 * @param engineId
	 * @return
	 */
	public static boolean engineExists(String engineId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINEID", "==", engineId));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (wrapper.hasNext()) {
				return true;
			}
		} catch (Exception e) {
			classLogger.error("Failed to check whether engine exists", e);
		}
		return false;
	}

	/**
	 * See if specific engine is global
	 * 
	 * @return
	 */
	public static boolean engineIsGlobal(String engineId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		// String query = "SELECT ENGINEID FROM ENGINE WHERE GLOBAL=TRUE and ENGINEID='"
		// + databaseId + "'";
		// IRawSelectWrapper wrapper =
		// WrapperManager.getInstance().getRawWrapper(securityDb, query);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__GLOBAL", "==", true, PixelDataType.BOOLEAN));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINEID", "==", engineId));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (wrapper.hasNext()) {
				return true;
			}
		} catch (Exception e) {
			classLogger.error("Failed to determine whether engine is global", e);
		}
		return false;
	}

	/**
	 * Determine if the user is the owner of an database
	 * 
	 * @param userFilters
	 * @param databaseId
	 * @return
	 */
	public static boolean userIsOwner(User user, String databaseId) {
		return SecurityUserEngineUtils.userIsOwner(getUserFiltersQs(user), databaseId)
				|| SecurityGroupEngineUtils.userGroupIsOwner(user, databaseId);
	}

	/**
	 * Determine if the user can modify the engine
	 * 
	 * @param databaseId
	 * @param userId
	 * @return
	 */
	public static boolean userCanEditEngine(User user, String databaseId) {
		return SecurityUserEngineUtils.userCanEditEngine(user, databaseId)
				|| SecurityGroupEngineUtils.userGroupCanEditEngine(user, databaseId);
	}

	/**
	 * Determine if a user can view a engine
	 * 
	 * @param user
	 * @param engineId
	 * @return
	 */
	public static boolean userCanViewEngine(User user, String engineId) {
		return SecurityUserEngineUtils.userCanViewEngine(user, engineId)
				|| SecurityGroupEngineUtils.userGroupCanViewEngine(user, engineId);
	}

	/**
	 * Determine if the user can edit the engine
	 * 
	 * @param userId
	 * @param engineId
	 * @return
	 */
	static int getMaxUserEnginePermission(User user, String engineId) {
		return SecurityUserEngineUtils.getMaxUserEnginePermission(user, engineId);
	}

	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////

	/*
	 * Query for database users
	 */

	/**
	 * 
	 * @param databaseId
	 * @return
	 */
	public static List<Map<String, Object>> getDisplayDatabaseOwnersAndEditors(String databaseId) {
		return SecurityUserEngineUtils.getDisplayEngineOwnersAndEditors(databaseId);
	}

	/**
	 * Retrieve the list of users for a given database
	 * 
	 * @param user
	 * @param engineId
	 * @param searchParam
	 * @param permission
	 * @param limit
	 * @param offset
	 * @return
	 * @throws IllegalAccessException
	 */
	public static List<Map<String, Object>> getEngineUsers(User user, String engineId, String searchParam,
			String permission, long limit, long offset) throws IllegalAccessException {
		if (!userCanViewEngine(user, engineId)) {
			throw new IllegalAccessException("The user does not have access to view this engine");
		}
		return SecurityUserEngineUtils.getEngineUsers(engineId, searchParam, permission, limit, offset);
	}

	/**
	 * 
	 * @param user
	 * @param engineId
	 * @param userId
	 * @param permission
	 * @return
	 * @throws IllegalAccessException
	 */
	public static long getEngineUsersCount(User user, String engineId, String searchParam, String permission)
			throws IllegalAccessException {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		if (!userCanViewEngine(user, engineId)) {
			throw new IllegalAccessException("The user does not have access to view this engine");
		}
		boolean hasSearchParam = searchParam != null && !(searchParam = searchParam.trim()).isEmpty();
		boolean hasPermission = permission != null && !(permission = permission.trim()).isEmpty();
		SelectQueryStruct qs = new SelectQueryStruct();
		QueryFunctionSelector fSelector = new QueryFunctionSelector();
		fSelector.setAlias("count");
		fSelector.setFunction(QueryFunctionHelper.COUNT);
		fSelector.addInnerSelector(new QueryColumnSelector("SMSS_USER__ID"));
		qs.addSelector(fSelector);
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__ENGINEID", "==", engineId));
		if (hasSearchParam) {
			OrQueryFilter or = new OrQueryFilter();
			or.addFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "?like", searchParam));
			or.addFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__NAME", "?like", searchParam));
			or.addFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__USERNAME", "?like", searchParam));
			or.addFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__EMAIL", "?like", searchParam));
			qs.addExplicitFilter(or);
		}
		if (hasPermission) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__PERMISSION", "==",
					AccessPermissionEnum.getIdByPermission(permission)));
		}
		qs.addRelation("SMSS_USER", "ENGINEPERMISSION", "inner.join");
		qs.addRelation("ENGINEPERMISSION", "PERMISSION", "inner.join");
		return QueryExecutionUtility.flushToLong(securityDb, qs);
	}

	/**
	 * 
	 * @param user
	 * @param newUserId
	 * @param engineId
	 * @param permission
	 * @param endDate
	 * @param usageRestriction
	 * @param usageFrequency
	 * @param maxTokens
	 * @param maxResponseTime
	 * @throws IllegalAccessException
	 */
	public static void addEngineUser(User user, String newUserId, String engineId, String permission, String endDate,
			String usageRestriction, String usageFrequency, int maxTokens, double maxResponseTime)
			throws IllegalAccessException {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		// make sure user can edit the database
		int userPermissionLvl = getMaxUserEnginePermission(user, engineId);
		if (!AccessPermissionEnum.isEditor(userPermissionLvl)) {
			throw new IllegalAccessException("Insufficient privileges to modify this engine's permissions.");
		}

		// make sure user doesn't already exist for this database
		if (getUserEnginePermission(newUserId, engineId) != null) {
			// that means there is already a value
			throw new IllegalArgumentException(
					"This user already has access to this engine. Please edit the existing permission level.");
		}

		// if i am not an owner
		// then i need to check if i can edit this users permission
		if (!AccessPermissionEnum.isOwner(userPermissionLvl)) {
			int newPermissionLvl = AccessPermissionEnum.getIdByPermission(permission);

			// cannot give some owner permission if i am just an editor
			if (AccessPermissionEnum.OWNER.getId() == newPermissionLvl) {
				throw new IllegalAccessException(
						"Cannot give owner level access to this engine since you are not currently an owner.");
			}
		}

		Timestamp startDate = Utility.getCurrentSqlTimestampUTC();
		Timestamp verifiedEndDate = null;
		if (endDate != null) {
			verifiedEndDate = AbstractSecurityUtils.calculateEndDate(endDate);
		}

		Pair<String, String> userDetails = User.getPrimaryUserIdAndTypePair(user);
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(
					"INSERT INTO ENGINEPERMISSION (USERID, ENGINEID, VISIBILITY, PERMISSION, PERMISSIONGRANTEDBY, PERMISSIONGRANTEDBYTYPE, DATEADDED, ENDDATE, USAGERESTRICTION, USAGEFREQUENCY, MAXTOKENS, MAXRESPONSETIME) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)");
			int parameterIndex = 1;
			ps.setString(parameterIndex++, newUserId);
			ps.setString(parameterIndex++, engineId);
			ps.setBoolean(parameterIndex++, true);
			ps.setInt(parameterIndex++, AccessPermissionEnum.getIdByPermission(permission));
			ps.setString(parameterIndex++, userDetails.getValue0());
			ps.setString(parameterIndex++, userDetails.getValue1());
			ps.setTimestamp(parameterIndex++, startDate);
			ps.setTimestamp(parameterIndex++, verifiedEndDate);
			if (usageRestriction == null || (usageRestriction = usageRestriction.trim()).isEmpty()) {
				ps.setNull(parameterIndex++, java.sql.Types.VARCHAR);
			} else {
				ps.setString(parameterIndex++, usageRestriction);
			}
			if (usageFrequency == null || (usageFrequency = usageFrequency.trim()).isEmpty()) {
				ps.setNull(parameterIndex++, java.sql.Types.VARCHAR);
			} else {
				ps.setString(parameterIndex++, usageFrequency);
			}
			if (maxTokens == 0) {
				ps.setNull(parameterIndex++, java.sql.Types.INTEGER);
			} else {
				ps.setInt(parameterIndex++, maxTokens);
			}
			if (maxResponseTime == 0.0) {
				ps.setNull(parameterIndex++, java.sql.Types.DOUBLE);
			} else {
				ps.setDouble(parameterIndex++, maxResponseTime);
			}
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
			UserAuditTrailUtils.recordPermissionAdd(user, "ENGINE", engineId, null, null, engineId, null, newUserId,
					null, permission, endDate == null ? null : Map.of("endDate", endDate));
		} catch (Exception e) {
			classLogger.error("Failed to add engine user", e);
			throw new IllegalArgumentException(
					"An error occurred adding the user permissions for this engine. Detailed error message = "
							+ e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * 
	 * @param user
	 * @param engineId
	 * @param permission
	 * @throws IllegalAccessException
	 */
	public static void addEngineUserPermissions(User user, String engineId, List<Map<String, Object>> permission)
			throws IllegalAccessException {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		// make sure user can edit the database
		int userPermissionLvl = getMaxUserEnginePermission(user, engineId);
		if (!AccessPermissionEnum.isEditor(userPermissionLvl)) {
			throw new IllegalAccessException("Insufficient privileges to modify this engine's permissions.");
		}

		// check to make sure these users do not already have permissions to database
		// get list of userids from permission list map
		List<String> userIds = permission.stream().map(map -> (String) map.get("userid")).collect(Collectors.toList());
		// this returns a list of existing permissions
		Map<String, Integer> existingUserPermission = SecurityUserEngineUtils.getUserEnginePermissions(userIds,
				engineId);
		if (!existingUserPermission.isEmpty()) {
			throw new IllegalArgumentException(
					"The following users already have access to this engine. Please edit the existing permission level: "
							+ String.join(",", existingUserPermission.keySet()));
		}

		// if user is not an owner, check to make sure they are not adding owner access
		if (!AccessPermissionEnum.isOwner(userPermissionLvl)) {
			List<String> permissionList = permission.stream().map(map -> (String) map.get("permission"))
					.collect(Collectors.toList());
			if (permissionList.contains("OWNER")) {
				throw new IllegalArgumentException("As a non-owner, you cannot add owner user access.");
			}
		}
		Pair<String, String> userDetails = User.getPrimaryUserIdAndTypePair(user);

		Timestamp startDate = Utility.getCurrentSqlTimestampUTC();

		// insert new user permissions in bulk
		String insertQ = "INSERT INTO ENGINEPERMISSION (USERID, ENGINEID, PERMISSION, VISIBILITY, PERMISSIONGRANTEDBY, PERMISSIONGRANTEDBYTYPE, DATEADDED, ENDDATE, USAGERESTRICTION, USAGEFREQUENCY, MAXTOKENS, MAXRESPONSETIME) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(insertQ);
			for (int i = 0; i < permission.size(); i++) {
				Map<String, Object> thisPermissionMap = permission.get(i);

				int parameterIndex = 1;
				ps.setString(parameterIndex++, (String) thisPermissionMap.get("userid"));
				ps.setString(parameterIndex++, engineId);
				ps.setInt(parameterIndex++,
						AccessPermissionEnum.getIdByPermission((String) thisPermissionMap.get("permission")));
				ps.setBoolean(parameterIndex++, true);
				ps.setString(parameterIndex++, userDetails.getValue0());
				ps.setString(parameterIndex++, userDetails.getValue1());
				ps.setTimestamp(parameterIndex++, startDate);
				// end date for this user
				Timestamp verifiedEndDate = null;
				if (thisPermissionMap.get("endDate") != null) {
					verifiedEndDate = AbstractSecurityUtils.calculateEndDate((String) thisPermissionMap.get("endDate"));
					ps.setTimestamp(parameterIndex++, verifiedEndDate);
				} else {
					ps.setNull(parameterIndex++, java.sql.Types.TIMESTAMP);
				}

				// engine usage restrictions
				if (thisPermissionMap.get("usageRestriction") != null
						&& !((String) thisPermissionMap.get("usageRestriction")).trim().isEmpty()) {
					ps.setString(parameterIndex++, ((String) thisPermissionMap.get("usageRestriction")).trim());
				} else {
					ps.setNull(parameterIndex++, java.sql.Types.VARCHAR);
				}
				if (thisPermissionMap.get("usageFrequency") != null
						&& !((String) thisPermissionMap.get("usageFrequency")).trim().isEmpty()) {
					ps.setString(parameterIndex++, ((String) thisPermissionMap.get("usageFrequency")).trim());
				} else {
					ps.setNull(parameterIndex++, java.sql.Types.VARCHAR);
				}
				if (thisPermissionMap.get("maxTokens") != null) {
					ps.setInt(parameterIndex++, ((Number) thisPermissionMap.get("maxTokens")).intValue());
				} else {
					ps.setNull(parameterIndex++, java.sql.Types.INTEGER);
				}
				if (thisPermissionMap.get("maxResponseTime") != null) {
					ps.setDouble(parameterIndex++, ((Number) thisPermissionMap.get("maxResponseTime")).doubleValue());
				} else {
					ps.setNull(parameterIndex++, java.sql.Types.DOUBLE);
				}

				ps.addBatch();
			}
			ps.executeBatch();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
			for (Map<String, Object> permissionRow : permission) {
				UserAuditTrailUtils.recordPermissionAdd(user, "ENGINE", engineId, null, null, engineId, null,
						(String) permissionRow.get("userid"), (String) permissionRow.get("type"),
						(String) permissionRow.get("permission"), permissionRow);
			}

			// Adding Notification
			if (Utility.isNotificationDatabaseEnabled()) {
				String engineType = String.valueOf(getEngineType(engineId)).toLowerCase();
				for (int i = 0; i < permission.size(); i++) {
					NotificationDbUtils.createNotification(user, (String) permission.get(i).get("userid"),
							(String) permission.get(i).get("type"), engineId, NotificationConstants.Type.USER_ADDITION,
							engineType, NotificationConstants.Priority.MEDIUM, null,
							(String) permission.get(i).get("permission"));
				}
			}

		} catch (Exception e) {
			classLogger.error("Failed to add engine user permissions", e);
			throw new IllegalArgumentException(
					"An error occurred adding the user permissions for this engine. Detailed error message = "
							+ e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * 
	 * @param user
	 * @param existingUserId
	 * @param engineId
	 * @param newPermission
	 * @param endDate
	 * @param usageRestriction
	 * @param usageFrequency
	 * @param maxTokens
	 * @param maxResponseTime
	 * @throws IllegalAccessException
	 */
	public static void editEngineUserPermission(User user, String existingUserId, String existingUserType,
			String engineId, String newPermission, String endDate, String usageRestriction, String usageFrequency,
			int maxTokens, double maxResponseTime) throws IllegalAccessException {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		// make sure user can edit the database
		int userPermissionLvl = getMaxUserEnginePermission(user, engineId);
		if (!AccessPermissionEnum.isEditor(userPermissionLvl)) {
			throw new IllegalAccessException("Insufficient privileges to modify this engine's permissions.");
		}

		// make sure we are trying to edit a permission that exists
		Integer existingUserPermission = getUserEnginePermission(existingUserId, engineId);
		if (existingUserPermission == null) {
			throw new IllegalArgumentException(
					"Attempting to modify engine permission for a user who does not currently have access to the engine");
		}

		int newPermissionLvl = AccessPermissionEnum.getIdByPermission(newPermission);

		// if i am not an owner
		// then i need to check if i can edit this users permission
		if (!AccessPermissionEnum.isOwner(userPermissionLvl)) {
			// not an owner, check if trying to edit an owner or an editor/reader
			// get the current permission
			if (AccessPermissionEnum.OWNER.getId() == existingUserPermission) {
				throw new IllegalAccessException(
						"The user doesn't have the high enough permissions to modify this users engine permission.");
			}

			// also, cannot give some owner permission if i am just an editor
			if (AccessPermissionEnum.OWNER.getId() == newPermissionLvl) {
				throw new IllegalAccessException(
						"Cannot give owner level access to this engine since you are not currently an owner.");
			}
		}

		Timestamp startDate = Utility.getCurrentSqlTimestampUTC();
		Timestamp verifiedEndDate = null;
		if (endDate != null) {
			verifiedEndDate = AbstractSecurityUtils.calculateEndDate(endDate);
		}

		Pair<String, String> userDetails = User.getPrimaryUserIdAndTypePair(user);
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(
					"UPDATE ENGINEPERMISSION SET PERMISSION=?, PERMISSIONGRANTEDBY=?, PERMISSIONGRANTEDBYTYPE=?, DATEADDED=?, ENDDATE=?, USAGERESTRICTION=?, USAGEFREQUENCY=?, MAXTOKENS=?, MAXRESPONSETIME=? WHERE USERID=? AND ENGINEID=?");
			int parameterIndex = 1;
			// SET
			ps.setInt(parameterIndex++, newPermissionLvl);
			ps.setString(parameterIndex++, userDetails.getValue0());
			ps.setString(parameterIndex++, userDetails.getValue1());
			ps.setTimestamp(parameterIndex++, startDate);
			ps.setTimestamp(parameterIndex++, verifiedEndDate);
			if (usageRestriction == null || (usageRestriction = usageRestriction.trim()).isEmpty()) {
				ps.setNull(parameterIndex++, java.sql.Types.VARCHAR);
			} else {
				ps.setString(parameterIndex++, usageRestriction);
			}
			if (usageFrequency == null || (usageFrequency = usageFrequency.trim()).isEmpty()) {
				ps.setNull(parameterIndex++, java.sql.Types.VARCHAR);
			} else {
				ps.setString(parameterIndex++, usageFrequency);
			}
			if (maxTokens == 0) {
				ps.setNull(parameterIndex++, java.sql.Types.INTEGER);
			} else {
				ps.setInt(parameterIndex++, maxTokens);
			}
			if (maxResponseTime == 0.0) {
				ps.setNull(parameterIndex++, java.sql.Types.DOUBLE);
			} else {
				ps.setDouble(parameterIndex++, maxResponseTime);
			}

			// WHERE
			ps.setString(parameterIndex++, existingUserId);
			ps.setString(parameterIndex++, engineId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
			UserAuditTrailUtils.recordPermissionUpdate(user, "ENGINE", engineId, null, null, engineId, null,
					existingUserId, existingUserType,
					AccessPermissionEnum.getPermissionValueById(existingUserPermission), newPermission,
					endDate == null ? null : Map.of("endDate", endDate));

			// Adding Notification
			if (Utility.isNotificationDatabaseEnabled()) {
				String engineType = String.valueOf(getEngineType(engineId)).toLowerCase();
				String existingPermission = AccessPermissionEnum.getPermissionValueById(existingUserPermission);
				NotificationDbUtils.createNotification(user, existingUserId, existingUserType, engineId,
						NotificationConstants.Type.PERMISSION_CHANGE, engineType, NotificationConstants.Priority.MEDIUM,
						existingPermission, newPermission);
			}
		} catch (Exception e) {
			classLogger.error("Failed to update engine user permission", e);
			throw new IllegalArgumentException(
					"An error occurred updating the user permissions for this engine. Detailed error message = "
							+ e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * 
	 * @param user
	 * @param engineId
	 * @param permission
	 * @throws IllegalAccessException
	 */
	public static void editEngineUserPermissions(User user, String engineId, List<Map<String, Object>> permission)
			throws IllegalAccessException {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		// make sure user can edit the database
		int userPermissionLvl = getMaxUserEnginePermission(user, engineId);
		if (!AccessPermissionEnum.isEditor(userPermissionLvl)) {
			throw new IllegalAccessException("Insufficient privileges to modify this engine's permissions.");
		}

		// get userid of all requests
		List<String> existingUserIds = new ArrayList<String>();
		for (Map<String, Object> i : permission) {
			existingUserIds.add((String) i.get("userid"));
		}

		// get user permissions to edit
		Map<String, Integer> existingUserPermission = SecurityUserEngineUtils.getUserEnginePermissions(existingUserIds,
				engineId);

		// make sure all users to edit currently has access to database
		Set<String> toRemoveUserIds = new HashSet<String>(existingUserIds);
		toRemoveUserIds.removeAll(existingUserPermission.keySet());
		if (!toRemoveUserIds.isEmpty()) {
			throw new IllegalArgumentException(
					"Attempting to modify user permission for the following users who do not currently have access to the engine: "
							+ String.join(",", toRemoveUserIds));
		}

		// if user is not an owner, check to make sure they are not editting owner
		// access
		if (!AccessPermissionEnum.isOwner(userPermissionLvl)) {
			List<Integer> permissionList = new ArrayList<>(existingUserPermission.values());
			if (permissionList.contains(AccessPermissionEnum.OWNER.getId())) {
				throw new IllegalArgumentException("As a non-owner, you cannot edit access of an owner.");
			}

			// also make sure, you are not adding an owner
			for (Map<String, Object> req : permission) {
				if (AccessPermissionEnum.OWNER.getId() == AccessPermissionEnum
						.getIdByPermission((String) req.get("permission"))) {
					throw new IllegalArgumentException("As a non-owner, you cannot give a user access as an owner.");
				}
			}
		}
		Pair<String, String> userDetails = User.getPrimaryUserIdAndTypePair(user);

		Timestamp startDate = Utility.getCurrentSqlTimestampUTC();

		String engineType = String.valueOf(getEngineType(engineId)).toLowerCase();
		// update user permissions in bulk
		String updateQ = "UPDATE ENGINEPERMISSION SET PERMISSION = ?, PERMISSIONGRANTEDBY = ?, PERMISSIONGRANTEDBYTYPE = ?, DATEADDED = ?, ENDDATE = ?, USAGERESTRICTION = ?, USAGEFREQUENCY = ?, MAXTOKENS = ?, MAXRESPONSETIME = ? WHERE USERID = ? AND ENGINEID = ?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(updateQ);
			for (int i = 0; i < permission.size(); i++) {
				Map<String, Object> thisPermissionMap = permission.get(i);

				int parameterIndex = 1;

				String newUserId = (String) thisPermissionMap.get("userid");
				String newUserType = (String) thisPermissionMap.get("type");
				String existingPermission = AccessPermissionEnum
						.getPermissionValueById(getUserEnginePermission(newUserId, engineId));
				// SET
				ps.setInt(parameterIndex++,
						AccessPermissionEnum.getIdByPermission((String) thisPermissionMap.get("permission")));
				ps.setString(parameterIndex++, userDetails.getValue0());
				ps.setString(parameterIndex++, userDetails.getValue1());
				ps.setTimestamp(parameterIndex++, startDate);
				// end date for this user
				Timestamp verifiedEndDate = null;
				if (thisPermissionMap.get("endDate") != null) {
					verifiedEndDate = AbstractSecurityUtils.calculateEndDate((String) thisPermissionMap.get("endDate"));
					ps.setTimestamp(parameterIndex++, verifiedEndDate);
				} else {
					ps.setNull(parameterIndex++, java.sql.Types.TIMESTAMP);
				}

				// engine usage restrictions
				if (thisPermissionMap.get("usageRestriction") != null
						&& !((String) thisPermissionMap.get("usageRestriction")).trim().isEmpty()) {
					ps.setString(parameterIndex++, ((String) thisPermissionMap.get("usageRestriction")).trim());
				} else {
					ps.setNull(parameterIndex++, java.sql.Types.VARCHAR);
				}
				if (thisPermissionMap.get("usageRestriction") != null
						&& !((String) thisPermissionMap.get("usageFrequency")).trim().isEmpty()) {
					ps.setString(parameterIndex++, ((String) thisPermissionMap.get("usageFrequency")).trim());
				} else {
					ps.setNull(parameterIndex++, java.sql.Types.VARCHAR);
				}
				if (thisPermissionMap.get("maxTokens") != null) {
					ps.setInt(parameterIndex++, ((Number) thisPermissionMap.get("maxTokens")).intValue());
				} else {
					ps.setNull(parameterIndex++, java.sql.Types.INTEGER);
				}
				if (thisPermissionMap.get("maxResponseTime") != null) {
					ps.setDouble(parameterIndex++, ((Number) thisPermissionMap.get("maxResponseTime")).doubleValue());
				} else {
					ps.setNull(parameterIndex++, java.sql.Types.DOUBLE);
				}
				// WHERE
				ps.setString(parameterIndex++, (String) thisPermissionMap.get("userid"));
				ps.setString(parameterIndex++, engineId);
				ps.addBatch();

				// Adding Notification
				if (Utility.isNotificationDatabaseEnabled()) {
					NotificationDbUtils.createNotification(user, newUserId, newUserType, engineId,
							NotificationConstants.Type.PERMISSION_CHANGE, engineType,
							NotificationConstants.Priority.MEDIUM, existingPermission,
							(String) thisPermissionMap.get("permission"));
				}
			}
			ps.executeBatch();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
			for (Map<String, Object> thisPermissionMap : permission) {
				String userId = (String) thisPermissionMap.get("userid");
				UserAuditTrailUtils.recordPermissionUpdate(user, "ENGINE", engineId, null, null, engineId, null,
						userId, (String) thisPermissionMap.get("type"),
						AccessPermissionEnum.getPermissionValueById(existingUserPermission.get(userId)),
						(String) thisPermissionMap.get("permission"), thisPermissionMap);
			}
		} catch (Exception e) {
			classLogger.error("Failed to update engine user permissions", e);
			throw new IllegalArgumentException(
					"An error occurred updating the user permissions for this engine. Detailed error message = "
							+ e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * Delete all values
	 * 
	 * @param engineId
	 */
	public static void deleteEngine(String engineId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		List<String> deletes = new ArrayList<>();
		deletes.add("DELETE FROM ENGINE WHERE ENGINEID=?");
		deletes.add("DELETE FROM ENGINEPERMISSION WHERE ENGINEID=?");
		deletes.add("DELETE FROM ENGINEMETA WHERE ENGINEID=?");
		deletes.add("DELETE FROM MODELMETADATA WHERE ENGINEID=?");
		deletes.add("DELETE FROM ENGINEACCESSREQUEST WHERE ENGINEID=?");

		for (String deleteQuery : deletes) {
			PreparedStatement ps = null;
			try {
				ps = securityDb.getPreparedStatement(deleteQuery);
				ps.setString(1, engineId);
				ps.execute();
				if (!ps.getConnection().getAutoCommit()) {
					ps.getConnection().commit();
				}
			} catch (SQLException e) {
				classLogger.error("Failed to delete engine", e);
			} finally {
				ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
			}
		}
	}

	/**
	 * 
	 * @param user
	 * @param editedUserId
	 * @param engineId
	 * @return
	 * @throws IllegalAccessException
	 */
	public static void removeEngineUser(User user, String existingUserId, String engineId)
			throws IllegalAccessException {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		// make sure user can edit the database
		int userPermissionLvl = getMaxUserEnginePermission(user, engineId);
		if (!AccessPermissionEnum.isEditor(userPermissionLvl)
				&& !user.getPrimaryLoginToken().getId().equals(existingUserId)) {
			throw new IllegalAccessException("Insufficient privileges to modify this engine's permissions.");
		}

		// make sure we are trying to edit a permission that exists
		Integer existingUserPermission = getUserEnginePermission(existingUserId, engineId);
		if (existingUserPermission == null) {
			throw new IllegalArgumentException(
					"Attempting to modify engine permission for a user who does not currently have access to the engine");
		}

		// if i am not an owner
		// then i need to check if i can remove this users permission
		if (!AccessPermissionEnum.isOwner(userPermissionLvl)) {
			// not an owner, check if trying to edit an owner or an editor/reader
			// get the current permission
			if (AccessPermissionEnum.OWNER.getId() == existingUserPermission) {
				throw new IllegalAccessException(
						"The user doesn't have the high enough permissions to modify this users engine permission.");
			}
		}

		String deleteQuery = "DELETE FROM ENGINEPERMISSION WHERE USERID=? AND ENGINEID=?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(deleteQuery);
			int parameterIndex = 1;
			ps.setString(parameterIndex++, existingUserId);
			ps.setString(parameterIndex++, engineId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error("Failed to remove engine user", e);
			throw new IllegalArgumentException("An error occurred removing the user permissions for this engine");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * 
	 * @param user
	 * @param existingUserIds
	 * @param engineId
	 * @throws IllegalAccessException
	 */
	public static void removeEngineUsers(User user, List<String> existingUserIds, String engineId)
			throws IllegalAccessException {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		// make sure user can edit the database
		int userPermissionLvl = getMaxUserEnginePermission(user, engineId);
		if (!AccessPermissionEnum.isEditor(userPermissionLvl)) {
			throw new IllegalAccessException("Insufficient privileges to modify this engine's permissions.");
		}

		// get user permissions to remove
		Map<String, Integer> existingUserPermission = SecurityUserEngineUtils.getUserEnginePermissions(existingUserIds,
				engineId);

		// make sure all users to remove currently has access to database
		Set<String> toRemoveUserIds = new HashSet<String>(existingUserIds);
		toRemoveUserIds.removeAll(existingUserPermission.keySet());
		if (!toRemoveUserIds.isEmpty()) {
			throw new IllegalArgumentException(
					"Attempting to modify engine permission for the following users who do not currently have access to the engine: "
							+ String.join(",", toRemoveUserIds));
		}

		// if user is not an owner, check to make sure they are not removing owner
		// access
		if (!AccessPermissionEnum.isOwner(userPermissionLvl)) {
			List<Integer> permissionList = new ArrayList<Integer>(existingUserPermission.values());
			if (permissionList.contains(AccessPermissionEnum.OWNER.getId())) {
				throw new IllegalAccessException("As a non-owner, you cannot remove access of an owner.");
			}
		}

		// first do a delete
		String deleteQ = "DELETE FROM ENGINEPERMISSION WHERE USERID=? AND ENGINEID=?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(deleteQ);
			for (int i = 0; i < existingUserIds.size(); i++) {
				int parameterIndex = 1;
				ps.setString(parameterIndex++, existingUserIds.get(i));
				ps.setString(parameterIndex++, engineId);
				ps.addBatch();
			}
			ps.executeBatch();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to remove engine users", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	public static void removeExpiredEngineUser(String userId, String engineId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String deleteQuery = "DELETE FROM ENGINEPERMISSION WHERE USERID=? AND ENGINEID=?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(deleteQuery);
			int parameterIndex = 1;
			ps.setString(parameterIndex++, userId);
			ps.setString(parameterIndex++, engineId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error("Failed to remove expired engine user", e);
			throw new IllegalArgumentException("An error occurred removing the user permissions for this engine");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * Set if the engine is public to all users on this instance
	 * 
	 * @param user
	 * @param engineId
	 * @param global
	 * @return
	 * @throws IllegalAccessException
	 */
	public static boolean setEngineGlobal(User user, String engineId, boolean global) throws IllegalAccessException {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		if (!SecurityUserEngineUtils.userIsOwner(user, engineId)) {
			throw new IllegalAccessException(
					"The user doesn't have the permission to set this engine as global. Only the owner can perform this action.");
		}

		String updateQ = "UPDATE ENGINE SET GLOBAL=? WHERE ENGINEID=?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(updateQ);
			ps.setBoolean(1, global);
			ps.setString(2, engineId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to update engine global visibility setting", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
		UserAuditTrailUtils.recordEngineLifecycle(user, "ENGINE_UPDATE", getAuditEngineTypeName(engineId), engineId,
				getEngineDisplayNameForId(engineId), Map.of("field", "global", "global", global));
		return true;
	}

	/**
	 * Set a engine to be global
	 * 
	 * @param engineId
	 */
	public static void setEngineCompletelyGlobal(String engineId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		{
			String update1 = "UPDATE ENGINE SET GLOBAL=? WHERE ENGINEID=?";
			PreparedStatement ps = null;
			try {
				ps = securityDb.getPreparedStatement(update1);
				int parameterIndex = 1;
				ps.setBoolean(parameterIndex++, true);
				ps.setString(parameterIndex++, engineId);
				ps.execute();
				if (!ps.getConnection().getAutoCommit()) {
					ps.getConnection().commit();
				}
			} catch (SQLException e) {
				classLogger.error("Failed to set engine visibility to globally accessible", e);
			} finally {
				ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
			}
		}
	}

	/**
	 * Set if the engine is discoverable to all users on this instance
	 * 
	 * @param user
	 * @param engineId
	 * @param discoverable
	 * @return
	 * @throws IllegalAccessException
	 */
	public static boolean setEngineDiscoverable(User user, String engineId, boolean discoverable)
			throws IllegalAccessException {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		if (!SecurityUserEngineUtils.userIsOwner(user, engineId)) {
			throw new IllegalAccessException(
					"The user doesn't have the permission to set this engine as discoverable. Only the owner or an admin can perform this action.");
		}

		String updateQ = "UPDATE ENGINE SET DISCOVERABLE=? WHERE ENGINEID=?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(updateQ);
			ps.setBoolean(1, discoverable);
			ps.setString(2, engineId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error("Failed to update engine discoverability setting", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
		UserAuditTrailUtils.recordEngineLifecycle(user, "ENGINE_UPDATE", getAuditEngineTypeName(engineId), engineId,
				getEngineDisplayNameForId(engineId), Map.of("field", "discoverable", "discoverable", discoverable));
		return true;
	}

	/**
	 * Change the user visibility (show/hide) for a engine. Without removing its
	 * permissions.
	 * 
	 * @param user
	 * @param engineId
	 * @param visibility
	 * @throws IllegalAccessException
	 */
	public static void setEngineVisibility(User user, String engineId, boolean visibility)
			throws IllegalAccessException {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		if (!SecurityUserEngineUtils.userCanViewEngine(user, engineId)) {
			throw new IllegalAccessException(
					"The user doesn't have the permission to modify his visibility of this engine");
		}
		Collection<String> userIdFilters = getUserFiltersQs(user);
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__ENGINEID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__ENGINEID", "==", engineId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", userIdFilters));

		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (wrapper.hasNext()) {
				// need to update
				PreparedStatement ps = securityDb
						.getPreparedStatement("UPDATE ENGINEPERMISSION SET VISIBILITY=? WHERE USERID=?");
				if (ps == null) {
					throw new IllegalArgumentException("Error generating prepared statement to set engine visibility");
				}
				try {
					// we will set the permission to read only
					for (AuthProvider loginType : user.getLogins()) {
						String userId = user.getAccessToken(loginType).getId();
						int parameterIndex = 1;
						ps.setBoolean(parameterIndex++, visibility);
						ps.setString(parameterIndex++, userId);
						ps.addBatch();
					}
					ps.executeBatch();
					if (!ps.getConnection().getAutoCommit()) {
						ps.getConnection().commit();
					}
				} catch (Exception e) {
					classLogger.error("Failed to update engine visibility", e);
					throw e;
				} finally {
					ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
				}
			} else {
				// need to insert
				PreparedStatement ps = securityDb.getPreparedStatement("INSERT INTO ENGINEPERMISSION "
						+ "(USERID, ENGINEID, VISIBILITY, FAVORITE, PERMISSION) VALUES (?,?,?,?,?)");
				if (ps == null) {
					throw new IllegalArgumentException("Error generating prepared statement to set engine visibility");
				}
				try {
					// we will set the permission to read only
					for (AuthProvider loginType : user.getLogins()) {
						String userId = user.getAccessToken(loginType).getId();
						int parameterIndex = 1;
						ps.setString(parameterIndex++, userId);
						ps.setString(parameterIndex++, engineId);
						ps.setBoolean(parameterIndex++, visibility);
						// default favorite as false
						ps.setBoolean(parameterIndex++, false);
						ps.setInt(parameterIndex++, 3);

						ps.addBatch();
					}
					ps.executeBatch();
					if (!ps.getConnection().getAutoCommit()) {
						ps.getConnection().commit();
					}
				} catch (Exception e) {
					classLogger.error("Failed to update engine visibility", e);
					throw e;
				} finally {
					ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
				}
			}
			UserAuditTrailUtils.recordEngineLifecycle(user, "ENGINE_UPDATE", getAuditEngineTypeName(engineId), engineId,
					getEngineDisplayNameForId(engineId), Map.of("field", "visibility", "visibility", visibility));

		} catch (Exception e) {
			classLogger.error("Failed to update engine visibility", e);
		}
	}

	/**
	 * Change the user favorite (is favorite / not favorite) for an engine. Without
	 * removing its permissions.
	 * 
	 * @param user
	 * @param engineId
	 * @param visibility
	 * @throws IllegalAccessException
	 */
	public static void setEngineFavorite(User user, String engineId, boolean isFavorite) throws IllegalAccessException {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		if (!engineIsGlobal(engineId) && !userCanViewEngine(user, engineId)) {
			throw new IllegalAccessException(
					"The user doesn't have the permission to modify his visibility of this engine");
		}
		Collection<String> userIdFilters = getUserFiltersQs(user);
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__ENGINEID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__ENGINEID", "==", engineId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", userIdFilters));

		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (wrapper.hasNext()) {
				// need to update
				PreparedStatement ps = securityDb
						.getPreparedStatement("UPDATE ENGINEPERMISSION SET FAVORITE=? WHERE USERID=? AND ENGINEID=?");
				if (ps == null) {
					throw new IllegalArgumentException("Error generating prepared statement to set engine favorites");
				}
				try {
					// we will set the permission to read only
					for (AuthProvider loginType : user.getLogins()) {
						String userId = user.getAccessToken(loginType).getId();
						int parameterIndex = 1;
						ps.setBoolean(parameterIndex++, isFavorite);
						ps.setString(parameterIndex++, userId);
						ps.setString(parameterIndex++, engineId);
						ps.addBatch();
					}
					ps.executeBatch();
					if (!ps.getConnection().getAutoCommit()) {
						ps.getConnection().commit();
					}
				} catch (Exception e) {
					classLogger.error("Failed to update engine favorite", e);
					throw e;
				} finally {
					ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
				}
			} else {
				// need to insert
				PreparedStatement ps = securityDb.getPreparedStatement("INSERT INTO ENGINEPERMISSION "
						+ "(USERID, ENGINEID, VISIBILITY, FAVORITE, PERMISSION) VALUES (?,?,?,?,?)");
				if (ps == null) {
					throw new IllegalArgumentException("Error generating prepared statement to set engine favorites");
				}
				try {
					// we will set the permission to read only
					for (AuthProvider loginType : user.getLogins()) {
						String userId = user.getAccessToken(loginType).getId();
						int parameterIndex = 1;
						ps.setString(parameterIndex++, userId);
						ps.setString(parameterIndex++, engineId);
						// default visibility as true
						ps.setBoolean(parameterIndex++, true);
						ps.setBoolean(parameterIndex++, isFavorite);
						ps.setInt(parameterIndex++, 3);

						ps.addBatch();
					}
					ps.executeBatch();
					if (!ps.getConnection().getAutoCommit()) {
						ps.getConnection().commit();
					}
				} catch (Exception e) {
					classLogger.error("Failed to update engine favorite", e);
					throw e;
				} finally {
					ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
				}
			}
		} catch (Exception e) {
			classLogger.error("Failed to update engine favorite", e);
		}
	}

	/**
	 * update the database name
	 * 
	 * @param user
	 * @param engineId
	 * @param isPublic
	 * @return
	 * @throws IllegalAccessException
	 */
	public static boolean setEngineName(User user, String engineId, String newEngineName)
			throws IllegalAccessException {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		if (!SecurityUserEngineUtils.userIsOwner(user, engineId)) {
			throw new IllegalAccessException(
					"The user doesn't have the permission to change the engine name. Only the owner can perform this action.");
		}

		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement("UPDATE ENGINE SET ENGINENAME=? WHERE ENGINEID=?");
			int parameterIndex = 1;
			// SET
			ps.setString(parameterIndex++, newEngineName);
			// WHERE
			ps.setString(parameterIndex++, engineId);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to update engine name", e);
			throw new IllegalArgumentException("An error occurred updating the engine name");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
		UserAuditTrailUtils.recordEngineLifecycle(user, "ENGINE_UPDATE", getAuditEngineTypeName(engineId), engineId,
				newEngineName, Map.of("field", "name", "name", newEngineName));
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
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINEMETAKEYS__METAKEY"));
		List<String> metakeys = QueryExecutionUtility.flushToListString(securityDb, qs);
		return metakeys;
	}

	/**
	 * Update the engine metadata Will delete existing values and then perform a
	 * bulk insert
	 * 
	 * @param engineId
	 * @param insightId
	 * @param tags
	 */
	public static void updateEngineMetadata(String engineId, Map<String, Object> metadata) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		// first do a delete
		String deleteQ = "DELETE FROM ENGINEMETA WHERE METAKEY=? AND ENGINEID=?";
		PreparedStatement deletePs = null;
		try {
			deletePs = securityDb.getPreparedStatement(deleteQ);
			for (String field : metadata.keySet()) {
				int parameterIndex = 1;
				deletePs.setString(parameterIndex++, field);
				deletePs.setString(parameterIndex++, engineId);
				deletePs.addBatch();
			}
			deletePs.executeBatch();
			if (!deletePs.getConnection().getAutoCommit()) {
				deletePs.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to update engine metadata", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, deletePs);
		}

		// now we do the new insert with the order of the tags
		String query = securityDb.getQueryUtil().createInsertPreparedStatementString("ENGINEMETA",
				new String[] { "ENGINEID", "METAKEY", "METAVALUE", "METAORDER" });
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			for (String field : metadata.keySet()) {
				Object val = metadata.get(field);
				List<Object> values = new ArrayList<>();
				if (val instanceof List) {
					values = (List<Object>) val;
				} else if (val instanceof Collection) {
					values.addAll((Collection<Object>) val);
				} else {
					values.add(val);
				}

				for (int i = 0; i < values.size(); i++) {
					int parameterIndex = 1;
					Object fieldVal = values.get(i);

					ps.setString(parameterIndex++, engineId);
					ps.setString(parameterIndex++, field);
					ps.setString(parameterIndex++, fieldVal + "");
					ps.setInt(parameterIndex++, i);
					ps.addBatch();
				}
			}
			ps.executeBatch();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to update engine metadata", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * Get the wrapper for additional database metadata
	 * 
	 * @param engineIds
	 * @param metaKeys
	 * @param ignoreMarkdown
	 * @return
	 * @throws Exception
	 */
	public static IRawSelectWrapper getEngineMetadataWrapper(Collection<String> engineIds, List<String> metaKeys,
			boolean ignoreMarkdown) throws Exception {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		// selectors
		qs.addSelector(new QueryColumnSelector("ENGINEMETA__ENGINEID"));
		qs.addSelector(new QueryColumnSelector("ENGINEMETA__METAKEY"));
		qs.addSelector(new QueryColumnSelector("ENGINEMETA__METAVALUE"));
		qs.addSelector(new QueryColumnSelector("ENGINEMETA__METAORDER"));
		// filters
		if (engineIds != null && !engineIds.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEMETA__ENGINEID", "==", engineIds));
		}
		if (metaKeys != null && !metaKeys.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEMETA__METAKEY", "==", metaKeys));
		}
		// exclude markdown metadata due to potential large data size
		if (ignoreMarkdown) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEMETA__METAKEY", "!=", Constants.MARKDOWN));
		}
		// order
		qs.addOrderBy("ENGINEMETA__METAORDER");
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
		return wrapper;
	}

	/**
	 * Get the metadata for a specific database
	 * 
	 * @param engineId
	 * @param metaKeys
	 * @param ignoreMarkdown
	 * @return
	 */
	public static Map<String, Object> getAggregateEngineMetadata(String engineId, List<String> metaKeys,
			boolean ignoreMarkdown) {
		Map<String, Object> retMap = new HashMap<String, Object>();

		List<String> engineIds = new ArrayList<>();
		engineIds.add(engineId);

		try (IRawSelectWrapper wrapper = getEngineMetadataWrapper(engineIds, metaKeys, ignoreMarkdown)) {
			while (wrapper.hasNext()) {
				Object[] data = wrapper.next().getValues();
				String metaKey = (String) data[1];
				String metaValue = (String) data[2];

				// always send as array
				// if multi, send as array
				if (retMap.containsKey(metaKey)) {
					Object obj = retMap.get(metaKey);
					if (obj instanceof List) {
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
			classLogger.error("Failed to aggregate engine metadata values", e);
		}

		return retMap;
	}

	/**
	 * Check if the user has access to the engine
	 * 
	 * @param engineId
	 * @param userId
	 * @return
	 * @throws Exception
	 */
	public static boolean checkUserHasAccessToDatabase(String engineId, String userId) throws Exception {
		return SecurityUserEngineUtils.checkUserHasAccessToEngine(engineId, userId);
	}

	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////

	/*
	 * Copying permissions
	 */

	/**
	 * Copy the engine permissions from one engine to another
	 * 
	 * @param sourceEngineId
	 * @param targetEngineId
	 * @param maxTokens
	 * @param maxResponseTime
	 * @param usageRestriction
	 * @param usageFrequency
	 * @throws Exception
	 */
	public static void copyEnginePermissions(String sourceEngineId, String targetEngineId) throws Exception {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();

		String insertTargetEnginePermissionSql = "INSERT INTO ENGINEPERMISSION (ENGINEID, USERID, PERMISSION, VISIBILITY, USAGERESTRICTION, USAGEFREQUENCY, MAXTOKENS, MAXRESPONSETIME) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
		PreparedStatement insertTargetEnginePermissionStatement = securityDb
				.getPreparedStatement(insertTargetEnginePermissionSql);

		// grab the permissions, filtered on the source database id
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__ENGINEID"));
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__USERID"));
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__PERMISSION"));
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__VISIBILITY"));
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__USAGERESTRICTION"));
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__USAGEFREQUENCY"));
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__MAXTOKENS"));
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__MAXRESPONSETIME"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__ENGINEID", "==", sourceEngineId));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			while (wrapper.hasNext()) {
				Object[] row = wrapper.next().getValues();
				// now loop through all the permissions
				// but with the target engine id instead of the source engine id
				insertTargetEnginePermissionStatement.setString(1, targetEngineId);
				insertTargetEnginePermissionStatement.setString(2, (String) row[1]);
				insertTargetEnginePermissionStatement.setInt(3, ((Number) row[2]).intValue());
				insertTargetEnginePermissionStatement.setBoolean(4, (Boolean) row[3]);
				if (row[4] == null) {
					insertTargetEnginePermissionStatement.setNull(5, java.sql.Types.VARCHAR);
				} else {
					insertTargetEnginePermissionStatement.setString(5, (String) row[4]);
				}
				if (row[5] == null) {
					insertTargetEnginePermissionStatement.setNull(6, java.sql.Types.VARCHAR);
				} else {
					insertTargetEnginePermissionStatement.setString(6, (String) row[5]);
				}
				if (row[6] == null) {
					insertTargetEnginePermissionStatement.setNull(7, java.sql.Types.INTEGER);
				} else {
					insertTargetEnginePermissionStatement.setInt(7, ((Number) row[6]).intValue());
				}
				if (row[7] == null) {
					insertTargetEnginePermissionStatement.setNull(8, java.sql.Types.DOUBLE);
				} else {
					insertTargetEnginePermissionStatement.setDouble(8, ((Number) row[7]).doubleValue());
				}
				// add to batch
				insertTargetEnginePermissionStatement.addBatch();
			}
		} catch (Exception e) {
			classLogger.error("Failed to copy engine permissions to the target engine", e);
			throw e;
		}

		// first delete the current project permissions
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement("DELETE FROM ENGINEPERMISSION WHERE ENGINEID=?");
			int parameterIndex = 1;
			ps.setString(parameterIndex++, targetEngineId);
			// here we delete
			ps.execute();
			// now we insert
			insertTargetEnginePermissionStatement.executeBatch();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
			if (!insertTargetEnginePermissionStatement.getConnection().getAutoCommit()) {
				insertTargetEnginePermissionStatement.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error("Failed to copy engine permissions to the target engine", e);
			throw new IllegalArgumentException("An error occurred transferring the engine permissions");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, insertTargetEnginePermissionStatement);
		}
	}

	/**
	 * Returns List of users that have no access credentials to a given engine
	 * 
	 * @param engineId
	 * @return
	 */
	public static List<Map<String, Object>> getEngineUsersNoCredentials(User user, String engineId, String searchTerm,
			long limit, long offset) throws IllegalAccessException {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		/*
		 * Security check to make sure that the user can view the application provided.
		 */
		if (!userCanViewEngine(user, engineId)) {
			throw new IllegalAccessException("The user does not have access to view this engine");
		}

		/*
		 * String Query = "SELECT SMSS_USER.ID, SMSS_USER.USERNAME, SMSS_USER.NAME,
		 * SMSS_USER.EMAIL FROM SMSS_USER WHERE ID NOT IN (SELECT e.USERID FROM
		 * ENGINEPERMISSION e WHERE e.ENGINEID = '"+ appID + "' e.PERMISSION IS NOT
		 * NULL);"
		 */

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__ID", "id"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__TYPE", "type"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__USERNAME", "username"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__NAME", "name"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__EMAIL", "email"));
		// filter for sub-query
		{
			SelectQueryStruct subQs = new SelectQueryStruct();
			qs.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery("SMSS_USER__ID", "!=", subQs));
			// Sub-query itself
			subQs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__USERID"));
			subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__ENGINEID", "==", engineId));
			subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__PERMISSION", "!=", null,
					PixelDataType.NULL_VALUE));
		}
		if (searchTerm != null && !(searchTerm = searchTerm.trim()).isEmpty()) {
			OrQueryFilter or = new OrQueryFilter();
			or.addFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__ID", "?like", searchTerm));
			or.addFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__NAME", "?like", searchTerm));
			or.addFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__USERNAME", "?like", searchTerm));
			or.addFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__EMAIL", "?like", searchTerm));
			qs.addExplicitFilter(or);
		}
		qs.addOrderBy(new QueryColumnOrderBySelector("SMSS_USER__NAME"));
		qs.addOrderBy(new QueryColumnOrderBySelector("SMSS_USER__EMAIL"));
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
	 * Return the engines the user has explicit access to
	 * 
	 * @param singleUserId
	 * @return
	 */
	public static Set<String> getEngineUserHasExplicitAccess(User user) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID"));
		OrQueryFilter orFilter = new OrQueryFilter();
		orFilter.addFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__GLOBAL", "==", true, PixelDataType.BOOLEAN));
		orFilter.addFilter(
				SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", getUserFiltersQs(user)));
		qs.addExplicitFilter(orFilter);
		qs.addRelation("ENGINE", "ENGINEPERMISSION", "left.outer.join");
		return QueryExecutionUtility.flushToSetString(securityDb, qs, false);
	}

	/**
	 * Return if user has explicit permissions to this engine
	 * 
	 * @param user
	 * @param engineId
	 * @return
	 */
	public static boolean userHasExplicitAccess(User user, String engineId) {
		return SecurityUserEngineUtils.getUserEnginePermission(user, engineId) != null;
	}

	/**
	 * Determine if a user can request a engine
	 * 
	 * @param engineId
	 * @return
	 */
	public static boolean engineIsDiscoverable(String engineId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID"));
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("ENGINE__DISCOVERABLE", "==", true, PixelDataType.BOOLEAN));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINEID", "==", engineId));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if (wrapper.hasNext()) {
				// if you are here, you can request
				return true;
			}
		} catch (Exception e) {
			classLogger.error("Failed to determine whether engine is discoverable", e);
		} finally {
			if (wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error("Failed to determine whether engine is discoverable", e);
				}
			}
		}
		return false;
	}

	/**
	 * set user access request
	 * 
	 * @param userId
	 * @param userType
	 * @param engineId
	 * @param requestReasonComment
	 * @param permission
	 * @param user
	 */
	public static void setUserAccessRequest(String userId, String userType, String engineId,
			String requestReasonComment, int permission, User user) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		// first mark previously undecided requests as old
		String updateQ = "UPDATE ENGINEACCESSREQUEST SET APPROVER_DECISION = 'OLD' WHERE REQUEST_USERID=? AND REQUEST_TYPE=? AND ENGINEID=? AND APPROVER_DECISION='NEW_REQUEST'";
		PreparedStatement updatePs = null;
		AbstractSqlQueryUtil securityQueryUtil = securityDb.getQueryUtil();
		try {
			int index = 1;
			updatePs = securityDb.getPreparedStatement(updateQ);
			updatePs.setString(index++, userId);
			updatePs.setString(index++, userType);
			updatePs.setString(index++, engineId);
			updatePs.execute();
			if (!updatePs.getConnection().getAutoCommit()) {
				updatePs.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to update the user access request record", e);
			throw new IllegalArgumentException(
					"An error occurred while marking old user access request with detailed message = "
							+ e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, updatePs);
		}

		// grab user info who is submitting request
		Pair<String, String> requesterDetails = User.getPrimaryUserIdAndTypePair(user);

		// now we do the new insert
		String insertQ = "INSERT INTO ENGINEACCESSREQUEST "
				+ "(ID, REQUEST_USERID, REQUEST_TYPE, REQUEST_TIMESTAMP, REQUEST_REASON, ENGINEID, PERMISSION, SUBMITTED_BY_USERID, SUBMITTED_BY_TYPE, APPROVER_DECISION) "
				+ "VALUES (?,?,?,?,?,?,?,?,?, 'NEW_REQUEST')";
		PreparedStatement insertPs = null;
		try {
			java.sql.Timestamp timestamp = Utility.getCurrentSqlTimestampUTC();

			int index = 1;
			insertPs = securityDb.getPreparedStatement(insertQ);
			insertPs.setString(index++, UUID.randomUUID().toString());
			insertPs.setString(index++, userId);
			insertPs.setString(index++, userType);
			insertPs.setTimestamp(index++, timestamp);
			securityQueryUtil.handleInsertionOfClob(insertPs.getConnection(), insertPs, requestReasonComment, index++,
					new Gson());
			insertPs.setString(index++, engineId);
			insertPs.setInt(index++, permission);
			insertPs.setString(index++, requesterDetails.getValue0());
			insertPs.setString(index++, requesterDetails.getValue1());
			insertPs.execute();
			if (!insertPs.getConnection().getAutoCommit()) {
				insertPs.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to update the user access request record", e);
			throw new IllegalArgumentException(
					"An error occurred while adding user access request detailed message = " + e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, insertPs);
		}
	}

	/**
	 * 
	 * @param user
	 * @param engineId
	 * @return
	 */
	public static int getUserPendingAccessRequest(User user, String engineId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		// grab user info who is submitting request
		Pair<String, String> requesterDetails = User.getPrimaryUserIdAndTypePair(user);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINEACCESSREQUEST__APPROVER_DECISION"));
		qs.addSelector(new QueryColumnSelector("ENGINEACCESSREQUEST__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEACCESSREQUEST__REQUEST_USERID", "==",
				requesterDetails.getValue0()));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEACCESSREQUEST__REQUEST_TYPE", "==",
				requesterDetails.getValue1()));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEACCESSREQUEST__ENGINEID", "==", engineId));
		qs.addOrderBy("ENGINEACCESSREQUEST__REQUEST_TIMESTAMP", "desc");
		IRawSelectWrapper it = null;
		try {
			it = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			while (it.hasNext()) {
				Object[] values = it.next().getValues();
				String mostRecentAction = (String) values[0];
				if (!mostRecentAction.equals("APPROVED") && !mostRecentAction.equals("DENIED")
						&& !mostRecentAction.equals("OLD")) {
					return ((Number) values[1]).intValue();
				}
			}
		} catch (Exception e) {
			classLogger.error("Failed to retrieve pending user access requests", e);
		} finally {
			if (it != null) {
				try {
					it.close();
				} catch (IOException e) {
					classLogger.error("Failed to retrieve pending user access requests", e);
				}
			}
		}

		return -1;
	}

	/**
	 * Get the list of engines the user does not have access to but can request
	 * 
	 * @param allUserEngines
	 * @throws Exception
	 */
	public static List<Map<String, Object>> getUserRequestableEngines(Collection<String> allUserEngines) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINENAME"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEDISPLAYNAME"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINEID", "!=", allUserEngines));
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("ENGINE__DISCOVERABLE", "==", true, PixelDataType.BOOLEAN));
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}

	public static List<Map<String, Object>> getEngineInfo(Collection<String> engineFilter) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINENAME"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEDISPLAYNAME"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINEID", "==", engineFilter));
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}

	/**
	 * Retrieve the engine owner
	 * 
	 * @param user
	 * @param engineId
	 * @param insightId
	 * @return
	 * @throws IllegalAccessException
	 */
	public static List<String> getEngineOwners(String engineId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__EMAIL", "email"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__ENGINEID", "==", engineId));
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("PERMISSION__ID", "==", AccessPermissionEnum.OWNER.getId()));
		qs.addRelation("SMSS_USER", "ENGINEPERMISSION", "inner.join");
		qs.addRelation("ENGINEPERMISSION", "PERMISSION", "inner.join");
		qs.addOrderBy(new QueryColumnOrderBySelector("SMSS_USER__ID"));
		return QueryExecutionUtility.flushToListString(securityDb, qs);
	}

	/**
	 * Get global engines
	 * 
	 * @return
	 */
	public static Set<String> getGlobalEngineIds() {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__GLOBAL", "==", true, PixelDataType.BOOLEAN));
		return QueryExecutionUtility.flushToSetString(securityDb, qs, false);
	}

	public static List<Map<String, Object>> getUserEngineList(User user, List<String> engineTypes,
			List<String> engineIdFilters, Boolean favoritesOnly, Map<String, Object> engineMetadataFilter,
			List<Integer> permissionFilters, String searchTerm, String limit, String offset) {
		return getUserEngineList(user, engineTypes, engineIdFilters, favoritesOnly, engineMetadataFilter,
				permissionFilters, searchTerm, limit, offset, null);
	}

	/**
	 * Get the list of the database information that the user has access to
	 * 
	 * @param user
	 * @param engineTypes
	 * @param engineIdFilters
	 * @param favoritesOnly
	 * @param engineMetadataFilter
	 * @param permissionFilters
	 * @param searchTerm
	 * @param limit
	 * @param offset
	 * @param sortFields
	 * @return
	 */
	public static List<Map<String, Object>> getUserEngineList(User user, List<String> engineTypes,
			List<String> engineIdFilters, Boolean favoritesOnly, Map<String, Object> engineMetadataFilter,
			List<Integer> permissionFilters, String searchTerm, String limit, String offset,
			Map<String, String> sortFields) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();

		String enginePrefix = "ENGINE__";
		String groupEnginePermission = "GROUPENGINEPERMISSION__";

		Collection<String> userIds = getUserFiltersQs(user);
		boolean hasSearchTerm = searchTerm != null && !(searchTerm = searchTerm.trim()).isEmpty();

		SelectQueryStruct qs1 = new SelectQueryStruct();
		// preferred selectors
		qs1.addSelector(new QueryColumnSelector("ENGINE__ENGINEID", "engine_id"));
		qs1.addSelector(new QueryColumnSelector("ENGINE__ENGINENAME", "engine_name"));
		qs1.addSelector(new QueryColumnSelector("ENGINE__ENGINEDISPLAYNAME", "engine_display_name"));
		qs1.addSelector(new QueryColumnSelector("ENGINE__ENGINETYPE", "engine_type"));
		qs1.addSelector(new QueryColumnSelector("ENGINE__ENGINESUBTYPE", "engine_subtype"));
		qs1.addSelector(new QueryColumnSelector("ENGINE__COST", "engine_cost"));
		qs1.addSelector(new QueryColumnSelector("ENGINE__DISCOVERABLE", "engine_discoverable"));
		qs1.addSelector(new QueryColumnSelector("ENGINE__GLOBAL", "engine_global"));
		qs1.addSelector(new QueryColumnSelector("ENGINE__TOOL_APP", "engine_tool_app"));
		qs1.addSelector(new QueryColumnSelector("ENGINE__CREATEDBY", "engine_created_by"));
		qs1.addSelector(new QueryColumnSelector("ENGINE__CREATEDBYTYPE", "engine_created_by_type"));
		qs1.addSelector(new QueryColumnSelector("ENGINE__DATECREATED", "engine_date_created"));
		qs1.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.LOWER, "ENGINE__ENGINENAME",
				"low_engine_name"));
		qs1.addSelector(new QueryColumnSelector("USER_PERMISSIONS__PERMISSION", "engine_user_permission"));
		qs1.addSelector(new QueryColumnSelector("GROUP_PERMISSIONS__PERMISSION", "engine_group_permission"));
		qs1.addSelector(new QueryColumnSelector("USER_PERMISSIONS__FAVORITE", "engine_favorite"));

		// legacy aliases (app_*): kept for backwards compatibility
		qs1.addSelector(new QueryColumnSelector("ENGINE__ENGINEID", "app_id"));
		qs1.addSelector(new QueryColumnSelector("ENGINE__ENGINENAME", "app_name"));
		qs1.addSelector(new QueryColumnSelector("ENGINE__ENGINEDISPLAYNAME", "app_display_name"));
		qs1.addSelector(new QueryColumnSelector("ENGINE__ENGINETYPE", "app_type"));
		qs1.addSelector(new QueryColumnSelector("ENGINE__ENGINESUBTYPE", "app_subtype"));
		qs1.addSelector(new QueryColumnSelector("ENGINE__COST", "app_cost"));
		qs1.addSelector(new QueryColumnSelector("USER_PERMISSIONS__FAVORITE", "app_favorite"));
		// legacy aliases (database_*/non-prefixed): kept for backwards compatibility
		qs1.addSelector(new QueryColumnSelector("ENGINE__ENGINEID", "database_id"));
		qs1.addSelector(new QueryColumnSelector("ENGINE__ENGINENAME", "database_name"));
		qs1.addSelector(new QueryColumnSelector("ENGINE__ENGINETYPE", "database_type"));
		qs1.addSelector(new QueryColumnSelector("ENGINE__ENGINESUBTYPE", "database_subtype"));
		qs1.addSelector(new QueryColumnSelector("ENGINE__COST", "database_cost"));
		qs1.addSelector(new QueryColumnSelector("ENGINE__DISCOVERABLE", "database_discoverable"));
		qs1.addSelector(new QueryColumnSelector("ENGINE__GLOBAL", "database_global"));
		qs1.addSelector(new QueryColumnSelector("ENGINE__CREATEDBY", "database_created_by"));
		qs1.addSelector(new QueryColumnSelector("ENGINE__CREATEDBYTYPE", "database_created_by_type"));
		qs1.addSelector(new QueryColumnSelector("ENGINE__DATECREATED", "database_date_created"));
		qs1.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.LOWER, "ENGINE__ENGINENAME",
				"low_database_name"));
		qs1.addSelector(new QueryColumnSelector("USER_PERMISSIONS__PERMISSION", "user_permission"));
		qs1.addSelector(new QueryColumnSelector("GROUP_PERMISSIONS__PERMISSION", "group_permission"));
		qs1.addSelector(new QueryColumnSelector("USER_PERMISSIONS__FAVORITE", "database_favorite"));

		// description from engine metadata
		qs1.addSelector(new QueryColumnSelector("ENGINE_DESCRIPTION__DESCRIPTION", "engine_description"));

		// this block is for max permissions
		// If both null - return null
		// if either not null - return the permission value that is not null
		// if both not null - return the max permissions (I.E lowest number)
		{
			AndQueryFilter and = new AndQueryFilter();
			and.addFilter(SimpleQueryFilter.makeColToValFilter("GROUP_PERMISSIONS__PERMISSION", "==", null,
					PixelDataType.CONST_INT));
			and.addFilter(SimpleQueryFilter.makeColToValFilter("USER_PERMISSIONS__PERMISSION", "==", null,
					PixelDataType.CONST_INT));

			AndQueryFilter and1 = new AndQueryFilter();
			and1.addFilter(SimpleQueryFilter.makeColToValFilter("GROUP_PERMISSIONS__PERMISSION", "!=", null,
					PixelDataType.CONST_INT));
			and1.addFilter(SimpleQueryFilter.makeColToValFilter("USER_PERMISSIONS__PERMISSION", "==", null,
					PixelDataType.CONST_INT));

			AndQueryFilter and2 = new AndQueryFilter();
			and2.addFilter(SimpleQueryFilter.makeColToValFilter("GROUP_PERMISSIONS__PERMISSION", "==", null,
					PixelDataType.CONST_INT));
			and2.addFilter(SimpleQueryFilter.makeColToValFilter("USER_PERMISSIONS__PERMISSION", "!=", null,
					PixelDataType.CONST_INT));

			SimpleQueryFilter maxPermFilter = SimpleQueryFilter.makeColToColFilter("USER_PERMISSIONS__PERMISSION", "<",
					"GROUP_PERMISSIONS__PERMISSION");

			QueryIfSelector qis3 = QueryIfSelector.makeQueryIfSelector(maxPermFilter,
					new QueryColumnSelector("USER_PERMISSIONS__PERMISSION"),
					new QueryColumnSelector("GROUP_PERMISSIONS__PERMISSION"), "permission");

			QueryIfSelector qis2 = QueryIfSelector.makeQueryIfSelector(and2,
					new QueryColumnSelector("USER_PERMISSIONS__PERMISSION"), qis3, "permission");

			QueryIfSelector qis1 = QueryIfSelector.makeQueryIfSelector(and1,
					new QueryColumnSelector("GROUP_PERMISSIONS__PERMISSION"), qis2, "permission");

			QueryIfSelector qis = QueryIfSelector.makeQueryIfSelector(and,
					new QueryColumnSelector("USER_PERMISSIONS__PERMISSION"), qis1, "permission");

			qs1.addSelector(qis);
		}

		// add a join to get the user permission level, if favorite, and the visibility
		{
			SelectQueryStruct qs2 = new SelectQueryStruct();
			qs2.addSelector(new QueryColumnSelector("ENGINEPERMISSION__ENGINEID", "ENGINEID"));

			QueryFunctionSelector castFavorite = QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.CAST,
					"ENGINEPERMISSION__FAVORITE", "castFavorite");
			castFavorite.setDataType(securityDb.getQueryUtil().getIntegerDataTypeName());
			qs2.addSelector(
					QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.MAX, castFavorite, "FAVORITE"));
			QueryFunctionSelector castVisibility = QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.CAST,
					"ENGINEPERMISSION__VISIBILITY", "castVisibility");
			castVisibility.setDataType(securityDb.getQueryUtil().getIntegerDataTypeName());
			qs2.addSelector(
					QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.MAX, castVisibility, "VISIBILITY"));

			qs2.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.MIN,
					"ENGINEPERMISSION__PERMISSION", "PERMISSION"));
			qs2.addGroupBy(new QueryColumnSelector("ENGINEPERMISSION__ENGINEID", "ENGINEID"));
			qs2.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", userIds));
			IRelation subQuery = new SubqueryRelationship(qs2, "USER_PERMISSIONS", "left.outer.join",
					new String[] { "USER_PERMISSIONS__ENGINEID", "ENGINE__ENGINEID", "=" });
			qs1.addRelation(subQuery);
		}

		// add a join to get the group permission level
		{
			SelectQueryStruct qs3 = new SelectQueryStruct();
			qs3.addSelector(new QueryColumnSelector(groupEnginePermission + "ENGINEID", "ENGINEID"));
			qs3.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.MIN,
					groupEnginePermission + "PERMISSION", "PERMISSION"));
			qs3.addGroupBy(new QueryColumnSelector(groupEnginePermission + "ENGINEID", "ENGINEID"));

			// filter on groups
			OrQueryFilter groupEngineOrFilters = new OrQueryFilter();
			List<AuthProvider> logins = user.getLogins();
			for (AuthProvider login : logins) {
				AccessToken accessToken = user.getAccessToken(login);
				Collection<String> userGroups = accessToken.getUserGroups();
				String userGroupType = accessToken.getUserGroupType();
				Collection<String> userCustomGroups = AdminSecurityGroupUtils.getUserCustomGroups(accessToken);
				if (!userCustomGroups.isEmpty()) {
					AndQueryFilter customAndFilter = new AndQueryFilter();
					customAndFilter.addFilter(
							SimpleQueryFilter.makeColToValFilter(groupEnginePermission + "TYPE", "==", "CUSTOM"));
					customAndFilter.addFilter(
							SimpleQueryFilter.makeColToValFilter(groupEnginePermission + "ID", "==", userCustomGroups));
					groupEngineOrFilters.addFilter(customAndFilter);
				}
				if (!userGroups.isEmpty()) {
					AndQueryFilter andFilter = new AndQueryFilter();
					andFilter.addFilter(
							SimpleQueryFilter.makeColToValFilter(groupEnginePermission + "TYPE", "==", userGroupType));
					andFilter.addFilter(
							SimpleQueryFilter.makeColToValFilter(groupEnginePermission + "ID", "==", userGroups));
					groupEngineOrFilters.addFilter(andFilter);
				}
			}
			if (!groupEngineOrFilters.isEmpty()) {
				qs3.addExplicitFilter(groupEngineOrFilters);
			} else {
				AndQueryFilter andFilter1 = new AndQueryFilter();
				andFilter1.addFilter(SimpleQueryFilter.makeColToValFilter(groupEnginePermission + "TYPE", "==", null));
				andFilter1.addFilter(SimpleQueryFilter.makeColToValFilter(groupEnginePermission + "ID", "==", null));
				qs3.addExplicitFilter(andFilter1);
			}

			IRelation subQuery = new SubqueryRelationship(qs3, "GROUP_PERMISSIONS", "left.outer.join",
					new String[] { "GROUP_PERMISSIONS__ENGINEID", "ENGINE__ENGINEID", "=" });
			qs1.addRelation(subQuery);
		}

		// add a join to get the engine description from metadata
		{
			SelectQueryStruct descQs = new SelectQueryStruct();
			descQs.addSelector(new QueryColumnSelector("ENGINEMETA__ENGINEID", "ENGINEID"));
			descQs.addSelector(new QueryColumnSelector("ENGINEMETA__METAVALUE", "DESCRIPTION"));
			descQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEMETA__METAKEY", "==", "description"));
			IRelation descSubQuery = new SubqueryRelationship(descQs, "ENGINE_DESCRIPTION", "left.outer.join",
					new String[] { "ENGINE_DESCRIPTION__ENGINEID", "ENGINE__ENGINEID", "=" });
			qs1.addRelation(descSubQuery);
		}

		// filters
		if (engineIdFilters != null && !engineIdFilters.isEmpty()) {
			qs1.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINEID", "==", engineIdFilters));
		}
		if (engineTypes != null && !engineTypes.isEmpty()) {
			qs1.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINETYPE", "==", engineTypes));
		}

		// filter based on permission filters
		if (permissionFilters != null && !permissionFilters.isEmpty()) {
			qs1.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USER_PERMISSIONS__PERMISSION", "==",
					permissionFilters, PixelDataType.CONST_INT));
		}

		OrQueryFilter orFilter = new OrQueryFilter();
		{
			orFilter.addFilter(
					SimpleQueryFilter.makeColToValFilter("ENGINE__GLOBAL", "==", true, PixelDataType.BOOLEAN));
			orFilter.addFilter(SimpleQueryFilter.makeColToValFilter("USER_PERMISSIONS__PERMISSION", "!=", null,
					PixelDataType.CONST_INT));
			qs1.addExplicitFilter(orFilter);
		}
		// only show those that are visible
		// remember, user permissions cast this to int
		qs1.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USER_PERMISSIONS__VISIBILITY", "==",
				Arrays.asList(new Object[] { 1, null }), PixelDataType.CONST_INT));
		// favorites only
		// remember, user permissions cast this to int
		if (favoritesOnly) {
			qs1.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USER_PERMISSIONS__FAVORITE", "==", 1,
					PixelDataType.CONST_INT));
		}
		// optional word filter on the engine name
		if (hasSearchTerm) {
			OrQueryFilter searchFilter = new OrQueryFilter();
			searchFilter.addFilter(securityDb.getQueryUtil().getSearchRegexFilter("ENGINE__ENGINENAME", searchTerm));
			searchFilter.addFilter(securityDb.getQueryUtil().getSearchRegexFilter("ENGINE__ENGINEID", searchTerm));
			searchFilter
					.addFilter(securityDb.getQueryUtil().getSearchRegexFilter("ENGINE__ENGINEDISPLAYNAME", searchTerm));
			qs1.addExplicitFilter(searchFilter);
		}
		// filtering by enginemeta key-value pairs (i.e. <tag>:value): for each pair,
		// add in-filter against engineids from subquery
		if (engineMetadataFilter != null && !engineMetadataFilter.isEmpty()) {
			for (String k : engineMetadataFilter.keySet()) {
				SelectQueryStruct subQs = new SelectQueryStruct();
				subQs.addSelector(new QueryColumnSelector("ENGINEMETA__ENGINEID"));
				subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEMETA__METAKEY", "==", k));
				subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEMETA__METAVALUE", "==",
						engineMetadataFilter.get(k)));
				qs1.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery("ENGINE__ENGINEID", "==", subQs));
			}
		}

		// group permissions
		{
			// first lets make sure we have any groups
			OrQueryFilter groupEngineOrFilters = new OrQueryFilter();
			List<AuthProvider> logins = user.getLogins();
			for (AuthProvider login : logins) {
				AccessToken accessToken = user.getAccessToken(login);
				Collection<String> userGroups = accessToken.getUserGroups();
				String userGroupType = accessToken.getUserGroupType();
				Collection<String> userCustomGroups = AdminSecurityGroupUtils.getUserCustomGroups(accessToken);
				if (userGroups.isEmpty() && userCustomGroups.isEmpty()) {
					continue;
				}
				if (!userCustomGroups.isEmpty()) {
					AndQueryFilter customAndFilter = new AndQueryFilter();
					customAndFilter.addFilter(
							SimpleQueryFilter.makeColToValFilter(groupEnginePermission + "TYPE", "==", "CUSTOM"));
					customAndFilter.addFilter(
							SimpleQueryFilter.makeColToValFilter(groupEnginePermission + "ID", "==", userCustomGroups));
					groupEngineOrFilters.addFilter(customAndFilter);
				}
				if (!userGroups.isEmpty()) {
					AndQueryFilter andFilter = new AndQueryFilter();
					andFilter.addFilter(
							SimpleQueryFilter.makeColToValFilter(groupEnginePermission + "TYPE", "==", userGroupType));
					andFilter.addFilter(
							SimpleQueryFilter.makeColToValFilter(groupEnginePermission + "ID", "==", userGroups));
					groupEngineOrFilters.addFilter(andFilter);
				}
			}
			// 4.a does the group have explicit access
			if (!groupEngineOrFilters.isEmpty()) {
				SelectQueryStruct subQs = new SelectQueryStruct();
				// store first and fill in sub query after
				orFilter.addFilter(SimpleQueryFilter.makeColToSubQuery(enginePrefix + "ENGINEID", "==", subQs));

				// we need to have the insight filters
				subQs.addSelector(new QueryColumnSelector(groupEnginePermission + "ENGINEID"));
				subQs.addExplicitFilter(groupEngineOrFilters);
			}
		}

		if (sortFields == null || sortFields.isEmpty()) {
			// Default Sorting
			qs1.addOrderBy(new QueryColumnOrderBySelector("low_database_name"));
		} else {
			Set<String> sortKeys = sortFields.keySet();
			Set<String> validSorts = new HashSet<>(Arrays.asList("ENGINENAME", "DATECREATED"));
			if (sortFields.containsKey(null) || sortFields.containsValue(null)) {
				throw new SemossPixelException("Sort parameters cannot contain null keys or values");
			}
			if (!validSorts.containsAll(sortKeys)) {
				throw new SemossPixelException(
						"Invalid Sort Parameters passed: Only \"ENGINENAME\" and \"DATECREATED\" are supported");
			}
			for (String s : sortKeys) {
				if ("ENGINENAME".equals(s)) {
					qs1.addOrderBy("low_database_name", sortFields.getOrDefault(s, "ASC"));
				} else {
					qs1.addOrderBy("ENGINE__" + s, sortFields.getOrDefault(s, "ASC"));
				}
			}
		}

		Long long_limit = -1L;
		Long long_offset = -1L;
		if (limit != null && !limit.trim().isEmpty()) {
			long_limit = ((Number) Double.parseDouble(limit)).longValue();
		}
		if (offset != null && !offset.trim().isEmpty()) {
			long_offset = ((Number) Double.parseDouble(offset)).longValue();
		}
		qs1.setLimit(long_limit);
		qs1.setOffSet(long_offset);

		return QueryExecutionUtility.flushRsToMap(securityDb, qs1);
	}

	/**
	 * Get the list of the database ids that the user has access to
	 * 
	 * @param user
	 * @param includeGlobal
	 * @param includeDiscoverable
	 * @param includeExistingAccess
	 * @return
	 */
	public static List<String> getUserEngineIdList(User user, List<String> engineTypes, boolean includeGlobal,
			boolean includeDiscoverable, boolean includeExistingAccess) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String enginePrefix = "ENGINE__";
		String enginePermissionPrefix = "ENGINEPERMISSION__";
		String groupEnginePermissionPrefix = "GROUPENGINEPERMISSION__";

		Collection<String> userIds = getUserFiltersQs(user);

		SelectQueryStruct qs1 = new SelectQueryStruct();
		// selectors
		qs1.addSelector(new QueryColumnSelector(enginePrefix + "ENGINEID", "database_id"));
		// filters
		OrQueryFilter orFilter = new OrQueryFilter();
		if (includeGlobal) {
			orFilter.addFilter(
					SimpleQueryFilter.makeColToValFilter(enginePrefix + "GLOBAL", "==", true, PixelDataType.BOOLEAN));
		}
		if (includeDiscoverable) {
			orFilter.addFilter(SimpleQueryFilter.makeColToValFilter(enginePrefix + "DISCOVERABLE", "==", true,
					PixelDataType.BOOLEAN));
		}
		if (engineTypes != null && !engineTypes.isEmpty()) {
			qs1.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(enginePrefix + "ENGINETYPE", "==", engineTypes));
		}
		String existingAccessComparator = "==";
		if (!includeExistingAccess) {
			existingAccessComparator = "!=";
		}
		if (!includeExistingAccess && !includeDiscoverable) {
			throw new IllegalArgumentException(
					"Fitler combinations can result in ids that the user does not have access to. Please adjust your parameters");
		}
		{
			// user access
			SelectQueryStruct qs2 = new SelectQueryStruct();
			qs2.addSelector(new QueryColumnSelector(enginePermissionPrefix + "ENGINEID", "ENGINEID"));
			qs2.addExplicitFilter(
					SimpleQueryFilter.makeColToValFilter(enginePermissionPrefix + "USERID", "==", userIds));
			orFilter.addFilter(
					SimpleQueryFilter.makeColToSubQuery(enginePrefix + "ENGINEID", existingAccessComparator, qs2));
		}
		{
			// filter on groups
			OrQueryFilter groupEngineOrFilters = new OrQueryFilter();
			List<AuthProvider> logins = user.getLogins();
			for (AuthProvider login : logins) {
				if (user.getAccessToken(login).getUserGroups().isEmpty()) {
					continue;
				}

				AndQueryFilter andFilter = new AndQueryFilter();
				andFilter.addFilter(SimpleQueryFilter.makeColToValFilter(groupEnginePermissionPrefix + "TYPE", "==",
						user.getAccessToken(login).getUserGroupType()));
				andFilter.addFilter(SimpleQueryFilter.makeColToValFilter(groupEnginePermissionPrefix + "ID", "==",
						user.getAccessToken(login).getUserGroups()));
				groupEngineOrFilters.addFilter(andFilter);
			}

			if (!groupEngineOrFilters.isEmpty()) {
				SelectQueryStruct qs3 = new SelectQueryStruct();
				qs3.addSelector(new QueryColumnSelector(groupEnginePermissionPrefix + "ENGINEID", "ENGINEID"));
				qs3.addExplicitFilter(groupEngineOrFilters);
				orFilter.addFilter(
						SimpleQueryFilter.makeColToSubQuery(enginePrefix + "ENGINEID", existingAccessComparator, qs3));
			}
		}

		qs1.addExplicitFilter(orFilter);

		return QueryExecutionUtility.flushToListString(securityDb, qs1);
	}

	/**
	 * Get all the available engine metadata and their counts for given keys
	 * 
	 * @param engineFilters
	 * @param metaKey
	 * @return
	 */
	public static List<Map<String, Object>> getAvailableMetaValues(List<String> engineFilters, List<String> metaKeys) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
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
		if (engineFilters != null && !engineFilters.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEMETA__ENGINEID", "==", engineFilters));
		}
		// group
		qs.addGroupBy(new QueryColumnSelector("ENGINEMETA__METAKEY"));
		qs.addGroupBy(new QueryColumnSelector("ENGINEMETA__METAVALUE"));

		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}

	/**
	 * Get all user database and database ids regardless of it being hidden or not
	 * 
	 * @param userId
	 * @return
	 */
	public static List<Map<String, Object>> getAllUserDatabaseList(User user) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();

		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID", "app_id"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINENAME", "app_name"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEDISPLAYNAME", "app_display_name"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINETYPE", "app_type"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINESUBTYPE", "app_subtype"));
		qs.addSelector(new QueryColumnSelector("ENGINE__COST", "app_cost"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__GLOBAL", "==", true, PixelDataType.BOOLEAN));
		List<Map<String, Object>> allGlobalEnginesMap = QueryExecutionUtility.flushRsToMap(securityDb, qs);

		SelectQueryStruct qs2 = new SelectQueryStruct();
		qs2.addSelector(new QueryColumnSelector("ENGINE__ENGINEID", "app_id"));
		qs2.addSelector(new QueryColumnSelector("ENGINE__ENGINENAME", "app_name"));
		qs2.addSelector(new QueryColumnSelector("ENGINE__ENGINEDISPLAYNAME", "app_display_name"));
		qs2.addSelector(new QueryColumnSelector("ENGINE__ENGINETYPE", "app_type"));
		qs2.addSelector(new QueryColumnSelector("ENGINE__ENGINESUBTYPE", "app_subtype"));
		qs2.addSelector(new QueryColumnSelector("ENGINE__COST", "app_cost"));
		qs2.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", getUserFiltersQs(user)));
		qs2.addRelation("ENGINE", "ENGINEPERMISSION", "inner.join");

		List<Map<String, Object>> databaseMap = QueryExecutionUtility.flushRsToMap(securityDb, qs2);
		databaseMap.addAll(allGlobalEnginesMap);
		return databaseMap;
	}

	// /**
	// * Get the database information
	// * @param databaseFilter
	// * @return
	// */
	// public static List<Map<String, Object>> getAllDatabaseList(String
	// databaseFilter) {
	// List<String> filters = null;
	// if(databaseFilter != null && !databaseFilter.isEmpty()) {
	// filters = new ArrayList<>();
	// filters.add(databaseFilter);
	// }
	// return getAllDatabaseList(filters);
	// }

	// /**
	// * Get the database information
	// * @param databaseFilter
	// * @return
	// */
	// public static List<Map<String, Object>> getAllDatabaseList(List<String>
	// databaseFilters) {
	// List<String> engineTypes = new ArrayList<>();
	// engineTypes.add(IEngine.CATALOG_TYPE.DATABASE.toString());
	// return getAllEngineList(engineTypes, databaseFilters, null, null, null,
	// null);
	// }

	// /**
	// * Get database information
	// * @param databaseFilters
	// * @param engineMetadataFilter
	// * @param searchTerm
	// * @param limit
	// * @param offset
	// * @return
	// */
	// public static List<Map<String, Object>> getAllEngineList(List<String>
	// engineType, List<String> engineIdFilters, Map<String, Object>
	// engineMetadataFilter,
	// String searchTerm, String limit, String offset) {
	//
	// boolean hasSearchTerm = searchTerm != null &&
	// !(searchTerm=searchTerm.trim()).isEmpty();
	//
	// SelectQueryStruct qs = new SelectQueryStruct();
	// qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID", "app_id"));
	// qs.addSelector(new QueryColumnSelector("ENGINE__ENGINENAME", "app_name"));
	// qs.addSelector(new QueryColumnSelector("ENGINE__ENGINETYPE", "app_type"));
	// qs.addSelector(new QueryColumnSelector("ENGINE__ENGINESUBTYPE",
	// "app_subtype"));
	// qs.addSelector(new QueryColumnSelector("ENGINE__COST", "app_cost"));
	//
	// qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID", "database_id"));
	// qs.addSelector(new QueryColumnSelector("ENGINE__ENGINENAME",
	// "database_name"));
	// qs.addSelector(new QueryColumnSelector("ENGINE__ENGINETYPE",
	// "database_type"));
	// qs.addSelector(new QueryColumnSelector("ENGINE__ENGINESUBTYPE",
	// "database_subtype"));
	// qs.addSelector(new QueryColumnSelector("ENGINE__COST", "database_cost"));
	// qs.addSelector(new QueryColumnSelector("ENGINE__CREATEDBY",
	// "database_created_by"));
	// qs.addSelector(new QueryColumnSelector("ENGINE__CREATEDBYTYPE",
	// "database_created_by_type"));
	// qs.addSelector(new QueryColumnSelector("ENGINE__DATECREATED",
	// "database_date_created"));
	// qs.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.LOWER,
	// "ENGINE__ENGINENAME", "low_database_name"));
	// if(engineType != null && !engineType.isEmpty()) {
	// qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINETYPE",
	// "==", engineType));
	// }
	// if(engineIdFilters != null && !engineIdFilters.isEmpty()) {
	// qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINEID",
	// "==", engineIdFilters));
	// }
	// // optional word filter on the engine name
	// if(hasSearchTerm) {
	// securityDb.getQueryUtil().appendSearchRegexFilter(qs, "ENGINE__ENGINENAME",
	// searchTerm);
	// }
	// // filtering by enginemeta key-value pairs (i.e. <tag>:value): for each pair,
	// add in-filter against engineids from subquery
	// if (engineMetadataFilter!=null && !engineMetadataFilter.isEmpty()) {
	// for (String k : engineMetadataFilter.keySet()) {
	// SelectQueryStruct subQs = new SelectQueryStruct();
	// subQs.addSelector(new QueryColumnSelector("ENGINEMETA__ENGINEID"));
	// subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEMETA__METAKEY",
	// "==", k));
	// subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEMETA__METAVALUE",
	// "==", engineMetadataFilter.get(k)));
	// qs.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery("ENGINE__ENGINEID",
	// "==", subQs));
	// }
	// }
	// qs.addRelation("ENGINE", "ENGINEPERMISSION", "left.outer.join");
	// // add the sort
	// qs.addOrderBy(new QueryColumnOrderBySelector("low_database_name"));
	//
	// Long long_limit = -1L;
	// Long long_offset = -1L;
	// if(limit != null && !limit.trim().isEmpty()) {
	// long_limit = ((Number) Double.parseDouble(limit)).longValue();
	// }
	// if(offset != null && !offset.trim().isEmpty()) {
	// long_offset = ((Number) Double.parseDouble(offset)).longValue();
	// }
	// qs.setLimit(long_limit);
	// qs.setOffSet(long_offset);
	//
	// return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	// }

	/**
	 * Get the list of the engine information that the user has access to
	 * 
	 * @param userId
	 * @return
	 */
	public static List<Map<String, Object>> getUserEngineList(User user, String engineFilter,
			List<String> engineTypeFilter) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEDISPLAYNAME", "engine_display_name"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID", "engine_id"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINENAME", "engine_name"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINETYPE", "engine_type"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINESUBTYPE", "engine_subtype"));
		qs.addSelector(new QueryColumnSelector("ENGINE__TOOL_APP", "engine_tool_app"));
		qs.addSelector(new QueryColumnSelector("ENGINE__COST", "engine_cost"));
		qs.addSelector(new QueryColumnSelector("ENGINE__DISCOVERABLE", "engine_discoverable"));
		qs.addSelector(new QueryColumnSelector("ENGINE__GLOBAL", "engine_global"));
		qs.addSelector(new QueryColumnSelector("ENGINE__CREATEDBY", "engine_created_by"));
		qs.addSelector(new QueryColumnSelector("ENGINE__CREATEDBYTYPE", "engine_created_by_type"));
		qs.addSelector(new QueryColumnSelector("ENGINE__DATECREATED", "engine_date_created"));
		// legacy aliases (database_*): kept for backwards compatibility
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID", "database_id"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINENAME", "database_name"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINETYPE", "database_type"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINESUBTYPE", "database_subtype"));
		qs.addSelector(new QueryColumnSelector("ENGINE__TOOL_APP", "database_tool_app"));
		qs.addSelector(new QueryColumnSelector("ENGINE__COST", "database_cost"));
		qs.addSelector(new QueryColumnSelector("ENGINE__DISCOVERABLE", "database_discoverable"));
		qs.addSelector(new QueryColumnSelector("ENGINE__GLOBAL", "database_global"));
		qs.addSelector(new QueryColumnSelector("ENGINE__CREATEDBY", "database_created_by"));
		qs.addSelector(new QueryColumnSelector("ENGINE__CREATEDBYTYPE", "database_created_by_type"));
		qs.addSelector(new QueryColumnSelector("ENGINE__DATECREATED", "database_date_created"));
		QueryFunctionSelector fun = new QueryFunctionSelector();
		fun.setFunction(QueryFunctionHelper.LOWER);
		fun.addInnerSelector(new QueryColumnSelector("ENGINE__ENGINENAME"));
		fun.setAlias("low_engine_name");
		qs.addSelector(fun);
		if (engineFilter != null && !engineFilter.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINEID", "==", engineFilter));
		}
		if (engineTypeFilter != null && !engineTypeFilter.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINETYPE", "==", engineTypeFilter));
		}
		boolean addGroupProjectPermissionJoin = false;
		{
			OrQueryFilter orFilter = new OrQueryFilter();
			orFilter.addFilter(
					SimpleQueryFilter.makeColToValFilter("ENGINE__GLOBAL", "==", true, PixelDataType.BOOLEAN));
			orFilter.addFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__DISCOVERABLE", "==",
					Arrays.asList(true, null), PixelDataType.BOOLEAN));
			orFilter.addFilter(
					SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", getUserFiltersQs(user)));
			Collection<String> groupIds = getUserGroupFiltersQs(user);
			if (!groupIds.isEmpty()) {
				addGroupProjectPermissionJoin = true;
				orFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPENGINEPERMISSION__ID", "==", groupIds));
			}
			qs.addExplicitFilter(orFilter);
		}
		qs.addRelation("ENGINE", "ENGINEPERMISSION", "left.outer.join");
		if (addGroupProjectPermissionJoin) {
			qs.addRelation("ENGINE", "GROUPENGINEPERMISSION", "left.outer.join");
		}
		qs.addOrderBy(new QueryColumnOrderBySelector("low_engine_name"));

		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}

	/**
	 * Get the list of the database information that the user has access to
	 * 
	 * @param user
	 * @param engineTypeFilter
	 * @return
	 */
	public static List<Map<String, Object>> getUserEngineList(User user, List<String> engineTypeFilter, Integer limit,
			Integer offset) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		Collection<String> userIds = getUserFiltersQs(user);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID", "app_id"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINENAME", "app_name"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINETYPE", "app_type"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINESUBTYPE", "app_subtype"));
		qs.addSelector(new QueryColumnSelector("ENGINE__COST", "app_cost"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID", "database_id"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINENAME", "database_name"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINETYPE", "database_type"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINESUBTYPE", "database_subtype"));
		qs.addSelector(new QueryColumnSelector("ENGINE__COST", "database_cost"));
		QueryFunctionSelector fun = new QueryFunctionSelector();
		fun.setFunction(QueryFunctionHelper.LOWER);
		fun.addInnerSelector(new QueryColumnSelector("ENGINE__ENGINENAME"));
		fun.setAlias("low_database_name");
		qs.addSelector(fun);
		if (engineTypeFilter != null && !engineTypeFilter.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINETYPE", "==", engineTypeFilter));
		}
		{
			OrQueryFilter orFilter = new OrQueryFilter();
			orFilter.addFilter(
					SimpleQueryFilter.makeColToValFilter("ENGINE__GLOBAL", "==", true, PixelDataType.BOOLEAN));
			orFilter.addFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", userIds));
			qs.addExplicitFilter(orFilter);
		}
		{
			SelectQueryStruct subQs = new SelectQueryStruct();
			// store first and fill in sub query after
			qs.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery("ENGINE__ENGINEID", "!=", subQs));

			// fill in the sub query with the necessary column output + filters
			subQs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__ENGINEID"));
			subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__VISIBILITY", "==", false,
					PixelDataType.BOOLEAN));
			subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", userIds));
		}
		// joins
		qs.addRelation("ENGINE", "ENGINEPERMISSION", "left.outer.join");
		qs.addOrderBy(new QueryColumnOrderBySelector("low_database_name"));

		// add the limit and offset
		if (limit != null && limit > 0) {
			qs.setLimit(limit);
		}
		if (offset != null && offset > 0) {
			qs.setOffSet(offset);
		}

		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}

	/**
	 * Get the list of the engine information that the user has access to
	 * 
	 * @param userId
	 * @return
	 */
	public static List<Map<String, Object>> getDiscoverableEngineList(String engineFilter,
			List<String> engineTypeFilter) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEDISPLAYNAME", "engine_display_name"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID", "engine_id"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINENAME", "engine_name"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINETYPE", "engine_type"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINESUBTYPE", "engine_subtype"));
		qs.addSelector(new QueryColumnSelector("ENGINE__COST", "engine_cost"));
		qs.addSelector(new QueryColumnSelector("ENGINE__CREATEDBY", "engine_created_by"));
		qs.addSelector(new QueryColumnSelector("ENGINE__CREATEDBYTYPE", "engine_created_by_type"));
		qs.addSelector(new QueryColumnSelector("ENGINE__DATECREATED", "engine_date_created"));
		// legacy aliases (database_*): kept for backwards compatibility
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID", "database_id"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINENAME", "database_name"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINETYPE", "database_type"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINESUBTYPE", "database_subtype"));
		qs.addSelector(new QueryColumnSelector("ENGINE__COST", "database_cost"));
		qs.addSelector(new QueryColumnSelector("ENGINE__CREATEDBY", "database_created_by"));
		qs.addSelector(new QueryColumnSelector("ENGINE__CREATEDBYTYPE", "database_created_by_type"));
		qs.addSelector(new QueryColumnSelector("ENGINE__DATECREATED", "database_date_created"));
		QueryFunctionSelector fun = new QueryFunctionSelector();
		fun.setFunction(QueryFunctionHelper.LOWER);
		fun.addInnerSelector(new QueryColumnSelector("ENGINE__ENGINENAME"));
		fun.setAlias("low_engine_name");
		qs.addSelector(fun);
		if (engineFilter != null && !engineFilter.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINEID", "==", engineFilter));
		}
		if (engineTypeFilter != null && !engineTypeFilter.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINETYPE", "==", engineTypeFilter));
		}
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("ENGINE__DISCOVERABLE", "==", true, PixelDataType.BOOLEAN));
		qs.addOrderBy(new QueryColumnOrderBySelector("low_engine_name"));
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}

	/**
	 * Get the list of the database information that the user does not have access
	 * to, but is discoverable
	 * 
	 * @param user
	 * @param engineTypes
	 * @param engineFilters
	 * @param engineMetadataFilter
	 * @param searchTerm
	 * @param limit
	 * @param offset
	 * @return
	 */
	public static List<Map<String, Object>> getUserDiscoverableEngineList(User user, List<String> engineTypes,
			List<String> engineFilters, Map<String, Object> engineMetadataFilter, String searchTerm, String limit,
			String offset) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		Collection<String> userIds = getUserFiltersQs(user);

		boolean hasSearchTerm = searchTerm != null && !(searchTerm = searchTerm.trim()).isEmpty();

		SelectQueryStruct qs1 = new SelectQueryStruct();
		// selectors
		qs1.addSelector(new QueryColumnSelector("ENGINE__ENGINEID", "engine_id"));
		qs1.addSelector(new QueryColumnSelector("ENGINE__ENGINENAME", "engine_name"));
		qs1.addSelector(new QueryColumnSelector("ENGINE__ENGINEDISPLAYNAME", "engine_display_name"));
		qs1.addSelector(new QueryColumnSelector("ENGINE__ENGINETYPE", "engine_type"));
		qs1.addSelector(new QueryColumnSelector("ENGINE__ENGINESUBTYPE", "engine_subtype"));
		qs1.addSelector(new QueryColumnSelector("ENGINE__COST", "engine_cost"));
		qs1.addSelector(new QueryColumnSelector("ENGINE__DISCOVERABLE", "engine_discoverable"));
		qs1.addSelector(new QueryColumnSelector("ENGINE__GLOBAL", "engine_global"));
		qs1.addSelector(new QueryColumnSelector("ENGINE__CREATEDBY", "engine_created_by"));
		qs1.addSelector(new QueryColumnSelector("ENGINE__CREATEDBYTYPE", "engine_created_by_type"));
		qs1.addSelector(new QueryColumnSelector("ENGINE__DATECREATED", "engine_date_created"));
		qs1.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.LOWER, "ENGINE__ENGINENAME",
				"low_engine_name"));
		// only care about discoverable engines
		qs1.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("ENGINE__DISCOVERABLE", "==", true, PixelDataType.BOOLEAN));
		qs1.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("ENGINE__GLOBAL", "==", false, PixelDataType.BOOLEAN));
		// remove user permission access
		{
			SelectQueryStruct subQsUser = new SelectQueryStruct();
			subQsUser.addSelector(new QueryColumnSelector("ENGINEPERMISSION__ENGINEID"));
			subQsUser
					.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", userIds));
			qs1.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery("ENGINE__ENGINEID", "!=", subQsUser));
		}
		{
			// remove group permission access
			SelectQueryStruct subQsGroup = new SelectQueryStruct();
			subQsGroup.addSelector(new QueryColumnSelector("GROUPENGINEPERMISSION__ENGINEID"));
			OrQueryFilter orFilter = new OrQueryFilter();
			List<AuthProvider> logins = user.getLogins();
			for (AuthProvider login : logins) {
				if (user.getAccessToken(login).getUserGroups().isEmpty()) {
					continue;
				}
				AndQueryFilter andFilter = new AndQueryFilter();
				andFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPENGINEPERMISSION__TYPE", "==",
						user.getAccessToken(login).getUserGroupType()));
				andFilter.addFilter(SimpleQueryFilter.makeColToValFilter("GROUPENGINEPERMISSION__ID", "==",
						user.getAccessToken(login).getUserGroups()));
				orFilter.addFilter(andFilter);
			}
			if (!orFilter.isEmpty()) {
				subQsGroup.addExplicitFilter(orFilter);
				qs1.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery("ENGINE__ENGINEID", "!=", subQsGroup));
			}
		}
		// filters
		if (engineFilters != null && !engineFilters.isEmpty()) {
			qs1.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINEID", "==", engineFilters));
		}
		if (engineTypes != null && !engineTypes.isEmpty()) {
			qs1.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINETYPE", "==", engineTypes));
		}

		// optional word filter on the engine name
		if (hasSearchTerm) {
			OrQueryFilter searchFilter = new OrQueryFilter();
			searchFilter.addFilter(securityDb.getQueryUtil().getSearchRegexFilter("ENGINE__ENGINENAME", searchTerm));
			searchFilter.addFilter(securityDb.getQueryUtil().getSearchRegexFilter("ENGINE__ENGINEID", searchTerm));
			searchFilter
					.addFilter(securityDb.getQueryUtil().getSearchRegexFilter("ENGINE__ENGINEDISPLAYNAME", searchTerm));
			qs1.addExplicitFilter(searchFilter);
		}
		// filtering by enginemeta key-value pairs (i.e. <tag>:value): for each pair,
		// add in-filter against engineids from subquery
		if (engineMetadataFilter != null && !engineMetadataFilter.isEmpty()) {
			for (String k : engineMetadataFilter.keySet()) {
				SelectQueryStruct subQs = new SelectQueryStruct();
				subQs.addSelector(new QueryColumnSelector("ENGINEMETA__ENGINEID"));
				subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEMETA__METAKEY", "==", k));
				subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEMETA__METAVALUE", "==",
						engineMetadataFilter.get(k)));
				qs1.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery("ENGINE__ENGINEID", "==", subQs));
			}
		}

		// add the sort
		qs1.addOrderBy(new QueryColumnOrderBySelector("low_engine_name"));

		Long long_limit = -1L;
		Long long_offset = -1L;
		if (limit != null && !limit.trim().isEmpty()) {
			long_limit = ((Number) Double.parseDouble(limit)).longValue();
		}
		if (offset != null && !offset.trim().isEmpty()) {
			long_offset = ((Number) Double.parseDouble(offset)).longValue();
		}
		qs1.setLimit(long_limit);
		qs1.setOffSet(long_offset);

		return QueryExecutionUtility.flushRsToMap(securityDb, qs1);
	}

	/**
	 * Get user engines + global engines
	 * 
	 * @param userId
	 * @return
	 */
	public static List<String> getFullUserEngineIds(User user) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		// String userFilters = getUserFilters(user);
		// String query = "SELECT DISTINCT ENGINEID FROM ENGINEPERMISSION WHERE USERID
		// IN " + userFilters;
		// IRawSelectWrapper wrapper =
		// WrapperManager.getInstance().getRawWrapper(securityDb, query);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__ENGINEID"));
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", getUserFiltersQs(user)));
		List<String> databaseList = QueryExecutionUtility.flushToListString(securityDb, qs);
		databaseList.addAll(SecurityEngineUtils.getGlobalEngineIds());
		return databaseList.stream().distinct().sorted().collect(Collectors.toList());
	}

	/**
	 * Get the visual user databases
	 * 
	 * @param userId
	 * @return
	 */
	public static List<String> getVisibleUserDatabaseIds(User user) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		Collection<String> userIds = getUserFiltersQs(user);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID"));
		{
			OrQueryFilter orFilter = new OrQueryFilter();
			orFilter.addFilter(
					SimpleQueryFilter.makeColToValFilter("ENGINE__GLOBAL", "==", true, PixelDataType.BOOLEAN));
			orFilter.addFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", userIds));
			qs.addExplicitFilter(orFilter);
		}
		{
			SelectQueryStruct subQs = new SelectQueryStruct();
			// store first and fill in sub query after
			qs.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery("ENGINE__ENGINEID", "!=", subQs));

			// fill in the sub query with the necessary column output + filters
			subQs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__ENGINEID"));
			subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__VISIBILITY", "==", false,
					PixelDataType.BOOLEAN));
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
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINEMETAKEYS__METAKEY", "metakey"));
		qs.addSelector(new QueryColumnSelector("ENGINEMETAKEYS__SINGLEMULTI", "single_multi"));
		qs.addSelector(new QueryColumnSelector("ENGINEMETAKEYS__DISPLAYORDER", "display_order"));
		qs.addSelector(new QueryColumnSelector("ENGINEMETAKEYS__DISPLAYOPTIONS", "display_options"));
		qs.addSelector(new QueryColumnSelector("ENGINEMETAKEYS__DEFAULTVALUES", "display_values"));
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
	public static boolean updateMetakeyOptions(List<Map<String, Object>> metaoptions) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		boolean valid = false;
		PreparedStatement insertPs = null;
		String tableName = "ENGINEMETAKEYS";
		try {
			// first truncate table clean
			String truncateSql = "DELETE FROM " + tableName + " WHERE 1=1";
			securityDb.removeData(truncateSql);
			insertPs = securityDb.bulkInsertPreparedStatement(new Object[] { tableName, Constants.METAKEY,
					Constants.SINGLE_MULTI, Constants.DISPLAY_ORDER, Constants.DISPLAY_OPTIONS });
			// then insert latest options
			for (int i = 0; i < metaoptions.size(); i++) {
				insertPs.setString(1, (String) metaoptions.get(i).get("metakey"));
				insertPs.setString(2, (String) metaoptions.get(i).get("singlemulti"));
				insertPs.setInt(3, ((Number) metaoptions.get(i).get("order")).intValue());
				insertPs.setString(4, (String) metaoptions.get(i).get("displayoptions"));
				insertPs.addBatch();
			}
			insertPs.executeBatch();
			if (!insertPs.getConnection().getAutoCommit()) {
				insertPs.getConnection().commit();
			}
			valid = true;
		} catch (Exception e) {
			classLogger.error("Failed to update metadata key options", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, insertPs);
		}
		return valid;
	}

	/**
	 * Get the engine user permission restriction
	 * 
	 * @param user
	 * @param engineId
	 * @return
	 */
	public static List<Map<String, Object>> getEngineUsagePermissionMap(User user, String engineId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		if (user == null || engineId == null || engineId.trim().isEmpty()) {
			return null;
		}

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__ID", "id"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__TYPE", "type"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__NAME", "name"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__EMAIL", "email"));
		qs.addSelector(
				new QueryColumnSelector("SMSS_USER__MODELUSAGERESTRICTION", Constants.USER_USAGE_RESTRICTION_KEY));
		qs.addSelector(
				new QueryColumnSelector("SMSS_USER__MODELUSAGEFREQUENCY", Constants.USER_MODEL_USAGE_FREQUENCY_KEY));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__MODELMAXTOKENS", Constants.USER_MODEL_MAX_TOKEN_KEY));
		qs.addSelector(
				new QueryColumnSelector("SMSS_USER__MODELMAXRESPONSETIME", Constants.USER_MODEL_MAX_RESPONSE_TIME_KEY));

		qs.addSelector(
				new QueryColumnSelector("ENGINEPERMISSION__USAGERESTRICTION", Constants.ENGINE_USAGE_RESTRICTION_KEY));
		qs.addSelector(
				new QueryColumnSelector("ENGINEPERMISSION__USAGEFREQUENCY", Constants.ENGINE_USAGE_FREQUENCY_KEY));
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__MAXTOKENS", Constants.ENGINE_MAX_TOKEN_KEY));
		qs.addSelector(
				new QueryColumnSelector("ENGINEPERMISSION__MAXRESPONSETIME", Constants.ENGINE_MAX_RESPONSE_TIME_KEY));

		// filter to the engine
		Pair<String, String> userDetails = User.getPrimaryUserIdAndTypePair(user);
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", userDetails.getValue0()));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__ENGINEID", "==", engineId));
		// relationship between SMSS_USER and ENGINEPERMISSION tables
		qs.addRelation("SMSS_USER", "ENGINEPERMISSION", "left.outer.join");

		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}

	/**
	 * Get a list of engine IDs where USAGERESTRICTION is set (not empty or null)
	 * 
	 * @param user
	 * @param engineId
	 * @return
	 */
	public static List<String> getModelEngineIdsWithRestrictions(User user, String engineId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		if (user == null || engineId == null || engineId.trim().isEmpty()) {
			return null;
		}

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__ENGINEID", "engineId"));
		Pair<String, String> userDetails = User.getPrimaryUserIdAndTypePair(user);
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", userDetails.getValue0()));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USAGERESTRICTION", "!=",
				Arrays.asList("", null)));
		qs.addRelation("SMSS_USER", "ENGINEPERMISSION", "left.outer.join");

		return QueryExecutionUtility.flushToListString(securityDb, qs);
	}

	/**
	 * Updates the permissions for a user on specific engines by replacing existing
	 * permissions.
	 *
	 * @param user
	 * @param enginePermissions
	 * @throws Exception
	 */
	public static List<Map<String, Object>> updateEngineUserPermissions(User user,
			List<Map<String, Object>> enginePermissions) throws Exception {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		List<Map<String, Object>> newEngines = new ArrayList<>();
		Pair<String, String> userDetails = User.getPrimaryUserIdAndTypePair(user);
		PreparedStatement deletePs = null;
		PreparedStatement insertPs = null;
		try {
			// Step 1: Delete existing permissions for the engine
			String deleteQuery = "DELETE FROM ENGINEPERMISSION WHERE USERID = ?";
			deletePs = securityDb.getPreparedStatement(deleteQuery);
			deletePs.setString(1, userDetails.getValue0());
			deletePs.execute();
			if (!deletePs.getConnection().getAutoCommit()) {
				deletePs.getConnection().commit();
			}

			if (enginePermissions != null && !enginePermissions.isEmpty()) {
				String insertQuery = "INSERT INTO ENGINEPERMISSION (USERID, PERMISSION, ENGINEID, DATEADDED) VALUES (?, ?, ?, ?)";
				insertPs = securityDb.getPreparedStatement(insertQuery);

				Timestamp currentTimestamp = Utility.getCurrentSqlTimestampUTC();
				// loop through to add the new permissions
				for (Map<String, Object> permissionMap : enginePermissions) {
					String engineId = (String) permissionMap.get("engineId");
					String engineName = (String) permissionMap.get("engineName");
					IEngine.CATALOG_TYPE engineType = (IEngine.CATALOG_TYPE) permissionMap.get("engineType");
					String engineSubType = (String) permissionMap.get("engineSubType");
					String permission = (String) permissionMap.get("permission");

					// Step 2: Validate EngineId exist in Engine table or not
					boolean engineExists = engineExists(engineId);
					if (!engineExists) {
						newEngines.add(permissionMap);
						addEngine(engineId, engineName, engineType, engineSubType, "", false, null);
					}

					// Step 3: Insert new permissions
					insertPs.setString(1, userDetails.getValue0());
					insertPs.setInt(2, AccessPermissionEnum.getIdByPermission(permission));
					insertPs.setString(3, engineId);
					insertPs.setTimestamp(4, currentTimestamp);
					insertPs.addBatch();
				}

				insertPs.executeBatch();
				if (!insertPs.getConnection().getAutoCommit()) {
					insertPs.getConnection().commit();
				}
			}

			return newEngines;
		} catch (Exception e) {
			classLogger.error("Failed to update engine user permissions", e);
			throw new IllegalArgumentException("An error occurred while updating the user engine permissions in db ");
		} finally {
			ConnectionUtils.closeAllDbConnectionsIfPooling(securityDb, deletePs, insertPs);
		}
	}

	public static void updateEngineToolApp(String engineId, String projectId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		PreparedStatement ps = null;
		try {
			String query = "UPDATE ENGINE SET TOOL_APP = ? WHERE ENGINEID = ?";
			ps = securityDb.getPreparedStatement(query);
			ps.setString(1, projectId);
			ps.setString(2, engineId);
			ps.executeUpdate();

			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error("Failed to update engine legacy tool_app alias", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * Get userDetails by using user's engine access request
	 * 
	 * @param requestId
	 * @return List of user details
	 */
	public static List<Map<String, Object>> getUserDetailsFromEngineAccessRequest(String engineRequestId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINEACCESSREQUEST__REQUEST_USERID", "userId"));
		qs.addSelector(new QueryColumnSelector("ENGINEACCESSREQUEST__REQUEST_TYPE", "type"));
		qs.addSelector(new QueryColumnSelector("PERMISSION__NAME", "permission"));
		qs.addSelector(new QueryColumnSelector("ENGINEACCESSREQUEST__PERMISSION", "permission"));
		qs.addRelation("ENGINEACCESSREQUEST__PERMISSION", "PERMISSION__ID", "inner.join");
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEACCESSREQUEST__ID", "==", engineRequestId));
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}

	/**
	 * Get all authors for a specific engine (for engine-related notifications)
	 * 
	 * @param engineId
	 * @return
	 */
	public static List<Map<String, Object>> getEngineAuthors(String engineId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__ID", "userId"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__TYPE", "userType"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__ENGINEID", "==", engineId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__PERMISSION", "==", 1));
		qs.addRelation("SMSS_USER", "ENGINEPERMISSION", "inner.join");

		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}

	/**
	 * 
	 * @param engineIds
	 * @return
	 */
	public static Map<String, String> getEngineNamesByIds(Collection<String> engineIds) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		Map<String, String> engineMap = new HashMap<>();
		if (engineIds == null || engineIds.isEmpty()) {
			return engineMap;
		}

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID", "id"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINENAME", "name"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINEID", "==", engineIds));

		List<Map<String, Object>> resultList = QueryExecutionUtility.flushRsToMap(securityDb, qs);
		if (resultList != null) {
			for (Map<String, Object> row : resultList) {
				engineMap.put(String.valueOf(row.get("id")), String.valueOf(row.get("name")));
			}
		}
		return engineMap;
	}

}
