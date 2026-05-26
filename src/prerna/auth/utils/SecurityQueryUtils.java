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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
import prerna.sablecc2.om.PixelDataType;
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;

public class SecurityQueryUtils extends AbstractSecurityUtils {

	private static final Logger classLogger = LogManager.getLogger(SecurityQueryUtils.class);

	/**
	 * Try to reconcile and get the engine id
	 * 
	 * @return
	 */
	@Deprecated
	public static String testUserEngineIdForAlias(User user, String potentialId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		List<String> ids = new ArrayList<String>();

//		String userFilters = getUserFilters(user);
//		String query = "SELECT DISTINCT ENGINEPERMISSION.ENGINEID "
//				+ "FROM ENGINEPERMISSION INNER JOIN ENGINE ON ENGINE.ENGINEID=ENGINEPERMISSION.ENGINEID "
//				+ "WHERE ENGINE.ENGINENAME='" + potentialId + "' AND ENGINEPERMISSION.USERID IN " + userFilters;
//		
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__ENGINEID"));
		OrQueryFilter nameOrDisplayFilter1 = new OrQueryFilter();
		nameOrDisplayFilter1.addFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINENAME", "==", potentialId));
		nameOrDisplayFilter1
				.addFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINEDISPLAYNAME", "==", potentialId));
		qs.addExplicitFilter(nameOrDisplayFilter1);
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", getUserFiltersQs(user)));
		qs.addRelation("ENGINE", "ENGINEPERMISSION", "inner.join");

		ids = QueryExecutionUtility.flushToListString(securityDb, qs);
		if (ids.isEmpty()) {
//			query = "SELECT DISTINCT ENGINE.ENGINEID FROM ENGINE WHERE ENGINE.ENGINENAME='" + potentialId + "' AND ENGINE.GLOBAL=TRUE";

			qs = new SelectQueryStruct();
			qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID"));
			OrQueryFilter nameOrDisplayFilter2 = new OrQueryFilter();
			nameOrDisplayFilter2
					.addFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINENAME", "==", potentialId));
			nameOrDisplayFilter2
					.addFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINEDISPLAYNAME", "==", potentialId));
			qs.addExplicitFilter(nameOrDisplayFilter2);
			qs.addExplicitFilter(
					SimpleQueryFilter.makeColToValFilter("ENGINE__GLOBAL", "==", true, PixelDataType.BOOLEAN));

			ids = QueryExecutionUtility.flushToListString(securityDb, qs);
		}

		if (ids.size() == 1) {
			potentialId = ids.get(0);
		} else if (ids.size() > 1) {
			throw new IllegalArgumentException("There are 2 databases with the name " + potentialId
					+ ". Please pass in the correct id to know which source you want to load from");
		}

		return potentialId;
	}

	/**
	 * Get the insight alias for a id
	 * 
	 * @return
	 */
	public static String getInsightNameForId(String projectId, String insightId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("INSIGHT__INSIGHTNAME"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHT__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHT__INSIGHTID", "==", insightId));
		List<String> results = QueryExecutionUtility.flushToListString(securityDb, qs);
		if (results.isEmpty()) {
			return null;
		}
		return results.get(0);
	}

	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////

	public static SemossDate getLastModifiedDateForInsightInProject(String projectId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
//		String query = "SELECT DISTINCT INSIGHT.LASTMODIFIEDON "
//				+ "FROM INSIGHT "
//				+ "WHERE INSIGHT.PROJECTID='" + projectId + "'"
//				+ "ORDER BY INSIGHT.LASTMODIFIEDON DESC LIMIT 1"
//				;
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("INSIGHT__LASTMODIFIEDON"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHT__PROJECTID", "==", projectId));
		qs.addOrderBy(new QueryColumnOrderBySelector("INSIGHT__LASTMODIFIEDON", "DESC"));
		qs.setLimit(1);

		SemossDate date = null;
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (wrapper.hasNext()) {
				Object[] row = wrapper.next().getValues();
				try {
					date = (SemossDate) row[0];
				} catch (Exception e) {
					// ignore
				}
			}
		} catch (Exception e) {
			classLogger.error("Unable to retrieve last modified date for insight in project.", e);
		}

		return date;
	}

	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////

	/*
	 * Querying user data
	 */

	/**
	 * Get user info from ids
	 * 
	 * @param userIds
	 * @return
	 * @throws IllegalArgumentException
	 */
	public static Map<String, Map<String, Object>> getUserInfo(List<String> userIds) throws IllegalArgumentException {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
//		String query = "SELECT DISTINCT ID, NAME, USERNAME, EMAIL, TYPE, ADMIN FROM SMSS_USER ";
//		String userFilter = createFilter(userIds);
//		query += " WHERE ID " + userFilter + ";";
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__ID"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__NAME"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__USERNAME"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__EMAIL"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__TYPE"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__ADMIN"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__PUBLISHER"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__EXPORTER"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__PHONE"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__PHONEEXTENSION"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__COUNTRYCODE"));

		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__ID", "==", userIds));

		Map<String, Map<String, Object>> userMap = new HashMap<>();
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			String[] names = wrapper.getHeaders();
			if (wrapper.hasNext()) {
				Object[] values = wrapper.next().getValues();
				Map<String, Object> userInfo = new HashMap<>();
				String userId = values[0].toString();
				userInfo.put(names[0], userId);
				userInfo.put(names[1], values[1].toString());
				userInfo.put(names[2], values[2].toString());
				userInfo.put(names[3], values[3].toString());
				userInfo.put(names[4], values[4].toString());
				userInfo.put(names[5], values[5].toString());
				userInfo.put(names[6], values[6].toString());
				userInfo.put(names[7], values[7].toString());
				userInfo.put(names[8], values[8].toString());
				userInfo.put(names[9], values[9].toString());
				userInfo.put(names[10], values[10].toString());
				userMap.put(userId, userInfo);
			}
		} catch (Exception e) {
			classLogger.error("Unable to retrieve user info.", e);
		}

		return userMap;
	}

	/**
	 * Get the total number of users
	 * 
	 * @return
	 */
	public static int getApplicationUserCount() {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		int userCount = 0;

		SelectQueryStruct qs = new SelectQueryStruct();
		QueryFunctionSelector fun = new QueryFunctionSelector();
		fun.addInnerSelector(new QueryColumnSelector("SMSS_USER__ID"));
		fun.setFunction(QueryFunctionHelper.COUNT);
		qs.addSelector(fun);

		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (wrapper.hasNext()) {
				userCount = ((Number) wrapper.next().getValues()[0]).intValue();
			}
		} catch (Exception e) {
			classLogger.error("Unable to retrieve application user count.", e);
		}

		return userCount;
	}

	//////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////

	/**
	 * Determine if the user has publisher rights
	 * 
	 * @param user
	 * @return
	 */
	public static boolean userIsPublisher(User user) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
//		String userFilters = getUserFilters(user);
//		String query = "SELECT * FROM SMSS_USER WHERE PUBLISHER=TRUE AND ID IN " + userFilters + " LIMIT 1;";
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__ID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__PUBLISHER", "==", "TRUE"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__ID", "==", getUserFiltersQs(user)));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			return wrapper.hasNext();
		} catch (Exception e) {
			classLogger.error("Unable to verify whether the user has publisher access.", e);
		}

		return false;
	}

	/**
	 * Determine if the user has exporting rights
	 * 
	 * @param user
	 * @return
	 */
	public static boolean userIsExporter(User user) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__ID"));
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("SMSS_USER__EXPORTER", "==", true, PixelDataType.BOOLEAN));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__ID", "==", getUserFiltersQs(user)));

		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			return wrapper.hasNext();
		} catch (Exception e) {
			classLogger.error("Unable to verify whether the user has exporter access.", e);
		}

		return false;
	}

	/**
	 * Search if there's users containing 'searchTerm' in their email or name
	 * 
	 * @param searchTerm
	 * @return
	 */
	public static List<Map<String, Object>> searchForUser(String searchTerm) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
//		String query = "SELECT DISTINCT SMSS_USER.ID AS ID, SMSS_USER.NAME AS NAME, SMSS_USER.EMAIL AS EMAIL FROM SMSS_USER "
//				+ "WHERE UPPER(SMSS_USER.NAME) LIKE UPPER('%" + searchTerm + "%') "
//				+ "OR UPPER(SMSS_USER.EMAIL) LIKE UPPER('%" + searchTerm + "%') "
//				+ "OR UPPER(SMSS_USER.ID) LIKE UPPER('%" + searchTerm + "%');";
//
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__ID", "id"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__NAME", "name"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__EMAIL", "email"));

		OrQueryFilter orFilter = new OrQueryFilter();
		orFilter.addFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__NAME", "?like", searchTerm));
		orFilter.addFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__EMAIL", "?like", searchTerm));
		orFilter.addFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__ID", "?like", searchTerm));
		qs.addExplicitFilter(orFilter);

		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}

	/*
	 * Check properties of user
	 */

	/**
	 * Check if a user (user name or email) exist in the security database
	 * 
	 * @param username
	 * @param email
	 * @return true if user is found otherwise false.
	 */
	public static boolean checkUserExist(String userId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
//		String query = "SELECT * FROM SMSS_USER WHERE ID='" + RdbmsQueryBuilder.escapeForSQLStatement(userId) + "'";
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__ID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__ID", "==", userId));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			return wrapper.hasNext();
		} catch (Exception e) {
			classLogger.error("Unable to verify whether the user exists.", e);
		}

		return false;
	}

	/**
	 * Check if a user (user name or email) exist in the security database
	 * 
	 * @param username
	 * @param email
	 * @return true if user is found otherwise false.
	 */
	public static boolean checkUserExist(String username, String email) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
//		String query = "SELECT * FROM SMSS_USER WHERE USERNAME='" + RdbmsQueryBuilder.escapeForSQLStatement(username) + 
//				"' OR EMAIL='" + RdbmsQueryBuilder.escapeForSQLStatement(email) + "'";
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__USERNAME"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__USERNAME", "==", username));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__EMAIL", "==", email));

		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			return wrapper.hasNext();
		} catch (Exception e) {
			classLogger.error("Unable to verify whether the user exists.", e);
		}

		return false;
	}

	public static boolean checkUserEmailExist(String email) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__USERNAME"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__EMAIL", "==", email));

		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			return wrapper.hasNext();
		} catch (Exception e) {
			classLogger.error("Unable to verify whether the user email exists.", e);
		}

		return false;
	}

	public static boolean checkUsernameExist(String username) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__USERNAME"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__USERNAME", "==", username));

		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			return wrapper.hasNext();
		} catch (Exception e) {
			classLogger.error("Unable to verify whether the username exists.", e);
		}

		return false;
	}

	/**
	 * Check if the user is of the type requested
	 * 
	 * @param userId String representing the id of the user to check
	 * @param type
	 */
	public static Boolean isUserType(String userId, AuthProvider type) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
//		String query = "SELECT NAME FROM SMSS_USER WHERE ID='" + RdbmsQueryBuilder.escapeForSQLStatement(userId) + "' AND TYPE = '"+ type + "';";
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__NAME"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__ID", "==", userId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__TYPE", "==", type.toString()));

		List<String[]> ret = QueryExecutionUtility.flushRsToListOfStrArray(securityDb, qs);
		if (!ret.isEmpty()) {
			return true;
		}
		return false;
	}

	/**
	 * Get an array containing a boolean for is locked, a semossdate for last login,
	 * a semossdate for last password change
	 * 
	 * @param userId
	 * @param type
	 * @return
	 */
	public static Object[] getUserLockAndLastLoginAndLastPassReset(String userId, AuthProvider type) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__LOCKED"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__LASTLOGIN"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__LASTPASSWORDRESET"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__ID", "==", userId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__TYPE", "==", type.toString()));

		List<Object[]> ret = QueryExecutionUtility.flushRsToListOfObjArray(securityDb, qs);
		if (!ret.isEmpty()) {
			return ret.get(0);
		}
		return new Object[qs.getSelectors().size()];
	}

	/**
	 * 
	 * @param userId
	 * @param engineId
	 * @return
	 */
	public static Map<String, Object> getEnginePermission(String userId, String engineId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", userId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__ENGINEID", "==", engineId));

		List<Map<String, Object>> results = QueryExecutionUtility.flushRsToMap(securityDb, qs);

		return (results != null && !results.isEmpty()) ? results.get(0) : null;
	}

	/**
	 * 
	 * @param userIds
	 * @return list of unlocked of user ids
	 */
	public static List<String> getUnlockedUsers(List<String> userIds) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct securityQs = new SelectQueryStruct();
		securityQs.addSelector(new QueryColumnSelector("SMSS_USER__ID"));
		securityQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__ID", "==", userIds));
		List<Boolean> falseFilter = new ArrayList<>();
		falseFilter.add(false);
		falseFilter.add(null);
		securityQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__LOCKED", "==", falseFilter));
		return QueryExecutionUtility.flushToListString(securityDb, securityQs);
	}
}
