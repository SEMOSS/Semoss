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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;

import com.google.common.collect.Lists;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;

public class SecurityUserUtils extends AbstractSecurityUtils {

	private static final Logger classLogger = LogManager.getLogger(SecurityUserUtils.class);

	/**
	 * Get the metadata for a specific user
	 * 
	 * @param userId
	 * @param userType
	 * @param metaKeys
	 * @param ignoreMarkdown
	 * @return
	 */
	public static Map<String, Collection<String>> getAggregateUserMetadata(String userId, AuthProvider userType,
			List<String> metaKeys, boolean ignoreMarkdown) {
		Map<String, Collection<String>> retMap = new HashMap<String, Collection<String>>();
		try (IRawSelectWrapper wrapper = getUserMetadataWrapper(Lists.newArrayList(userId),
				Lists.newArrayList(userType), metaKeys, ignoreMarkdown)) {
			while (wrapper.hasNext()) {
				Object[] data = wrapper.next().getValues();
				String metaKey = (String) data[2];
				String metaValue = (String) data[3];

				if (retMap.containsKey(metaKey)) {
					retMap.get(metaKey).add(metaValue);
				} else {
					retMap.put(metaKey, Lists.newArrayList(metaValue));
				}
			}
		} catch (Exception e) {
			classLogger.error("Unable to retrieve aggregate user metadata.", e);
		}

		return retMap;
	}

	/**
	 * 
	 * @param metakey
	 * @return
	 */
	public static List<Map<String, Object>> getMetakeyOptions(Collection<String> metakeys) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("USERMETAKEYS__METAKEY", "metakey"));
		qs.addSelector(new QueryColumnSelector("USERMETAKEYS__SINGLEMULTI", "single_multi"));
		qs.addSelector(new QueryColumnSelector("USERMETAKEYS__DISPLAYORDER", "display_order"));
		qs.addSelector(new QueryColumnSelector("USERMETAKEYS__DISPLAYOPTIONS", "display_options"));
		qs.addSelector(new QueryColumnSelector("USERMETAKEYS__DEFAULTVALUES", "display_values"));
		if (metakeys != null && !metakeys.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERMETAKEYS__METAKEY", "==", metakeys));
		}
		qs.addOrderBy("USERMETAKEYS__DISPLAYORDER");
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}

	/**
	 * Get the wrapper for additional user metadata
	 * 
	 * @param userIds
	 * @param userTypes
	 * @param metaKeys
	 * @param ignoreMarkdown
	 * @return
	 * @throws Exception
	 */
	public static IRawSelectWrapper getUserMetadataWrapper(Collection<String> userIds,
			Collection<AuthProvider> userTypes, List<String> metaKeys, boolean ignoreMarkdown) throws Exception {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		// selectors
		qs.addSelector(new QueryColumnSelector("USERMETA__USERID"));
		qs.addSelector(new QueryColumnSelector("USERMETA__TYPE"));
		qs.addSelector(new QueryColumnSelector("USERMETA__METAKEY"));
		qs.addSelector(new QueryColumnSelector("USERMETA__METAVALUE"));
		qs.addSelector(new QueryColumnSelector("USERMETA__METAORDER"));
		// filters
		if (userIds != null && !userIds.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERMETA__USERID", "==", userIds));
		}
		if (userTypes != null && !userTypes.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERMETA__TYPE", "==", userTypes));
		}
		if (metaKeys != null && !metaKeys.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERMETA__METAKEY", "==", metaKeys));
		}
		// exclude markdown metadata due to potential large data size
		if (ignoreMarkdown) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERMETA__METAKEY", "!=", Constants.MARKDOWN));
		}
		// order
		qs.addOrderBy("USERMETA__METAORDER");
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
		return wrapper;
	}

	/**
	 * Update user information.
	 * 
	 * @param user
	 * @param userInfo
	 * @return
	 * @throws IllegalArgumentException
	 */
	public static boolean editUser(User user, Map<String, Object> userInfo) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		Pair<String, String> loggedInUser = User.getPrimaryUserIdAndTypePair(user);

		String userId = loggedInUser.getValue0();
		String userType = loggedInUser.getValue1();

		// input fields
		String name = userInfo.get("name") != null ? userInfo.get("name").toString().trim() : "";
		String email = userInfo.get("newEmail") != null ? userInfo.get("newEmail").toString().trim().toLowerCase() : "";

		if (email != null && !email.isEmpty()) {
			try {
				validEmail(email, false);
			} catch (Exception e) {
				classLogger.error("Unable to update user.", e);
				throw new IllegalArgumentException("Email " + email + " is not valid");
			}
		}

		String[] whereCol = { "ID", "TYPE" };
		String[] columnsToUpdate = new String[] { "EMAIL", "NAME" };
		String editUserQuery = securityDb.getQueryUtil().createUpdatePreparedStatementString("SMSS_USER",
				columnsToUpdate, whereCol);
		PreparedStatement editUserPs = null;
		int updateCount = 0;
		try {
			editUserPs = securityDb.getPreparedStatement(editUserQuery);
			int i = 1;
			editUserPs.setString(i++, email);
			editUserPs.setString(i++, name);
			// Where
			editUserPs.setString(i++, userId);
			editUserPs.setString(i++, userType);
			updateCount = editUserPs.executeUpdate();
			if (!editUserPs.getConnection().getAutoCommit()) {
				editUserPs.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Unable to update user.", e);
			throw new IllegalArgumentException(e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, editUserPs);
		}
		if (updateCount > 0) {
			return true;
		}
		return false;
	}

	/**
	 * 
	 * @return
	 */
	public static List<String> getAllMetakeys() {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("USERMETAKEYS__METAKEY"));
		List<String> metakeys = QueryExecutionUtility.flushToListString(securityDb, qs);
		return metakeys;
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
		try {
			// first truncate table clean
			String truncateSql = "DELETE FROM USERMETAKEYS WHERE 1=1";
			securityDb.removeData(truncateSql);
			insertPs = securityDb.bulkInsertPreparedStatement(
					new Object[] { "USERMETAKEYS", Constants.METAKEY, Constants.SINGLE_MULTI, Constants.DISPLAY_ORDER,
							Constants.DISPLAY_OPTIONS, Constants.DEFAULT_VALUES });
			// then insert latest options
			for (int i = 0; i < metaoptions.size(); i++) {
				Map<String, Object> m = metaoptions.get(i);
				insertPs.setString(1, (String) m.get("metakey"));
				insertPs.setString(2, (String) m.get("single_multi"));
				Number n = ((Number) m.get("display_order"));
				if (n == null) {
					insertPs.setNull(3, java.sql.Types.INTEGER);
				} else {
					insertPs.setInt(3, n.intValue());
				}
				insertPs.setString(4, (String) m.get("display_options"));
				insertPs.setString(5, (String) m.get("display_values"));
				insertPs.addBatch();
			}
			insertPs.executeBatch();
			if (!insertPs.getConnection().getAutoCommit()) {
				insertPs.getConnection().commit();
			}
			valid = true;
		} catch (Exception e) {
			classLogger.error("Unable to update metakey options.", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, insertPs);
		}
		return valid;
	}

	/**
	 * Update the current user's metadata
	 * 
	 * @param user
	 * @param metaKey
	 * @param val
	 */
	public static void updateUserMetadata(User user, String metaKey, Object val) {
		AccessToken token = user.getPrimaryLoginToken();
		updateUserMetadata(token, metaKey, val);
		loadUserMetadata(token);
	}

	/**
	 * Update the current user's metadata
	 * 
	 * @param user
	 * @param metaKey
	 * @param val
	 */
	public static void updateUserMetadata(AccessToken token, String metaKey, Object val) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String userId = token.getId();
		String userType = token.getProvider().getLabel();
		String deleteQ = "DELETE FROM USERMETA WHERE USERID=? AND TYPE=? AND METAKEY=?";
		try (PreparedStatement ps = securityDb.getPreparedStatement(deleteQ)) {
			ps.setString(1, userId);
			ps.setString(2, userType);
			ps.setString(3, metaKey);
			ps.executeUpdate();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error("Unable to update user metadata.", e);
		}

		// now we do the new insert with the order of the tags
		String query = securityDb.getQueryUtil().createInsertPreparedStatementString("USERMETA",
				new String[] { "USERID", "TYPE", "METAKEY", "METAVALUE", "METAORDER" });
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			List<Object> values = new ArrayList<>();
			if (val instanceof Collection) {
				values.addAll((Collection<Object>) val);
			} else {
				values.add(val);
			}

			for (int i = 0; i < values.size(); i++) {
				int parameterIndex = 1;
				Object fieldVal = values.get(i);

				ps.setString(parameterIndex++, userId);
				ps.setString(parameterIndex++, userType);
				ps.setString(parameterIndex++, metaKey);
				ps.setString(parameterIndex++, fieldVal + "");
				ps.setInt(parameterIndex++, i);
				ps.addBatch();
			}
			ps.executeBatch();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Unable to update user metadata.", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * Load the user metadata from the db and set into the User object
	 * 
	 * @param user
	 */
	public static void loadUserMetadata(AccessToken token) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String userId = token.getId();
		String userType = token.getProvider().getLabel();

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("USERMETA__METAKEY"));
		qs.addSelector(new QueryColumnSelector("USERMETA__METAVALUE"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERMETA__USERID", "==", userId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERMETA__TYPE", "==", userType));
		qs.addOrderBy("USERMETA__METAORDER");

		Map<String, Collection<String>> metadata = new HashMap<>();
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			while (wrapper.hasNext()) {
				Object[] data = wrapper.next().getValues();
				String metaKey = (String) data[0];
				String metaValue = (String) data[1];
				if (metaValue == null) {
					continue;
				}

				if (metadata.containsKey(metaKey)) {
					Collection<String> array = metadata.get(metaKey);
					array.add(metaValue);
				} else {
					List<String> array = new ArrayList<>();
					array.add(metaValue);
					metadata.put(metaKey, array);
				}
			}
		} catch (Exception e) {
			classLogger.error("Unable to load user metadata.", e);
		}

		token.setMeta(metadata);
	}

	/**
	 * Update the current user's metadata
	 * 
	 * @param user
	 * @param metadata
	 */
	public static void updateUserMetadata(User user, Map<String, Collection<String>> metadata) {
		AccessToken token = user.getPrimaryLoginToken();
		updateUserMetadata(token, metadata);
		loadUserMetadata(token);
	}

	/**
	 * Update the current user's metadata
	 * 
	 * @param user
	 * @param metaKey
	 * @param val
	 */
	public static void updateUserMetadata(AccessToken token, Map<String, Collection<String>> metadata) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String userId = token.getId();
		String userType = token.getProvider().getLabel();
		String deleteQ = "DELETE FROM USERMETA WHERE USERID=? AND TYPE=? AND METAKEY=?";
		try (PreparedStatement ps = securityDb.getPreparedStatement(deleteQ)) {
			for (String metaKey : metadata.keySet()) {
				ps.setString(1, userId);
				ps.setString(2, userType);
				ps.setString(3, metaKey);
				ps.addBatch();
			}
			ps.executeBatch();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error("Unable to update user metadata.", e);
		}

		// now we do the new insert with the order of the tags
		String query = securityDb.getQueryUtil().createInsertPreparedStatementString("USERMETA",
				new String[] { "USERID", "TYPE", "METAKEY", "METAVALUE", "METAORDER" });
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			for (String metaKey : metadata.keySet()) {
				Collection<String> values = metadata.get(metaKey);

				int counter = 0;
				Iterator<String> it = values.iterator();
				while (it.hasNext()) {
					int parameterIndex = 1;
					Object fieldVal = it.next();
					ps.setString(parameterIndex++, userId);
					ps.setString(parameterIndex++, userType);
					ps.setString(parameterIndex++, metaKey);
					ps.setString(parameterIndex++, fieldVal + "");
					ps.setInt(parameterIndex++, counter++);
					ps.addBatch();
				}
			}
			ps.executeBatch();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Unable to update user metadata.", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * Get the userName and user email by using userId
	 * 
	 * @param userId
	 * @return userType
	 */
	public static List<Map<String, Object>> getUserNameEmailByUserId(String userId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__NAME", "userName"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__EMAIL", "userEmail"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__ID", "==", userId));
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}

	/**
	 * 
	 * @param userIds
	 * @return
	 */
	public static Map<String, String> getUserNamesByIds(Collection<String> userIds) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		Map<String, String> userMap = new HashMap<>();
		if (userIds == null || userIds.isEmpty()) {
			return userMap;
		}

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__ID", "id"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__NAME", "name"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__ID", "==", userIds));

		List<Map<String, Object>> resultList = QueryExecutionUtility.flushRsToMap(securityDb, qs);
		if (resultList != null) {
			for (Map<String, Object> row : resultList) {
				userMap.put(String.valueOf(row.get("id")), String.valueOf(row.get("name")));
			}
		}
		return userMap;
	}

}
