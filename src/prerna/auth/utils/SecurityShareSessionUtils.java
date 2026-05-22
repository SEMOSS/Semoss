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
import java.time.ZonedDateTime;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.date.SemossDate;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.ConnectionUtils;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;

public class SecurityShareSessionUtils extends AbstractSecurityUtils {

	/*
	 * This class is primarily being used for share the session We are now extending
	 * to also handle the authentication only w/o caring about redirect to the same
	 * session
	 */

	private static final Logger classLogger = LogManager.getLogger(SecurityShareSessionUtils.class);

	private static final String SESSION_SHARE_TABLE_NAME = "SESSION_SHARE";
	private static final String SHARE_VAL = "SHARE_VAL";
	private static final String SESSION_VAL = "SESSION_VAL";
	private static final String ROUTE_VAL = "ROUTE_VAL";
	private static final String DATE_ADDED = "DATE_ADDED";
	private static final String DATE_USED = "DATE_USED";
	private static final String USE_VALID = "USE_VALID";
	private static final String USERID = "USERID";
	private static final String TYPE = "TYPE";
	private static final String SESSION_SHARE = "IS_SESSION_SHARE";
	private static final String AUTH_SHARE = "IS_AUTH_SHARE";

	private static final String QS_SHARE_VAL = SESSION_SHARE_TABLE_NAME + "__" + SHARE_VAL;
	private static final String QS_SESSION_VAL = SESSION_SHARE_TABLE_NAME + "__" + SESSION_VAL;
	private static final String QS_ROUTE_VAL = SESSION_SHARE_TABLE_NAME + "__" + ROUTE_VAL;
	private static final String QS_DATE_ADDED = SESSION_SHARE_TABLE_NAME + "__" + DATE_ADDED;
	private static final String QS_DATE_USED = SESSION_SHARE_TABLE_NAME + "__" + DATE_USED;
	private static final String QS_USE_VALID = SESSION_SHARE_TABLE_NAME + "__" + USE_VALID;
	private static final String QS_USERID = SESSION_SHARE_TABLE_NAME + "__" + USERID;
	private static final String QS_TYPE = SESSION_SHARE_TABLE_NAME + "__" + TYPE;
	private static final String QS_SESSION_SHARE = SESSION_SHARE_TABLE_NAME + "__" + SESSION_SHARE;
	private static final String QS_AUTH_SHARE = SESSION_SHARE_TABLE_NAME + "__" + AUTH_SHARE;

	private SecurityShareSessionUtils() {

	}

	/**
	 * 
	 * @param user
	 * @param sessionId
	 * @param routeId
	 * @return
	 * @throws SQLException
	 */
	public static String createShareToken(User user, String sessionId, String routeId) throws SQLException {
		return createToken(user, sessionId, routeId, true, false);
	}

	/**
	 * 
	 * @param user
	 * @param sessionId
	 * @param routeId
	 * @return
	 * @throws SQLException
	 */
	public static String createAuthToken(User user, String sessionId, String routeId) throws SQLException {
		return createToken(user, sessionId, routeId, false, true);
	}

	/**
	 * 
	 * @param user
	 * @param sessionId
	 * @param routeId
	 * @param sessionToken
	 * @param authToken
	 * @return
	 * @throws SQLException
	 */
	public static String createToken(User user, String sessionId, String routeId, boolean sessionToken,
			boolean authToken) throws SQLException {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		if (user == null || user.isAnonymous()) {
			throw new IllegalArgumentException("Cannot share a session for a user who is not logged in");
		}

		Pair<String, String> loginDetails = User.getPrimaryUserIdAndTypePair(user);

		java.sql.Timestamp timestamp = Utility.getCurrentSqlTimestampUTC();
		String shareToken = UUID.randomUUID().toString();

		PreparedStatement ps = null;
		try {
			ps = securityDb.bulkInsertPreparedStatement(new Object[] { SESSION_SHARE_TABLE_NAME, SHARE_VAL, SESSION_VAL,
					ROUTE_VAL, DATE_ADDED, SESSION_SHARE, AUTH_SHARE, USERID, TYPE });
			int parameterIndex = 1;
			ps.setString(parameterIndex++, shareToken);
			ps.setString(parameterIndex++, sessionId);
			ps.setString(parameterIndex++, routeId);
			ps.setTimestamp(parameterIndex++, timestamp);
			ps.setBoolean(parameterIndex++, sessionToken);
			ps.setBoolean(parameterIndex++, authToken);
			ps.setString(parameterIndex++, loginDetails.getValue0());
			ps.setString(parameterIndex++, loginDetails.getValue1());
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error("Unable to create a share-session authentication token.", e);
			throw new IllegalArgumentException("Error occurred inserting to create a new token");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}

		return shareToken;
	}

	/**
	 * 
	 * @param shareToken
	 * @return
	 * @throws SQLException
	 */
	public static boolean validateShareSessionDetails(Object[] shareDetails) throws SQLException {
		ZonedDateTime zdtCurrentUTC = Utility.getCurrentZonedDateTimeUTC();

		int index = 0;
		String shareToken = (String) shareDetails[index++];
		String sessionVal = (String) shareDetails[index++];
		String routeVal = (String) shareDetails[index++];
		SemossDate dateAddedVal = (SemossDate) shareDetails[index++];
		SemossDate dateUsedVal = (SemossDate) shareDetails[index++];
		Boolean useValid = (Boolean) shareDetails[index++];

		if (useValid != null || dateUsedVal != null) {
			throw new IllegalArgumentException("share key has already been used");
		}

		ZonedDateTime zdtAdded = dateAddedVal.getZonedDateTime();
		// if when it was added + 5 min is BEFORE the current time
		// then it means this session has been shared for over 5 min
		// without anyone picking it up
		// we shouldn't allow it
		if (zdtAdded.plusMinutes(5).isBefore(zdtCurrentUTC)) {
			logSessionUsed(shareToken, zdtCurrentUTC, false);
			throw new IllegalArgumentException("The share key has already expired");
		}
		logSessionUsed(shareToken, zdtCurrentUTC, true);
		return true;
	}

	/**
	 * 
	 * @param shareToken
	 * @return
	 */
	public static Object[] getShareSessionDetails(String shareToken) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(QS_SHARE_VAL));
		qs.addSelector(new QueryColumnSelector(QS_SESSION_VAL));
		qs.addSelector(new QueryColumnSelector(QS_ROUTE_VAL));
		qs.addSelector(new QueryColumnSelector(QS_DATE_ADDED));
		qs.addSelector(new QueryColumnSelector(QS_DATE_USED));
		qs.addSelector(new QueryColumnSelector(QS_USE_VALID));
		qs.addSelector(new QueryColumnSelector(QS_SESSION_SHARE));
		qs.addSelector(new QueryColumnSelector(QS_AUTH_SHARE));
		qs.addSelector(new QueryColumnSelector(QS_USERID));
		qs.addSelector(new QueryColumnSelector(QS_TYPE));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(QS_SHARE_VAL, "==", shareToken));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (wrapper.hasNext()) {
				return wrapper.next().getValues();
			}
		} catch (Exception e) {
			classLogger.error("Unable to retrieve share session details.", e);
		}
		return null;
	}

	/**
	 * 
	 * @param shareToken
	 * @param zdt
	 * @param success
	 * @return
	 * @throws SQLException
	 */
	public static String logSessionUsed(String shareToken, ZonedDateTime zdt, boolean success) throws SQLException {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement("UPDATE " + SESSION_SHARE_TABLE_NAME + " SET " + DATE_USED + "=?, "
					+ USE_VALID + "=? " + "WHERE " + SHARE_VAL + "=?");
			int parameterIndex = 1;
			ps.setTimestamp(parameterIndex++, java.sql.Timestamp.from(zdt.toInstant()));
			ps.setBoolean(parameterIndex++, success);
			ps.setString(parameterIndex++, shareToken);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error("Unable to record share-session usage.", e);
			throw new IllegalArgumentException("Error occurred logging the session share result");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}

		return shareToken;
	}

	/**
	 * 
	 * @param shareDetails
	 * @return
	 * @throws IllegalAccessException
	 */
	public static AccessToken generateAccessTokenForShareAuth(Object[] shareDetails) throws IllegalAccessException {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		int index = 0;
		String shareToken = (String) shareDetails[index++];
		String sessionVal = (String) shareDetails[index++];
		String routeVal = (String) shareDetails[index++];
		SemossDate dateAddedVal = (SemossDate) shareDetails[index++];
		SemossDate dateUsedVal = (SemossDate) shareDetails[index++];
		Boolean useValid = (Boolean) shareDetails[index++];
		boolean shareSession = (Boolean) shareDetails[index++];
		boolean shareAuth = (Boolean) shareDetails[index++];
		String userId = (String) shareDetails[index++];
		String userType = (String) shareDetails[index++];

		if (!shareAuth) {
			throw new IllegalAccessException("Token is not an auth token");
		} else if (useValid != null) {
			throw new IllegalAccessException("Token has already been used. Please get a new token");
		}

		final String SMSS_USER_TABLE_NAME = "SMSS_USER";
		final String SMSS_USER_USERID_COL = SMSS_USER_TABLE_NAME + "__ID";
		final String SMSS_USER_USERTYPE_COL = SMSS_USER_TABLE_NAME + "__TYPE";
		final String SMSS_USER_NAME_COL = SMSS_USER_TABLE_NAME + "__NAME";
		final String SMSS_USER_USERNAME_COL = SMSS_USER_TABLE_NAME + "__USERNAME";
		final String SMSS_USER_EMAIL_COL = SMSS_USER_TABLE_NAME + "__EMAIL";

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(SMSS_USER_USERID_COL));
		qs.addSelector(new QueryColumnSelector(SMSS_USER_USERTYPE_COL));
		qs.addSelector(new QueryColumnSelector(SMSS_USER_NAME_COL));
		qs.addSelector(new QueryColumnSelector(SMSS_USER_USERNAME_COL));
		qs.addSelector(new QueryColumnSelector(SMSS_USER_EMAIL_COL));
		// filter to this user id and type
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(SMSS_USER_USERID_COL, "==", userId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(SMSS_USER_USERTYPE_COL, "==", userType));

		AccessToken token = new AccessToken();
		AuthProvider provider = AuthProvider.getProviderFromString(userType);
		token.setProvider(provider);

		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (wrapper.hasNext()) {
				Object[] values = wrapper.next().getValues();
				String name = (String) values[2];
				String username = (String) values[3];
				String email = (String) values[4];

				token.setName(name);
				token.setUsername(username);
				token.setEmail(email);
			}
		} catch (Exception e) {
			classLogger.error("Unable to generate an access token for share-session authentication.", e);
		}

		return token;
	}
}
