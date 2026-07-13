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
package prerna.usertracking;

import java.sql.PreparedStatement;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.engine.api.IRDBMSEngine;
import prerna.util.ConnectionUtils;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;

public abstract class AbstractUserTrackingUtils implements IUserTracking {

	private static Logger classLogger = LogManager.getLogger(AbstractUserTrackingUtils.class);

	/**
	 * Persists a login session for the user, keyed by the anonymous id for
	 * anonymous users or by the access token id for the given auth provider
	 * otherwise.
	 *
	 * @param sessionId the HTTP session id
	 * @param utd       the tracking details (IP and geolocation) for the session
	 * @param user      the user logging in
	 * @param ap        the auth provider the login was performed against
	 */
	protected static void saveSession(String sessionId, UserTrackingDetails utd, User user, AuthProvider ap) {
		java.sql.Timestamp timestamp = Utility.getCurrentSqlTimestampUTC();
		if (user.isAnonymous()) {
			addSession(sessionId, utd, user.getAnonymousId(), "ANONYMOUS", timestamp);
		} else {
			// since we dont want to insert the same login multiple times
			// we will only store for the parameter ap
			AccessToken token = user.getAccessToken(ap);
			addSession(sessionId, utd, token.getId(), ap.toString(), timestamp);
		}
	}

	/**
	 * Inserts a user-tracking session row with the resolved user id, type, and
	 * geolocation details.
	 *
	 * @param sessionId the HTTP session id
	 * @param utd       the tracking details (IP and geolocation) for the session
	 * @param userId    the resolved user id (anonymous id or access token id)
	 * @param type      the credential type (e.g. "ANONYMOUS" or the auth provider)
	 * @param timestamp the session creation time
	 */
	private static void addSession(String sessionId, UserTrackingDetails utd, String userId, String type,
			java.sql.Timestamp timestamp) {
		String query = "INSERT INTO USER_TRACKING " + "(SESSIONID, USERID, TYPE, CREATED_ON, ENDED_ON, "
				+ "IP_ADDR, IP_LAT, IP_LONG, IP_COUNTRY, IP_STATE, IP_CITY) " + "VALUES (?,?,?,?,?,?,?,?,?,?,?)";

		PreparedStatement ps = null;
		IRDBMSEngine engine = SystemEngineRegistry.getUserTrackingDb();
		try {
			ps = engine.getPreparedStatement(query);
			int index = 1;
			ps.setString(index++, sessionId);
			ps.setString(index++, userId);
			ps.setString(index++, type);
			ps.setTimestamp(index++, timestamp);
			ps.setNull(index++, java.sql.Types.TIMESTAMP);
			if (utd.getIpAddr() == null) {
				ps.setNull(index++, java.sql.Types.VARCHAR);
			} else {
				ps.setString(index++, utd.getIpAddr());
			}
			if (utd.getIpLat() == null) {
				ps.setNull(index++, java.sql.Types.VARCHAR);
			} else {
				ps.setString(index++, utd.getIpLat());
			}
			if (utd.getIpLong() == null) {
				ps.setNull(index++, java.sql.Types.VARCHAR);
			} else {
				ps.setString(index++, utd.getIpLong());
			}
			if (utd.getIpCountry() == null) {
				ps.setNull(index++, java.sql.Types.VARCHAR);
			} else {
				ps.setString(index++, utd.getIpCountry());
			}
			if (utd.getIpState() == null) {
				ps.setNull(index++, java.sql.Types.VARCHAR);
			} else {
				ps.setString(index++, utd.getIpState());
			}
			if (utd.getIpCity() == null) {
				ps.setNull(index++, java.sql.Types.VARCHAR);
			} else {
				ps.setString(index++, utd.getIpCity());
			}

			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to save user tracking session {} for user {} ({})", sessionId, userId, type, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(engine, ps);
		}
	}

	/**
	 * Marks the session's tracking row as ended by stamping its end time.
	 *
	 * @param sessionId the HTTP session id that ended
	 */
	@Override
	public void registerLogout(String sessionId) {
		java.sql.Timestamp timestamp = Utility.getCurrentSqlTimestampUTC();
		String query = "UPDATE USER_TRACKING SET ENDED_ON = ? WHERE SESSIONID = ?";

		PreparedStatement ps = null;
		IRDBMSEngine engine = SystemEngineRegistry.getUserTrackingDb();
		try {
			ps = engine.getPreparedStatement(query);
			int index = 1;
			ps.setTimestamp(index++, timestamp);
			ps.setString(index++, sessionId);

			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to update user tracking logout time for session {}", sessionId, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(engine, ps);
		}
	}

}
