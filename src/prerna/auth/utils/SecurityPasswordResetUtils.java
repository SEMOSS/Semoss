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
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AuthProvider;
import prerna.date.SemossDate;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.ConnectionUtils;
import prerna.util.SocialPropertiesUtil;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;
import prerna.util.ldap.ILdapAuthenticator;

public class SecurityPasswordResetUtils extends AbstractSecurityUtils {

	private static final Logger classLogger = LogManager.getLogger(SecurityPasswordResetUtils.class);

	private static final String SMSS_USER_TABLE_NAME = "SMSS_USER";
	private static final String USERID_COL = SMSS_USER_TABLE_NAME + "__ID";
	private static final String NAME_COL = SMSS_USER_TABLE_NAME + "__NAME";
	private static final String USERNAME_COL = SMSS_USER_TABLE_NAME + "__USERNAME";
	private static final String EMAIL_COL = SMSS_USER_TABLE_NAME + "__EMAIL";
	private static final String TYPE_COL = SMSS_USER_TABLE_NAME + "__TYPE";
	private static final String ADMIN_COL = SMSS_USER_TABLE_NAME + "__ADMIN";
	private static final String PASSWORD_COL = SMSS_USER_TABLE_NAME + "__PASSWORD";
	private static final String SALT_COL = SMSS_USER_TABLE_NAME + "__SALT";
	private static final String LASTLOGIN_COL = SMSS_USER_TABLE_NAME + "__LASTLOGIN";
	private static final String PHONE_COL = SMSS_USER_TABLE_NAME + "__PHONE";
	private static final String PHONE_EXTENSION_COL = SMSS_USER_TABLE_NAME + "__PHONEEXTENSION";
	private static final String COUNTRY_CODE_COL = SMSS_USER_TABLE_NAME + "__COUNTRYCODE";

	private SecurityPasswordResetUtils() {

	}

	/**
	 * Check if the email exists
	 * 
	 * @param email
	 * @return
	 */
	public static boolean userEmailExists(String email, String type) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(EMAIL_COL));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(EMAIL_COL, "==", email));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(TYPE_COL, "==", type));

		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			return wrapper.hasNext();
		} catch (Exception e) {
			classLogger.error("Unable to verify whether the user email exists.", e);
		}

		return false;
	}

	/**
	 * Get the user id from the email
	 * 
	 * @param email
	 * @return
	 */
	public static String getUserIdFromEmail(String email, String type) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(USERID_COL));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(EMAIL_COL, "==", email));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(TYPE_COL, "==", type));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (wrapper.hasNext()) {
				return (String) wrapper.next().getValues()[0];
			}
		} catch (Exception e) {
			classLogger.error("Unable to retrieve user ID from email.", e);
		}

		return null;
	}

	/**
	 * Generate and return a one time token to allow the user to reset the password
	 * 
	 * @param email
	 * @return
	 * @throws Exception
	 */
	public static String allowUserResetPassword(String email, String type) throws Exception {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		AuthProvider provider = null;
		try {
			provider = AuthProvider.valueOf(type);
			if (provider != AuthProvider.NATIVE && provider != AuthProvider.LINOTP && provider != AuthProvider.LDAP) {
				throw new IllegalArgumentException("Cannot reset password for type = '" + type + "'");
			}
		} catch (Exception e) {
			throw e;
		}

		if (!SecurityPasswordResetUtils.userEmailExists(email, provider.toString())) {
			throw new IllegalArgumentException("The email '" + email + "' does not exist for provider " + provider);
		}

		java.sql.Timestamp timestamp = Utility.getCurrentSqlTimestampUTC();
		String uniqueToken = UUID.randomUUID().toString();

		PreparedStatement ps = null;
		try {
			ps = securityDb.bulkInsertPreparedStatement(
					new Object[] { "PASSWORD_RESET", "EMAIL", "TYPE", "TOKEN", "DATE_ADDED" });
			int parameterIndex = 1;
			ps.setString(parameterIndex++, email);
			ps.setString(parameterIndex++, type);
			ps.setString(parameterIndex++, uniqueToken);
			ps.setTimestamp(parameterIndex++, timestamp);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error("Unable to determine whether the user is allowed to reset the password.", e);
			throw new IllegalArgumentException("Error occurred inserting the request to update the password");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}

		return uniqueToken;
	}

	/**
	 * 
	 * @param token
	 * @param newPassword
	 * @return
	 * @throws Exception
	 */
	public static Map<String, Object> userResetPassword(String token, String newPassword) throws Exception {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		Map<String, Object> retMap = new HashMap<>();
		SemossDate dateTokenAdded = null;
		String email = null;
		String type = null;
		AuthProvider provider = null;

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PASSWORD_RESET__DATE_ADDED"));
		qs.addSelector(new QueryColumnSelector("PASSWORD_RESET__EMAIL"));
		qs.addSelector(new QueryColumnSelector("PASSWORD_RESET__TYPE"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PASSWORD_RESET__TOKEN", "==", token));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (wrapper.hasNext()) {
				Object[] row = wrapper.next().getValues();
				dateTokenAdded = (SemossDate) row[0];
				email = (String) row[1];
				type = (String) row[2];
			}
		} catch (Exception e) {
			classLogger.error("Unable to reset the user password.", e);
		}

		if (dateTokenAdded == null) {
			throw new IllegalArgumentException("Invalid attempt trying to update password");
		}

		provider = AuthProvider.valueOf(type);

		ZonedDateTime curTimeUtc = ZonedDateTime.now(ZoneId.of("UTC"));
		// if i added the token more than 15 minutes ago
		// then the link has expired
		if (dateTokenAdded.getZonedDateTime().isBefore(curTimeUtc.minusMinutes(15))) {
			throw new IllegalArgumentException(
					"This link to reset the password has expired, please request a new link");
		}

		String userId = getUserIdFromEmail(email, provider.toString());
		if (provider == AuthProvider.NATIVE) {
			SecurityNativeUserUtils.performResetPassword(userId, newPassword);
		} else if (provider == AuthProvider.LDAP || provider == AuthProvider.LINOTP) {
			ILdapAuthenticator authenticator = SocialPropertiesUtil.getInstance().getLdapAuthenticator();
			authenticator.updateForgottenPassword(userId, newPassword);
		} else {
			throw new IllegalArgumentException(
					"The ability to update the password for this provider (" + type + ") has not been implemented.");
		}

		retMap.put("userId", userId);
		retMap.put("email", email);
		retMap.put("dateAdded", dateTokenAdded);
		return retMap;
	}

	/**
	 * Generate and return a one time token to allow the user to reset the password
	 * 
	 * @param email
	 * @return
	 * @throws Exception
	 */
	public static boolean deleteToken(String token) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement("DELETE FROM PASSWORD_RESET WHERE TOKEN=?");
			int parameterIndex = 1;
			ps.setString(parameterIndex++, token);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error("Unable to delete token.", e);
			return false;
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}

		return true;
	}

}
