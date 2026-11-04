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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.PasswordRequirements;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.util.Constants;
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;
import prerna.util.ValueUtils;
import prerna.util.sql.AbstractSqlQueryUtil;

public class SecurityNativeUserUtils extends AbstractSecurityUtils {

	private static final Logger classLogger = LogManager.getLogger(SecurityNativeUserUtils.class);

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

	/**
	 * Gives the row an admin added a person's details and password: the row's id
	 * last
	 */
	private static final String ADOPT_ADMIN_ADDED_USER = "UPDATE SMSS_USER SET ID=?, NAME=?, USERNAME=?, EMAIL=?, TYPE=?, "
			+ "PASSWORD=?, SALT=?, LASTLOGIN=?, PHONE=?, PHONEEXTENSION=?, COUNTRYCODE=? WHERE ID=?";
	/** Adds a native user, who is never an admin and starts unlocked */
	private static final String INSERT_NATIVE_USER = "INSERT INTO SMSS_USER (ID, NAME, USERNAME, EMAIL, TYPE, ADMIN, "
			+ "PASSWORD, SALT, DATECREATED, LOCKED, PHONE, PHONEEXTENSION, COUNTRYCODE) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)";
	/** Stores a password in the password history */
	private static final String INSERT_PASSWORD_HISTORY = "INSERT INTO PASSWORD_HISTORY (ID, USERID, TYPE, PASSWORD, "
			+ "SALT, DATE_ADDED) VALUES (?,?,?,?,?,?)";
	/** The ids of a user's stored passwords for a login type, the newest first */
	private static final String PASSWORD_HISTORY_NEWEST_FIRST = "SELECT ID FROM PASSWORD_HISTORY WHERE USERID=? "
			+ "AND TYPE=? ORDER BY DATE_ADDED DESC";
	/** Deletes a stored password */
	private static final String DELETE_PASSWORD_HISTORY = "DELETE FROM PASSWORD_HISTORY WHERE ID=?";
	/** Sets a user's salt and password: salt, password, then user id */
	private static final String RESET_PASSWORD = "UPDATE SMSS_USER SET SALT=?, PASSWORD=? WHERE ID=?";

	private static final String FIND_ADMIN_ADDED_USER = "SELECT ID, TYPE FROM SMSS_USER WHERE NAME=? AND (ID=? OR ID=?)";

	private SecurityNativeUserUtils() {

	}

	/*
	 * Native user CRUD
	 */

	/**
	 * Adds a new user to the database with no all-models default, as
	 * self-registration does. A person an admin added keeps the default the admin
	 * set: their row takes the person's details and password, and what the admin
	 * set up moves to the person's id and login type. The user row, the move and
	 * the first password in the password history are saved in one transaction, so a
	 * failure saves none of them.
	 * 
	 * @param newUser  the person
	 * @param password their password
	 * @return whether the person was added; false when they could not be saved
	 * @throws IllegalArgumentException when the person's information is not valid
	 */
	public static Boolean addNativeUser(AccessToken newUser, String password) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		// also add the max user limit check
		String userLimitStr = Utility.getDIHelperProperty(Constants.MAX_USER_LIMIT);
		if (userLimitStr != null && !userLimitStr.trim().isEmpty()) {
			try {
				int userLimit = Integer.parseInt(userLimitStr);
				int currentUserCount = SecurityQueryUtils.getApplicationUserCount();

				if (userLimit > 0 && currentUserCount + 1 > userLimit) {
					throw new SemossPixelException("User limit exceeded the max value of " + userLimit);
				}
			} catch (NumberFormatException e) {
				classLogger.error("Unable to add native user.", e);
				classLogger.error("User limit is not a valid numeric value");
			}
		}

		validInformation(newUser, password, true);
		String newType = ValueUtils.toStringOrNull(newUser.getProvider());

		try {
			AbstractSqlQueryUtil queryUtil = securityDb.getQueryUtil();
			String salt = AbstractSecurityUtils.generateSalt();
			String hashedPassword = (AbstractSecurityUtils.hash(password, salt));
			java.sql.Timestamp timestamp = Utility.getCurrentSqlTimestampUTC();
			// read before the transaction, since the rules can be loaded from the database
			int passReuseCount = PasswordRequirements.getInstance().getPassReuseCount();
			boolean limitsMoved = QueryExecutionUtility.write(securityDb, connection -> {
				boolean moved = false;
				String[] adminAdded = QueryExecutionUtility.queryOne(connection, FIND_ADMIN_ADDED_USER, ps -> {
					int parameterIndex = 1;
					ps.setString(parameterIndex++, ADMIN_ADDED_USER);
					ps.setString(parameterIndex++, newUser.getId());
					queryUtil.setNullableString(ps, parameterIndex++, newUser.getEmail());
				}, rs -> new String[] { rs.getString(1), rs.getString(2) });
				if (adminAdded != null) {
					// this was the old id and type that was added when the admin
					String oldId = adminAdded[0];
					String oldType = adminAdded[1];
					String newId = newUser.getId();

					// this user was added by the user and we need to update
					QueryExecutionUtility.executeUpdate(connection, ADOPT_ADMIN_ADDED_USER, ps -> {
						int parameterIndex = 1;
						ps.setString(parameterIndex++, newId);
						queryUtil.setNullableString(ps, parameterIndex++, newUser.getName());
						queryUtil.setNullableString(ps, parameterIndex++, newUser.getUsername());
						queryUtil.setNullableString(ps, parameterIndex++, newUser.getEmail());
						ps.setString(parameterIndex++, newUser.getProvider().toString());
						ps.setString(parameterIndex++, hashedPassword);
						ps.setString(parameterIndex++, salt);
						ps.setTimestamp(parameterIndex++, timestamp);
						queryUtil.setNullableString(ps, parameterIndex++, newUser.getPhone());
						queryUtil.setNullableString(ps, parameterIndex++, newUser.getPhoneExtension());
						queryUtil.setNullableString(ps, parameterIndex++, newUser.getCountryCode());
						// where
						ps.setString(parameterIndex++, oldId);
					});

					// need to update any other permissions that were set for this user, and the
					// model usage limits the admin set, which are kept
					moved = SecurityUpdateUtils.moveAdminAddedUser(connection, oldId, oldType, newId, newType);
				} else {
					// not added by admin
					QueryExecutionUtility.executeUpdate(connection, INSERT_NATIVE_USER, ps -> {
						int parameterIndex = 1;
						ps.setString(parameterIndex++, newUser.getId());
						queryUtil.setNullableString(ps, parameterIndex++, newUser.getName());
						queryUtil.setNullableString(ps, parameterIndex++, newUser.getUsername());
						queryUtil.setNullableString(ps, parameterIndex++, newUser.getEmail());
						ps.setString(parameterIndex++, newUser.getProvider().toString());
						// we never add ADMIN this way
						ps.setBoolean(parameterIndex++, false);
						ps.setString(parameterIndex++, hashedPassword);
						ps.setString(parameterIndex++, salt);
						ps.setTimestamp(parameterIndex++, timestamp);
						// not locked ...
						ps.setBoolean(parameterIndex++, false);
						queryUtil.setNullableString(ps, parameterIndex++, newUser.getPhone());
						queryUtil.setNullableString(ps, parameterIndex++, newUser.getPhoneExtension());
						queryUtil.setNullableString(ps, parameterIndex++, newUser.getCountryCode());
					});
				}

				storeUserPassword(connection, newUser.getId(), newUser.getProvider().toString(), hashedPassword, salt,
						timestamp, passReuseCount);
				return moved;
			});
			if (limitsMoved) {
				SecurityModelUsageLimitUtils.markChanged();
			}
			return true;
		} catch (Exception e) {
			classLogger.error("Unable to add native user.", e);
		}

		return false;
	}

	/**
	 * Stores a password in the user's password history, and deletes their older
	 * passwords beyond the newest ones the password reuse rule checks, in one
	 * transaction.
	 * 
	 * @param userId       the user id
	 * @param type         the user's login type
	 * @param hashPassword the hashed password
	 * @param salt         the password's salt
	 * @param timestamp    when the password was set
	 * @throws Exception when the password rules cannot be read, or the history
	 *                   cannot be saved, in which case nothing is saved
	 */
	public static void storeUserPassword(String userId, String type, String hashPassword, String salt,
			java.sql.Timestamp timestamp) throws Exception {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		// read before the transaction, since the rules can be loaded from the database
		int passReuseCount = PasswordRequirements.getInstance().getPassReuseCount();
		QueryExecutionUtility.write(securityDb, connection -> {
			storeUserPassword(connection, userId, type, hashPassword, salt, timestamp, passReuseCount);
			return null;
		});
	}

	/**
	 * Stores a password in the user's password history, and deletes their older
	 * passwords beyond the newest ones the password reuse rule checks, in the
	 * caller's transaction. Nothing is deleted when the rule checks no passwords.
	 *
	 * @param connection     the connection of the caller's transaction
	 * @param userId         the user id
	 * @param type           the user's login type
	 * @param hashPassword   the hashed password
	 * @param salt           the password's salt
	 * @param timestamp      when the password was set
	 * @param passReuseCount how many of the newest passwords the reuse rule checks
	 * @throws Exception when a statement fails
	 */
	static void storeUserPassword(Connection connection, String userId, String type, String hashPassword, String salt,
			java.sql.Timestamp timestamp, int passReuseCount) throws Exception {
		QueryExecutionUtility.executeUpdate(connection, INSERT_PASSWORD_HISTORY, ps -> {
			int parameterIndex = 1;
			ps.setString(parameterIndex++, UUID.randomUUID().toString());
			ps.setString(parameterIndex++, userId);
			ps.setString(parameterIndex++, type);
			ps.setString(parameterIndex++, hashPassword);
			ps.setString(parameterIndex++, salt);
			ps.setTimestamp(parameterIndex++, timestamp);
		});

		if (passReuseCount > 0) {
			// do we have too many stored passwords?
			// keep the newest ones, which are the ones isPreviousPassword checks
			List<String> storedIds = QueryExecutionUtility.queryList(connection, PASSWORD_HISTORY_NEWEST_FIRST, ps -> {
				ps.setString(1, userId);
				ps.setString(2, type);
			}, rs -> rs.getString(1));
			if (storedIds.size() > passReuseCount) {
				List<String> deleteIds = storedIds.subList(passReuseCount, storedIds.size());
				QueryExecutionUtility.executeBatch(connection, DELETE_PASSWORD_HISTORY, deleteIds,
						(statement, deleteId) -> statement.setString(1, deleteId));
			}
		}
	}

	/**
	 * Basic validation of the user information before creating it.
	 * 
	 * @param newUser
	 * @param password
	 * @param newUser
	 */
	static void validInformation(AccessToken newUser, String password, boolean isNewUser) {
		String error = "";
		// User name validation
		try {
			validUsername(newUser.getUsername());
		} catch (Exception e) {
			classLogger.error("Unable to validate required native-user fields.", e);
			error += e.getMessage();
		}
		try {
			validEmail(newUser.getEmail(), isNewUser);
		} catch (Exception e) {
			classLogger.error("Unable to validate required native-user fields.", e);
			error += e.getMessage();
		}
		try {
			validPassword(newUser.getId(), newUser.getProvider(), password);
		} catch (Exception e) {
			classLogger.error("Unable to validate required native-user fields.", e);
			error += e.getMessage();
		}
		try {
			newUser.setPhone(formatPhone(newUser.getPhone()));
		} catch (Exception e) {
			classLogger.error("Unable to validate required native-user fields.", e);
			error += e.getMessage();
		}
		if (!error.isEmpty()) {
			throw new IllegalArgumentException(error);
		}
	}

	/**
	 * Verifies user information provided in the log in screen to allow or not the
	 * entry in the application.
	 * 
	 * @param user     user name
	 * @param password
	 * @return true if user exist and password is correct otherwise false.
	 */
	public static boolean logIn(String user, String password) {
		Map<String, String> databaseUser = getUserFromDatabase(user);
		if (databaseUser.isEmpty()) {
			return false;
		}

		String storedSalt = databaseUser.get("SALT");
		if (!credentialMatches(password, databaseUser.get("PASSWORD"), storedSalt)) {
			return false;
		}

		if (isLegacySalt(storedSalt)) {
			migrateCredentialToApprovedHash(databaseUser.get("ID"), password);
		}

		return true;
	}

	/**
	 * Rehash the password with PBKDF2 if the stored salt is still legacy. Called
	 * after the password is verified, since that is when the plaintext is
	 * available. Failures are logged so a valid login is not rejected.
	 *
	 * @param userId
	 * @param password
	 */
	private static void migrateCredentialToApprovedHash(String userId, String password) {
		if (userId == null) {
			classLogger.warn("Cannot migrate a stored password hash without a user id");
			return;
		}

		runCredentialMigration(userId, () -> {
			IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
			String salt = AbstractSecurityUtils.generateSalt();
			String hashedPassword = AbstractSecurityUtils.hash(password, salt);
			String updateQuery = "UPDATE SMSS_USER SET SALT=?, PASSWORD=? WHERE ID=? AND TYPE=?";

			try {
				QueryExecutionUtility.write(securityDb, connection -> {
					try (PreparedStatement ps = connection.prepareStatement(updateQuery)) {
						int parameterIndex = 1;
						ps.setString(parameterIndex++, salt);
						ps.setString(parameterIndex++, hashedPassword);
						ps.setString(parameterIndex++, userId);
						ps.setString(parameterIndex++, AuthProvider.NATIVE.getLabel());
						ps.execute();

						classLogger.info("Migrated the stored password hash for user id={} to the approved scheme",
								userId);
					}
					return null;
				});
			} catch (RuntimeException e) {
				throw e;
			} catch (Exception e) {
				classLogger.error("Unable to migrate the stored password hash to the approved scheme.", e);
			}
		});
	}

	static String getUsernameByUserId(String userId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		/*
		 * String query = "SELECT NAME FROM USER WHERE ID = '?1'"; query =
		 * query.replace("?1", userId); IRawSelectWrapper wrapper =
		 * WrapperManager.getInstance().getRawWrapper(securityDb, query);
		 */

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(NAME_COL));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(USERID_COL, "==", userId));
		String username = null;
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (wrapper.hasNext()) {
				username = (String) wrapper.next().getValues()[0];
			}
		} catch (Exception e) {
			classLogger.error("Unable to retrieve username by user ID.", e);
		}
		return username;
	}

	/**
	 * Brings the user id from database.
	 * 
	 * @param username
	 * @return userId if it exists otherwise null
	 */
	public static String getUserId(String username) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		/*
		 * String query = "SELECT ID FROM SMSS_USER WHERE USERNAME = '?1'"; query =
		 * query.replace("?1", username); IRawSelectWrapper wrapper =
		 * WrapperManager.getInstance().getRawWrapper(securityDb, query);
		 */

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(USERID_COL));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(USERNAME_COL, "==", username));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(TYPE_COL, "==", AuthProvider.NATIVE.getLabel()));
		String userId = null;
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (wrapper.hasNext()) {
				userId = (String) wrapper.next().getValues()[0];
			}
		} catch (Exception e) {
			classLogger.error("Unable to retrieve user ID.", e);
		}
		return userId;

	}

	/**
	 * Brings the email from database.
	 * 
	 * @param username
	 * @return email if it exists otherwise null
	 */
	public static String getUserEmail(String username) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		/*
		 * String query = "SELECT EMAIL FROM SMSS_USER WHERE USERNAME = '?1'"; query =
		 * query.replace("?1", username); IRawSelectWrapper wrapper =
		 * WrapperManager.getInstance().getRawWrapper(securityDb, query);
		 */

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(EMAIL_COL));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(USERNAME_COL, "==", username));
		String email = null;
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (wrapper.hasNext()) {
				email = (String) wrapper.next().getValues()[0];
			}
		} catch (Exception e) {
			classLogger.error("Unable to retrieve user email.", e);
		}
		return email;
	}

	/**
	 * Check if the email exists
	 * 
	 * @param email
	 * @return
	 */
	public static boolean userEmailExists(String email) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(EMAIL_COL));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(EMAIL_COL, "==", email));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(TYPE_COL, "==", AuthProvider.NATIVE.getLabel()));

		boolean exists = false;
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			exists = wrapper.hasNext();
		} catch (Exception e) {
			classLogger.error("Unable to verify whether the user email exists.", e);
		}

		return exists;
	}

	/**
	 * Brings the user name from database.
	 * 
	 * @param username
	 * @return userId if it exists otherwise null
	 */
	public static String getNameUser(String username) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		/*
		 * String query = "SELECT NAME FROM SMSS_USER WHERE USERNAME = '?1'"; query =
		 * query.replace("?1", username); IRawSelectWrapper wrapper =
		 * WrapperManager.getInstance().getRawWrapper(securityDb, query);
		 */

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(NAME_COL));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(USERNAME_COL, "==", username));
		String name = null;
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (wrapper.hasNext()) {
				IHeadersDataRow sjss = wrapper.next();
				name = (String) sjss.getValues()[0];
			}
		} catch (Exception e) {
			classLogger.error("Unable to retrieve the user name.", e);
		}

		return name;
	}

	/**
	 * Brings all the user basic information from the database.
	 * 
	 * @param username
	 * @return User retrieved from the database otherwise null.
	 */
	private static Map<String, String> getUserFromDatabase(String username) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		Map<String, String> user = new HashMap<>();

		/*
		 * String query =
		 * "SELECT ID, NAME, USERNAME, EMAIL, TYPE, ADMIN, PASSWORD, SALT FROM USER WHERE USERNAME='"
		 * + username + "'"; IRawSelectWrapper wrapper =
		 * WrapperManager.getInstance().getRawWrapper(securityDb, query);
		 */

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(USERID_COL));
		qs.addSelector(new QueryColumnSelector(NAME_COL));
		qs.addSelector(new QueryColumnSelector(USERNAME_COL));
		qs.addSelector(new QueryColumnSelector(EMAIL_COL));
		qs.addSelector(new QueryColumnSelector(TYPE_COL));
		qs.addSelector(new QueryColumnSelector(ADMIN_COL));
		qs.addSelector(new QueryColumnSelector(PASSWORD_COL));
		qs.addSelector(new QueryColumnSelector(SALT_COL));
		qs.addSelector(new QueryColumnSelector(PHONE_COL));
		qs.addSelector(new QueryColumnSelector(PHONE_EXTENSION_COL));
		qs.addSelector(new QueryColumnSelector(COUNTRY_CODE_COL));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(USERNAME_COL, "==", username));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(TYPE_COL, "==", AuthProvider.NATIVE.getLabel()));

		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			String[] names = wrapper.getHeaders();
			if (wrapper.hasNext()) {
				Object[] values = wrapper.next().getValues();
				user.put(names[0], (String) values[0]);
				user.put(names[1], (String) values[1]);
				user.put(names[2], (String) values[2]);
				user.put(names[3], (String) values[3]);
				user.put(names[4], (String) values[4]);
				user.put(names[5], values[5] + "");
				user.put(names[6], (String) values[6]);
				user.put(names[7], (String) values[7]);
				user.put(names[8], (String) values[8]);
				user.put(names[9], (String) values[9]);
				user.put(names[10], (String) values[10]);
			}
		} catch (Exception e) {
			classLogger.error("Unable to retrieve user details from the native user store.", e);
		}

		return user;
	}

	/**
	 * Whether the password matches the user's current password, by hashing it with
	 * the stored salt.
	 * 
	 * @param userId
	 * @param type
	 * @param password
	 * @return
	 */
	public static boolean isCurrentPassword(String userId, AuthProvider type, String password) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String hashedPassword = null;
		String salt = null;

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(PASSWORD_COL));
		qs.addSelector(new QueryColumnSelector(SALT_COL));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(USERID_COL, "==", userId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(TYPE_COL, "==", type.toString()));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (wrapper.hasNext()) {
				Object[] values = wrapper.next().getValues();
				hashedPassword = (String) values[0];
				salt = (String) values[1];
			}
		} catch (Exception e) {
			classLogger.error("Unable to verify whether the provided password matches the current password.", e);
		}

		String typedHash = hash(password, salt);
		return hashedPassword.equals(typedHash);
	}

	/**
	 * Whether the password matches one of the user's newest passwords in the
	 * password history, as many as the password reuse rule covers. False when the
	 * reuse count is less than one.
	 * 
	 * @param userId
	 * @param type
	 * @param password
	 * @return
	 */
	public static boolean isPreviousPassword(String userId, AuthProvider type, String password) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		int passReuseCount = -1;
		try {
			passReuseCount = PasswordRequirements.getInstance().getPassReuseCount();
		} catch (Exception e) {
			classLogger.error("Unable to verify whether the provided password matches a previous password.", e);
		}
		if (passReuseCount < 1) {
			return false;
		}

		// the newest passwords, as many as the reuse rule covers
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PASSWORD_HISTORY__PASSWORD"));
		qs.addSelector(new QueryColumnSelector("PASSWORD_HISTORY__SALT"));
		qs.addSelector(new QueryColumnSelector("PASSWORD_HISTORY__DATE_ADDED"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PASSWORD_HISTORY__USERID", "==", userId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PASSWORD_HISTORY__TYPE", "==", type));
		qs.addOrderBy(new QueryColumnOrderBySelector("PASSWORD_HISTORY__DATE_ADDED", "DESC"));
		qs.setLimit(passReuseCount);

		boolean previousPassword = false;
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			while (wrapper.hasNext()) {
				Object[] previousPass = wrapper.next().getValues();
				String hashPass = (String) previousPass[0];
				String salt = (String) previousPass[1];
				String testPass = hash(password, salt);
				if (hashPass.equals(testPass)) {
					previousPassword = true;
					break;
				}
			}
		} catch (Exception e) {
			classLogger.error("Unable to verify whether the provided password matches a previous password.", e);
		}

		return previousPassword;
	}

	/**
	 * Resets a native user's password: the new salt and hash, and the password
	 * history with its older passwords deleted, are saved in one transaction.
	 * 
	 * @param userId   the user id
	 * @param password the new password
	 * @return the user id
	 * @throws IllegalArgumentException when the password is not valid, or cannot be
	 *                                  saved, in which case nothing is saved
	 * @throws Exception                when the password rules cannot be read
	 */
	public static String performResetPassword(String userId, String password) throws Exception {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		// make sure the new password is valid or throw error
		validPassword(userId, AuthProvider.NATIVE, password);

		// validate the new password to run the edit logic
		String salt = AbstractSecurityUtils.generateSalt();
		String hashPassword = AbstractSecurityUtils.hash(password, salt);
		java.sql.Timestamp timestamp = Utility.getCurrentSqlTimestampUTC();
		// read before the transaction, since the rules can be loaded from the database
		int passReuseCount = PasswordRequirements.getInstance().getPassReuseCount();
		try {
			QueryExecutionUtility.write(securityDb, connection -> {
				QueryExecutionUtility.executeUpdate(connection, RESET_PASSWORD, ps -> {
					int parameterIndex = 1;
					ps.setString(parameterIndex++, salt);
					ps.setString(parameterIndex++, hashPassword);
					ps.setString(parameterIndex++, userId);
				});
				storeUserPassword(connection, userId, AuthProvider.NATIVE.getLabel(), hashPassword, salt, timestamp,
						passReuseCount);
				return null;
			});
		} catch (Exception e) {
			classLogger.error("Unable to reset the user password in the native user store.", e);
			throw new IllegalArgumentException("Unable to reset the password", e);
		}

		return userId;
	}

}
