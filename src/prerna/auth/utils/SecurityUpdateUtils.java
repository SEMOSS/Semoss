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
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.PasswordRequirements;
import prerna.auth.access.ModelUsageLimit;
import prerna.auth.access.ModelUsageLimitKey;
import prerna.date.SemossDate;
import prerna.engine.api.IRDBMSEngine;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.usertracking.AuditEventType;
import prerna.usertracking.AuditSeverity;
import prerna.usertracking.AuditTargetType;
import prerna.usertracking.UserAuditTrailUtils;
import prerna.util.Constants;
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

public class SecurityUpdateUtils extends AbstractSecurityUtils {

	private static final Logger classLogger = LogManager.getLogger(SecurityUpdateUtils.class);

	/** Moves a person's permissions to a new id: new id, then old id */
	private static final String[] MOVE_PERMISSIONS = new String[] {
			"UPDATE ENGINEPERMISSION SET USERID=? WHERE USERID=?",
			"UPDATE PROJECTPERMISSION SET USERID=? WHERE USERID=?",
			"UPDATE USERINSIGHTPERMISSION SET USERID=? WHERE USERID=?" };
	/**
	 * Moves the model usage limits on a person's grants to a new id and login type:
	 * new id, new type, the key type of a person's limit, old id, then the old type
	 * when there is one; the limits kept with no login type move too
	 */
	private static final String MOVE_DIRECT_LIMITS = "UPDATE MODELUSAGELIMIT SET SUBJECTID=?, SUBJECTTYPE=? "
			+ "WHERE LIMITTYPE=? AND ENGINEID IS NOT NULL AND SUBJECTID=? AND (SUBJECTTYPE IS NULL";
	/**
	 * Moves a person's all-models default to a new id and login type: new id, new
	 * type, the key type of a person's limit, old id, then the old type's condition
	 */
	private static final String MOVE_ALL_MODELS_DEFAULT = "UPDATE MODELUSAGELIMIT SET SUBJECTID=?, SUBJECTTYPE=? "
			+ "WHERE LIMITTYPE=? AND ENGINEID IS NULL AND SUBJECTID=? AND SUBJECTTYPE";
	/**
	 * Finds the row an admin added for a person: ADMIN_ADDED_USER as its name or
	 * username, then the person's id and their email, or their id again when they
	 * have no email
	 */
	private static final String FIND_ADMIN_ADDED_USER = "SELECT ID, TYPE FROM SMSS_USER WHERE (NAME=? OR USERNAME=?) "
			+ "AND (ID=? OR ID=?)";
	/**
	 * Gives the row an admin added the id, login type, name, username, email and
	 * last login a person signs in with: the row's id last
	 */
	private static final String ADOPT_ADMIN_ADDED_USER = "UPDATE SMSS_USER SET ID=?, TYPE=?, NAME=?, USERNAME=?, "
			+ "EMAIL=?, LASTLOGIN=? WHERE ID=?";
	/** Adds a person who signs in with a login provider */
	private static final String INSERT_OAUTH_USER = "INSERT INTO SMSS_USER (ID, NAME, USERNAME, EMAIL, TYPE, ADMIN, "
			+ "PUBLISHER, EXPORTER, DATECREATED, LASTLOGIN) VALUES (?,?,?,?,?,?,?,?,?,?)";
	/** Finds a user id, whatever its login type */
	private static final String FIND_USER_ID = "SELECT ID FROM SMSS_USER WHERE ID=?";

	/**
	 * Only used for static references
	 */
	private SecurityUpdateUtils() {

	}

	/**
	 * Adds a person who signs in with a login provider. Does not create any
	 * relations, simply the node.
	 * <p>
	 * A person an admin added, under their id or their email, takes over that row
	 * the first time they sign in: the row gets the id, login type, name, username,
	 * email and last login they sign in with, and the permissions and model usage
	 * limits the admin set move to their id and login type, all in one transaction.
	 * Anyone else is added in one transaction, unless they exist already or the
	 * user limit is reached.
	 *
	 * @param newUser the person's access token; the email is lower cased
	 * @return true when the person is added, false when they took over a row an
	 *         admin added, exist already, or cannot be saved, in which case nothing
	 *         is saved
	 * @throws IllegalArgumentException when the token has no id
	 */
	public static boolean addOAuthUser(AccessToken newUser) throws IllegalArgumentException {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		if (newUser.getId() == null || newUser.getId().isEmpty()) {
			throw new IllegalArgumentException("User id for the token is null or empty. Must provide a valid id.");
		}
		// lower case the emails coming in
		if (newUser.getEmail() != null) {
			newUser.setEmail(newUser.getEmail().toLowerCase());
		}

		AbstractSqlQueryUtil queryUtil = securityDb.getQueryUtil();
		try {
			// see if the user was added by an admin, and if so take over that row
			Boolean limitsMoved = QueryExecutionUtility.write(securityDb,
					connection -> adoptAdminAddedUser(connection, queryUtil, newUser));
			if (limitsMoved != null) {
				if (limitsMoved) {
					SecurityModelUsageLimitUtils.markChanged();
				}
				return false;
			}

			// not added by admin
			// lets see if he exists or not
			boolean userExists = SecurityQueryUtils.checkUserExist(newUser.getId());
			if (userExists) {
				classLogger.info("User {} already exists", newUser.getId());
				return false;
			}

			// need to synchronize the adding of new users
			// so that we do not enter here from different threads
			// and add the same user twice
			synchronized (SecurityUpdateUtils.class) {

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
						classLogger.error("Unable to add or synchronize OAuth user records.", e);
						classLogger.error("User limit is not a valid numeric value");
					}
				}

				// the check and the insert share a transaction that commits inside this
				// lock, so 2 threads cannot add the same user
				java.sql.Timestamp timestamp = Utility.getCurrentSqlTimestampUTC();
				return QueryExecutionUtility.write(securityDb, connection -> {
					if (userIdExists(connection, newUser.getId())) {
						return false;
					}
					QueryExecutionUtility.executeUpdate(connection, INSERT_OAUTH_USER, ps -> {
						int parameterIndex = 1;
						ps.setString(parameterIndex++, newUser.getId());
						queryUtil.setNullableString(ps, parameterIndex++, newUser.getName());
						queryUtil.setNullableString(ps, parameterIndex++, newUser.getUsername());
						queryUtil.setNullableString(ps, parameterIndex++, newUser.getEmail());
						ps.setString(parameterIndex++, newUser.getProvider().toString());
						// we never add ADMIN this way
						ps.setBoolean(parameterIndex++, false);
						ps.setBoolean(parameterIndex++, !adminSetPublisher());
						ps.setBoolean(parameterIndex++, !adminSetExporter());
						ps.setTimestamp(parameterIndex++, timestamp);
						ps.setTimestamp(parameterIndex++, timestamp);
					});
					return true;
				});
			}
		} catch (Exception e) {
			classLogger.error("Unable to add or synchronize OAuth user records.", e);
		}

		return false;
	}

	/**
	 * Takes over the row an admin added for a person signing in for the first time,
	 * in the caller's transaction. The admin added the row under the person's id or
	 * email, with {@code ADMIN_ADDED_USER} as its name or username. The row gets
	 * the id, login type, name, username, email and last login they sign in with,
	 * and what the admin set up moves with it (see {@link #moveAdminAddedUser}).
	 *
	 * @param connection the connection of the caller's transaction
	 * @param queryUtil  the security database's query utility
	 * @param newUser    the person's access token
	 * @return null when no admin added the person, otherwise whether model usage
	 *         limits moved, so the caller marks them changed after the commit
	 * @throws Exception when a statement fails
	 */
	private static Boolean adoptAdminAddedUser(Connection connection, AbstractSqlQueryUtil queryUtil,
			AccessToken newUser) throws Exception {
		// need to account for a null check on email
		// since that is not necessarily required
		// id is always required
		// this matching the ID field to the email because admin added user only sets
		// the id field
		String email = newUser.getEmail();
		String idOrEmail = email == null || email.trim().isEmpty() ? newUser.getId() : email;
		String[] adminAdded = QueryExecutionUtility.queryOne(connection, FIND_ADMIN_ADDED_USER, ps -> {
			int parameterIndex = 1;
			ps.setString(parameterIndex++, ADMIN_ADDED_USER);
			ps.setString(parameterIndex++, ADMIN_ADDED_USER);
			ps.setString(parameterIndex++, newUser.getId());
			ps.setString(parameterIndex++, idOrEmail);
		}, rs -> new String[] { rs.getString(1), rs.getString(2) });
		if (adminAdded == null) {
			return null;
		}
		// this was the old id and type that was added when the admin
		String oldId = adminAdded[0];
		String oldType = adminAdded[1];
		String newId = newUser.getId();
		String newType = newUser.getProvider().toString();
		java.sql.Timestamp timestamp = Utility.getCurrentSqlTimestampUTC();
		QueryExecutionUtility.executeUpdate(connection, ADOPT_ADMIN_ADDED_USER, ps -> {
			int parameterIndex = 1;
			ps.setString(parameterIndex++, newId);
			ps.setString(parameterIndex++, newType);
			queryUtil.setNullableString(ps, parameterIndex++, newUser.getName());
			queryUtil.setNullableString(ps, parameterIndex++, newUser.getUsername());
			queryUtil.setNullableString(ps, parameterIndex++, newUser.getEmail());
			ps.setTimestamp(parameterIndex++, timestamp);
			ps.setString(parameterIndex++, oldId);
		});
		// need to update any other permissions that were set for this user, and the
		// model usage limits the admin set, which are kept
		return moveAdminAddedUser(connection, oldId, oldType, newId, newType);
	}

	/**
	 * Whether the security database has a row with this user id, read in the
	 * caller's transaction.
	 *
	 * @param connection the connection of the caller's transaction
	 * @param userId     the user id
	 * @return whether a row has the id, whatever its login type
	 * @throws Exception when the query fails
	 */
	private static boolean userIdExists(Connection connection, String userId) throws Exception {
		return QueryExecutionUtility.queryOne(connection, FIND_USER_ID, ps -> ps.setString(1, userId),
				rs -> rs.getString(1)) != null;
	}

	/**
	 * Moves what an admin set up for a person to the id and login type they sign in
	 * with the first time, in the caller's transaction: their permissions, the
	 * model usage limits on their grants and their all-models default, which keeps
	 * the limits the admin set. The caller marks the limits changed after its
	 * transaction commits, when this returns true.
	 *
	 * @param connection the connection of the caller's transaction
	 * @param oldId      the id the admin added the person with
	 * @param oldType    the login type the admin added the person with, or null
	 * @param newId      the id they sign in with
	 * @param newType    the login type they sign in with
	 * @return whether anything moved: false when the id and the login type are the
	 *         ones the admin used
	 * @throws Exception when a statement fails
	 */
	static boolean moveAdminAddedUser(Connection connection, String oldId, String oldType, String newId, String newType)
			throws Exception {
		boolean idChanges = !oldId.equals(newId);
		if (!idChanges && Objects.equals(oldType, newType)) {
			return false;
		}
		String userKeyType = ModelUsageLimitKey.Type.USER.name();
		String moveAllModelsDefault = MOVE_ALL_MODELS_DEFAULT + (oldType == null ? " IS NULL" : "=?");
		String moveDirectLimits = MOVE_DIRECT_LIMITS + (oldType == null ? ")" : " OR SUBJECTTYPE=?)");
		if (idChanges) {
			for (String updateQuery : MOVE_PERMISSIONS) {
				QueryExecutionUtility.executeUpdate(connection, updateQuery, ps -> {
					ps.setString(1, newId);
					ps.setString(2, oldId);
				});
			}
		}
		QueryExecutionUtility.executeUpdate(connection, moveDirectLimits, ps -> {
			int parameterIndex = 1;
			ps.setString(parameterIndex++, newId);
			ps.setString(parameterIndex++, newType);
			ps.setString(parameterIndex++, userKeyType);
			ps.setString(parameterIndex++, oldId);
			if (oldType != null) {
				ps.setString(parameterIndex++, oldType);
			}
		});
		QueryExecutionUtility.executeUpdate(connection, moveAllModelsDefault, ps -> {
			int parameterIndex = 1;
			ps.setString(parameterIndex++, newId);
			ps.setString(parameterIndex++, newType);
			ps.setString(parameterIndex++, userKeyType);
			ps.setString(parameterIndex++, oldId);
			if (oldType != null) {
				ps.setString(parameterIndex++, oldType);
			}
		});
		return true;
	}

	/**
	 * Checks a login against its stored account. It sets the token's locked flag,
	 * last login and last password reset, and locks the account when its last login
	 * is older than the configured days to lock. When the login is not locked, it
	 * records the login time and saves or loads the token's user metadata.
	 * 
	 * @param newUser
	 * @return
	 * @throws Exception
	 */
	public static void validateUserLogin(AccessToken newUser) throws Exception {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		// make sure user is not locked out
		Object[] lastLoginDetails = SecurityQueryUtils.getUserLockAndLastLoginAndLastPassReset(newUser.getId(),
				newUser.getProvider());
		if (lastLoginDetails != null) {
			Boolean isLocked = (Boolean) lastLoginDetails[0];
			if (isLocked == null) {
				isLocked = false;
			}
			SemossDate lastLogin = null;
			SemossDate lastPassReset = null;
			if (lastLoginDetails[1] != null) {
				Object potentialDateValue = lastLoginDetails[1];
				if (potentialDateValue instanceof SemossDate) {
					lastLogin = (SemossDate) potentialDateValue;
				} else if (potentialDateValue instanceof String) {
					lastLogin = SemossDate.genTimeStampDateObj(potentialDateValue + "", securityDb.getDatabaseZoneId());
				}
			}
			if (lastLoginDetails[2] != null) {
				Object potentialDateValue = lastLoginDetails[2];
				if (potentialDateValue instanceof SemossDate) {
					lastPassReset = (SemossDate) potentialDateValue;
				} else if (potentialDateValue instanceof String) {
					lastPassReset = SemossDate.genTimeStampDateObj(potentialDateValue + "",
							securityDb.getDatabaseZoneId());
				}
			}

			int daysToLock = PasswordRequirements.getInstance().getDaysToLock();
			int daysToResetPass = PasswordRequirements.getInstance().getPasswordExpirationDays();

			newUser.setLocked(isLocked);
			newUser.setLastLogin(lastLogin);
			newUser.setLastPasswordReset(lastPassReset);

			if (isLocked) {
				classLogger.info("User {} is locked", newUser.getId());
				return;
			}

			if (daysToLock > 0 && lastLogin != null) {
				// check to make sure user is not locked
				ZonedDateTime currentTime = ZonedDateTime.now(ZoneId.of("UTC"));
				if (currentTime.isAfter(lastLogin.getZonedDateTime().plusDays(daysToLock))) {
					classLogger.info("User {} is now locked due to not logging in for over {} days", newUser.getId(),
							daysToLock);
					// we should lock the account
					SecurityUpdateUtils.lockUserAccount(true, newUser.getId(), newUser.getProvider());
					newUser.setLocked(true);
					return;
				}
			}

//			if(daysToResetPass > 0) {
//				// check to make sure user is not locked
//				TimeZone tz = TimeZone.getTimeZone(Utility.getApplicationTimeZoneId());
//				LocalDateTime currentTime = Instant.ofEpochMilli(new Date().getTime()).atZone(tz.toZoneId()).toLocalDateTime();
//				if(currentTime.isAfter(lastLogin.getLocalDateTime().plusDays(daysToResetPass))) {
//					logger.info("User " + newUser.getId() + " is now locked due to not resetting password for over " + daysToResetPass + " days");
//					// we should lock the account
//					SecurityUpdateUtils.lockUserAccount(true, newUser.getId(), newUser.getProvider());
//					newUser.setLocked(true);
//					return false;
//				}
//			}
		}

		// if not locked
		// update the last success login
		if (!newUser.isLocked()) {
			SecurityUpdateUtils.updateUserLastLogin(newUser.getId(), newUser.getProvider());

			// if metadata is set on the token, persist it. otherwise load it
			if (newUser.getMeta() != null) {
				SecurityUserUtils.updateUserMetadata(newUser, newUser.getMeta());
			} else {
				newUser.setMeta(
						SecurityUserUtils.getAggregateUserMetadata(newUser.getId(), newUser.getProvider(), null, true));
			}
		}
	}

	/**
	 * Update OAuth user credentials
	 * 
	 * @param existingUser
	 * @return
	 * @throws IllegalArgumentException
	 */
	public static boolean updateOAuthUser(AccessToken existingToken) throws IllegalArgumentException {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String name = existingToken.getName();
		String username = existingToken.getUsername();
		String email = existingToken.getEmail();
		boolean updateName = name != null && !(name = name.trim()).isEmpty();
		boolean updateUsername = username != null && !(username = username.trim()).isEmpty();
		boolean updateEmail = email != null && !(email = email.trim().toLowerCase()).isEmpty();

		StringBuilder updateQuery = new StringBuilder("UPDATE SMSS_USER SET ");
		List<String> set = new ArrayList<>(Arrays.asList("NAME=?", "USERNAME=?", "EMAIL=?"));
		List<Boolean> hasVal = new ArrayList<>(Arrays.asList(updateName, updateUsername, updateEmail));
		List<String> values = new ArrayList<>(Arrays.asList(name, username, email));

		boolean first = true;
		for (int i = 0; i < set.size(); i++) {
			if (!hasVal.get(i)) {
				continue;
			}
			if (!first) {
				updateQuery.append(",");
			}

			updateQuery.append(set.get(i));
			first = false;
		}
		// nothing here to update...
		if (first) {
			return false;
		}
		updateQuery.append("WHERE ID=? AND TYPE=?");
		try {
			QueryExecutionUtility.executeUpdate(securityDb, updateQuery.toString(), ps -> {
				int parameterIndex = 1;
				// loop through the set for the values
				for (int i = 0; i < hasVal.size(); i++) {
					if (hasVal.get(i)) {
						ps.setString(parameterIndex++, values.get(i));
					}
				}
				// always ahve the where clause
				ps.setString(parameterIndex++, existingToken.getId());
				ps.setString(parameterIndex++, existingToken.getProvider().toString());
			});
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Unable to update OAuth user profile information.", e);
			return false;
		}

		return true;
	}

	public static void lockUserAccount(boolean isLocked, String userId, AuthProvider type) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String updateQuery = "UPDATE SMSS_USER SET LOCKED=? WHERE ID=? AND TYPE=?";
		try {
			AuthProvider boundType = type;
			QueryExecutionUtility.executeUpdate(securityDb, updateQuery, ps -> {
				int parameterIndex = 1;
				ps.setBoolean(parameterIndex++, isLocked);
				ps.setString(parameterIndex++, userId);
				ps.setString(parameterIndex++, boundType.toString());
			});
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Unable to update the user lock status.", e);
		}
	}

	public static void updateUserLastLogin(String userId, AuthProvider type) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		// update the user last login
		java.sql.Timestamp timestamp = Utility.getCurrentSqlTimestampUTC();
		String updateQuery = "UPDATE SMSS_USER SET LASTLOGIN=? WHERE ID=? AND TYPE=?";
		try {
			AuthProvider boundType = type;
			QueryExecutionUtility.executeUpdate(securityDb, updateQuery, ps -> {
				int parameterIndex = 1;
				ps.setTimestamp(parameterIndex++, timestamp);
				ps.setString(parameterIndex++, userId);
				ps.setString(parameterIndex++, boundType.toString());
			});
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Unable to update the user last-login timestamp.", e);
		}
	}

	/**
	 * Adds a new user to the database, with their all-models default, in one
	 * transaction. Does not create any relations, simply the node. The default is
	 * kept when the person signs in for the first time.
	 * 
	 * @param id
	 * @param name
	 * @param email
	 * @param password
	 * @param type
	 * @param admin
	 * @param publisher
	 * @param exporter
	 * @param modelUsageLimit the person's model usage limit across all models, or
	 *                        null for no limit defined
	 * @return
	 * @throws IllegalArgumentException when the limit is not valid
	 */
	public static boolean registerUser(String id, String name, String email, String password, String type, String phone,
			String phoneextension, String countrycode, boolean admin, boolean publisher, boolean exporter,
			ModelUsageLimit modelUsageLimit) throws IllegalArgumentException {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		boolean isExistingUser = SecurityQueryUtils.checkUserExist(id);
		if (isExistingUser) {
			return false;
		}

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
				classLogger.error("Unable to update the user last-login timestamp.", e);
				classLogger.error("User limit is not a valid numeric value");
			}
		}

		String userName = ADMIN_ADDED_USER;
		boolean isNative = false;
		String salt = null;
		String hashedPassword = null;
		if (type != null) {
			isNative = type.toLowerCase().equals("native");
			if (isNative) {
				userName = id;
				if (password != null && !password.isEmpty()) {
					salt = AbstractSecurityUtils.generateSalt();
					hashedPassword = (AbstractSecurityUtils.hash(password, salt));
				}
			}
		}
		// if username or name is null
		// switch to admin_added_user
		// the {@link #addOAuthUser} will fill these in when the user
		// logins from their provider
		if (userName == null) {
			userName = ADMIN_ADDED_USER;
		}
		if (name == null) {
			name = ADMIN_ADDED_USER;
		}
		if (email == null) {
			email = "";
		}
		if (hashedPassword == null) {
			hashedPassword = "";
		}
		if (salt == null) {
			salt = "";
		}
		if (type == null) {
			type = "";
		}
		if (phone == null) {
			phone = "";
		}
		if (phoneextension == null) {
			phoneextension = "";
		}
		if (countrycode == null) {
			countrycode = "";
		}
		ModelUsageLimitKey allModelsKey = ModelUsageLimitKey.allModels(id, type);
		ModelUsageLimit allModelsDefault = SecurityModelUsageLimitUtils.requireValid(modelUsageLimit, allModelsKey,
				true);

		java.sql.Timestamp timestamp = Utility.getCurrentSqlTimestampUTC();

		String query = "INSERT INTO SMSS_USER (ID, USERNAME, NAME, EMAIL, PASSWORD, SALT, TYPE, "
				+ "PHONE, PHONEEXTENSION, COUNTRYCODE, ADMIN, PUBLISHER, EXPORTER, DATECREATED) "
				+ "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		try {
			String boundUserName = userName;
			String boundName = name;
			String boundEmail = email;
			String boundHashedPassword = hashedPassword;
			String boundSalt = salt;
			String boundType = type;
			String boundPhone = phone;
			String boundPhoneextension = phoneextension;
			String boundCountrycode = countrycode;
			QueryExecutionUtility.write(securityDb, connection -> {
				QueryExecutionUtility.executeUpdate(connection, query, ps -> {
					int parameterIndex = 1;
					ps.setString(parameterIndex++, id);
					ps.setString(parameterIndex++, boundUserName);
					ps.setString(parameterIndex++, boundName);
					ps.setString(parameterIndex++, boundEmail.toLowerCase());
					ps.setString(parameterIndex++, boundHashedPassword);
					ps.setString(parameterIndex++, boundSalt);
					ps.setString(parameterIndex++, boundType);
					ps.setString(parameterIndex++, boundPhone);
					ps.setString(parameterIndex++, boundPhoneextension);
					ps.setString(parameterIndex++, boundCountrycode);
					ps.setBoolean(parameterIndex++, admin);
					ps.setBoolean(parameterIndex++, publisher);
					ps.setBoolean(parameterIndex++, exporter);
					ps.setTimestamp(parameterIndex++, timestamp);
				});
				if (allModelsDefault != null) {
					SecurityModelUsageLimitUtils.setLimit(connection, allModelsKey, allModelsDefault, null);
				}
				return null;
			});
			if (allModelsDefault != null) {
				SecurityModelUsageLimitUtils.markChanged();
				ModelUsageLimitAudit.recordAdminChange(null, allModelsKey, null, allModelsDefault);
			}
			Map<String, Object> created = new HashMap<>();
			created.put("name", name);
			created.put("email", email);
			created.put("admin", admin);
			created.put("publisher", publisher);
			created.put("exporter", exporter);
			UserAuditTrailUtils.recordEvent(new UserAuditTrailUtils.AuditEvent().eventType(AuditEventType.USER_CREATE)
					.severity(admin ? AuditSeverity.CRITICAL : null).subject(id, type, name)
					.target(AuditTargetType.USER, id, name).newValue(created));
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Unable to update the user last-login timestamp.", e);
		}
		return true;
	}

}
