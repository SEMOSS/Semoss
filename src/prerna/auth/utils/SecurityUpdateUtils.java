package prerna.auth.utils;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.PasswordRequirements;
import prerna.date.SemossDate;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.update.UpdateQueryStruct;
import prerna.query.querystruct.update.UpdateSqlInterpreter;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class SecurityUpdateUtils extends AbstractSecurityUtils {

	private static final Logger classLogger = LogManager.getLogger(SecurityUpdateUtils.class);

	/**
	 * Only used for static references
	 */
	private SecurityUpdateUtils() {
		
	}
	
	/**
	 * Adds a new user to the database. Does not create any relations, simply the node.
	 * @param userName	String representing the name of the user to add
	 */
	public static boolean addOAuthUser(AccessToken newUser) throws IllegalArgumentException {
		if(newUser.getId() == null || newUser.getId().isEmpty()) {
			throw new IllegalArgumentException("User id for the token is null or empty. Must provide a valid id.");
		}
		// lower case the emails coming in
		if(newUser.getEmail() != null) {
			newUser.setEmail(newUser.getEmail().toLowerCase());
		}
		
		// see if the user was added by an admin
		// this means it could be on the ID or the EMAIL
		// but name is the admin_added_user constant
		SelectQueryStruct adminAddedUserQs = new SelectQueryStruct();
		adminAddedUserQs.addSelector(new QueryColumnSelector("SMSS_USER__ID"));
		AndQueryFilter nameAndIdMatchFiltre = new AndQueryFilter();
		{
			OrQueryFilter or = new OrQueryFilter();
			or.addFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__NAME", "==", ADMIN_ADDED_USER));
			or.addFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__USERNAME", "==", ADMIN_ADDED_USER));
			nameAndIdMatchFiltre.addFilter(or);
		}
		{
			// need to account for a null check on email
			// since that is not necessarily required
			// id is always required
			if(newUser.getEmail() == null || newUser.getEmail().trim().isEmpty()) {
				nameAndIdMatchFiltre.addFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__ID", "==", newUser.getId()));
			} else {
				// this matching the ID field to the email because admin added user only sets the id field
				OrQueryFilter or = new OrQueryFilter();
				or.addFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__ID", "==", newUser.getId()));
				or.addFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__ID", "==", newUser.getEmail()));
				nameAndIdMatchFiltre.addFilter(or);
			}
		}
		adminAddedUserQs.addExplicitFilter(nameAndIdMatchFiltre);
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, adminAddedUserQs);
			if(wrapper.hasNext()) {
				// this was the old id that was added when the admin 
				String oldId = wrapper.next().getValues()[0].toString();
				String newId = newUser.getId();
				// this user was added by the user
				// and we need to update
				{
					java.sql.Timestamp timestamp = Utility.getCurrentSqlTimestampUTC();

					String updateQuery = "UPDATE SMSS_USER SET ID=?, NAME=?, USERNAME=?, EMAIL=?, TYPE=?, LASTLOGIN=? WHERE ID=?";
					PreparedStatement ps = null;
					try {
						int parameterIndex = 1;
						ps = securityDb.getPreparedStatement(updateQuery);
						ps.setString(parameterIndex++, newId);
						if(newUser.getName() == null) {
							ps.setNull(parameterIndex++, java.sql.Types.VARCHAR);
						} else {
							ps.setString(parameterIndex++, newUser.getName());
						}
						if(newUser.getUsername() == null) {
							ps.setNull(parameterIndex++, java.sql.Types.VARCHAR);
						} else {
							ps.setString(parameterIndex++, newUser.getUsername());
						}
						if(newUser.getEmail() == null) {
							ps.setNull(parameterIndex++, java.sql.Types.VARCHAR);
						} else {
							ps.setString(parameterIndex++, newUser.getEmail());
						}
						ps.setString(parameterIndex++, newUser.getProvider().toString());
						ps.setTimestamp(parameterIndex++, timestamp);
						ps.setString(parameterIndex++, oldId);
						ps.execute();
						if(!ps.getConnection().getAutoCommit()) {
							ps.getConnection().commit();
						}
					} catch (SQLException e) {
						classLogger.error(Constants.STACKTRACE, e);
					} finally {
						if(ps != null) {
							ps.close();
						}
						if(ps != null && securityDb.isConnectionPooling()) {
							ps.getConnection().close();
						}
					}
				}
				
				// need to update any other permissions that were set for this user
				String[] queries = new String[] {
						"UPDATE ENGINEPERMISSION SET USERID=? WHERE USERID=?",
						"UPDATE PROJECTPERMISSION SET USERID=? WHERE USERID=?",
						"UPDATE USERINSIGHTPERMISSION SET USERID=? WHERE USERID=?",
				};
				for(String updateQuery : queries) {
					PreparedStatement ps = null;
					try {
						int parameterIndex = 1;
						ps = securityDb.getPreparedStatement(updateQuery);
						ps.setString(parameterIndex++, newId);
						ps.setString(parameterIndex++, oldId);
						ps.execute();
						if(!ps.getConnection().getAutoCommit()) {
							ps.getConnection().commit();
						}
					} catch (SQLException e) {
						classLogger.error(Constants.STACKTRACE, e);
					} finally {
						if(ps != null) {
							ps.close();
						}
						if(ps != null && securityDb.isConnectionPooling()) {
							ps.getConnection().close();
						}
					}
				}
				
			} else {
				// not added by admin
				// lets see if he exists or not
				boolean userExists = SecurityQueryUtils.checkUserExist(newUser.getId());
				if (userExists) {
					classLogger.info("User " + newUser.getId() + " already exists");
					return validateUserLogin(newUser);
				}

				// need to synchronize the adding of new users
				// so that we do not enter here from different threads 
				// and add the same user twice
				synchronized(SecurityUpdateUtils.class) {
					
					// also add the max user limit check
					String userLimitStr = DIHelper.getInstance().getProperty(Constants.MAX_USER_LIMIT);
					if(userLimitStr != null && !userLimitStr.trim().isEmpty()) {
						try {
							int userLimit = Integer.parseInt(userLimitStr);
							int currentUserCount = SecurityQueryUtils.getApplicationUserCount();
							
							if(userLimit > 0 && currentUserCount+1 > userLimit) {
								throw new SemossPixelException("User limit exceeded the max value of " + userLimit);
							}
						} catch(NumberFormatException e) {
							classLogger.error(Constants.STACKTRACE, e);
							classLogger.error("User limit is not a valid numeric value");
						}
					}
					
					// need to prevent 2 threads attempting to add the same user
					userExists = SecurityQueryUtils.checkUserExist(newUser.getId());
					if(!userExists) {
						java.sql.Timestamp timestamp = Utility.getCurrentSqlTimestampUTC();

						String insertQuery = "INSERT INTO SMSS_USER (ID, NAME, USERNAME, EMAIL, TYPE, ADMIN, PUBLISHER, EXPORTER, DATECREATED, LASTLOGIN) "
								+ "VALUES (?,?,?,?,?,?,?,?,?,?)";
						PreparedStatement ps = null;
						try {
							ps = securityDb.getPreparedStatement(insertQuery);
							int parameterIndex = 1;
							ps.setString(parameterIndex++, newUser.getId());
							if(newUser.getName() == null) {
								ps.setNull(parameterIndex++, java.sql.Types.VARCHAR);
							} else {
								ps.setString(parameterIndex++, newUser.getName());
							}
							if(newUser.getUsername() == null) {
								ps.setNull(parameterIndex++, java.sql.Types.VARCHAR);
							} else {
								ps.setString(parameterIndex++, newUser.getUsername());
							}
							if(newUser.getEmail() == null) {
								ps.setNull(parameterIndex++, java.sql.Types.VARCHAR);
							} else {
								ps.setString(parameterIndex++, newUser.getEmail());
							}
							ps.setString(parameterIndex++, newUser.getProvider().toString());
							// we never add ADMIN this way
							ps.setBoolean(parameterIndex++, false);
							ps.setBoolean(parameterIndex++, !adminSetPublisher());
							ps.setBoolean(parameterIndex++, !adminSetExporter());
							ps.setTimestamp(parameterIndex++, timestamp);
							ps.setTimestamp(parameterIndex++, timestamp);
							ps.execute();
							if(!ps.getConnection().getAutoCommit()) {
								ps.getConnection().commit();
							}
						} catch (SQLException e) {
							classLogger.error(Constants.STACKTRACE, e);
						} finally {
							if(ps != null) {
								try {
									ps.close();
									if(securityDb.isConnectionPooling()) {
										try {
											ps.getConnection().close();
										} catch (SQLException e) {
											classLogger.error(Constants.STACKTRACE, e);
										}
									}
								} catch (SQLException e) {
									classLogger.error(Constants.STACKTRACE, e);
								}
							}
						}
						
						return true;
					}
				}
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		return false;
	}
	
	/**
	 * 
	 * @param newUser
	 * @return
	 * @throws Exception 
	 */
	public static boolean validateUserLogin(AccessToken newUser) throws Exception {
		// make sure user is not locked out
		Object[] lastLoginDetails = SecurityQueryUtils.getUserLockAndLastLoginAndLastPassReset(newUser.getId(), newUser.getProvider());
		if(lastLoginDetails != null) {
			Boolean isLocked = (Boolean) lastLoginDetails[0];
			if(isLocked == null) {
				isLocked = false;
			}
			SemossDate lastLogin = null;
			SemossDate lastPassReset = null;
			if(lastLoginDetails[1] != null) {
				Object potentialDateValue = lastLoginDetails[1];
				if(potentialDateValue instanceof SemossDate) {
					lastLogin = (SemossDate) potentialDateValue;
				} else if(potentialDateValue instanceof String) {
					lastLogin = SemossDate.genTimeStampDateObj(potentialDateValue + "");
				}
			}
			if(lastLoginDetails[2] != null) {
				Object potentialDateValue = lastLoginDetails[2];
				if(potentialDateValue instanceof SemossDate) {
					lastPassReset = (SemossDate) potentialDateValue;
				} else if(potentialDateValue instanceof String) {
					lastPassReset = SemossDate.genTimeStampDateObj(potentialDateValue + "");
				}
			}
			
			int daysToLock = PasswordRequirements.getInstance().getDaysToLock();
			int daysToResetPass = PasswordRequirements.getInstance().getPasswordExpirationDays();
			
			newUser.setLocked(isLocked);
			newUser.setLastLogin(lastLogin);
			newUser.setLastPasswordReset(lastPassReset);
			
			if(isLocked) {
				classLogger.info("User " + newUser.getId() + " is locked");
				return false;
			} 
			
			if(daysToLock > 0 && lastLogin != null) {
				// check to make sure user is not locked
				ZonedDateTime currentTime = ZonedDateTime.now(ZoneId.of("UTC"));
				if(currentTime.isAfter(lastLogin.getLocalDateTime().plusDays(daysToLock).atZone(ZoneId.of("UTC")))) {
					classLogger.info("User " + newUser.getId() + " is now locked due to not logging in for over " + daysToLock + " days");
					// we should lock the account
					SecurityUpdateUtils.lockUserAccount(true, newUser.getId(), newUser.getProvider());
					newUser.setLocked(true);
					return false;
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
		if(!newUser.isLocked()) {
			SecurityUpdateUtils.updateUserLastLogin(newUser.getId(), newUser.getProvider());
		}
		return false;
	}
	
	/**
	 * Update OAuth user credentials
	 * @param existingUser
	 * @return
	 * @throws IllegalArgumentException
	 */
	public static boolean updateOAuthUser(AccessToken existingToken) throws IllegalArgumentException {
		// lower case the emails coming in
		if(existingToken.getEmail() != null) {
			existingToken.setEmail(existingToken.getEmail().toLowerCase());
		}
		String name = existingToken.getName();
		String username = existingToken.getUsername();
		String email = existingToken.getEmail();
		
		UpdateQueryStruct uqs = new UpdateQueryStruct();
		uqs.setEngine(securityDb);
		uqs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__ID", "==", existingToken.getId()));
		
		List<IQuerySelector> selectors = new Vector<>();
		selectors.add(new QueryColumnSelector("SMSS_USER__NAME"));
		selectors.add(new QueryColumnSelector("SMSS_USER__USERNAME"));
		selectors.add(new QueryColumnSelector("SMSS_USER__EMAIL"));
		List<Object> values = new Vector<>();
		values.add(name);
		values.add(username);
		values.add(email);
		
		uqs.setSelectors(selectors);
		uqs.setValues(values);
		
		UpdateSqlInterpreter updateInterp = new UpdateSqlInterpreter(uqs);
		String updateQuery = updateInterp.composeQuery();

		try {
			securityDb.insertData(updateQuery);
			return true;
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}

		return false;
	}
	
	public static void lockUserAccount(boolean isLocked, String userId, AuthProvider type) {
		String updateQuery = "UPDATE SMSS_USER SET LOCKED=? WHERE ID=? AND TYPE=?";
		PreparedStatement ps = null;
		try {
			int parameterIndex = 1;
			ps = securityDb.getPreparedStatement(updateQuery);
			ps.setBoolean(parameterIndex++, isLocked);
			ps.setString(parameterIndex++, userId);
			ps.setString(parameterIndex++, type.toString());
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
			if(ps != null && securityDb.isConnectionPooling()) {
				try {
					ps.getConnection().close();
				} catch (SQLException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}

	public static void updateUserLastLogin(String userId, AuthProvider type) {
		// update the user last login
		java.sql.Timestamp timestamp = Utility.getCurrentSqlTimestampUTC();
		String updateQuery = "UPDATE SMSS_USER SET LASTLOGIN=? WHERE ID=? AND TYPE=?";
		PreparedStatement ps = null;
		try {
			int parameterIndex = 1;
			ps = securityDb.getPreparedStatement(updateQuery);
			ps.setTimestamp(parameterIndex++, timestamp);
			ps.setString(parameterIndex++, userId);
			ps.setString(parameterIndex++, type.toString());
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
			if(ps != null && securityDb.isConnectionPooling()) {
				try {
					ps.getConnection().close();
				} catch (SQLException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}
	
	/**
 	 * Adds a new user to the database. Does not create any relations, simply the node.
	 * @param id
	 * @param name
	 * @param email
	 * @param password
	 * @param type
	 * @param admin
	 * @param publisher
	 * @param exporter
	 * @return
	 * @throws IllegalArgumentException
	 */
	public static boolean registerUser(String id, String name, String email, String password, String type, String phone, String phoneextension, String countrycode, boolean admin, boolean publisher, boolean exporter) throws IllegalArgumentException {
		boolean isExistingUser = SecurityQueryUtils.checkUserExist(id);
		if(isExistingUser) {
			return false;
		}
		
		// also add the max user limit check
		String userLimitStr = DIHelper.getInstance().getProperty(Constants.MAX_USER_LIMIT);
		if(userLimitStr != null && !userLimitStr.trim().isEmpty()) {
			try {
				int userLimit = Integer.parseInt(userLimitStr);
				int currentUserCount = SecurityQueryUtils.getApplicationUserCount();
				
				if(userLimit > 0 && currentUserCount+1 > userLimit) {
					throw new SemossPixelException("User limit exceeded the max value of " + userLimit);
				}
			} catch(NumberFormatException e) {
				classLogger.error(Constants.STACKTRACE, e);
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
				if (name != null && !name.isEmpty()) {
					userName = id;
					salt = SecurityQueryUtils.generateSalt();
					hashedPassword = (SecurityQueryUtils.hash(password, salt));
				}
			}
		}
		// if username or name is null
		// switch to admin_added_user
		// the {@link #addOAuthUser} will fill these in when the user 
		// logins from their provider
		if(userName == null) userName = ADMIN_ADDED_USER;
		if(name == null) name = ADMIN_ADDED_USER;
		if(email == null) email = "";
		if(hashedPassword == null) hashedPassword = "";
		if(salt == null) salt = "";
		if(type == null) type = "";
		if(phone == null) phone = "";
		if(phoneextension == null) phoneextension = "";
		if(countrycode == null) countrycode = "";
		 
		java.sql.Timestamp timestamp = Utility.getCurrentSqlTimestampUTC();
		
		String query = "INSERT INTO SMSS_USER (ID, USERNAME, NAME, EMAIL, PASSWORD, SALT, TYPE, "
				+ "PHONE, PHONEEXTENSION, COUNTRYCODE, ADMIN, PUBLISHER, EXPORTER, DATECREATED) "
				+ "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			int parameterIndex = 1;
			ps.setString(parameterIndex++, id);
			ps.setString(parameterIndex++, userName);
			ps.setString(parameterIndex++, name);
			ps.setString(parameterIndex++, email.toLowerCase());
			ps.setString(parameterIndex++, hashedPassword);
			ps.setString(parameterIndex++, salt);
			ps.setString(parameterIndex++, type);
			ps.setString(parameterIndex++, phone);
			ps.setString(parameterIndex++, phoneextension);
			ps.setString(parameterIndex++, countrycode);
			ps.setBoolean(parameterIndex++, admin);
			ps.setBoolean(parameterIndex++, publisher);
			ps.setBoolean(parameterIndex++, exporter);
			ps.setTimestamp(parameterIndex++, timestamp);
			ps.execute();
			ps.getConnection().commit();
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(ps != null) {
				try {
					ps.close();
					if(securityDb.isConnectionPooling()) {
						ps.getConnection().close();
					}
				} catch (SQLException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		return true;
	}
	
}
