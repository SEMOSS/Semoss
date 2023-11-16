package prerna.auth.utils;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
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
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class SecurityNativeUserUtils extends AbstractSecurityUtils {

	private static final Logger logger = LogManager.getLogger(SecurityNativeUserUtils.class);

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
	
	private SecurityNativeUserUtils() {

	}

	/*
	 * Native user CRUD 
	 */

	/**
	 * Adds a new user to the database
	 * 
	 * @param userName String representing the name of the user to add
	 * @throws IllegalArgumentException
	 */
	public static Boolean addNativeUser(AccessToken newUser, String password) {
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
				logger.error(Constants.STACKTRACE, e);
				logger.error("User limit is not a valid numeric value");
			}
		}
		
		validInformation(newUser, password, true);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(USERID_COL));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(NAME_COL, "==", ADMIN_ADDED_USER));
		OrQueryFilter orFilter = new OrQueryFilter();
		orFilter.addFilter(SimpleQueryFilter.makeColToValFilter(USERID_COL, "==", newUser.getId()));
		orFilter.addFilter(SimpleQueryFilter.makeColToValFilter(USERID_COL, "==", newUser.getEmail()));
		qs.addExplicitFilter(orFilter);
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if (wrapper.hasNext()) {
				// this was the old id that was added when the admin
				String oldId = wrapper.next().getValues()[0].toString();
				String newId = newUser.getId();

				// this user was added by the user and we need to update
				String salt = SecurityQueryUtils.generateSalt();
				String hashedPassword = (SecurityQueryUtils.hash(password, salt));

				java.sql.Timestamp timestamp = Utility.getCurrentSqlTimestampUTC();

				String updateQuery = "UPDATE SMSS_USER SET ID=?, NAME=?, USERNAME=?, EMAIL=?, TYPE=?, "
						+ "PASSWORD=?, SALT=?, LASTLOGIN=?, PHONE=?, PHONEEXTENSION=?, COUNTRYCODE=? WHERE ID=?";
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
					ps.setString(parameterIndex++, hashedPassword);
					ps.setString(parameterIndex++, salt);
					ps.setTimestamp(parameterIndex++, timestamp);
					if(newUser.getPhone() == null) {
						ps.setNull(parameterIndex++, java.sql.Types.VARCHAR);
					} else {
						ps.setString(parameterIndex++, newUser.getPhone());
					}
					if(newUser.getPhoneExtension() == null) {
						ps.setNull(parameterIndex++, java.sql.Types.VARCHAR);
					} else {
						ps.setString(parameterIndex++, newUser.getPhoneExtension());
					}
					if(newUser.getCountryCode() == null) {
						ps.setNull(parameterIndex++, java.sql.Types.VARCHAR);
					} else {
						ps.setString(parameterIndex++, newUser.getCountryCode());
					}
					// where 
					ps.setString(parameterIndex++, oldId);
					ps.execute();
					if(!ps.getConnection().getAutoCommit()) {
						ps.getConnection().commit();
					}
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				} finally {
					if(ps != null) {
						ps.close();
					}
					if(ps != null && securityDb.isConnectionPooling()) {
						ps.getConnection().close();
					}
				}
				
				// need to update any other permissions that were set for this user
				if(!oldId.equals(newId)) {
					String[] updateQueries = new String[] {
							"UPDATE ENGINEPERMISSION SET USERID=? WHERE USERID=?",
							"UPDATE PROJECTPERMISSION SET USERID=? WHERE USERID=?",
							"UPDATE USERINSIGHTPERMISSION SET USERID=? WHERE USERID=?"
					};
					for(String uQuery : updateQueries) {
						try {
							int parameterIndex = 1;
							ps = securityDb.getPreparedStatement(uQuery);
							ps.setString(parameterIndex++, newId);
							ps.setString(parameterIndex++, oldId);
							ps.execute();
							if(!ps.getConnection().getAutoCommit()) {
								ps.getConnection().commit();
							}
						} catch (SQLException e) {
							logger.error(Constants.STACKTRACE, e);
						} finally {
							if(ps != null) {
								ps.close();
							}
							if(ps != null && securityDb.isConnectionPooling()) {
								ps.getConnection().close();
							}
						}
					}
				}

				storeUserPassword(newUser.getId(), newUser.getProvider().toString(), hashedPassword, salt, timestamp);
				return true;
			} else {
				// not added by admin
				// lets see if he exists or not
				boolean userExists = SecurityQueryUtils.checkUserExist(newUser.getUsername(), newUser.getEmail());
				if (!userExists) {
					String salt = AbstractSecurityUtils.generateSalt();
					String hashedPassword = (AbstractSecurityUtils.hash(password, salt));

					java.sql.Timestamp timestamp = Utility.getCurrentSqlTimestampUTC();
					
					String insertQuery = "INSERT INTO SMSS_USER (ID, NAME, USERNAME, EMAIL, TYPE, ADMIN, PASSWORD, SALT, DATECREATED, "
							+ "LOCKED, PHONE, PHONEEXTENSION, COUNTRYCODE) "
							+ "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)";
					
					PreparedStatement ps = null;
					try {
						int parameterIndex = 1;
						ps = securityDb.getPreparedStatement(insertQuery);
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
						ps.setString(parameterIndex++, hashedPassword);
						ps.setString(parameterIndex++, salt);
						ps.setTimestamp(parameterIndex++, timestamp);
						// not locked ...
						ps.setBoolean(parameterIndex++, false);
						if(newUser.getPhone() == null) {
							ps.setNull(parameterIndex++, java.sql.Types.VARCHAR);
						} else {
							ps.setString(parameterIndex++, newUser.getPhone());
						}
						if(newUser.getPhoneExtension() == null) {
							ps.setNull(parameterIndex++, java.sql.Types.VARCHAR);
						} else {
							ps.setString(parameterIndex++, newUser.getPhoneExtension());
						}
						if(newUser.getCountryCode() == null) {
							ps.setNull(parameterIndex++, java.sql.Types.VARCHAR);
						} else {
							ps.setString(parameterIndex++, newUser.getCountryCode());
						}
						ps.execute();
						if(!ps.getConnection().getAutoCommit()) {
							ps.getConnection().commit();
						}
					} catch (SQLException e) {
						logger.error(Constants.STACKTRACE, e);
					} finally {
						if(ps != null) {
							ps.close();
						}
						if(ps != null && securityDb.isConnectionPooling()) {
							ps.getConnection().close();
						}
					}
					
					storeUserPassword(newUser.getId(), newUser.getProvider().toString(), hashedPassword, salt, timestamp);
					return true;
				}
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}

		return false;
	}

	/**
	 * Store the password for the user
	 * @param userId
	 * @param type
	 * @param hashPassword
	 * @param salt
	 * @param timestamp
	 * @param cal
	 * @throws Exception 
	 */
	public static void storeUserPassword(String userId, String type, String hashPassword, String salt, java.sql.Timestamp timestamp) throws Exception {
		String insertQuery = "INSERT INTO PASSWORD_HISTORY (ID, USERID, TYPE, PASSWORD, SALT, DATE_ADDED) "
				+ "VALUES (?,?,?,?,?,?)";
		
		PreparedStatement ps = null;
		try {
			int parameterIndex = 1;
			ps = securityDb.getPreparedStatement(insertQuery);
			ps.setString(parameterIndex++, UUID.randomUUID().toString());
			ps.setString(parameterIndex++, userId);
			ps.setString(parameterIndex++, type);
			ps.setString(parameterIndex++, hashPassword);
			ps.setString(parameterIndex++, salt);
			ps.setTimestamp(parameterIndex++, timestamp);
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(ps != null) {
				ps.close();
			}
			if(ps != null && securityDb.isConnectionPooling()) {
				ps.getConnection().close();
			}
		}
		
		int passReuseCount = PasswordRequirements.getInstance().getPassReuseCount();
		if(passReuseCount > 0) {
			List<String> deleteIds = new ArrayList<>();
			
			// do we have too many stored passwords?
			SelectQueryStruct qs = new SelectQueryStruct();
			qs.addSelector(new QueryColumnSelector("PASSWORD_HISTORY__ID"));
			qs.addSelector(new QueryColumnSelector("PASSWORD_HISTORY__USERID"));
			qs.addSelector(new QueryColumnSelector("PASSWORD_HISTORY__DATE_ADDED"));
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PASSWORD_HISTORY__USERID", "==", userId));
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PASSWORD_HISTORY__TYPE", "==", type));
			qs.addOrderBy("PASSWORD_HISTORY__DATE_ADDED");
			int counter = 0;
			IRawSelectWrapper wrapper = null;
			try {
				wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
				while (wrapper.hasNext()) {
					if(passReuseCount > counter) {
						wrapper.next();
						counter++;
						continue;
					}
					String idToDelete = wrapper.next().getValues()[0].toString();
					deleteIds.add(idToDelete);
				}
			} catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
			} finally {
				if(wrapper != null) {
					try {
						wrapper.close();
					} catch (IOException e) {
						logger.error(Constants.STACKTRACE, e);
					}
				}
			}
			
			if(!deleteIds.isEmpty()) {
				String deleteQuery = "DELETE FROM PASSWORD_HISTORY WHERE ID=?";
				try {
					for(String deleteId : deleteIds) {
						ps = securityDb.getPreparedStatement(deleteQuery);
						ps.setString(1, deleteId);
						ps.addBatch();
					}
					ps.executeBatch();
					if(!ps.getConnection().getAutoCommit()) {
						ps.getConnection().commit();
					}
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				} finally {
					if(ps != null) {
						ps.close();
					}
					if(ps != null && securityDb.isConnectionPooling()) {
						ps.getConnection().close();
					}
				}
			}
		}
	}
	
	
	/**
	 * Basic validation of the user information before creating it.
	 * @param newUser
	 * @param password
	 * @param newUser
	 */
	static void validInformation(AccessToken newUser, String password, boolean isNewUser) {
		String error = "";
		if (newUser.getUsername() == null || newUser.getUsername().isEmpty()) {
			error += "User name can not be empty. ";
		}
		try {
			validEmail(newUser.getEmail(), isNewUser);
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
			error += e.getMessage();
		}
		try {
			validPassword(newUser.getId(), newUser.getProvider(), password);
		} catch(Exception e) {
			logger.error(Constants.STACKTRACE, e);
			error += e.getMessage();
		}
		try {
			newUser.setPhone(formatPhone(newUser.getPhone()));
		} catch(Exception e) {
			logger.error(Constants.STACKTRACE, e);
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
		if (!databaseUser.isEmpty()) {
			String typedHash = hash(password, databaseUser.get("SALT"));
			return databaseUser.get("PASSWORD").equals(typedHash);
		} else {
			return false;
		}
	}

	static String getUsernameByUserId(String userId) {
		/*	String query = "SELECT NAME FROM USER WHERE ID = '?1'";
			query = query.replace("?1", userId);
			IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);*/

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(NAME_COL));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(USERID_COL, "==", userId));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if (wrapper.hasNext()) {
				return (String) wrapper.next().getValues()[0];
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}
		return null;
	}

	/**
	 * Brings the user id from database.
	 * 
	 * @param username
	 * @return userId if it exists otherwise null
	 */
	public static String getUserId(String username) {
		/*	String query = "SELECT ID FROM SMSS_USER WHERE USERNAME = '?1'";
			query = query.replace("?1", username);
			IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);*/

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(USERID_COL));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(USERNAME_COL, "==", username));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(TYPE_COL, "==", AuthProvider.NATIVE.toString()));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if (wrapper.hasNext()) {
				return (String) wrapper.next().getValues()[0];
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}
		return null;

	}

	/**
	 * Brings the email from database.
	 * 
	 * @param username
	 * @return email if it exists otherwise null
	 */
	public static String getUserEmail(String username) {
		/*	String query = "SELECT EMAIL FROM SMSS_USER WHERE USERNAME = '?1'";
			query = query.replace("?1", username);
			IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);*/

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(EMAIL_COL));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(USERNAME_COL, "==", username));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if (wrapper.hasNext()) {
				return (String) wrapper.next().getValues()[0];
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}
		return null;
	}

	/**
	 * Check if the email exists
	 * @param email
	 * @return
	 */
	public static boolean userEmailExists(String email) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(EMAIL_COL));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(EMAIL_COL, "==", email));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(TYPE_COL, "==", AuthProvider.NATIVE.toString()));

		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			return wrapper.hasNext();
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		return false;
	}
	
	/**
	 * Brings the user name from database.
	 * 
	 * @param username
	 * @return userId if it exists otherwise null
	 */
	public static String getNameUser(String username) {
		/*	String query = "SELECT NAME FROM SMSS_USER WHERE USERNAME = '?1'";
			query = query.replace("?1", username);
			IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);*/

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(NAME_COL));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(USERNAME_COL, "==", username));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if (wrapper.hasNext()) {
				IHeadersDataRow sjss = wrapper.next();
				return (String) sjss.getValues()[0];
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		return null;
	}

	/**
	 * Brings all the user basic information from the database.
	 * 
	 * @param username
	 * @return User retrieved from the database otherwise null.
	 */
	private static Map<String, String> getUserFromDatabase(String username) {
		Map<String, String> user = new HashMap<>();

		/*	String query = "SELECT ID, NAME, USERNAME, EMAIL, TYPE, ADMIN, PASSWORD, SALT FROM USER WHERE USERNAME='" + username + "'";
			IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);*/

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
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(TYPE_COL, "==", AuthProvider.NATIVE.toString()));

		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
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
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}

		return user;
	}
	
	/**
	 * 
	 * @param userId
	 * @param type
	 * @param password
	 * @return
	 */
	public static boolean isPreviousPassword(String userId, AuthProvider type, String password) {
		int passReuseCount = -1;
		try {
			passReuseCount = PasswordRequirements.getInstance().getPassReuseCount();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PASSWORD_HISTORY__PASSWORD"));
		qs.addSelector(new QueryColumnSelector("PASSWORD_HISTORY__SALT"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PASSWORD_HISTORY__USERID", "==", userId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PASSWORD_HISTORY__TYPE", "==", type));
		qs.addOrderBy(new QueryColumnOrderBySelector("PASSWORD_HISTORY__DATE_ADDED", "DESC"));

		IRawSelectWrapper iterator = null;
		try {
			iterator = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			int counter = 0;
			while(iterator.hasNext()) {
				if(passReuseCount > 0 && counter < passReuseCount) {
					Object[] previousPass = iterator.next().getValues();
					String hashPass = (String) previousPass[0];
					String salt = (String) previousPass[1];
					String testPass = hash(password, salt);
					if(hashPass.equals(testPass)) {
						return true;
					}
				}
				counter++;
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(iterator != null) {
				try {
					iterator.close();
				} catch (IOException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		return false;
	}
	
	/**
	 * Reset the user password
	 * @param token
	 * @param password
	 * @return
	 * @throws Exception
	 */
	public static String performResetPassword(String userId, String password) throws Exception {
		// make sure the new password is valid or throw error
		validPassword(userId, AuthProvider.NATIVE, password);
		
		// validate the new password to run the edit logic
		String salt = SecurityQueryUtils.generateSalt();
		String hashPassword = SecurityQueryUtils.hash(password, salt);
		String updateQuery = "UPDATE SMSS_USER SET SALT=?, PASSWORD=? WHERE ID=?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(updateQuery);
			int parameterIndex = 1;
			ps.setString(parameterIndex++, salt);
			ps.setString(parameterIndex++, hashPassword);
			ps.setString(parameterIndex++, userId);
			ps.execute();
			
			java.sql.Timestamp timestamp = Utility.getCurrentSqlTimestampUTC();
			storeUserPassword(userId, AuthProvider.NATIVE.toString(), hashPassword, salt, timestamp);

			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch(SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(ps != null) {
				ps.close();
			}
			if(securityDb.isConnectionPooling()) {
				if(ps != null) {
					ps.getConnection().close();
				}
			}
		}
		
		return userId;
	}
	
}
