package prerna.auth.utils;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.ds.util.RdbmsQueryBuilder;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class SecurityNativeUserUtils extends AbstractSecurityUtils {

	private static final Logger logger = LogManager.getLogger(SecurityNativeUserUtils.class);

	private static final String SMSS_USER_ID_KEY = "SMSS_USER__ID";
	private static final String SMSS_USER_NAME_KEY = "SMSS_USER__NAME";
	private static final String SMSS_USER_USERNAME_KEY = "SMSS_USER__USERNAME";
	private static final String SMSS_USER_EMAIL_KEY = "SMSS_USER__EMAIL";
	private static final String SMSS_USER_TYPE_KEY = "SMSS_USER__TYPE";
	private static final String SMSS_USER_ADMIN_KEY = "SMSS_USER__ADMIN";
	private static final String SMSS_USER_PASSWORD_KEY = "SMSS_USER__PASSWORD";
	private static final String SMSS_USER_SALT_KEY = "SMSS_USER__SALT";

	private SecurityNativeUserUtils() {

	}

	/*
	 * Native user CRUD 
	 */

	/**
	 * Adds a new user to the database. Does not create any relations, simply the
	 * node.
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
		
		validInformation(newUser, password);
		
		// is this an admin added user???
		/*String query = "SELECT ID FROM USER WHERE "
				+ "NAME='" + ADMIN_ADDED_USER + "' AND "
				// this matching the ID field to the email because admin added user only sets the id field
				+ "(ID='" + RdbmsQueryBuilder.escapeForSQLStatement(newUser.getId()) + "' OR ID='" + RdbmsQueryBuilder.escapeForSQLStatement(newUser.getEmail()) + "')";
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);*/

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(SMSS_USER_ID_KEY));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(SMSS_USER_NAME_KEY, "==", ADMIN_ADDED_USER));
		OrQueryFilter orFilter = new OrQueryFilter();
		orFilter.addFilter(SimpleQueryFilter.makeColToValFilter(SMSS_USER_ID_KEY, "==", newUser.getId()));
		orFilter.addFilter(SimpleQueryFilter.makeColToValFilter(SMSS_USER_ID_KEY, "==", newUser.getEmail()));
		qs.addExplicitFilter(orFilter);
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if (wrapper.hasNext()) {
				// this was the old id that was added when the admin
				String oldId = RdbmsQueryBuilder.escapeForSQLStatement(wrapper.next().getValues()[0].toString());
				String newId = RdbmsQueryBuilder.escapeForSQLStatement(newUser.getId());

				// this user was added by the user and we need to update
				String salt = SecurityQueryUtils.generateSalt();
				String hashedPassword = (SecurityQueryUtils.hash(password, salt));

				String updateQuery = "UPDATE SMSS_USER SET " + "ID='" + newId + "', " + "NAME='"
						+ RdbmsQueryBuilder.escapeForSQLStatement(newUser.getName()) + "', " + "USERNAME='"
						+ RdbmsQueryBuilder.escapeForSQLStatement(newUser.getUsername()) + "', " + "EMAIL='"
						+ RdbmsQueryBuilder.escapeForSQLStatement(newUser.getEmail()) + "', " + "TYPE='"
						+ newUser.getProvider().toString() + "'," + "PASSWORD='" + hashedPassword + "'," + "SALT='" + salt + "' "
						+ "WHERE ID='" + oldId + "';";
				insertData(updateQuery);

				// need to update any other permissions that were set for this user
				updateQuery = "UPDATE ENGINEPERMISSION SET USERID='" + newId + "' WHERE USERID='" + oldId + "'";
				insertData(updateQuery);

				// need to update all the places the user id is used
				updateQuery = "UPDATE USERINSIGHTPERMISSION SET USERID='" + newId + "' WHERE USERID='" + oldId + "'";
				insertData(updateQuery);

				securityDb.commit();
				return true;
			} else {
				// not added by admin
				// lets see if he exists or not
				boolean isNewUser = SecurityQueryUtils.checkUserExist(newUser.getUsername(), newUser.getEmail());
				if (!isNewUser) {
					String salt = AbstractSecurityUtils.generateSalt();
					String hashedPassword = (AbstractSecurityUtils.hash(password, salt));

					String query = "INSERT INTO SMSS_USER (ID, NAME, USERNAME, EMAIL, TYPE, ADMIN, PASSWORD, SALT) VALUES ('"
							+ RdbmsQueryBuilder.escapeForSQLStatement(newUser.getId()) + "', '"
							+ RdbmsQueryBuilder.escapeForSQLStatement(newUser.getName()) + "', '"
							+ RdbmsQueryBuilder.escapeForSQLStatement(newUser.getUsername()) + "', '"
							+ RdbmsQueryBuilder.escapeForSQLStatement(newUser.getEmail()) + "', '"
							+ newUser.getProvider().toString() + "', 'FALSE', '" + hashedPassword + "', '" + salt + "');";
					try {
						securityDb.insertData(query);
						securityDb.commit();
					} catch (SQLException e) {
						logger.error(Constants.STACKTRACE, e);
					}
					return true;
				}
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if (wrapper != null) {
				wrapper.cleanUp();
			}
		}

		return false;
	}

	private static void insertData(String updateQuery) {
		try {
			securityDb.insertData(updateQuery);
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		}
	}

	/**
	 * Basic validation of the user information before creating it.
	 * 
	 * @param newUser
	 * @throws IllegalArgumentException
	 */
	static void validInformation(AccessToken newUser, String password) {
		String error = "";
		if (newUser.getUsername() == null || newUser.getUsername().isEmpty()) {
			error += "User name can not be empty. ";
		}
		error += validEmail(newUser.getEmail());
		error += validPassword(password);
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
		qs.addSelector(new QueryColumnSelector(SMSS_USER_NAME_KEY));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(SMSS_USER_ID_KEY, "==", userId));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if (wrapper.hasNext()) {
				return (String) wrapper.next().getValues()[0];
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if (wrapper != null) {
				wrapper.cleanUp();
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
		qs.addSelector(new QueryColumnSelector(SMSS_USER_ID_KEY));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(SMSS_USER_USERNAME_KEY, "==", username));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(SMSS_USER_TYPE_KEY, "==", AuthProvider.NATIVE.toString()));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if (wrapper.hasNext()) {
				return (String) wrapper.next().getValues()[0];
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if (wrapper != null) {
				wrapper.cleanUp();
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
		qs.addSelector(new QueryColumnSelector(SMSS_USER_EMAIL_KEY));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(SMSS_USER_USERNAME_KEY, "==", username));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if (wrapper.hasNext()) {
				return (String) wrapper.next().getValues()[0];
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if (wrapper != null) {
				wrapper.cleanUp();
			}
		}
		return null;
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
		qs.addSelector(new QueryColumnSelector(SMSS_USER_NAME_KEY));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(SMSS_USER_USERNAME_KEY, "==", username));
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
			if (wrapper != null) {
				wrapper.cleanUp();
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
		qs.addSelector(new QueryColumnSelector(SMSS_USER_ID_KEY));
		qs.addSelector(new QueryColumnSelector(SMSS_USER_NAME_KEY));
		qs.addSelector(new QueryColumnSelector(SMSS_USER_USERNAME_KEY));
		qs.addSelector(new QueryColumnSelector(SMSS_USER_EMAIL_KEY));
		qs.addSelector(new QueryColumnSelector(SMSS_USER_TYPE_KEY));
		qs.addSelector(new QueryColumnSelector(SMSS_USER_ADMIN_KEY));
		qs.addSelector(new QueryColumnSelector(SMSS_USER_PASSWORD_KEY));
		qs.addSelector(new QueryColumnSelector(SMSS_USER_SALT_KEY));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(SMSS_USER_USERNAME_KEY, "==", username));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(SMSS_USER_TYPE_KEY, "==", AuthProvider.NATIVE.toString()));

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
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if (wrapper != null) {
				wrapper.cleanUp();
			}
		}

		return user;
	}
}
