package prerna.auth.utils;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import prerna.auth.AccessToken;
import prerna.ds.util.RdbmsQueryBuilder;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.rdf.engine.wrappers.WrapperManager;

public class NativeUserSecurityUtils extends AbstractSecurityUtils {

	private NativeUserSecurityUtils() {
		
	}
	
	/*
	 * Native user CRUD 
	 */
	
	/**
	 * Adds a new user to the database. Does not create any relations, simply the node.
	 * @param userName	String representing the name of the user to add
	 */
	public static Boolean addNativeUser(AccessToken newUser, String password) throws IllegalArgumentException{
		validInformation(newUser, password);
		
		// is this an admin added user???
//		String query = "SELECT ID FROM USER WHERE "
//				+ "NAME='" + ADMIN_ADDED_USER + "' AND "
//				// this matching the ID field to the email because admin added user only sets the id field
//				+ "(ID='" + RdbmsQueryBuilder.escapeForSQLStatement(newUser.getId()) + "' OR ID='" + RdbmsQueryBuilder.escapeForSQLStatement(newUser.getEmail()) + "')";
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("USER__ID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USER__NAME", "==", ADMIN_ADDED_USER));
		OrQueryFilter orFilter = new OrQueryFilter();
		orFilter.addFilter(SimpleQueryFilter.makeColToValFilter("USER__ID", "==", newUser.getId()));
		orFilter.addFilter(SimpleQueryFilter.makeColToValFilter("USER__ID", "==", newUser.getEmail()));
		qs.addExplicitFilter(orFilter);
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if(wrapper.hasNext()) {
				// this was the old id that was added when the admin 
				String oldId = RdbmsQueryBuilder.escapeForSQLStatement(wrapper.next().getValues()[0].toString());
				
				String newId = RdbmsQueryBuilder.escapeForSQLStatement(newUser.getId());
				// this user was added by the user
				// and we need to update
				
				String salt = SecurityQueryUtils.generateSalt();
				String hashedPassword = (SecurityQueryUtils.hash(password, salt));
				
				String updateQuery = "UPDATE USER SET "
						+ "ID='"+ newId + "', "
						+ "NAME='"+ RdbmsQueryBuilder.escapeForSQLStatement(newUser.getName()) + "', "
						+ "USERNAME='" + RdbmsQueryBuilder.escapeForSQLStatement(newUser.getUsername()) + "', "
						+ "EMAIL='" + RdbmsQueryBuilder.escapeForSQLStatement(newUser.getEmail()) + "', "
						+ "TYPE='" + newUser.getProvider() + "',"
						+ "PASSWORD='" + hashedPassword + "',"
						+ "SALT='" + salt + "' "
						+ "WHERE ID='" + oldId + "';";
				try {
					securityDb.insertData(updateQuery);
				} catch (SQLException e) {
					e.printStackTrace();
				}
				
				// need to update any other permissions that were set for this user
				updateQuery = "UPDATE ENGINEPERMISSION SET USERID='" +  newId +"' WHERE USERID='" + oldId + "'";
				try {
					securityDb.insertData(updateQuery);
				} catch (SQLException e) {
					e.printStackTrace();
				}
				
				// need to update all the places the user id is used
				updateQuery = "UPDATE USERINSIGHTPERMISSION SET USERID='" +  newId +"' WHERE USERID='" + oldId + "'";
				try {
					securityDb.insertData(updateQuery);
				} catch (SQLException e) {
					e.printStackTrace();
				}
				
				securityDb.commit();
				return true;
			} else {
				// not added by admin
				// lets see if he exists or not
				boolean isNewUser = SecurityQueryUtils.checkUserExist(newUser.getUsername(), newUser.getEmail());
				if(!isNewUser) {
					String salt = SecurityQueryUtils.generateSalt();
					String hashedPassword = (SecurityQueryUtils.hash(password, salt));
					
					String query = "INSERT INTO USER (ID, NAME, USERNAME, EMAIL, TYPE, ADMIN, PASSWORD, SALT) VALUES ('" + 
							RdbmsQueryBuilder.escapeForSQLStatement(newUser.getId()) + "', '" + 
							RdbmsQueryBuilder.escapeForSQLStatement(newUser.getName()) + "', '" + 
							RdbmsQueryBuilder.escapeForSQLStatement(newUser.getUsername()) + "', '" + 
							RdbmsQueryBuilder.escapeForSQLStatement(newUser.getEmail()) + "', '" + 
							newUser.getProvider() + "', 'FALSE', '" + hashedPassword + "', '" + salt + "');";
					try {
						securityDb.insertData(query);
						securityDb.commit();
					} catch (SQLException e) {
						e.printStackTrace();
					}
					return true;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			wrapper.cleanUp();
		}
		
		return false;
	}
	
	/**
	 * Basic validation of the user information before creating it.
	 * @param newUser
	 * @throws IllegalArgumentException
	 */
	static void validInformation(AccessToken newUser, String password) throws IllegalArgumentException {
		String error = "";
		if(newUser.getUsername() == null || newUser.getUsername().isEmpty()){
			error += "User name can not be empty. ";
		}
		error += validEmail(newUser.getEmail());
		error += validPassword(password);
		if(!error.isEmpty()){
			throw new IllegalArgumentException(error);
		}
	}
	
	/**
	 * Verifies user information provided in the log in screen to allow or not 
	 * the entry in the application.
	 * @param user user name
	 * @param password
	 * @return true if user exist and password is correct otherwise false.
	 */
	public static boolean logIn(String user, String password){
		Map<String, String> databaseUser = getUserFromDatabase(user);
		if(!databaseUser.isEmpty()){
			String typedHash = hash(password, databaseUser.get("SALT"));
			return databaseUser.get("PASSWORD").equals(typedHash);
		} else {
			return false;
		}
	}
	
	static String getUsernameByUserId(String userId) {
//		String query = "SELECT NAME FROM USER WHERE ID = '?1'";
//		query = query.replace("?1", userId);
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("USER__NAME"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USER__ID", "==", userId));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if(wrapper.hasNext()) {
				return wrapper.next().getValues()[0].toString();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
		}
		return null;
	}
	
	/**
	 * Brings the user id from database.
	 * @param username
	 * @return userId if it exists otherwise null
	 */
	public static String getUserId(String username) {
//		String query = "SELECT ID FROM USER WHERE USERNAME = '?1'";
//		query = query.replace("?1", username);
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("USER__ID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USER__USERNAME", "==", username));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if(wrapper.hasNext()) {
				return wrapper.next().getValues()[0].toString();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
		}
		return null;

	}
	
	/**
	 * Brings the email from database.
	 * @param username
	 * @return email if it exists otherwise null
	 */
	public static String getUserEmail(String username) {
//		String query = "SELECT EMAIL FROM USER WHERE USERNAME = '?1'";
//		query = query.replace("?1", username);
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("USER__EMAIL"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USER__USERNAME", "==", username));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if (wrapper.hasNext()) {
				return wrapper.next().getValues()[0].toString();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
		}
		return null;
	}
	
	/**
	 * Brings the user name from database.
	 * @param username
	 * @return userId if it exists otherwise null
	 */
	public static String getNameUser(String username) {
//		String query = "SELECT NAME FROM USER WHERE USERNAME = '?1'";
//		query = query.replace("?1", username);
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("USER__NAME"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USER__USERNAME", "==", username));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if(wrapper.hasNext()) {
				IHeadersDataRow sjss = wrapper.next();
				return sjss.getValues()[0].toString();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
		}
		return null;

	}

	/**
	 * Brings all the user basic information from the database.
	 * @param username 
	 * @return User retrieved from the database otherwise null.
	 */
	private static Map<String, String> getUserFromDatabase(String username) {
		Map<String, String> user = new HashMap<String, String>();

//		String query = "SELECT ID, NAME, USERNAME, EMAIL, TYPE, ADMIN, PASSWORD, SALT FROM USER WHERE USERNAME='" + username + "'";
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("USER__ID"));
		qs.addSelector(new QueryColumnSelector("USER__NAME"));
		qs.addSelector(new QueryColumnSelector("USER__USERNAME"));
		qs.addSelector(new QueryColumnSelector("USER__EMAIL"));
		qs.addSelector(new QueryColumnSelector("USER__TYPE"));
		qs.addSelector(new QueryColumnSelector("USER__ADMIN"));
		qs.addSelector(new QueryColumnSelector("USER__PASSWORD"));
		qs.addSelector(new QueryColumnSelector("USER__SALT"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USER__USERNAME", "==", username));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			String[] names = wrapper.getHeaders();
			if(wrapper.hasNext()) {
				Object[] values = wrapper.next().getValues();
				user.put(names[0], values[0].toString());
				user.put(names[1], values[1].toString());
				user.put(names[2], values[2].toString());
				user.put(names[3], values[3].toString());
				user.put(names[4], values[4].toString());
				user.put(names[5], values[5].toString());
				user.put(names[6], values[6].toString());
				user.put(names[7], values[7].toString());
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
		}
		
		return user;
	}
}
