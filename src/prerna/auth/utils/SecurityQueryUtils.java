package prerna.auth.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.date.SemossDate;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.PixelDataType;
import prerna.util.Constants;
import prerna.util.QueryExecutionUtility;

public class SecurityQueryUtils extends AbstractSecurityUtils {
	
	private static final Logger logger = LogManager.getLogger(SecurityQueryUtils.class);
	
	/**
	 * Try to reconcile and get the engine id
	 * @return
	 */
	public static String testUserEngineIdForAlias(User user, String potentialId) {
		List<String> ids = new ArrayList<String>();
		
//		String userFilters = getUserFilters(user);
//		String query = "SELECT DISTINCT ENGINEPERMISSION.ENGINEID "
//				+ "FROM ENGINEPERMISSION INNER JOIN ENGINE ON ENGINE.ENGINEID=ENGINEPERMISSION.ENGINEID "
//				+ "WHERE ENGINE.ENGINENAME='" + potentialId + "' AND ENGINEPERMISSION.USERID IN " + userFilters;
//		
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__ENGINEID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINENAME", "==", potentialId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", getUserFiltersQs(user)));
		qs.addRelation("ENGINE", "ENGINEPERMISSION", "inner.join");

		ids = QueryExecutionUtility.flushToListString(securityDb, qs);
		if(ids.isEmpty()) {
//			query = "SELECT DISTINCT ENGINE.ENGINEID FROM ENGINE WHERE ENGINE.ENGINENAME='" + potentialId + "' AND ENGINE.GLOBAL=TRUE";

			qs = new SelectQueryStruct();
			qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID"));
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINENAME", "==", potentialId));
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__GLOBAL", "==", true, PixelDataType.BOOLEAN));
			
			ids = QueryExecutionUtility.flushToListString(securityDb, qs);
		}
		
		if (ids.size() == 1) {
			potentialId = ids.get(0);
		} else if (ids.size() > 1) {
			throw new IllegalArgumentException("There are 2 databases with the name " + potentialId + ". Please pass in the correct id to know which source you want to load from");
		}
		
		return potentialId;
	}

	/**
	 * Get the insight alias for a id
	 * @return
	 */
	public static String getInsightNameForId(String projectId, String insightId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("INSIGHT__INSIGHTNAME"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHT__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHT__INSIGHTID", "==", insightId));
		List<String> results = QueryExecutionUtility.flushToListString(securityDb, qs);
		if (results.isEmpty()) {
			return null;
		}
		return results.get(0);
	}
		
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	
	
	public static SemossDate getLastModifiedDateForInsightInProject(String projectId) {
//		String query = "SELECT DISTINCT INSIGHT.LASTMODIFIEDON "
//				+ "FROM INSIGHT "
//				+ "WHERE INSIGHT.PROJECTID='" + projectId + "'"
//				+ "ORDER BY INSIGHT.LASTMODIFIEDON DESC LIMIT 1"
//				;
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("INSIGHT__LASTMODIFIEDON"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHT__PROJECTID", "==", projectId));
		qs.addOrderBy(new QueryColumnOrderBySelector("INSIGHT__LASTMODIFIEDON", "DESC"));
		qs.setLimit(1);
		
		SemossDate date = null;
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if(wrapper.hasNext()) {
				Object[] row = wrapper.next().getValues();
				try {
					date = (SemossDate) row[0];
				} catch(Exception e) {
					// ignore
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
		
		return date;
	}
	
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	
	/*
	 * Querying user data
	 */
	
	/**
	 * Get user info from ids
	 * 
	 * @param userIds
	 * @return
	 * @throws IllegalArgumentException
	 */
	public static Map<String, Map<String, Object>> getUserInfo(List<String> userIds) throws IllegalArgumentException {
//		String query = "SELECT DISTINCT ID, NAME, USERNAME, EMAIL, TYPE, ADMIN FROM SMSS_USER ";
//		String userFilter = createFilter(userIds);
//		query += " WHERE ID " + userFilter + ";";
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__ID"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__NAME"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__USERNAME"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__EMAIL"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__TYPE"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__ADMIN"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__PUBLISHER"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__EXPORTER"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__PHONE"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__PHONEEXTENSION"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__COUNTRYCODE"));		
		
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__ID", "==", userIds));

		Map<String, Map<String, Object>> userMap = new HashMap<>();
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			String[] names = wrapper.getHeaders();
			if (wrapper.hasNext()) {
				Object[] values = wrapper.next().getValues();
				Map<String, Object> userInfo = new HashMap<>();
				String userId = values[0].toString();
				userInfo.put(names[0], userId);
				userInfo.put(names[1], values[1].toString());
				userInfo.put(names[2], values[2].toString());
				userInfo.put(names[3], values[3].toString());
				userInfo.put(names[4], values[4].toString());
				userInfo.put(names[5], values[5].toString());
				userInfo.put(names[6], values[6].toString());
				userInfo.put(names[7], values[7].toString());
				userInfo.put(names[8], values[8].toString());
				userInfo.put(names[9], values[9].toString());
				userInfo.put(names[10], values[10].toString());
				userMap.put(userId, userInfo);
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
		
		return userMap;
	}
	
	/**
	 * Get the total number of users
	 * @return
	 */
	public static int getApplicationUserCount() {
		int userCount = 0;
		
		SelectQueryStruct qs = new SelectQueryStruct();
		QueryFunctionSelector fun = new QueryFunctionSelector();
		fun.addInnerSelector(new QueryColumnSelector("SMSS_USER__ID"));
		fun.setFunction(QueryFunctionHelper.COUNT);
		qs.addSelector(fun);
		
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if(wrapper.hasNext()) {
				userCount = ((Number) wrapper.next().getValues()[0]).intValue();
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
		
		return userCount;
	}
	
	//////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////

	/**
	 * Determine if the user has publisher rights
	 * @param user
	 * @return
	 */
	public static boolean userIsPublisher(User user) {
//		String userFilters = getUserFilters(user);
//		String query = "SELECT * FROM SMSS_USER WHERE PUBLISHER=TRUE AND ID IN " + userFilters + " LIMIT 1;";
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__ID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__PUBLISHER", "==", "TRUE"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__ID", "==", getUserFiltersQs(user)));
		
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
	 * Determine if the user has exporting rights
	 * @param user
	 * @return
	 */
	public static boolean userIsExporter(User user) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__ID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__EXPORTER", "==", true, PixelDataType.BOOLEAN));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__ID", "==", getUserFiltersQs(user)));
		
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
	 * Search if there's users containing 'searchTerm' in their email or name
	 * @param searchTerm
	 * @return
	 */
	public static List<Map<String, Object>> searchForUser(String searchTerm) {
//		String query = "SELECT DISTINCT SMSS_USER.ID AS ID, SMSS_USER.NAME AS NAME, SMSS_USER.EMAIL AS EMAIL FROM SMSS_USER "
//				+ "WHERE UPPER(SMSS_USER.NAME) LIKE UPPER('%" + searchTerm + "%') "
//				+ "OR UPPER(SMSS_USER.EMAIL) LIKE UPPER('%" + searchTerm + "%') "
//				+ "OR UPPER(SMSS_USER.ID) LIKE UPPER('%" + searchTerm + "%');";
//
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__ID", "id"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__NAME", "name"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__EMAIL", "email"));

		OrQueryFilter orFilter = new OrQueryFilter();
		orFilter.addFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__NAME", "?like", searchTerm));
		orFilter.addFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__EMAIL", "?like", searchTerm));
		orFilter.addFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__ID", "?like", searchTerm));
		qs.addExplicitFilter(orFilter);
		
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}
	
	/*
	 * Check properties of user
	 */
	
	/**
	 * Check if a user (user name or email) exist in the security database
	 * @param username
	 * @param email
	 * @return true if user is found otherwise false.
	 */
	public static boolean checkUserExist(String userId) {
//		String query = "SELECT * FROM SMSS_USER WHERE ID='" + RdbmsQueryBuilder.escapeForSQLStatement(userId) + "'";
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__ID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__ID", "==", userId));
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
	 * Check if a user (user name or email) exist in the security database
	 * @param username
	 * @param email
	 * @return true if user is found otherwise false.
	 */
	public static boolean checkUserExist(String username, String email){
//		String query = "SELECT * FROM SMSS_USER WHERE USERNAME='" + RdbmsQueryBuilder.escapeForSQLStatement(username) + 
//				"' OR EMAIL='" + RdbmsQueryBuilder.escapeForSQLStatement(email) + "'";
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__USERNAME"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__USERNAME", "==", username));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__EMAIL", "==", email));
		
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
	
	public static boolean checkUserEmailExist(String email){
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__USERNAME"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__EMAIL", "==", email));
		
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
	
	public static boolean checkUsernameExist(String username){
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__USERNAME"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__USERNAME", "==", username));
		
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
	 * Check if the user is of the type requested
	 * @param userId	String representing the id of the user to check
	 * @param type
	 */
	public static Boolean isUserType(String userId, AuthProvider type) {
//		String query = "SELECT NAME FROM SMSS_USER WHERE ID='" + RdbmsQueryBuilder.escapeForSQLStatement(userId) + "' AND TYPE = '"+ type + "';";
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__NAME"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__ID", "==", userId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__TYPE", "==", type.toString()));
		
		List<String[]> ret = QueryExecutionUtility.flushRsToListOfStrArray(securityDb, qs);
		if(!ret.isEmpty()) {
			return true;
		}
		return false;
	}

	/**
	 * Get an array containing a boolean for is locked, a semossdate for last login, a semossdate for last password change
	 * @param userId
	 * @param type
	 * @return
	 */
	public static Object[] getUserLockAndLastLoginAndLastPassReset(String userId, AuthProvider type) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__LOCKED"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__LASTLOGIN"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__LASTPASSWORDRESET"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__ID", "==", userId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__TYPE", "==", type.toString()));
		
		List<Object[]> ret = QueryExecutionUtility.flushRsToListOfObjArray(securityDb, qs);
		if(!ret.isEmpty()) {
			return ret.get(0);
		}
		return new Object[qs.getSelectors().size()];
	}
}
