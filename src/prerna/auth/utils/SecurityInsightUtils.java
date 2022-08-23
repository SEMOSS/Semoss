package prerna.auth.utils;

import java.sql.Clob;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.api.SemossDataType;
import prerna.auth.AccessPermissionEnum;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.date.SemossDate;
import prerna.ds.util.RdbmsQueryBuilder;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.joins.IRelation;
import prerna.query.querystruct.joins.SubqueryRelationship;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryConstantSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.query.querystruct.selectors.QueryIfSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.PixelDataType;
import prerna.util.Constants;
import prerna.util.QueryExecutionUtility;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

public class SecurityInsightUtils extends AbstractSecurityUtils {

	private static final Logger logger = LogManager.getLogger(SecurityInsightUtils.class);
	
	/**
	 * See if the insight name exists within the engine
	 * @param projectId
	 * @param insightName
	 * @return
	 */
	public static String insightNameExists(String projectId, String insightName) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("INSIGHT__INSIGHTID"));
		QueryFunctionSelector fun = new QueryFunctionSelector();
		fun.setFunction(QueryFunctionHelper.LOWER);
		fun.addInnerSelector(new QueryColumnSelector("INSIGHT__INSIGHTNAME"));
		fun.setAlias("low_name");
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(fun, "==", insightName.toLowerCase(), PixelDataType.CONST_STRING));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHT__PROJECTID", "==", projectId));
		
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if(wrapper.hasNext()) {
				return wrapper.next().getValues()[0].toString();
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
		}
		
		return null;
	}
	
	/**
	 * See if the insight name exists within the project
	 * @param projectId
	 * @param insightName
	 * @return
	 */
	public static boolean insightNameExistsMinusId(String projectId, String insightName, String currInsightId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("INSIGHT__INSIGHTID"));
		QueryFunctionSelector fun = new QueryFunctionSelector();
		fun.setFunction(QueryFunctionHelper.LOWER);
		fun.addInnerSelector(new QueryColumnSelector("INSIGHT__INSIGHTNAME"));
		fun.setAlias("low_name");
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(fun, "==", insightName.toLowerCase(), PixelDataType.CONST_STRING));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHT__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHT__INSIGHTID", "!=", currInsightId));

		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			return wrapper.hasNext();
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
		}
		
		return false;
	}
	
	/**
	 * Get what permission the user has for a given insight
	 * @param userId
	 * @param projectId
	 * @param insightId
	 * @return
	 */
	public static String getActualUserInsightPermission(User user, String projectId, String insightId) {
		return SecurityUserInsightUtils.getActualUserInsightPermission(user, projectId, insightId);
	}
	
	/**
	 * Get the insights the user has edit access to
	 * @param user
	 * @param appId
	 * @return
	 */
	public static List<Map<String, Object>> getUserEditableInsighs(User user, String projectId) {
		String permission = SecurityUserProjectUtils.getActualUserProjectPermission(user, projectId);
		if(permission == null || permission.equals(AccessPermissionEnum.READ_ONLY.getPermission())) {
			return new Vector<>();
		}
		
		// you are either an owner or an editor
		if(permission.equals(AccessPermissionEnum.OWNER.getPermission())) {
			// you are the owner
			// you get all the insights
			SelectQueryStruct qs = new SelectQueryStruct();
			qs.addSelector(new QueryColumnSelector("INSIGHT__PROJECTID", "project_id"));
			qs.addSelector(new QueryColumnSelector("INSIGHT__INSIGHTID", "project_insight_id"));
			qs.addSelector(new QueryColumnSelector("INSIGHT__INSIGHTNAME", "name"));
			qs.addSelector(new QueryColumnSelector("INSIGHT__GLOBAL", "insight_global"));
			qs.addSelector(new QueryColumnSelector("INSIGHT__EXECUTIONCOUNT", "exec_count"));
			qs.addSelector(new QueryColumnSelector("INSIGHT__CREATEDON", "created_on"));
			qs.addSelector(new QueryColumnSelector("INSIGHT__LASTMODIFIEDON", "last_modified_on"));
			qs.addSelector(new QueryColumnSelector("INSIGHT__CACHEABLE", "cacheable"));
			qs.addSelector(new QueryConstantSelector("OWNER"));
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHT__PROJECTID", "==", projectId));
			qs.addOrderBy(new QueryColumnOrderBySelector("INSIGHT__INSIGHTNAME"));
			return QueryExecutionUtility.flushRsToMap(securityDb, qs);
		} else {
			// you are an editor
			Collection<String> userIds = getUserFiltersQs(user);

			SelectQueryStruct qs = new SelectQueryStruct();
			qs.addSelector(new QueryColumnSelector("INSIGHT__PROJECTID", "project_id"));
			qs.addSelector(new QueryColumnSelector("INSIGHT__INSIGHTID", "project_insight_id"));
			qs.addSelector(new QueryColumnSelector("INSIGHT__INSIGHTNAME", "name"));
			qs.addSelector(new QueryColumnSelector("INSIGHT__GLOBAL", "insight_global"));
			qs.addSelector(new QueryColumnSelector("INSIGHT__EXECUTIONCOUNT", "exec_count"));
			qs.addSelector(new QueryColumnSelector("INSIGHT__CREATEDON", "created_on"));
			qs.addSelector(new QueryColumnSelector("INSIGHT__LASTMODIFIEDON", "last_modified_on"));
			qs.addSelector(new QueryColumnSelector("INSIGHT__CACHEABLE", "cacheable"));
			qs.addSelector(new QueryColumnSelector("PERMISSION__NAME", "permission"));
			// must have explicit access to the insight
			qs.addRelation("INSIGHT__INSIGHTID", "USERINSIGHTPERMISSION__INSIGHTID", "inner.join");
			qs.addRelation("USERINSIGHTPERMISSION", "PERMISSION", "inner.join");
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHT__PROJECTID", "==", projectId));
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__USERID", "==", userIds));
			List<Integer> permissions = new Vector<>();
			permissions.add(AccessPermissionEnum.OWNER.getId());
			permissions.add(AccessPermissionEnum.EDIT.getId());
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__PERMISSION", "==", permissions));
			qs.addOrderBy(new QueryColumnOrderBySelector("INSIGHT__INSIGHTNAME"));
			return QueryExecutionUtility.flushRsToMap(securityDb, qs);
		}
	}
	
	/**
	 * 
	 * @param singleUserId
	 * @param projectId
	 * @param insightId
	 * @return
	 */
	static Integer getUserInsightPermission(String singleUserId, String projectId, String insightId) {
//		String query = "SELECT DISTINCT USERINSIGHTPERMISSION.PERMISSION FROM USERINSIGHTPERMISSION  "
//				+ "WHERE ENGINEID='" + engineId + "' AND INSIGHTID='" + insightId + "' AND USERID='" + singleUserId + "'";
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("USERINSIGHTPERMISSION__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__INSIGHTID", "==", insightId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__USERID", "==", singleUserId));
		
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if(wrapper.hasNext()) {
				Object val = wrapper.next().getValues()[0];
				if(val != null && val instanceof Number) {
					return ((Number) val).intValue();
				}
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
		}
		
		return null;
	}
	
	/**
	 * 
	 * @param projectId
	 * @param insightId
	 * @return
	 */
	public static boolean insightIsGlobal(String projectId, String insightId) {
//		String query = "SELECT DISTINCT INSIGHT.GLOBAL FROM INSIGHT  "
//				+ "WHERE ENGINEID='" + engineId + "' AND INSIGHTID='" + insightId + "' AND INSIGHT.GLOBAL=TRUE";
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("INSIGHT__GLOBAL"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHT__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHT__INSIGHTID", "==", insightId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHT__GLOBAL", "==", true, PixelDataType.BOOLEAN));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if(wrapper.hasNext()) {
				// i already bound that global must be true
				return true;
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
		}
		return false;
	}
	
	/**
	 * Determine if the user can edit the insight
	 * User must be database owner OR be given explicit permissions on the insight
	 * @param userId
	 * @param projectId
	 * @param insightId
	 * @return
	 */
	public static boolean userCanViewInsight(User user, String projectId, String insightId) {
		// insight is global
		return SecurityInsightUtils.insightIsGlobal(projectId, insightId)
				// if user is owner
				// they can do whatever they want
				|| SecurityProjectUtils.userIsOwner(user, projectId)
				// user can view
				|| SecurityUserInsightUtils.userCanViewInsight(user, projectId, insightId)
				// or group can view
				|| SecurityGroupInsightsUtils.userGroupCanViewInsight(user, projectId, insightId);
	}
	
	/**
	 * Determine if the user can edit the insight
	 * User must be database owner OR be given explicit permissions on the insight
	 * @param userId
	 * @param projectId
	 * @param insightId
	 * @return
	 */
	public static boolean userCanEditInsight(User user, String projectId, String insightId) {
		// if user is owner
		// they can do whatever they want
		return SecurityProjectUtils.userIsOwner(user, projectId)
				// user can edit
				|| SecurityUserInsightUtils.userCanEditInsight(user, projectId, insightId)
				// or group can edit
				|| SecurityGroupInsightsUtils.userGroupCanEditInsight(user, projectId, insightId);
	}
	
	/**
	 * Determine if the user is an owner of an insight
	 * User must be database owner OR be given explicit permissions on the insight
	 * @param userId
	 * @param projectId
	 * @param insightId
	 * @return
	 */
	public static boolean userIsInsightOwner(User user, String projectId, String insightId) {
		// if user is owner
		// they can do whatever they want
		return SecurityProjectUtils.userIsOwner(user, projectId)
				// user is owner
				|| SecurityUserInsightUtils.userIsInsightOwner(user, projectId, insightId)
				// or group is owner
				|| SecurityGroupInsightsUtils.userGroupIsOwner(user, projectId, insightId);
	}
	
	/**
	 * Determine if the user can edit the insight
	 * User must be database owner OR be given explicit permissions on the insight
	 * @param userId
	 * @param projectId
	 * @param insightId
	 * @return
	 */
	static int getMaxUserInsightPermission(User user, String projectId, String insightId) {
		return SecurityUserInsightUtils.getMaxUserInsightPermission(user, projectId, insightId);
	}

	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	
	/*
	 * Modify insight details
	 */
	
	/**
	 * 
	 * @param user
	 * @param projectId
	 * @param insightId
	 * @param isPublic
	 * @throws IllegalAccessException
	 */
	public static void setInsightGlobalWithinApp(User user, String projectId, String insightId, boolean isPublic) throws IllegalAccessException {
		if(!userIsInsightOwner(user, projectId, insightId)) {
			throw new IllegalAccessException("The user doesn't have the permission to set this database as global. Only the owner or an admin can perform this action.");
		}
		
		String query = "UPDATE INSIGHT SET GLOBAL=? WHERE PROJECTID=? AND INSIGHTID=?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			int parameterIndex = 1;
			ps.setBoolean(parameterIndex++, isPublic);
			ps.setString(parameterIndex++, projectId);
			ps.setString(parameterIndex++, insightId);
			ps.execute();
			securityDb.commit();
		} catch(SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
				if(securityDb.isConnectionPooling()) {
					try {
						ps.getConnection().close();
					} catch (SQLException e) {
						logger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}
	}
	
	/**
	 * Change the user favorite (is favorite / not favorite) for a database. Without removing its permissions.
	 * @param user
	 * @param projectId
	 * @param visibility
	 * @throws SQLException 
	 * @throws IllegalAccessException 
	 */
	public static void setInsightFavorite(User user, String projectId, String insightId, boolean isFavorite) throws SQLException, IllegalAccessException {
		SecurityUserInsightUtils.setInsightFavorite(user, projectId, insightId, isFavorite);
	}
	
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	
	/*
	 * Query for insight users
	 */
	
	/**
	 * Retrieve the list of users for a given insight
	 * @param user
	 * @param appId
	 * @param insightId
	 * @return
	 * @throws IllegalAccessException
	 */
	public static List<Map<String, Object>> getInsightUsers(User user, String projectId, String insightId) throws IllegalAccessException {
		return SecurityUserInsightUtils.getInsightUsers(user, projectId, insightId);
	}
	
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////

	/*
	 * Adding Insight
	 */

	/**
	 * 
	 * @param projectId
	 * @param insightId
	 * @param insightName
	 * @param global
	 * @param layout
	 * @param cacheable
	 * @param cacheMinutes
	 * @param cacheEncrypt
	 * @param recipe
	 */
	public static void addInsight(String projectId, String insightId, String insightName, boolean global, 
			String layout, boolean cacheable, int cacheMinutes, 
			String cacheCron, LocalDateTime cachedOn, boolean cacheEncrypt, 
			List<String> recipe) {
		String insertQuery = "INSERT INTO INSIGHT (PROJECTID, INSIGHTID, INSIGHTNAME, GLOBAL, EXECUTIONCOUNT, "
				+ "CREATEDON, LASTMODIFIEDON, LAYOUT, CACHEABLE, CACHEMINUTES, CACHECRON, CACHEDON, CACHEENCRYPT, RECIPE) "
				+ "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		
		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone(Utility.getApplicationTimeZoneId()));
		java.sql.Timestamp timestamp = java.sql.Timestamp.valueOf(LocalDateTime.now());

		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(insertQuery);
			int parameterIndex = 1;
			ps.setString(parameterIndex++, projectId);
			ps.setString(parameterIndex++, insightId);
			ps.setString(parameterIndex++, insightName);
			ps.setBoolean(parameterIndex++, global);
			ps.setInt(parameterIndex++, 0);
			ps.setTimestamp(parameterIndex++, timestamp, cal);
			ps.setTimestamp(parameterIndex++, timestamp, cal);
			ps.setString(parameterIndex++, layout);
			ps.setBoolean(parameterIndex++, cacheable);
			ps.setInt(parameterIndex++, cacheMinutes);
			if(cacheCron == null) {
				ps.setNull(parameterIndex++, java.sql.Types.VARCHAR);
			} else {
				ps.setString(parameterIndex++, cacheCron);
			}
			if(cachedOn == null) {
				ps.setNull(parameterIndex++, java.sql.Types.TIMESTAMP);
			} else {
				ps.setTimestamp(parameterIndex++, java.sql.Timestamp.valueOf(cachedOn), cal);
			}
			ps.setBoolean(parameterIndex++, cacheEncrypt);
			if(securityDb.getQueryUtil().allowClobJavaObject()) {
				Clob clob = securityDb.createClob(ps.getConnection());
				clob.setString(1, securityGson.toJson(recipe));
				ps.setClob(parameterIndex++, clob);
			} else {
				ps.setString(parameterIndex++, securityGson.toJson(recipe));
			}
			ps.execute();
			securityDb.commit();
		} catch(SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
				if(securityDb.isConnectionPooling()) {
					try {
						ps.getConnection().close();
					} catch (SQLException e) {
						logger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}
	}
	
	/**
	 * 
	 * @param user
	 * @param projectId
	 * @param insightId
	 */
	public static void addUserInsightCreator(User user, String projectId, String insightId) {
		List<AuthProvider> logins = user.getLogins();
		
		int ownerId = AccessPermissionEnum.OWNER.getId();
		String query = "INSERT INTO USERINSIGHTPERMISSION (USERID, PROJECTID, INSIGHTID, PERMISSION) VALUES (?,?,?,?)";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			for(AuthProvider login : logins) {
				String id = user.getAccessToken(login).getId();
				int parameterIndex = 1;
				ps.setString(parameterIndex++, id);
				ps.setString(parameterIndex++, projectId);
				ps.setString(parameterIndex++, insightId);
				ps.setInt(parameterIndex++, ownerId);
				ps.addBatch();
			}
			ps.executeBatch();
			securityDb.commit();
		} catch(SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
				if(securityDb.isConnectionPooling()) {
					try {
						ps.getConnection().close();
					} catch (SQLException e) {
						logger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}
	}
	
	// TODO >>>timb: push app here on create/update
	/**
	 * 
	 * @param projectId
	 * @param insightId
	 * @param insightName
	 * @param global
	 * @param layout
	 * @param cacheable
	 * @param cacheMinutes
	 * @param cacheEncrypt
	 * @param recipe
	 */
	public static void updateInsight(String projectId, String insightId, String insightName, boolean global, 
			String layout, boolean cacheable, int cacheMinutes, String cacheCron, LocalDateTime cachedOn, 
			boolean cacheEncrypt, List<String> recipe) {
		String updateQuery = "UPDATE INSIGHT SET INSIGHTNAME=?, GLOBAL=?, LASTMODIFIEDON=?, "
				+ "LAYOUT=?, CACHEABLE=?, CACHEMINUTES=?, CACHECRON=?, CACHEDON=?, CACHEENCRYPT=?,"
				+ "RECIPE=? WHERE INSIGHTID = ? AND PROJECTID=?";

		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone(Utility.getApplicationTimeZoneId()));
		java.sql.Timestamp timestamp = java.sql.Timestamp.valueOf(LocalDateTime.now());

		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(updateQuery);
			int parameterIndex = 1;
			ps.setString(parameterIndex++, insightName);
			ps.setBoolean(parameterIndex++, global);
			ps.setTimestamp(parameterIndex++, timestamp, cal);
			ps.setString(parameterIndex++, layout);
			ps.setBoolean(parameterIndex++, cacheable);
			ps.setInt(parameterIndex++, cacheMinutes);
			if(cacheCron == null || cacheCron.isEmpty()) {
				ps.setNull(parameterIndex++, java.sql.Types.VARCHAR);
			} else {
				ps.setString(parameterIndex++, cacheCron);
			}
			if(cachedOn == null) {
				ps.setNull(parameterIndex++, java.sql.Types.TIMESTAMP);
			} else {
				ps.setTimestamp(parameterIndex++, java.sql.Timestamp.valueOf(cachedOn), cal);
			}
			ps.setBoolean(parameterIndex++, cacheEncrypt);
			if(securityDb.getQueryUtil().allowClobJavaObject()) {
				Clob clob = securityDb.createClob(ps.getConnection());
				clob.setString(1, securityGson.toJson(recipe));
				ps.setClob(parameterIndex++, clob);
			} else {
				ps.setString(parameterIndex++, securityGson.toJson(recipe));
			}
			ps.setString(parameterIndex++, insightId);
			ps.setString(parameterIndex++, projectId);
			ps.execute();
			securityDb.commit();
		} catch(SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
				if(securityDb.isConnectionPooling()) {
					try {
						ps.getConnection().close();
					} catch (SQLException e) {
						logger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}
	}

	/**
	 * Update the insight name
	 * @param projectId
	 * @param insightId
	 * @param insightName
	 */
	public static void updateInsightName(String projectId, String insightId, String insightName) {
		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone(Utility.getApplicationTimeZoneId()));
		java.sql.Timestamp timestamp = java.sql.Timestamp.valueOf(LocalDateTime.now());

		String query = "UPDATE INSIGHT SET INSIGHTNAME=?, LASTMODIFIEDON=? WHERE INSIGHTID=? AND PROJECTID=?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			int parameterIndex = 1;
			ps.setString(parameterIndex++, insightName);
			ps.setTimestamp(parameterIndex++, timestamp, cal);
			ps.setString(parameterIndex++, insightId);
			ps.setString(parameterIndex++, projectId);
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch(SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
				if(securityDb.isConnectionPooling()) {
					try {
						ps.getConnection().close();
					} catch (SQLException e) {
						logger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}
	}
	
	/**
	 * Update if an insight should be cached
	 * @param projectId
	 * @param insightId
	 * @param cacheInsight
	 * @param cacheMinutes
	 * @param cacheEncrypt
	 */
	public static void updateInsightCache(String projectId, String insightId, boolean cacheInsight, int cacheMinutes, String cacheCron, LocalDateTime cachedOn, boolean cacheEncrypt) {
		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone(Utility.getApplicationTimeZoneId()));
		java.sql.Timestamp timestamp = java.sql.Timestamp.valueOf(LocalDateTime.now());
		
		String query = "UPDATE INSIGHT SET CACHEABLE=?, CACHEMINUTES=?, CACHECRON=?, CACHEDON=?, CACHEENCRYPT=?, LASTMODIFIEDON=? WHERE INSIGHTID=? AND PROJECTID=?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			int parameterIndex = 1;
			ps.setBoolean(parameterIndex++, cacheInsight);
			ps.setInt(parameterIndex++, cacheMinutes);
			if(cacheCron == null) {
				ps.setNull(parameterIndex++, java.sql.Types.VARCHAR);
			} else {
				ps.setString(parameterIndex++, cacheCron);
			}
			if(cachedOn == null) {
				ps.setNull(parameterIndex++, java.sql.Types.TIMESTAMP);
			} else {
				ps.setTimestamp(parameterIndex++, java.sql.Timestamp.valueOf(cachedOn), cal);
			}
			ps.setBoolean(parameterIndex++, cacheEncrypt);
			ps.setTimestamp(parameterIndex++, timestamp, cal);
			ps.setString(parameterIndex++, insightId);
			ps.setString(parameterIndex++, projectId);
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch(SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
				if(securityDb.isConnectionPooling()) {
					try {
						ps.getConnection().close();
					} catch (SQLException e) {
						logger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}
	}
	
	/**
	 * Update if an insight should be cached
	 * @param projectId
	 * @param insightId
	 * @param cacheInsight
	 * @param cacheMinutes
	 * @param cacheEncrypt
	 */
	public static void updateInsightCachedOn(String projectId, String insightId, LocalDateTime cachedOn) {
		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone(Utility.getApplicationTimeZoneId()));
		
		String query = "UPDATE INSIGHT SET CACHEDON=? WHERE INSIGHTID=? AND PROJECTID=?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			int parameterIndex = 1;
			if(cachedOn == null) {
				ps.setNull(parameterIndex++, java.sql.Types.TIMESTAMP);
			} else {
				ps.setTimestamp(parameterIndex++, java.sql.Timestamp.valueOf(cachedOn), cal);
			}
			ps.setString(parameterIndex++, insightId);
			ps.setString(parameterIndex++, projectId);
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch(SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
				if(securityDb.isConnectionPooling()) {
					try {
						ps.getConnection().close();
					} catch (SQLException e) {
						logger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}
	}
	
	/**
	 * Update the insight metadata for the insight
	 * Will delete existing values and then perform a bulk insert
	 * @param projectId
	 * @param insightId
	 * @param metadata
	 */
	public static void updateInsightMetadata(String projectId, String insightId, Map<String, Object> metadata) {
		// first do a delete
		final String metaKey = "tag";
		String query = "DELETE FROM INSIGHTMETA WHERE METAKEY=? AND INSIGHTID=? AND PROJECTID=?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			for(String field : metadata.keySet()) {
				int parameterIndex = 1;
				ps.setString(parameterIndex++, field);
				ps.setString(parameterIndex++, insightId);
				ps.setString(parameterIndex++, projectId);
				ps.addBatch();
			}
			ps.executeBatch();
			ps.getConnection().commit();
		} catch(Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
				if(securityDb.isConnectionPooling()) {
					try {
						ps.getConnection().close();
					} catch (SQLException e) {
						logger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}
		
		// now we do the new insert with the order of the tags
		query = securityDb.getQueryUtil().createInsertPreparedStatementString("INSIGHTMETA", 
				new String[]{"PROJECTID", "INSIGHTID", "METAKEY", "METAVALUE", "METAORDER"});
		ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			for(String field : metadata.keySet()) {
				Object val = metadata.get(field);
				List<Object> values = new ArrayList<>();
				if(val instanceof List) {
					values = (List<Object>) val;
				} else if(val instanceof Collection) {
					values.addAll( (Collection<Object>) val);
				} else {
					values.add(val);
				}
				
				for(int i = 0; i < values.size(); i++) {
					int parameterIndex = 1;
					Object fieldVal = values.get(i);
					
					ps.setString(parameterIndex++, projectId);
					ps.setString(parameterIndex++, insightId);
					ps.setString(parameterIndex++, field);
					ps.setString(parameterIndex++, fieldVal + "");
					ps.setInt(parameterIndex++, i);
					ps.addBatch();
				}
			}
			ps.executeBatch();
			ps.getConnection().commit();
		} catch(Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
				if(securityDb.isConnectionPooling()) {
					try {
						ps.getConnection().close();
					} catch (SQLException e) {
						logger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}
	}
	
	/**
	 * Update the insight description
	 * Will perform an insert if the description doesn't currently exist
	 * @param projectId
	 * @param insideId
	 * @param description
	 */
	public static void updateInsightDescription(String projectId, String insightId, String description) {
		// try to do an update
		// if nothing is updated
		// do an insert

		int updateCount = 0;
		final String META_KEY = "description";
		
		String query = "UPDATE INSIGHTMETA SET METAVALUE=? WHERE METAKEY=? AND INSIGHTID=? AND PROJECTID=?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			int parameterIndex = 1;
			ps.setString(parameterIndex++, description);
			ps.setString(parameterIndex++, META_KEY);
			ps.setString(parameterIndex++, insightId);
			ps.setString(parameterIndex++, projectId);
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
			updateCount = ps.getUpdateCount();
		} catch(SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
				if(securityDb.isConnectionPooling()) {
					try {
						ps.getConnection().close();
					} catch (SQLException e) {
						logger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}
		
		// no updates, insert
		if(updateCount <= 0) {
			try {
				query = "INSERT INTO INSIGHTMETA (PROJECTID, INSIGHTID, METAKEY, METAVALUE, METAORDER) VALUES (?,?,?,?,?)";
				ps = securityDb.getPreparedStatement(query);
				int parameterIndex = 1;
				ps.setString(parameterIndex++, projectId);
				ps.setString(parameterIndex++, insightId);
				ps.setString(parameterIndex++, META_KEY);
				ps.setString(parameterIndex++, description);
				ps.setInt(parameterIndex++, 0);
				ps.execute();
				if(!ps.getConnection().getAutoCommit()) {
					ps.getConnection().commit();
				}
			} catch(SQLException e) {
				logger.error(Constants.STACKTRACE, e);
			} finally {
				if(ps != null) {
					try {
						ps.close();
					} catch (SQLException e) {
						logger.error(Constants.STACKTRACE, e);
					}
					if(securityDb.isConnectionPooling()) {
						try {
							ps.getConnection().close();
						} catch (SQLException e) {
							logger.error(Constants.STACKTRACE, e);
						}
					}
				}
			}
		}
	}
	
	/**
	 * Update the insight tags for the insight
	 * Will delete existing values and then perform a bulk insert
	 * @param projectId
	 * @param insightId
	 * @param tags
	 */
	public static void updateInsightTags(String projectId, String insightId, List<String> tags) {
		// first do a delete
		final String metaKey = "tag";
		String query = "DELETE FROM INSIGHTMETA WHERE METAKEY=? AND INSIGHTID=? AND PROJECTID=?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			int parameterIndex = 1;
			ps.setString(parameterIndex++, metaKey);
			ps.setString(parameterIndex++, insightId);
			ps.setString(parameterIndex++, projectId);
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch(Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
				if(securityDb.isConnectionPooling()) {
					try {
						ps.getConnection().close();
					} catch (SQLException e) {
						logger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}
		
		// now we do the new insert with the order of the tags
		query = securityDb.getQueryUtil().createInsertPreparedStatementString("INSIGHTMETA", 
				new String[]{"PROJECTID", "INSIGHTID", "METAKEY", "METAVALUE", "METAORDER"});
		ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			for(int i = 0; i < tags.size(); i++) {
				String tag = tags.get(i);
				int parameterIndex = 1;
				ps.setString(parameterIndex++, projectId);
				ps.setString(parameterIndex++, insightId);
				ps.setString(parameterIndex++, metaKey);
				ps.setString(parameterIndex++, tag);
				ps.setInt(parameterIndex++, i);
				ps.addBatch();
			}
			
			ps.executeBatch();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch(Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
				if(securityDb.isConnectionPooling()) {
					try {
						ps.getConnection().close();
					} catch (SQLException e) {
						logger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}
	}
	
	/**
	 * Update the insight tags for the insight
	 * Will delete existing values and then perform a bulk insert
	 * @param projectId
	 * @param insightId
	 * @param tags
	 */
	public static void updateInsightTags(String projectId, String insightId, String[] tags) {
		// first do a delete
		final String metaKey = "tag";
		String query = "DELETE FROM INSIGHTMETA WHERE METAKEY=? AND INSIGHTID=? AND PROJECTID=?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			int parameterIndex = 1;
			ps.setString(parameterIndex++, metaKey);
			ps.setString(parameterIndex++, insightId);
			ps.setString(parameterIndex++, projectId);
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch(Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
				if(securityDb.isConnectionPooling()) {
					try {
						ps.getConnection().close();
					} catch (SQLException e) {
						logger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}
		
		// now we do the new insert with the order of the tags
		query = securityDb.getQueryUtil().createInsertPreparedStatementString("INSIGHTMETA", 
				new String[]{"PROJECTID", "INSIGHTID", "METAKEY", "METAVALUE", "METAORDER"});
		ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			for(int i = 0; i < tags.length; i++) {
				String tag = tags[i];
				int parameterIndex = 1;
				ps.setString(parameterIndex++, projectId);
				ps.setString(parameterIndex++, insightId);
				ps.setString(parameterIndex++, metaKey);
				ps.setString(parameterIndex++, tag);
				ps.setInt(parameterIndex++, i);
				ps.addBatch();
			}
			ps.executeBatch();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch(Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
				if(securityDb.isConnectionPooling()) {
					try {
						ps.getConnection().close();
					} catch (SQLException e) {
						logger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}
	}
	
	/**
	 * Update the frame information in the insight frames table
	 * @param projectId
	 * @param insightId
	 * @param insightFrames
	 */
	public static void updateInsightFrames(String projectId, String insightId, Set<ITableDataFrame> insightFrames) {
		// first do a delete
		String query = "DELETE FROM INSIGHTFRAMES WHERE INSIGHTID =? AND PROJECTID=?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			int parameterIndex = 1;
			ps.setString(parameterIndex++, insightId);
			ps.setString(parameterIndex++, projectId);
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch(Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
				if(securityDb.isConnectionPooling()) {
					try {
						ps.getConnection().close();
					} catch (SQLException e) {
						logger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}
		
		// now we do the new insert with the order of the tags
		query = securityDb.getQueryUtil().createInsertPreparedStatementString("INSIGHTFRAMES", 
				new String[]{"PROJECTID", "INSIGHTID", "TABLENAME", "TABLETYPE", "COLUMNNAME", "COLUMNTYPE"});
		ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			// loop through an add all the frames
			for(ITableDataFrame frame : insightFrames) {
				String tableName = frame.getName();
				String tableType = frame.getFrameType().getTypeAsString();
				Map<String, SemossDataType> colToTypeMap = frame.getMetaData().getHeaderToTypeMap();
				
				for(String colName : colToTypeMap.keySet()) {
					String colType = colToTypeMap.get(colName).toString().toUpperCase();
					if(colName.contains("__")) {
						colName = colName.split("__")[1];
					}
					int parameterIndex = 1;
					ps.setString(parameterIndex++, projectId);
					ps.setString(parameterIndex++, insightId);
					ps.setString(parameterIndex++, tableName);
					ps.setString(parameterIndex++, tableType);
					ps.setString(parameterIndex++, colName);
					ps.setString(parameterIndex++, colType);
					ps.addBatch();
				}
			}
			ps.executeBatch();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch(Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
				if(securityDb.isConnectionPooling()) {
					try {
						ps.getConnection().close();
					} catch (SQLException e) {
						logger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}
		
	}
	
	/**
	 * 
	 * @param projectId
	 * @param insightId
	 */
	public static void deleteInsight(String projectId, String insightId) {
		SecurityUserInsightUtils.deleteInsight(projectId, insightId);
		//TODO: delete group
	}
	
	/**
	 * 
	 * @param projectId
	 * @param insightId
	 */
	public static void deleteInsight(String projectId, String... insightId) {
		SecurityUserInsightUtils.deleteInsight(projectId, insightId);
		//TODO: delete group
	}
	
	/**
	 * Update the total execution count
	 * @param engineId
	 * @param insightId
	 */
	public static void updateExecutionCount(String projectId, String insightId) {
		String updateQuery = "UPDATE INSIGHT SET EXECUTIONCOUNT = EXECUTIONCOUNT + 1 "
				+ "WHERE PROJECTID='" + projectId + "' AND INSIGHTID='" + insightId + "'";
		try {
			securityDb.insertData(updateQuery);
			securityDb.commit();
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		}
	}
	
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////

	/**
	 * 
	 * @param user
	 * @param newUserId
	 * @param projectId
	 * @param insightId
	 * @param permission
	 * @return
	 */
	public static void addInsightUser(User user, String newUserId, String projectId, String insightId, String permission) {
		// make sure user can edit the insight
		int userPermissionLvl = getMaxUserInsightPermission(user, projectId, insightId);
		if(!AccessPermissionEnum.isEditor(userPermissionLvl)) {
			throw new IllegalArgumentException("Insufficient privileges to modify this insight's permissions.");
		}
		
		// make sure user doesn't already exist for this insight
		if(getUserInsightPermission(newUserId, projectId, insightId) != null) {
			// that means there is already a value
			throw new IllegalArgumentException("This user already has access to this insight. Please edit the existing permission level.");
		}
		
		// if i am not an owner
		// then i need to check if i can edit this users permission
		if(!AccessPermissionEnum.isOwner(userPermissionLvl)) {
			int newPermissionLvl = AccessPermissionEnum.getIdByPermission(permission);
			// also, cannot give some owner permission if i am just an editor
			if(AccessPermissionEnum.OWNER.getId() == newPermissionLvl) {
				throw new IllegalArgumentException("Cannot give owner level access to this insight since you are not currently an owner.");
			}
		}
		
		String query = "INSERT INTO USERINSIGHTPERMISSION (USERID, PROJECTID, INSIGHTID, PERMISSION) VALUES('"
				+ RdbmsQueryBuilder.escapeForSQLStatement(newUserId) + "', '"
				+ RdbmsQueryBuilder.escapeForSQLStatement(projectId) + "', '"
				+ RdbmsQueryBuilder.escapeForSQLStatement(insightId) + "', "
				+ AccessPermissionEnum.getIdByPermission(permission) + ");";

		try {
			securityDb.insertData(query);
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occured adding user permissions for this insight");
		}
	}
	
	/**
	 * 
	 * @param user
	 * @param existingUserId
	 * @param projectId
	 * @param insightId
	 * @param newPermission
	 * @return
	 * @throws IllegalAccessException 
	 */
	public static void editInsightUserPermission(User user, String existingUserId, String projectId, String insightId, String newPermission) throws IllegalAccessException {
		// make sure user can edit the insight
		int userPermissionLvl = getMaxUserInsightPermission(user, projectId, insightId);
		if(!AccessPermissionEnum.isEditor(userPermissionLvl)) {
			throw new IllegalAccessException("Insufficient privileges to modify this insight's permissions.");
		}
		
		// make sure we are trying to edit a permission that exists
		Integer existingUserPermission = getUserInsightPermission(existingUserId, projectId, insightId);
		if(existingUserPermission == null) {
			throw new IllegalArgumentException("Attempting to modify user permission for a user who does not currently have access to the insight");
		}
		
		int newPermissionLvl = AccessPermissionEnum.getIdByPermission(newPermission);
		
		// if i am not an owner
		// then i need to check if i can edit this users permission
		if(!AccessPermissionEnum.isOwner(userPermissionLvl)) {
			// not an owner, check if trying to edit an owner or an editor/reader
			// get the current permission
			if(AccessPermissionEnum.OWNER.getId() == existingUserPermission) {
				throw new IllegalAccessException("The user doesn't have the high enough permissions to modify this users insight permission.");
			}
			
			// also, cannot give some owner permission if i am just an editor
			if(AccessPermissionEnum.OWNER.getId() == newPermissionLvl) {
				throw new IllegalAccessException("Cannot give owner level access to this insight since you are not currently an owner.");
			}
		}
		
		String query = "UPDATE USERINSIGHTPERMISSION SET PERMISSION=" + newPermissionLvl
				+ " WHERE USERID='" + RdbmsQueryBuilder.escapeForSQLStatement(existingUserId) + "' "
				+ "AND PROJECTID='"	+ RdbmsQueryBuilder.escapeForSQLStatement(projectId) + "' "
				+ "AND INSIGHTID='" + RdbmsQueryBuilder.escapeForSQLStatement(insightId) + "';";
		try {
			securityDb.insertData(query);
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occured updating the user permissions for this insight");
		}
	}
	
	/**
	 * 
	 * @param user
	 * @param editedUserId
	 * @param projectId
	 * @param insightId
	 * @return
	 * @throws IllegalAccessException 
	 */
	public static void removeInsightUser(User user, String existingUserId, String projectId, String insightId) throws IllegalAccessException {
		// make sure user can edit the insight
		int userPermissionLvl = getMaxUserInsightPermission(user, projectId, insightId);
		if(!AccessPermissionEnum.isEditor(userPermissionLvl)) {
			throw new IllegalAccessException("Insufficient privileges to modify this insight's permissions.");
		}
		
		// make sure we are trying to edit a permission that exists
		Integer existingUserPermission = getUserInsightPermission(existingUserId, projectId, insightId);
		if(existingUserPermission == null) {
			throw new IllegalArgumentException("Attempting to modify user permission for a user who does not currently have access to the insight");
		}
		
		// if i am not an owner
		// then i need to check if i can remove this users permission
		if(!AccessPermissionEnum.isOwner(userPermissionLvl)) {
			// not an owner, check if trying to edit an owner or an editor/reader
			// get the current permission
			if(AccessPermissionEnum.OWNER.getId() == existingUserPermission) {
				throw new IllegalAccessException("The user doesn't have the high enough permissions to modify this users insight permission.");
			}
		}
		
		String query = "DELETE FROM USERINSIGHTPERMISSION WHERE "
				+ "USERID='" + RdbmsQueryBuilder.escapeForSQLStatement(existingUserId) + "' "
				+ "AND PROJECTID='"	+ RdbmsQueryBuilder.escapeForSQLStatement(projectId) + "' "
				+ "AND INSIGHTID='" + RdbmsQueryBuilder.escapeForSQLStatement(insightId) + "';";
		try {
			securityDb.insertData(query);
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occured removing the user permissions for this insight");
		}
	}
	
	
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	
	/*
	 * Querying for insight lists
	 */
	
	/**
	 * User has access to specific insights within a project
	 * User can access if:
	 * 	1) Is Owner, Editer, or Reader of insight
	 * 	2) Insight is global
	 * 	3) Is Owner of project
	 * 
	 * TODO >>> Kunal: change app_name and app_name_id to project references
	 * @param projectId
	 * @param userId
	 * @param searchTerm
	 * @param tags
	 * @param favoritesOnly 
	 * @param limit
	 * @param offset
	 * @return
	 */
	public static List<Map<String, Object>> searchUserInsights(User user, List<String> projectFilter, String searchTerm, 
			Boolean favoritesOnly, QueryColumnOrderBySelector sortBy, Map<String, Object> insightMetadataFilter, String limit, String offset) {
		
		Collection<String> userIds = getUserFiltersQs(user);
		// if we have filters
//		boolean tagFiltering = tags != null && !tags.isEmpty();
		boolean hasProjectFilters = projectFilter != null && !projectFilter.isEmpty();
		boolean hasSearchTerm = searchTerm != null && !searchTerm.trim().isEmpty();
		
		String insightPrefix = "INSIGHT__";
		String projectPrefix = "PROJECT__";
		String userInsightPrefix = "USERINSIGHTPERMISSION__";
		String userProjectPrefix = "PROJECTPERMISSION__";
		String groupProjectPermission = "GROUPPROJECTPERMISSION__";
		String groupInsightPermission = "GROUPINSIGHTPERMISSION__";
		
		SelectQueryStruct qs = new SelectQueryStruct();
		// TODO: delete the below 3 in the future once FE moves to project_
		qs.addSelector(new QueryColumnSelector("INSIGHT__PROJECTID", "app_id"));
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTNAME", "app_name"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__INSIGHTID", "app_insight_id"));
		// base selectors
		qs.addSelector(new QueryColumnSelector("INSIGHT__PROJECTID", "project_id"));
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTNAME", "project_name"));
		qs.addSelector(new QueryColumnSelector("PROJECT__GLOBAL", "project_global"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__INSIGHTID", "project_insight_id"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__INSIGHTNAME", "name"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__EXECUTIONCOUNT", "view_count"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__LAYOUT", "layout"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__CREATEDON", "created_on"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__LASTMODIFIEDON", "last_modified_on"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__CACHEABLE", "cacheable"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__CACHEMINUTES", "cacheMinutes"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__CACHECRON", "cacheCron"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__CACHEDON", "cachedOn"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__CACHEENCRYPT", "cacheEncrypt"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__GLOBAL", "insight_global"));
		// lower name for sorting
		qs.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.LOWER, insightPrefix + "INSIGHTNAME", "low_name"));
		// add the USER PERMISSIONS subquery returns
		qs.addSelector(new QueryColumnSelector("INSIGHT_USER_PERMISSIONS__PERMISSION", "insight_permission"));
		qs.addSelector(new QueryColumnSelector("PROJECT_USER_PERMISSIONS__PERMISSION", "project_permission"));
		qs.addSelector(new QueryColumnSelector("INSIGHT_GROUP_PERMISSIONS__PERMISSION", "insight_group_permission"));
		qs.addSelector(new QueryColumnSelector("PROJECT_GROUP_PERMISSIONS__PERMISSION", "project_group_permission"));
		qs.addSelector(new QueryColumnSelector("INSIGHT_USER_PERMISSIONS__FAVORITE", "insight_favorite"));

		// if user project owner - return owner
		// if group project owner - return owner
		// if user and group null - return User (which will be null, but that is desired)
		// if user or group null - return non null permission level
		// if both non null - return max permissions
		{
			// setup
			AndQueryFilter and = new AndQueryFilter();
			and.addFilter(SimpleQueryFilter.makeColToValFilter("INSIGHT_GROUP_PERMISSIONS__PERMISSION", "==", null, PixelDataType.CONST_INT));
			and.addFilter(SimpleQueryFilter.makeColToValFilter("INSIGHT_USER_PERMISSIONS__PERMISSION", "==", null, PixelDataType.CONST_INT));
				
			AndQueryFilter and1 = new AndQueryFilter();
			and1.addFilter(SimpleQueryFilter.makeColToValFilter("INSIGHT_GROUP_PERMISSIONS__PERMISSION", "!=", null, PixelDataType.CONST_INT));
			and1.addFilter(SimpleQueryFilter.makeColToValFilter("INSIGHT_USER_PERMISSIONS__PERMISSION", "==", null, PixelDataType.CONST_INT));
		
			AndQueryFilter and2 = new AndQueryFilter();
			and2.addFilter(SimpleQueryFilter.makeColToValFilter("INSIGHT_GROUP_PERMISSIONS__PERMISSION", "==", null, PixelDataType.CONST_INT));
			and2.addFilter(SimpleQueryFilter.makeColToValFilter("INSIGHT_USER_PERMISSIONS__PERMISSION", "!=", null, PixelDataType.CONST_INT));
			
			SimpleQueryFilter maxPermFilter = SimpleQueryFilter.makeColToColFilter("INSIGHT_USER_PERMISSIONS__PERMISSION", "<", "INSIGHT_GROUP_PERMISSIONS__PERMISSION");
			SimpleQueryFilter userOwnerFilter = SimpleQueryFilter.makeColToValFilter("PROJECT_USER_PERMISSIONS__PERMISSION", "==", AccessPermissionEnum.OWNER.getId(), PixelDataType.CONST_INT);
			SimpleQueryFilter groupOwnerFilter = SimpleQueryFilter.makeColToValFilter("PROJECT_GROUP_PERMISSIONS__PERMISSION", "==", AccessPermissionEnum.OWNER.getId(), PixelDataType.CONST_INT);


			// logic
			QueryIfSelector qis5 = QueryIfSelector.makeQueryIfSelector(maxPermFilter,
						new QueryColumnSelector("INSIGHT_USER_PERMISSIONS__PERMISSION"),
						new QueryColumnSelector("INSIGHT_GROUP_PERMISSIONS__PERMISSION"),
						"permission"
					);
			
			QueryIfSelector qis4 = QueryIfSelector.makeQueryIfSelector(and2,
						new QueryColumnSelector("INSIGHT_USER_PERMISSIONS__PERMISSION"),
						qis5,
						"permission"
					);
			
			QueryIfSelector qis3 = QueryIfSelector.makeQueryIfSelector(and1,
						new QueryColumnSelector("INSIGHT_GROUP_PERMISSIONS__PERMISSION"),
						qis4,
						"permission"
					);

			QueryIfSelector qis2 = QueryIfSelector.makeQueryIfSelector(and,
						new QueryColumnSelector("INSIGHT_USER_PERMISSIONS__PERMISSION"),
						qis3,
						"permission"
					);
			
			QueryIfSelector qis1 = QueryIfSelector.makeQueryIfSelector(groupOwnerFilter,
						new QueryConstantSelector(AccessPermissionEnum.OWNER.getId()),
						qis2,
						"permission"
					);
			
			QueryIfSelector qis = QueryIfSelector.makeQueryIfSelector(userOwnerFilter,
						new QueryConstantSelector(AccessPermissionEnum.OWNER.getId()),
						qis1,
						"permission"
					);
			
			qs.addSelector(qis);
		}
		
		// add PROJECT relation
		qs.addRelation("PROJECT", "INSIGHT", "inner.join");
		// add a join to get the user permission level and if favorite
		{
			SelectQueryStruct qs2 = new SelectQueryStruct();
			qs2.addSelector(new QueryColumnSelector(userInsightPrefix + "INSIGHTID", "INSIGHTID"));
			qs2.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.MAX, userInsightPrefix + "FAVORITE", "FAVORITE"));
			qs2.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.MIN, userInsightPrefix + "PERMISSION", "PERMISSION"));
			qs2.addGroupBy(new QueryColumnSelector(userInsightPrefix + "INSIGHTID", "INSIGHTID"));
			qs2.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(userInsightPrefix + "USERID", "==", userIds));
			IRelation subQuery = null;
			if(favoritesOnly) {
				qs2.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(userInsightPrefix + "FAVORITE", "==", true, PixelDataType.BOOLEAN));
				// we can set this to inner join if only favorites
				subQuery = new SubqueryRelationship(qs2, "INSIGHT_USER_PERMISSIONS", "inner.join", new String[] {"INSIGHT_USER_PERMISSIONS__INSIGHTID", insightPrefix + "INSIGHTID", "="});
			} else {
				subQuery = new SubqueryRelationship(qs2, "INSIGHT_USER_PERMISSIONS", "left.outer.join", new String[] {"INSIGHT_USER_PERMISSIONS__INSIGHTID", insightPrefix + "INSIGHTID", "="});
			}
			qs.addRelation(subQuery);
		}
		// add a join to get the user project permission
		{
			SelectQueryStruct qs2 = new SelectQueryStruct();
			qs2.addSelector(new QueryColumnSelector(userProjectPrefix + "PROJECTID", "PROJECTID"));
			qs2.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.MIN, userProjectPrefix + "PERMISSION", "PERMISSION"));
			qs2.addGroupBy(new QueryColumnSelector(userProjectPrefix + "PROJECTID", "PROJECTID"));
			qs2.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(userProjectPrefix + "USERID", "==", userIds));
			IRelation subQuery = new SubqueryRelationship(qs2, "PROJECT_USER_PERMISSIONS", "left.outer.join", new String[] {"PROJECT_USER_PERMISSIONS__PROJECTID", insightPrefix + "PROJECTID", "="});
			qs.addRelation(subQuery);
		}
		
		// add a join to get the group insight permission level
		{
			SelectQueryStruct qs3 = new SelectQueryStruct();
			qs3.addSelector(new QueryColumnSelector(groupInsightPermission + "INSIGHTID", "INSIGHTID"));
			qs3.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.MIN, groupInsightPermission + "PERMISSION", "PERMISSION"));
			qs3.addGroupBy(new QueryColumnSelector(groupInsightPermission + "INSIGHTID", "INSIGHTID"));
			
			// filter on groups
			OrQueryFilter groupInsightOrFilters = new OrQueryFilter();
			OrQueryFilter groupProjectOrFilters = new OrQueryFilter();
			List<AuthProvider> logins = user.getLogins();
			for(AuthProvider login : logins) {
				if(user.getAccessToken(login).getUserGroups().isEmpty()) {
					continue;
				}
				AndQueryFilter andFilter1 = new AndQueryFilter();
				andFilter1.addFilter(SimpleQueryFilter.makeColToValFilter(groupInsightPermission + "TYPE", "==", user.getAccessToken(login).getUserGroupType()));
				andFilter1.addFilter(SimpleQueryFilter.makeColToValFilter(groupInsightPermission + "ID", "==", user.getAccessToken(login).getUserGroups()));
				groupInsightOrFilters.addFilter(andFilter1);
				
				AndQueryFilter andFilter2 = new AndQueryFilter();
				andFilter2.addFilter(SimpleQueryFilter.makeColToValFilter(groupProjectPermission + "TYPE", "==", user.getAccessToken(login).getUserGroupType()));
				andFilter2.addFilter(SimpleQueryFilter.makeColToValFilter(groupProjectPermission + "ID", "==", user.getAccessToken(login).getUserGroups()));
				groupProjectOrFilters.addFilter(andFilter2);
			}
			
			if (!groupInsightOrFilters.isEmpty()) {
				qs3.addExplicitFilter(groupInsightOrFilters);
			} else {
				AndQueryFilter andFilter1 = new AndQueryFilter();
				andFilter1.addFilter(SimpleQueryFilter.makeColToValFilter(groupInsightPermission + "TYPE", "==", null));
				andFilter1.addFilter(SimpleQueryFilter.makeColToValFilter(groupInsightPermission + "ID", "==", null));
				qs3.addExplicitFilter(andFilter1);
			}
			
			IRelation subQuery = new SubqueryRelationship(qs3, "INSIGHT_GROUP_PERMISSIONS", "left.outer.join", new String[] {"INSIGHT_GROUP_PERMISSIONS__INSIGHTID", "INSIGHT__INSIGHTID", "="});
			qs.addRelation(subQuery);
		}
		
		
		// add a join to get the group project permission level
		{
			SelectQueryStruct qs4 = new SelectQueryStruct();
			qs4.addSelector(new QueryColumnSelector(groupProjectPermission + "PROJECTID", "PROJECTID"));
			qs4.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.MIN, groupProjectPermission + "PERMISSION", "PERMISSION"));
			qs4.addGroupBy(new QueryColumnSelector(groupProjectPermission + "PROJECTID", "PROJECTID"));
			
			// filter on groups
			OrQueryFilter groupProjectOrFilters = new OrQueryFilter();
			List<AuthProvider> logins = user.getLogins();
			for(AuthProvider login : logins) {
				if(user.getAccessToken(login).getUserGroups().isEmpty()) {
					continue;
				}
				
				AndQueryFilter andFilter = new AndQueryFilter();
				andFilter.addFilter(SimpleQueryFilter.makeColToValFilter(groupProjectPermission + "TYPE", "==", user.getAccessToken(login).getUserGroupType()));
				andFilter.addFilter(SimpleQueryFilter.makeColToValFilter(groupProjectPermission + "ID", "==", user.getAccessToken(login).getUserGroups()));
				groupProjectOrFilters.addFilter(andFilter);
			}
			
			if (!groupProjectOrFilters.isEmpty()) {
				qs4.addExplicitFilter(groupProjectOrFilters);
			} else {
				AndQueryFilter andFilter1 = new AndQueryFilter();
				andFilter1.addFilter(SimpleQueryFilter.makeColToValFilter(groupProjectPermission + "TYPE", "==", null));
				andFilter1.addFilter(SimpleQueryFilter.makeColToValFilter(groupProjectPermission + "ID", "==", null));
				qs4.addExplicitFilter(andFilter1);
			}
			
			IRelation subQuery = new SubqueryRelationship(qs4, "PROJECT_GROUP_PERMISSIONS", "left.outer.join", new String[] {"PROJECT_GROUP_PERMISSIONS__PROJECTID", insightPrefix + "PROJECTID", "="});
			qs.addRelation(subQuery);
		}

		
		// remove hidden projects
		{
			SelectQueryStruct subQs = new SelectQueryStruct();
			// store first and fill in sub query after
			qs.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery(projectPrefix + "PROJECTID", "!=", subQs));
			
			// fill in the sub query with the single return + filters
			subQs.addSelector(new QueryColumnSelector(userProjectPrefix + "PROJECTID"));
			subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(userProjectPrefix + "VISIBILITY", "==", false, PixelDataType.BOOLEAN));
			subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(userProjectPrefix + "USERID", "==", userIds));
		}
		// optional filters
		// on the project
		if(hasProjectFilters) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(insightPrefix + "PROJECTID", "==", projectFilter));
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(projectPrefix + "PROJECTID", "==", projectFilter));
		}
		// on the insight name
		if(hasSearchTerm) {
			securityDb.getQueryUtil().appendSearchRegexFilter(qs, insightPrefix + "INSIGHTNAME", searchTerm);
		}
		// filtering by insight meta key-value pairs (i.e. <tag>:value): for each pair, add in-filter against insightids from subquery
		if (insightMetadataFilter!=null && !insightMetadataFilter.isEmpty()) {
			for (String k : insightMetadataFilter.keySet()) {
				SelectQueryStruct subQs = new SelectQueryStruct();
				subQs.addSelector(new QueryColumnSelector("INSIGHTMETA__INSIGHTID"));
				subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHTMETA__METAKEY", "==", k));
				subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHTMETA__METAVALUE", "==", insightMetadataFilter.get(k)));
				qs.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery("INSIGHT__INSIGHTID", "==", subQs));
			}
		}
		// filter the insight ids based on
		OrQueryFilter orFilter = new OrQueryFilter();
		qs.addExplicitFilter(orFilter);
		// 1 - insights i have access to
		{
			SelectQueryStruct subQs = new SelectQueryStruct();
			// store first and fill in sub query after
			orFilter.addFilter(SimpleQueryFilter.makeColToSubQuery(insightPrefix + "INSIGHTID", "==", subQs));
			
			// fill in the sub query with the single return + filters
			subQs.addSelector(new QueryColumnSelector(userInsightPrefix + "INSIGHTID"));
			subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(userInsightPrefix + "USERID", "==", userIds));
		}
		// 2 - insight that are global within projects i have access to
		{
			SelectQueryStruct subQs = new SelectQueryStruct();
			// store first and fill in sub query after
			orFilter.addFilter(SimpleQueryFilter.makeColToSubQuery(insightPrefix + "INSIGHTID", "==", subQs));
			
			// fill in the sub query with the single return + filters
			subQs.addSelector(new QueryColumnSelector(insightPrefix + "INSIGHTID"));
			subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(insightPrefix + "GLOBAL", "==", true, PixelDataType.BOOLEAN));
			if(hasProjectFilters) {
				subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(insightPrefix + "PROJECTID", "==", projectFilter));
			} else {
				SelectQueryStruct subQs2 = new SelectQueryStruct();
				// store the subquery
				subQs.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery(insightPrefix + "PROJECTID", "==", subQs2));

				subQs2.addSelector(new QueryColumnSelector(projectPrefix + "PROJECTID"));
				// joins
				subQs2.addRelation("PROJECT", "PROJECTPERMISSION", "left.outer.join");
				// project and insight must be global must be global
				OrQueryFilter projectSubset = new OrQueryFilter();
				projectSubset.addFilter(SimpleQueryFilter.makeColToValFilter(projectPrefix + "GLOBAL", "==", true, PixelDataType.BOOLEAN));
				projectSubset.addFilter(SimpleQueryFilter.makeColToValFilter(userProjectPrefix + "USERID", "==", userIds));
				subQs2.addExplicitFilter(projectSubset);
			}
		}
		// 3 insights where i am the owner of the project
		{
			SelectQueryStruct subQs = new SelectQueryStruct();
			// store first and fill in sub query after
			orFilter.addFilter(SimpleQueryFilter.makeColToSubQuery(insightPrefix + "PROJECTID", "==", subQs));
			
			// fill in the sub query with the single return + filters
			subQs.addSelector(new QueryColumnSelector(userProjectPrefix + "PROJECTID"));
			subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(userProjectPrefix + "USERID", "==", userIds));
			subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(userProjectPrefix + "PERMISSION", "==", AccessPermissionEnum.OWNER.getId(), PixelDataType.CONST_INT));
		}
		// 4 insights i have access to from group permissions
		{
			// first lets make sure we have any groups
			OrQueryFilter groupInsightOrFilters = new OrQueryFilter();
			OrQueryFilter groupProjectOrFilters = new OrQueryFilter();
			List<AuthProvider> logins = user.getLogins();
			for(AuthProvider login : logins) {
				if(user.getAccessToken(login).getUserGroups().isEmpty()) {
					continue;
				}
				AndQueryFilter andFilter1 = new AndQueryFilter();
				andFilter1.addFilter(SimpleQueryFilter.makeColToValFilter(groupInsightPermission + "TYPE", "==", user.getAccessToken(login).getUserGroupType()));
				andFilter1.addFilter(SimpleQueryFilter.makeColToValFilter(groupInsightPermission + "ID", "==", user.getAccessToken(login).getUserGroups()));
				groupInsightOrFilters.addFilter(andFilter1);
				
				AndQueryFilter andFilter2 = new AndQueryFilter();
				andFilter2.addFilter(SimpleQueryFilter.makeColToValFilter(groupProjectPermission + "TYPE", "==", user.getAccessToken(login).getUserGroupType()));
				andFilter2.addFilter(SimpleQueryFilter.makeColToValFilter(groupProjectPermission + "ID", "==", user.getAccessToken(login).getUserGroups()));
				groupProjectOrFilters.addFilter(andFilter2);
			}
			// 4.a does the group have explicit access
			if(!groupInsightOrFilters.isEmpty()) {
				SelectQueryStruct subQs = new SelectQueryStruct();
				// store first and fill in sub query after
				orFilter.addFilter(SimpleQueryFilter.makeColToSubQuery(insightPrefix + "INSIGHTID", "==", subQs));
				
				// we need to have the insight filters
				subQs.addSelector(new QueryColumnSelector(groupInsightPermission + "INSIGHTID"));
				subQs.addExplicitFilter(groupInsightOrFilters);
			}
			// 4.b does the group have project owner access
			if(!groupProjectOrFilters.isEmpty()) {
				SelectQueryStruct subQs = new SelectQueryStruct();
				// store first and fill in sub query after
				orFilter.addFilter(SimpleQueryFilter.makeColToSubQuery(projectPrefix + "PROJECTID", "==", subQs));
				
				// we need to have the insight filters
				subQs.addSelector(new QueryColumnSelector(groupProjectPermission + "PROJECTID"));
				subQs.addExplicitFilter(groupProjectOrFilters);
				subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(groupProjectPermission + "PERMISSION", "==", AccessPermissionEnum.OWNER.getId(), PixelDataType.CONST_INT));
			}
		}
		
		// add limit and offset
		if(sortBy == null) {
			qs.addOrderBy("low_name");;
		} else {
			qs.addOrderBy(sortBy);
		}
		
		Long long_limit = -1L;
		Long long_offset = -1L;
		if(limit != null && !limit.trim().isEmpty()) {
			long_limit = Long.parseLong(limit);
		}
		if(offset != null && !offset.trim().isEmpty()) {
			long_offset = Long.parseLong(offset);
		}

		qs.setLimit(long_limit);
		qs.setOffSet(long_offset);

		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}
	
	/**
	 * TODO >>> Kunal: change app_name and app_name_id to project references
	 * Search through all insights with an optional filter on engines and an optional search term
	 * @param projectFilter
	 * @param searchTerm
	 * @param tags
	 * @param limit
	 * @param offset
	 * @return
	 */
	public static List<Map<String, Object>> searchInsights(List<String> projectFilter, String searchTerm, 
			QueryColumnOrderBySelector sortBy, Map<String, Object> insightMetadataFilter, String limit, String offset) {
		// NOTE - IF YOU CHANGE THE SELECTOR ALIAS - YOU NEED TO UPDATE THE PLACES
		// THAT CALL THIS METHOD AS THAT IS PASSED IN THE SORT BY FIELD
		SelectQueryStruct qs = new SelectQueryStruct();
		// selectors
		qs.addSelector(new QueryColumnSelector("INSIGHT__PROJECTID", "app_id"));
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTNAME", "app_name"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__INSIGHTID", "app_insight_id"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__INSIGHTNAME", "name"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__EXECUTIONCOUNT", "view_count"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__LAYOUT", "layout"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__CREATEDON", "created_on"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__LASTMODIFIEDON", "last_modified_on"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__CACHEABLE", "cacheable"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__CACHEMINUTES", "cacheMinutes"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__CACHECRON", "cacheCron"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__CACHEDON", "cachedOn"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__CACHEENCRYPT", "cacheEncrypt"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__GLOBAL", "insight_global"));
		
		QueryFunctionSelector fun = new QueryFunctionSelector();
		fun.setFunction(QueryFunctionHelper.LOWER);
		fun.addInnerSelector(new QueryColumnSelector("INSIGHT__INSIGHTNAME"));
		fun.setAlias("low_name");
		qs.addSelector(fun);
		// filters
		if(projectFilter != null && !projectFilter.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHT__PROJECTID", "==", projectFilter));
		}
		if(searchTerm != null && !searchTerm.trim().isEmpty()) {
			securityDb.getQueryUtil().appendSearchRegexFilter(qs, "INSIGHT__INSIGHTNAME", searchTerm);
		}
//		// if we have tag filters
//		boolean tagFiltering = tags != null && !tags.isEmpty();
//		if(tagFiltering) {
//			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHTMETA__METAKEY", "==", "tag"));
//			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHTMETA__METAVALUE", "==", tags));
//		}
//		// joins
		qs.addRelation("PROJECT", "INSIGHT", "inner.join");
//		if(tagFiltering) {
//			qs.addRelation("INSIGHT__INSIGHTID", "INSIGHTMETA__INSIGHTID", "inner.join");
//			qs.addRelation("INSIGHT__PROJECTID", "INSIGHTMETA__PROJECTID", "inner.join");
//		}
		// sort
		if(sortBy == null) {
			qs.addOrderBy(new QueryColumnOrderBySelector("low_name"));
		} else {
			qs.addOrderBy(sortBy);
		}
		// limit 
		if(limit != null && !limit.trim().isEmpty()) {
			qs.setLimit(Long.parseLong(limit));
		}
		// offset
		if(offset != null && !offset.trim().isEmpty()) {
			qs.setOffSet(Long.parseLong(offset));
		}
		// filtering by insightmeta key-value pairs (i.e. <tag>:value): for each pair, add in-filter against insightids from subquery
		if (insightMetadataFilter!=null && !insightMetadataFilter.isEmpty()) {
			for (String k : insightMetadataFilter.keySet()) {
				SelectQueryStruct subQs = new SelectQueryStruct();
				subQs.addSelector(new QueryColumnSelector("INSIGHTMETA__INSIGHTID"));
				subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHTMETA__METAKEY", "==", k));
				subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHTMETA__METAVALUE", "==", insightMetadataFilter.get(k)));
				qs.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery("INSIGHT__INSIGHTID", "==", subQs));
			}
		}
		
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}
	
	
	/**
	 * User has access to specific insights within a project
	 * User can access if:
	 * 	1) Is Owner, Editer, or Reader of insight
	 * 	2) Insight is global
	 * 	3) Is Owner of database
	 * 
	 * @param projectId
	 * @param userId
	 * @param searchTerm
	 * @param tags
	 * @return
	 */
	public static SelectQueryStruct searchUserInsightsUsage(User user, List<String> projectFilter, String searchTerm, List<String> tags) {
		boolean hasEngineFilters = projectFilter != null && !projectFilter.isEmpty();
		
		Collection<String> userIds = getUserFiltersQs(user);
		SelectQueryStruct qs = new SelectQueryStruct();
		// selectors
		qs.addSelector(new QueryColumnSelector("INSIGHT__PROJECTID", "app_id"));
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTNAME", "app_name"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__INSIGHTID", "app_insight_id"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__INSIGHTNAME", "name"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__EXECUTIONCOUNT", "view_count"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__CREATEDON", "created_on"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__LASTMODIFIEDON", "last_modified_on"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__CACHEABLE", "cacheable"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__GLOBAL", "insight_global"));
		qs.addSelector(new QueryColumnSelector("INSIGHTMETA__METAVALUE", "insight_tags"));
		
		// filters
		// if we have an engine filter
		// i'm assuming you want these even if visibility is false
		if(hasEngineFilters) {
			// will filter to the list of engines
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHT__PROJECTID", "==", projectFilter));
			// make sure you have access to each of these insights
			// 1) you have access based on user insight permission table -- or
			// 2) the insight is global -- or 
			// 3) you are the owner of this engine (defined by the embedded and)
			OrQueryFilter orFilter = new OrQueryFilter();
			{
				orFilter.addFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__USERID", "==", userIds));
				orFilter.addFilter(SimpleQueryFilter.makeColToValFilter("INSIGHT__GLOBAL", "==", true, PixelDataType.BOOLEAN));
				AndQueryFilter embedAndFilter = new AndQueryFilter();
				embedAndFilter.addFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__PERMISSION", "==", 1, PixelDataType.CONST_INT));
				embedAndFilter.addFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", userIds));
				orFilter.addFilter(embedAndFilter);
			}
			qs.addExplicitFilter(orFilter);
		} else {
			// search across all engines
			// so guessing you only want those you have visible to you
			// 1) the engine is global -- or
			// 2) you have access to it
			
			OrQueryFilter firstOrFilter = new OrQueryFilter();
			{
				firstOrFilter.addFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__GLOBAL", "==", true, PixelDataType.BOOLEAN));
				firstOrFilter.addFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", userIds));
			}
			qs.addExplicitFilter(firstOrFilter);

			// subquery time
			// remove those engines you have visibility as false
			{
				SelectQueryStruct subQs = new SelectQueryStruct();
				// store first and fill in sub query after
				qs.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery("PROJECT__PROJECTID", "!=", subQs));
				
				// fill in the sub query with the single return + filters
				subQs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__PROJECTID"));
				subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__VISIBILITY", "==", false, PixelDataType.BOOLEAN));
				subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", userIds));
			}
			
			OrQueryFilter secondOrFilter = new OrQueryFilter();
			{
				secondOrFilter.addFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__USERID", "==", userIds));
				secondOrFilter.addFilter(SimpleQueryFilter.makeColToValFilter("INSIGHT__GLOBAL", "==", true, PixelDataType.BOOLEAN));
				AndQueryFilter embedAndFilter = new AndQueryFilter();
				embedAndFilter.addFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__PERMISSION", "==", 1, PixelDataType.CONST_INT));
				embedAndFilter.addFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", userIds));
				embedAndFilter.addFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__VISIBILITY", "==", true, PixelDataType.BOOLEAN));
				secondOrFilter.addFilter(embedAndFilter);
			}
			qs.addExplicitFilter(secondOrFilter);
		}
		// add the search term filter
		if(searchTerm != null && !searchTerm.trim().isEmpty()) {
			securityDb.getQueryUtil().appendSearchRegexFilter(qs, "INSIGHT__INSIGHTNAME", searchTerm);
		}
		// if we have tag filters
		boolean tagFiltering = tags != null && !tags.isEmpty();
		if(tagFiltering) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHTMETA__METAKEY", "==", "tag"));
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHTMETA__METAVALUE", "==", tags));
		}
		// joins
		qs.addRelation("PROJECT", "INSIGHT", "inner.join");
		// always adding the tags as returns
//		if(tagFiltering) {
			qs.addRelation("INSIGHT__INSIGHTID", "INSIGHTMETA__INSIGHTID", "left.outer.join");
			qs.addRelation("INSIGHT__PROJECTID", "INSIGHTMETA__PROJECTID", "left.outer.join");
//		}
		qs.addRelation("ENGINE", "ENGINEPERMISSION", "left.outer.join");
		qs.addRelation("INSIGHT", "USERINSIGHTPERMISSION", "left.outer.join");
		
		return qs;
	}
	
	/**
	 * Search through all insights with an optional filter on engines and an optional search term
	 * @param projectFilter
	 * @param searchTerm
	 * @param tags
	 * @return
	 */
	public static SelectQueryStruct searchInsightsUsage(List<String> projectFilter, String searchTerm, List<String> tags) {
		SelectQueryStruct qs = new SelectQueryStruct();
		// selectors
		qs.addSelector(new QueryColumnSelector("INSIGHT__PROJECTID", "app_id"));
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTNAME", "app_name"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__INSIGHTID", "app_insight_id"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__INSIGHTNAME", "name"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__EXECUTIONCOUNT", "view_count"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__CREATEDON", "created_on"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__LASTMODIFIEDON", "last_modified_on"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__CACHEABLE", "cacheable"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__GLOBAL", "insight_global"));
		qs.addSelector(new QueryColumnSelector("INSIGHTMETA__METAVALUE", "insight_tags"));

		// filters
		if(projectFilter != null && !projectFilter.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHT__PROJECTID", "==", projectFilter));
		}
		if(searchTerm != null && !searchTerm.trim().isEmpty()) {
			securityDb.getQueryUtil().appendSearchRegexFilter(qs, "INSIGHT__INSIGHTNAME", searchTerm);
		}
		// if we have tag filters
		boolean tagFiltering = tags != null && !tags.isEmpty();
		if(tagFiltering) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHTMETA__METAKEY", "==", "tag"));
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHTMETA__METAVALUE", "==", tags));
		}
		// joins
		qs.addRelation("PROJECT", "INSIGHT", "inner.join");
		// always add tags
//		if(tagFiltering) {
		qs.addRelation("INSIGHT__INSIGHTID", "INSIGHTMETA__INSIGHTID", "left.outer.join");
		qs.addRelation("INSIGHT__PROJECTID", "INSIGHTMETA__PROJECTID", "left.outer.join");
//		}

		return qs;
	}
	
	/**
	 * Get the wrapper for additional insight metadata
	 * @param projectId
	 * @param insightIds
	 * @param metaKeys
	 * @return
	 */
	public static IRawSelectWrapper getInsightMetadataWrapper(String projectId, Collection<String> insightIds, List<String> metaKeys) {
		SelectQueryStruct qs = new SelectQueryStruct();
		// selectors
		qs.addSelector(new QueryColumnSelector("INSIGHTMETA__PROJECTID"));
		qs.addSelector(new QueryColumnSelector("INSIGHTMETA__INSIGHTID"));
		qs.addSelector(new QueryColumnSelector("INSIGHTMETA__METAKEY"));
		qs.addSelector(new QueryColumnSelector("INSIGHTMETA__METAVALUE"));
		qs.addSelector(new QueryColumnSelector("INSIGHTMETA__METAORDER"));
		// filters
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHTMETA__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHTMETA__INSIGHTID", "==", insightIds));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHTMETA__METAKEY", "==", metaKeys));
		// order
		qs.addSelector(new QueryColumnSelector("INSIGHTMETA__METAORDER"));
		
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
		} catch(Exception e) {
			logger.error(Constants.STACKTRACE, e);
		}
		return wrapper;
	}
	
	/**
	 * Get the wrapper for additional insight metadata
	 * @param projectId
	 * @param insightIds
	 * @param metaKeys
	 * @return
	 */
	public static IRawSelectWrapper getInsightMetadataWrapper(Map<String, List<String>> projectToInsightMap, List<String> metaKeys) {
		SelectQueryStruct qs = new SelectQueryStruct();
		// selectors
		qs.addSelector(new QueryColumnSelector("INSIGHTMETA__PROJECTID"));
		qs.addSelector(new QueryColumnSelector("INSIGHTMETA__INSIGHTID"));
		qs.addSelector(new QueryColumnSelector("INSIGHTMETA__METAKEY"));
		qs.addSelector(new QueryColumnSelector("INSIGHTMETA__METAVALUE"));
		qs.addSelector(new QueryColumnSelector("INSIGHTMETA__METAORDER"));
		// filters
		OrQueryFilter orFilters = new OrQueryFilter();
		for(String projectId : projectToInsightMap.keySet()) {
			AndQueryFilter andFilter = new AndQueryFilter();
			andFilter.addFilter(SimpleQueryFilter.makeColToValFilter("INSIGHTMETA__PROJECTID", "==", projectId));
			// grab the insight ids from the map
			andFilter.addFilter(SimpleQueryFilter.makeColToValFilter("INSIGHTMETA__INSIGHTID", "==", projectToInsightMap.get(projectId)));
			
			// store the and filter
			// in the list of or filters
			orFilters.addFilter(andFilter);
		}
		qs.addExplicitFilter(orFilters);
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHTMETA__METAKEY", "==", metaKeys));
		// order
		qs.addOrderBy(new QueryColumnOrderBySelector("INSIGHTMETA__METAORDER"));
		
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
		} catch(Exception e) {
			logger.error(Constants.STACKTRACE, e);
		}
		return wrapper;
	}
	
	/**
	 * Get the insight metadata for a specific insight
	 * @param projectId
	 * @param insightId
	 * @param metaKeys
	 * @return
	 */
	public static Map<String, Object> getSpecificInsightMetadata(String projectId, String insightId, List<String> metaKeys) {
		SelectQueryStruct qs = new SelectQueryStruct();
		// selectors
		qs.addSelector(new QueryColumnSelector("INSIGHTMETA__PROJECTID"));
		qs.addSelector(new QueryColumnSelector("INSIGHTMETA__INSIGHTID"));
		qs.addSelector(new QueryColumnSelector("INSIGHTMETA__METAKEY"));
		qs.addSelector(new QueryColumnSelector("INSIGHTMETA__METAVALUE"));
		qs.addSelector(new QueryColumnSelector("INSIGHTMETA__METAORDER"));
		// filters
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHTMETA__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHTMETA__INSIGHTID", "==", insightId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHTMETA__METAKEY", "==", metaKeys));
		// order
		qs.addSelector(new QueryColumnSelector("INSIGHTMETA__METAORDER"));
		
		Map<String, Object> retMap = new HashMap<String, Object>();

		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			while(wrapper.hasNext()) {
				Object[] data = wrapper.next().getValues();
				String metaKey = (String) data[2];
				String metaValue = (String) data[3];

				// AS THIS LIST EXPANDS
				// WE NEED TO KNOW IF THESE ARE MULTI VALUED OR SINGLE
				if(metaKey.equals("tag")) {
					List<String> listVal = null;
					if(retMap.containsKey("tags")) {
						listVal = (List<String>) retMap.get("tags");
					} else {
						listVal = new Vector<String>();
						retMap.put("tags", listVal);
					}
					listVal.add(metaValue);
				}
				// these will be the single valued parameters
				else {
					retMap.put(metaKey, metaValue);
				}
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
		}

		return retMap;
	}
	
	/**
	 * Get if the insight is cacheable and the number of minutes it is cacheable for
	 * @param projectId
	 * @param insightId
	 * @return
	 */
	public static Map<String, Object> getSpecificInsightCacheDetails(String projectId, String insightId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		// selectors
		qs.addSelector(new QueryColumnSelector("INSIGHT__CACHEABLE"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__CACHEMINUTES"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__CACHECRON"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__CACHEDON"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__CACHEENCRYPT"));
		// filters
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHT__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHT__INSIGHTID", "==", insightId));
		
		Map<String, Object> retMap = new HashMap<String, Object>();
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if(wrapper.hasNext()) {
				Object[] data = wrapper.next().getValues();
				Boolean cacheable = (Boolean) data[0];
				Number cacheMinutes = (Number) data[1];
				if(cacheMinutes == null) {
					cacheMinutes = -1;
				}
				String cacheCron = (String) data[2];
				SemossDate cacheOn = (SemossDate) data[3];
				Boolean cacheEncrypt = (Boolean) data[4];
				if(cacheEncrypt == null) {
					cacheEncrypt = false;
				}
				
				retMap.put("cacheable", cacheable);
				retMap.put("cacheMinutes", cacheMinutes);
				retMap.put("cacheCron", cacheCron);
				retMap.put("cacheOn", cacheOn);
				retMap.put("cacheEncrypt", cacheEncrypt);
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
		}

		return retMap;
	}

	/**
	 * Get all the available tags and their count
	 * @param engineFilters
	 * @return
	 */
	public static List<Map<String, Object>> getAvailableInsightTagsAndCounts(List<String> projectFilters) {
		SelectQueryStruct qs = new SelectQueryStruct();
		// selectors
		qs.addSelector(new QueryColumnSelector("INSIGHTMETA__METAVALUE", "tag"));
		QueryFunctionSelector fSelector = new QueryFunctionSelector();
		fSelector.setAlias("count");
		fSelector.setFunction(QueryFunctionHelper.COUNT);
		fSelector.addInnerSelector(new QueryColumnSelector("INSIGHTMETA__METAVALUE"));
		qs.addSelector(fSelector);
		// filters
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHTMETA__METAKEY", "==", "tag"));
		if(projectFilters != null && !projectFilters.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHTMETA__PROJECTID", "==", projectFilters));
		}
		// group
		qs.addGroupBy(new QueryColumnSelector("INSIGHTMETA__METAVALUE", "tag"));
		
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}
	
	/**
	 * Get the insight frames
	 * @param projectId
	 * @param insightId
	 * @return
	 */
	public static List<Object[]> getInsightFrames(String projectId, String insightId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		// selectors
		qs.addSelector(new QueryColumnSelector("INSIGHTFRAMES__INSIGHTID"));
		qs.addSelector(new QueryColumnSelector("INSIGHTFRAMES__PROJECTID"));
		qs.addSelector(new QueryColumnSelector("INSIGHTFRAMES__TABLENAME"));
		qs.addSelector(new QueryColumnSelector("INSIGHTFRAMES__TABLETYPE"));
		qs.addSelector(new QueryColumnSelector("INSIGHTFRAMES__COLUMNNAME"));
		qs.addSelector(new QueryColumnSelector("INSIGHTFRAMES__COLUMNTYPE"));
		// filters
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHTFRAMES__INSIGHTID", "==", insightId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHTFRAMES__PROJECTID", "==", projectId));
		return QueryExecutionUtility.flushRsToListOfObjArray(securityDb, qs);
	}
	
	//////////////////////////////////////////////////////////////////
	
	/*
	 * For autocompletion of user searching
	 */
	
	/**
	 * User will see specific insight predictions for their searches
	 * User can see records if:
	 * 	1) Is Owner, Editer, or Reader of insight
	 * 	2) Insight is global
	 * 	3) Is Owner of database
	 * 
	 * @param userId
	 * @param searchTerm
	 * @param limit
	 * @param offset
	 * @return
	 */
	public static List<String> predictUserInsightSearch(User user, String searchTerm, String limit, String offset) {
//		String userFilters = getUserFilters(user);
//
//		String query = "SELECT DISTINCT "
//				+ "INSIGHT.INSIGHTNAME as \"name\", "
//				+ "LOWER(INSIGHT.INSIGHTNAME) as \"low_name\" "
//				+ "FROM INSIGHT "
//				+ "LEFT JOIN ENGINEPERMISSION ON INSIGHT.ENGINEID=ENGINEPERMISSION.ENGINEID "
//				+ "LEFT JOIN USERINSIGHTPERMISSION ON USERINSIGHTPERMISSION.ENGINEID=INSIGHT.ENGINEID "
//				+ "WHERE "
//				+ "(USERINSIGHTPERMISSION.USERID IN " + userFilters + " OR INSIGHT.GLOBAL=TRUE OR "
//						+ "(ENGINEPERMISSION.PERMISSION=1 AND ENGINEPERMISSION.USERID IN " + userFilters + ") ) "
//				+ ( (searchTerm != null && !searchTerm.trim().isEmpty()) ? "AND REGEXP_LIKE(INSIGHT.INSIGHTNAME, '"+ RdbmsQueryBuilder.escapeForSQLStatement(RdbmsQueryBuilder.escapeRegexCharacters(searchTerm)) + "', 'i')" : "")
//				+ "ORDER BY LOWER(INSIGHT.INSIGHTNAME) "
//				+ ( (limit != null && !limit.trim().isEmpty()) ? "LIMIT " + limit + " " : "")
//				+ ( (offset != null && !offset.trim().isEmpty()) ? "OFFSET " + offset + " ": "")
//				;
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		Collection<String> userIds = getUserFiltersQs(user);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("INSIGHT__INSIGHTNAME", "name"));
		QueryFunctionSelector fun = new QueryFunctionSelector();
		fun.setFunction(QueryFunctionHelper.LOWER);
		fun.addInnerSelector(new QueryColumnSelector("INSIGHT__INSIGHTNAME"));
		fun.setAlias("low_name");
		qs.addSelector(fun);
		OrQueryFilter orFilters = new OrQueryFilter();
		{
			// i have access to the insight
			orFilters.addFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__USERID", "==", userIds));
			// or, the insight is global
			orFilters.addFilter(SimpleQueryFilter.makeColToValFilter("INSIGHT__GLOBAL", "==", true, PixelDataType.BOOLEAN));
			// or, i'm the app owner ( you can't hide your stuff from me O_O )
			AndQueryFilter andFilter = new AndQueryFilter();
			{
				andFilter.addFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__PERMISSION", "==", 1, PixelDataType.CONST_INT));
				andFilter.addFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", userIds));
			}
			orFilters.addFilter(andFilter);
		}
		qs.addExplicitFilter(orFilters);
		if(searchTerm != null && !searchTerm.trim().isEmpty()) {
			securityDb.getQueryUtil().appendSearchRegexFilter(qs, "INSIGHT__INSIGHTNAME", searchTerm);
		}
		// sort
		qs.addOrderBy(new QueryColumnOrderBySelector("low_name"));
		// limit 
		if(limit != null && !limit.trim().isEmpty()) {
			qs.setLimit(Long.parseLong(limit));
		}
		// offset
		if(offset != null && !offset.trim().isEmpty()) {
			qs.setOffSet(Long.parseLong(offset));
		}
		
		return QueryExecutionUtility.flushToListString(securityDb, qs);
	}
	
	public static List<String> predictInsightSearch(String searchTerm, String limit, String offset) {
//		String query = "SELECT DISTINCT "
//				+ "INSIGHT.INSIGHTNAME as \"name\", "
//				+ "LOWER(INSIGHT.INSIGHTNAME) as \"low_name\" "
//				+ "FROM INSIGHT "
//				+ ( (searchTerm != null && !searchTerm.trim().isEmpty()) ? "WHERE REGEXP_LIKE(INSIGHT.INSIGHTNAME, '"+ RdbmsQueryBuilder.escapeForSQLStatement(RdbmsQueryBuilder.escapeRegexCharacters(searchTerm)) + "', 'i')" : "")
//				+ "ORDER BY LOWER(INSIGHT.INSIGHTNAME) "
//				+ ( (limit != null && !limit.trim().isEmpty()) ? "LIMIT " + limit + " " : "")
//				+ ( (offset != null && !offset.trim().isEmpty()) ? "OFFSET " + offset + " ": "")
//				;
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("INSIGHT__INSIGHTNAME", "name"));
		QueryFunctionSelector fun = new QueryFunctionSelector();
		fun.setFunction(QueryFunctionHelper.LOWER);
		fun.addInnerSelector(new QueryColumnSelector("INSIGHT__INSIGHTNAME"));
		fun.setAlias("low_name");
		qs.addSelector(fun);
		if(searchTerm != null && !searchTerm.trim().isEmpty()) {
			securityDb.getQueryUtil().appendSearchRegexFilter(qs, "INSIGHT__INSIGHTNAME", searchTerm);
		}
		// sort
		qs.addOrderBy(new QueryColumnOrderBySelector("low_name"));
		// limit 
		if(limit != null && !limit.trim().isEmpty()) {
			qs.setLimit(Long.parseLong(limit));
		}
		// offset
		if(offset != null && !offset.trim().isEmpty()) {
			qs.setOffSet(Long.parseLong(offset));
		}
		
		return QueryExecutionUtility.flushToListString(securityDb, qs);
	}
	
	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////

	/*
	 * Copying permissions
	 */
	
	/**
	 * Copy the insight permissions from one project to another
	 * @param sourceProjectId
	 * @param targetProjectId
	 * @throws SQLException
	 */
	public static void copyInsightPermissions(String sourceProjectId, String sourceInsightId, String targetProjectId, String targetInsightId) throws Exception {
		String insertTargetAppInsightPermissionSql = "INSERT INTO USERINSIGHTPERMISSION (ENGINEID, INSIGHTID, USERID, PERMISSION) VALUES (?, ?, ?, ?)";
		PreparedStatement insertTargetAppInsightPermissionStatement = securityDb.getPreparedStatement(insertTargetAppInsightPermissionSql);
		
		// grab the permissions, filtered on the source engine id and source insight id
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("USERINSIGHTPERMISSION__PROJECTID"));
		qs.addSelector(new QueryColumnSelector("USERINSIGHTPERMISSION__INSIGHTID"));
		qs.addSelector(new QueryColumnSelector("USERINSIGHTPERMISSION__USERID"));
		qs.addSelector(new QueryColumnSelector("USERINSIGHTPERMISSION__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__PERMISSION", "==", sourceProjectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__INSIGHTID", "==", sourceInsightId));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			while(wrapper.hasNext()) {
				Object[] row = wrapper.next().getValues();
				// now loop through all the permissions
				// but with the target engine/insight id instead of the source engine/insight id
				insertTargetAppInsightPermissionStatement.setString(1, targetProjectId);
				insertTargetAppInsightPermissionStatement.setString(2, targetInsightId);
				insertTargetAppInsightPermissionStatement.setString(3, (String) row[2]);
				insertTargetAppInsightPermissionStatement.setInt(4, ((Number) row[3]).intValue());
				// add to batch
				insertTargetAppInsightPermissionStatement.addBatch();
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
			throw e;
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
		}
		
		// first delete the current app permissions on the database
		String deleteTargetAppPermissionsSql = "DELETE FROM USERINSIGHTPERMISSION WHERE PROJECTID = '" 
				+ AbstractSqlQueryUtil.escapeForSQLStatement(targetProjectId) + "' AND INSIGHTID = '" 
				+ AbstractSqlQueryUtil.escapeForSQLStatement(targetInsightId) + "'";
		securityDb.removeData(deleteTargetAppPermissionsSql);
		// execute the query
		insertTargetAppInsightPermissionStatement.executeBatch();
	}
	
	/**
	 * Returns List of users that have no access credentials to a given insight 
	 * @param insightID
	 * @return 
	 */
	public static List<Map<String, Object>> getInsightUsersNoCredentials(User user, String projectId, String insightId) throws IllegalAccessException {
		/*
		 * Security check to ensure the user can access the insight provided. 
		 */
		if(!userCanViewInsight(user, projectId, insightId)) {
			throw new IllegalAccessException("The user does not have access to view this insight");
		}
		
		/*
		 * String Query = 
		 * "SELECT SMSS_USER.ID, SMSS_USER.USERNAME, SMSS_USER.NAME, SMSS_USER.EMAIL FROM SMSS_USER WHERE SMSS_USER.ID NOT IN 
		 * (SELECT u.USERID FROM USERINSIGHTPERMISSION u WHERE u.ENGINEID == '" + appID + "' AND u.INSIGHTID == '"+insightID +"'AND u.PERMISSION IS NOT NULL);"
		 */
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__ID", "id"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__USERNAME", "username"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__NAME", "name"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__EMAIL", "email"));
		//Filter for sub-query
		{
			SelectQueryStruct subQs = new SelectQueryStruct();
			qs.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery("SMSS_USER__ID", "!=", subQs));
			//Sub-query itself
			subQs.addSelector(new QueryColumnSelector("USERINSIGHTPERMISSION__USERID"));
			subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__PROJECTID", "==", projectId));
			subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__INSIGHTID", "==", insightId));
			subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__PERMISSION","!=",null,PixelDataType.NULL_VALUE));
		}
		
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}
	
	/**
	 * 
	 * @param metakey
	 * @return
	 */
	public static List<Map<String, Object>> getMetakeyOptions(String metakey) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("INSIGHTMETAKEYS__METAKEY"));
		qs.addSelector(new QueryColumnSelector("INSIGHTMETAKEYS__SINGLEMULTI"));
		qs.addSelector(new QueryColumnSelector("INSIGHTMETAKEYS__DISPLAYORDER"));
		qs.addSelector(new QueryColumnSelector("INSIGHTMETAKEYS__DISPLAYOPTIONS"));
		if (metakey != null) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHTMETAKEYS__METAKEY", "==", metakey));
		}
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}
	
	/**
	 * 
	 * @param metaoptions
	 * @return
	 */
	public static boolean updateMetakeyOptions(List<Map<String,Object>> metaoptions) {
		boolean valid = false;
		String[] colNames = new String[]{Constants.METAKEY, Constants.SINGLE_MULTI, Constants.DISPLAY_ORDER, Constants.DISPLAY_OPTIONS};
        PreparedStatement insertPs = null;
        String tableName = "INSIGHTMETAKEYS";
        try {
			// first truncate table clean 
			String truncateSql = "TRUNCATE TABLE " + tableName;
			securityDb.removeData(truncateSql);
			insertPs = securityDb.getPreparedStatement(RdbmsQueryBuilder.createInsertPreparedStatementString(tableName, colNames));
			// then insert latest options
			for (int i = 0; i < metaoptions.size(); i++) {
				insertPs.setString(1, (String) metaoptions.get(i).get("metakey"));
				insertPs.setString(2, (String) metaoptions.get(i).get("singlemulti"));
				insertPs.setInt(3, ((Number) metaoptions.get(i).get("order")).intValue());
				insertPs.setString(4, (String) metaoptions.get(i).get("displayoptions"));
				insertPs.addBatch();
			}
			insertPs.executeBatch();
			valid = true;
        } catch (SQLException e) {
        	logger.error(Constants.STACKTRACE, e);
        } finally {
        	if(insertPs != null) {
				try {
					insertPs.close();
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
				if(securityDb.isConnectionPooling()) {
					try {
						insertPs.getConnection().close();
					} catch (SQLException e) {
						logger.error(Constants.STACKTRACE, e);
					}
				}
			}
        }
		return valid;
	}

}