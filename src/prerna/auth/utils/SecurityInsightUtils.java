package prerna.auth.utils;

import java.sql.Clob;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessPermission;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.ds.util.RdbmsQueryBuilder;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.querystruct.GenExpression;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.query.querystruct.SQLQueryUtils;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryConstantSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.query.querystruct.update.UpdateQueryStruct;
import prerna.query.querystruct.update.UpdateSqlInterpreter;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.Join;
import prerna.sablecc2.om.PixelDataType;
import prerna.util.Constants;
import prerna.util.QueryExecutionUtility;
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
		Collection<String> userIds = getUserFiltersQs(user);

		// if user is owner
		// they can do whatever they want
		if(SecurityProjectUtils.userIsOwner(userIds, projectId)) {
			return AccessPermission.OWNER.getPermission();
		}
		
//		// query the database
//		String query = "SELECT DISTINCT USERINSIGHTPERMISSION.PERMISSION FROM USERINSIGHTPERMISSION  "
//				+ "WHERE ENGINEID='" + engineId + "' AND INSIGHTID='" + insightId + "' AND USERID IN " + userFilters;
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("USERINSIGHTPERMISSION__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__INSIGHTID", "==", insightId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__USERID", "==", userIds));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if(wrapper.hasNext()) {
				Object val = wrapper.next().getValues()[0];
				if(val != null && val instanceof Number) {
					return AccessPermission.getPermissionValueById( ((Number) val).intValue() );
				}
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
		}
		
		if(SecurityQueryUtils.insightIsGlobal(projectId, insightId)) {
			return AccessPermission.READ_ONLY.getPermission();
		}
		
		return null;
	}
	
	/**
	 * TODO >>> Kunal: change app_id to projectId. Have to cordinate with FE.
	 * Get the insights the user has edit access to
	 * @param user
	 * @param appId
	 * @return
	 */
	public static List<Map<String, Object>> getUserEditableInsighs(User user, String projectId) {
		
		String permission = SecurityProjectUtils.getActualUserProjectPermission(user, projectId);
		if(permission == null || permission.equals(AccessPermission.READ_ONLY.getPermission())) {
			return new Vector<>();
		}
		
		// you are either an owner or an editor
		if(permission.equals(AccessPermission.OWNER.getPermission())) {
			// you are the owner
			// you get all the insights
			SelectQueryStruct qs = new SelectQueryStruct();
			qs.addSelector(new QueryColumnSelector("INSIGHT__PROJECTID", "app_id"));
			qs.addSelector(new QueryColumnSelector("INSIGHT__INSIGHTID", "app_insight_id"));
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
			qs.addSelector(new QueryColumnSelector("INSIGHT__PROJECTID", "app_id"));
			qs.addSelector(new QueryColumnSelector("INSIGHT__INSIGHTID", "app_insight_id"));
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
			permissions.add(AccessPermission.OWNER.getId());
			permissions.add(AccessPermission.EDIT.getId());
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
	 * Determine if the user can edit the insight
	 * User must be database owner OR be given explicit permissions on the insight
	 * @param userId
	 * @param projectId
	 * @param insightId
	 * @return
	 */
	public static boolean userCanViewInsight(User user, String projectId, String insightId) {
		Collection<String> userIds = getUserFiltersQs(user);

		if(SecurityQueryUtils.insightIsGlobal(projectId, insightId)) {
			return true;
		}
		
		// if user is owner
		// they can do whatever they want
		if(SecurityProjectUtils.userIsOwner(userIds, projectId)) {
			return true;
		}
		
		// else query the database
//		String query = "SELECT DISTINCT USERINSIGHTPERMISSION.PERMISSION FROM USERINSIGHTPERMISSION  "
//				+ "WHERE ENGINEID='" + engineId + "' AND INSIGHTID='" + insightId + "' AND USERID IN " + userFilters;
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("USERINSIGHTPERMISSION__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__INSIGHTID", "==", insightId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__USERID", "==", userIds));
		
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if(wrapper.hasNext()) {
				// do not care if owner/edit/read
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
	public static boolean userCanEditInsight(User user, String projectId, String insightId) {
		Collection<String> userIds = getUserFiltersQs(user);

		// if user is owner
		// they can do whatever they want
		if(SecurityProjectUtils.userIsOwner(userIds, projectId)) {
			return true;
		}
		
		// else query the database
//		String query = "SELECT DISTINCT USERINSIGHTPERMISSION.PERMISSION FROM USERINSIGHTPERMISSION "
//				+ "WHERE ENGINEID='" + engineId + "' AND INSIGHTID='" + insightId + "' AND USERID IN " + userFilters;
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("USERINSIGHTPERMISSION__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__INSIGHTID", "==", insightId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__USERID", "==", userIds));
		
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			while(wrapper.hasNext()) {
				Object val = wrapper.next().getValues()[0];
				if(val == null) {
					return false;
				}
				int permission = ((Number) val).intValue();
				if(AccessPermission.isEditor(permission)) {
					return true;
				}
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
	 * Determine if the user is an owner of an insight
	 * User must be database owner OR be given explicit permissions on the insight
	 * @param userId
	 * @param projectId
	 * @param insightId
	 * @return
	 */
	public static boolean userIsInsightOwner(User user, String projectId, String insightId) {
		Collection<String> userIds = getUserFiltersQs(user);

		// if user is owner of app
		// they can do whatever they want
		if(SecurityProjectUtils.userIsOwner(userIds, projectId)) {
			return true;
		}
		
		// else query the database
//		String query = "SELECT DISTINCT USERINSIGHTPERMISSION.PERMISSION FROM USERINSIGHTPERMISSION "
//				+ "WHERE ENGINEID='" + engineId + "' AND INSIGHTID='" + insightId + "' AND USERID IN " + userFilters;
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("USERINSIGHTPERMISSION__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__INSIGHTID", "==", insightId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__USERID", "==", userIds));
		
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			while(wrapper.hasNext()) {
				Object val = wrapper.next().getValues()[0];
				if(val == null) {
					return false;
				}
				int permission = ((Number) val).intValue();
				if(AccessPermission.isOwner(permission)) {
					return true;
				}
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
	static int getMaxUserInsightPermission(User user, String projectId, String insightId) {
		Collection<String> userIds = getUserFiltersQs(user);

		// if user is owner of the app
		// they can do whatever they want
		if(SecurityProjectUtils.userIsOwner(userIds, projectId)) {
			// owner of project is owner of all the insights
			return AccessPermission.OWNER.getId();
		}
		
		// else query the database
//		String query = "SELECT DISTINCT USERINSIGHTPERMISSION.PERMISSION FROM USERINSIGHTPERMISSION "
//				+ "WHERE ENGINEID='" + engineId + "' AND INSIGHTID='" + insightId + "' AND USERID IN " + userFilters + " ORDER BY PERMISSION";
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("USERINSIGHTPERMISSION__PERMISSION"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__INSIGHTID", "==", insightId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__USERID", "==", userIds));
		qs.addOrderBy(new QueryColumnOrderBySelector("USERINSIGHTPERMISSION__PERMISSION"));
		
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			while(wrapper.hasNext()) {
				Object val = wrapper.next().getValues()[0];
				if(val == null) {
					return AccessPermission.READ_ONLY.getId();
				}
				int permission = ((Number) val).intValue();
				return permission;
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
		}
		return AccessPermission.READ_ONLY.getId();
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
					if(securityDb.isConnectionPooling()) {
						try {
							ps.getConnection().close();
						} catch (SQLException e) {
							logger.error(Constants.STACKTRACE, e);
						}
					}
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
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
		if(!SecurityInsightUtils.userCanViewInsight(user, projectId, insightId)) {
			throw new IllegalAccessException("The user doesn't have the permission to modify this insight");
		}
		Collection<String> userIdFilters = getUserFiltersQs(user);
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("USERINSIGHTPERMISSION__PROJECTID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__INSIGHTID", "==", insightId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__USERID", "==", userIdFilters));

		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if(wrapper.hasNext()){
				UpdateQueryStruct uqs = new UpdateQueryStruct();
				uqs.setEngine(securityDb);
				uqs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__PROJECTID", "==", projectId));
				uqs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__INSIGHTID", "==", insightId));
				uqs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__USERID", "==", userIdFilters));

				List<IQuerySelector> selectors = new Vector<>();
				selectors.add(new QueryColumnSelector("USERINSIGHTPERMISSION__FAVORITE"));
				List<Object> values = new Vector<>();
				values.add(isFavorite);
				uqs.setSelectors(selectors);
				uqs.setValues(values);
				
				UpdateSqlInterpreter updateInterp = new UpdateSqlInterpreter(uqs);
				String updateQuery = updateInterp.composeQuery();
				securityDb.insertData(updateQuery);
				
			} else {
				// need to insert
				PreparedStatement ps = securityDb.getPreparedStatement("INSERT INTO USERINSIGHTPERMISSION "
						+ "(USERID, PROJECTID, INSIGHTID, FAVORITE, PERMISSION) VALUES (?,?,?,?,?)");
				if(ps == null) {
					throw new IllegalArgumentException("Error generating prepared statement to set app visibility");
				}
				try {
					// we will set the permission to read only
					for(AuthProvider loginType : user.getLogins()) {
						String userId = user.getAccessToken(loginType).getId();
						int parameterIndex = 1;
						ps.setString(parameterIndex++, userId);
						ps.setString(parameterIndex++, projectId);
						ps.setString(parameterIndex++, insightId);
						ps.setBoolean(parameterIndex++, isFavorite);
						ps.setInt(parameterIndex++, 3);
	
						ps.addBatch();
					}
					ps.executeBatch();
				} catch(Exception e) {
					logger.error(Constants.STACKTRACE, e);
					throw e;
				} finally {
					if(ps != null) {
						ps.close();
					}
				}
			}
			
			// commit regardless of insert or update
			securityDb.commit();
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
		}
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
		if(!userCanViewInsight(user, projectId, insightId)) {
			throw new IllegalAccessException("The user does not have access to view this insight");
		}
		
//		String query = "SELECT SMSS_USER.ID AS \"id\", "
//				+ "SMSS_USER.NAME AS \"name\", "
//				+ "PERMISSION.NAME AS \"permission\" "
//				+ "FROM SMSS_USER "
//				+ "INNER JOIN USERINSIGHTPERMISSION ON (USER.ID = USERINSIGHTPERMISSION.USERID) "
//				+ "INNER JOIN PERMISSION ON (USERINSIGHTPERMISSION.PERMISSION = PERMISSION.ID) "
//				+ "WHERE USERINSIGHTPERMISSION.ENGINEID='" + appId + "'"
//				+ " AND USERINSIGHTPERMISSION.INSIGHTID='" + insightId + "'"
//				;
//		
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__ID", "id"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__NAME", "name"));
		qs.addSelector(new QueryColumnSelector("PERMISSION__NAME", "permission"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__EMAIL", "email"));
		qs.addRelation("SMSS_USER", "USERINSIGHTPERMISSION", "inner.join");
		qs.addRelation("USERINSIGHTPERMISSION", "PERMISSION", "inner.join");
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__INSIGHTID", "==", insightId));
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
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
	 * @param cacheable
	 * @param layout
	 * @param recipe
	 */
	public static void addInsight(String projectId, String insightId, String insightName, boolean global, boolean cacheable, String layout, List<String> recipe) {
		String insertQuery = "INSERT INTO INSIGHT (PROJECTID, INSIGHTID, INSIGHTNAME, GLOBAL, EXECUTIONCOUNT, "
				+ "CREATEDON, LASTMODIFIEDON, LAYOUT, CACHEABLE, RECIPE) "
				+ "VALUES (?,?,?,?,?,?,?,?,?,?)";
		
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(insertQuery);
			int parameterIndex = 1;
			ps.setString(parameterIndex++, projectId);
			ps.setString(parameterIndex++, insightId);
			ps.setString(parameterIndex++, insightName);
			ps.setBoolean(parameterIndex++, global);
			ps.setInt(parameterIndex++, 0);
			java.sql.Timestamp timestamp = java.sql.Timestamp.valueOf(LocalDateTime.now());
			ps.setTimestamp(parameterIndex++, timestamp);
			ps.setTimestamp(parameterIndex++, timestamp);
			ps.setString(parameterIndex++, layout);
			ps.setBoolean(parameterIndex++, cacheable);
			if(securityDb.getQueryUtil().allowClobJavaObject()) {
				Clob clob = securityDb.createClob();
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
					if(securityDb.isConnectionPooling()) {
						try {
							ps.getConnection().close();
						} catch (SQLException e) {
							logger.error(Constants.STACKTRACE, e);
						}
					}
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
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
		
		int ownerId = AccessPermission.OWNER.getId();
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
					if(securityDb.isConnectionPooling()) {
						try {
							ps.getConnection().close();
						} catch (SQLException e) {
							logger.error(Constants.STACKTRACE, e);
						}
					}
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
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
	 * @param exCount
	 * @param lastModified
	 * @param layout
	 */
	public static void updateInsight(String projectId, String insightId, String insightName, boolean global, String layout, List<String> recipe) {
		String updateQuery = "UPDATE INSIGHT SET INSIGHTNAME=?, GLOBAL=?, LASTMODIFIEDON=?, "
				+ "LAYOUT=?, RECIPE=? WHERE INSIGHTID = ? AND PROJECTID=?";

		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(updateQuery);
			int parameterIndex = 1;
			ps.setString(parameterIndex++, insightName);
			ps.setBoolean(parameterIndex++, global);
			java.sql.Timestamp timestamp = java.sql.Timestamp.valueOf(LocalDateTime.now());
			ps.setTimestamp(parameterIndex++, timestamp);
			ps.setString(parameterIndex++, layout);
			if(securityDb.getQueryUtil().allowClobJavaObject()) {
				Clob clob = securityDb.createClob();
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
					if(securityDb.isConnectionPooling()) {
						try {
							ps.getConnection().close();
						} catch (SQLException e) {
							logger.error(Constants.STACKTRACE, e);
						}
					}
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
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
		String query = "UPDATE INSIGHT SET INSIGHTNAME=?, LASTMODIFIEDON=? WHERE INSIGHTID=? AND PROJECTID=?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			int parameterIndex = 1;
			ps.setString(parameterIndex++, insightName);
			ps.setTimestamp(parameterIndex++, java.sql.Timestamp.valueOf(LocalDateTime.now()));
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
					if(securityDb.isConnectionPooling()) {
						try {
							ps.getConnection().close();
						} catch (SQLException e) {
							logger.error(Constants.STACKTRACE, e);
						}
					}
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}
	
	/**
	 * Update if an insight should be cached
	 * @param appId
	 * @param existingId
	 * @param cacheInsight
	 */
	public static void updateInsightCache(String projectId, String insightId, boolean cacheInsight) {
		String query = "UPDATE INSIGHT SET CACHEABLE=?, LASTMODIFIEDON=? WHERE INSIGHTID=? AND PROJECTID=?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			int parameterIndex = 1;
			ps.setBoolean(parameterIndex++, cacheInsight);
			ps.setTimestamp(parameterIndex++, java.sql.Timestamp.valueOf(LocalDateTime.now()));
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
					if(securityDb.isConnectionPooling()) {
						try {
							ps.getConnection().close();
						} catch (SQLException e) {
							logger.error(Constants.STACKTRACE, e);
						}
					}
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
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
			updateCount = ps.getUpdateCount();
		} catch(SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(ps != null) {
				try {
					ps.close();
					if(securityDb.isConnectionPooling()) {
						try {
							ps.getConnection().close();
						} catch (SQLException e) {
							logger.error(Constants.STACKTRACE, e);
						}
					}
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
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
			} catch(SQLException e) {
				logger.error(Constants.STACKTRACE, e);
			} finally {
				if(ps != null) {
					try {
						ps.close();
						if(securityDb.isConnectionPooling()) {
							try {
								ps.getConnection().close();
							} catch (SQLException e) {
								logger.error(Constants.STACKTRACE, e);
							}
						}
					} catch (SQLException e) {
						logger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}
		
		securityDb.commit();
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
			securityDb.commit();
		} catch(Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(ps != null) {
				try {
					ps.close();
					if(securityDb.isConnectionPooling()) {
						try {
							ps.getConnection().close();
						} catch (SQLException e) {
							logger.error(Constants.STACKTRACE, e);
						}
					}
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
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
			securityDb.commit();
		} catch(Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(ps != null) {
				try {
					ps.close();
					if(securityDb.isConnectionPooling()) {
						try {
							ps.getConnection().close();
						} catch (SQLException e) {
							logger.error(Constants.STACKTRACE, e);
						}
					}
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
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
			securityDb.commit();
		} catch(Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(ps != null) {
				try {
					ps.close();
					if(securityDb.isConnectionPooling()) {
						try {
							ps.getConnection().close();
						} catch (SQLException e) {
							logger.error(Constants.STACKTRACE, e);
						}
					}
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
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
		} catch(Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(ps != null) {
				try {
					ps.close();
					if(securityDb.isConnectionPooling()) {
						try {
							ps.getConnection().close();
						} catch (SQLException e) {
							logger.error(Constants.STACKTRACE, e);
						}
					}
				} catch (SQLException e) {
					logger.error(Constants.STACKTRACE, e);
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
		String query = "DELETE FROM INSIGHT WHERE INSIGHTID ='" + insightId + "' AND PROJECTID='" + projectId + "'";
		try {
			securityDb.insertData(query);
			securityDb.commit();
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		}		
		query = "DELETE FROM USERINSIGHTPERMISSION WHERE INSIGHTID ='" + insightId + "' AND PROJECTID='" + projectId + "'";
		try {
			securityDb.insertData(query);
			securityDb.commit();
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		}
		query = "DELETE FROM INSIGHTMETA WHERE INSIGHTID ='" + insightId + "' AND PROJECTID='" + projectId + "'";
		try {
			securityDb.insertData(query);
			securityDb.commit();
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		}
	}
	
	/**
	 * 
	 * @param projectId
	 * @param insightId
	 */
	public static void deleteInsight(String projectId, String... insightId) {
		String insightFilter = createFilter(insightId);
		String query = "DELETE FROM INSIGHT WHERE INSIGHTID " + insightFilter + " AND PROJECTID='" + projectId + "'";
		try {
			securityDb.insertData(query);
			securityDb.commit();
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		}
		query = "DELETE FROM USERINSIGHTPERMISSION WHERE INSIGHTID " + insightFilter + " AND PROJECTID='" + projectId + "'";
		try {
			securityDb.insertData(query);
			securityDb.commit();
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		}
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
		if(!userCanEditInsight(user, projectId, insightId)) {
			throw new IllegalArgumentException("Insufficient privileges to modify this insight's permissions.");
		}
		
		// make sure user doesn't already exist for this insight
		if(getUserInsightPermission(newUserId, projectId, insightId) != null) {
			// that means there is already a value
			throw new IllegalArgumentException("This user already has access to this insight. Please edit the existing permission level.");
		}
		
		String query = "INSERT INTO USERINSIGHTPERMISSION (USERID, PROJECTID, INSIGHTID, PERMISSION) VALUES('"
				+ RdbmsQueryBuilder.escapeForSQLStatement(newUserId) + "', '"
				+ RdbmsQueryBuilder.escapeForSQLStatement(projectId) + "', '"
				+ RdbmsQueryBuilder.escapeForSQLStatement(insightId) + "', "
				+ AccessPermission.getIdByPermission(permission) + ");";

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
		if(!AccessPermission.isEditor(userPermissionLvl)) {
			throw new IllegalAccessException("Insufficient privileges to modify this insight's permissions.");
		}
		
		// make sure we are trying to edit a permission that exists
		Integer existingUserPermission = getUserInsightPermission(existingUserId, projectId, insightId);
		if(existingUserPermission == null) {
			throw new IllegalArgumentException("Attempting to modify user permission for a user who does not currently have access to the insight");
		}
		
		int newPermissionLvl = AccessPermission.getIdByPermission(newPermission);
		
		// if i am not an owner
		// then i need to check if i can edit this users permission
		if(!AccessPermission.isOwner(userPermissionLvl)) {
			// not an owner, check if trying to edit an owner or an editor/reader
			// get the current permission
			if(AccessPermission.OWNER.getId() == existingUserPermission) {
				throw new IllegalAccessException("The user doesn't have the high enough permissions to modify this users insight permission.");
			}
			
			// also, cannot give some owner permission if i am just an editor
			if(AccessPermission.OWNER.getId() == newPermissionLvl) {
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
		if(!AccessPermission.isEditor(userPermissionLvl)) {
			throw new IllegalAccessException("Insufficient privileges to modify this insight's permissions.");
		}
		
		// make sure we are trying to edit a permission that exists
		Integer existingUserPermission = getUserInsightPermission(existingUserId, projectId, insightId);
		if(existingUserPermission == null) {
			throw new IllegalArgumentException("Attempting to modify user permission for a user who does not currently have access to the insight");
		}
		
		// if i am not an owner
		// then i need to check if i can remove this users permission
		if(!AccessPermission.isOwner(userPermissionLvl)) {
			// not an owner, check if trying to edit an owner or an editor/reader
			// get the current permission
			if(AccessPermission.OWNER.getId() == existingUserPermission) {
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
	 * 	3) Is Owner of database
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
	public static List<Map<String, Object>> searchUserInsights(User user, List<String> projectFilter, String searchTerm, List<String> tags, 
			Boolean favoritesOnly, QueryColumnOrderBySelector sortBy, String limit, String offset) {
//		String userFilters = getUserFilters(user);
//
//		String query = "SELECT DISTINCT "
//				+ "INSIGHT.ENGINEID AS \"app_id\", "
//				+ "ENGINE.ENGINENAME AS \"app_name\", "
//				+ "INSIGHT.INSIGHTID as \"app_insight_id\", "
//				+ "INSIGHT.INSIGHTNAME as \"name\", "
//				+ "INSIGHT.LAYOUT as \"layout\", "
//				+ "INSIGHT.CREATEDON as \"created_on\", "
//				+ "INSIGHT.LASTMODIFIEDON as \"last_modified_on\", "
//				+ "INSIGHT.CACHEABLE as \"cacheable\", "
//				+ "INSIGHT.GLOBAL as \"insight_global\", "
//				+ "LOWER(INSIGHT.INSIGHTNAME) as \"low_name\" "
//				+ "FROM INSIGHT "
//				+ "INNER JOIN ENGINE ON ENGINE.ENGINEID=INSIGHT.ENGINEID "
//				+ "LEFT JOIN ENGINEPERMISSION ON ENGINE.ENGINEID=ENGINEPERMISSION.ENGINEID "
//				+ "LEFT JOIN USERINSIGHTPERMISSION ON USERINSIGHTPERMISSION.INSIGHTID=INSIGHT.INSIGHTID "
//				+ "WHERE "
//				+ "INSIGHT.ENGINEID " + createFilter(engineFilter)+ " "
//				+ " AND (USERINSIGHTPERMISSION.USERID IN " + userFilters + " OR INSIGHT.GLOBAL=TRUE OR "
//						+ "(ENGINEPERMISSION.PERMISSION=1 AND ENGINEPERMISSION.USERID IN " + userFilters + ") ) "
//				+ ( (searchTerm != null && !searchTerm.trim().isEmpty()) ? "AND REGEXP_LIKE(INSIGHT.INSIGHTNAME, '"+ RdbmsQueryBuilder.escapeForSQLStatement(escapeRegexCharacters(searchTerm)) + "', 'i')" : "")
//				+ "ORDER BY LOWER(INSIGHT.INSIGHTNAME), \"last_modified_on\" DESC "
//				+ ( (limit != null && !limit.trim().isEmpty()) ? "LIMIT " + limit + " " : "")
//				+ ( (offset != null && !offset.trim().isEmpty()) ? "OFFSET " + offset + " ": "")
//				;
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
//		
//		// if no engine filters
//		
//		String userFilters = getUserFilters(user);
//
//		String query = "SELECT DISTINCT "
//				+ "INSIGHT.ENGINEID AS \"app_id\", "
//				+ "ENGINE.ENGINENAME AS \"app_name\", "
//				+ "INSIGHT.INSIGHTID as \"app_insight_id\", "
//				+ "INSIGHT.INSIGHTNAME as \"name\", "
//				+ "INSIGHT.EXECUTIONCOUNT as \"view_count\", "
//				+ "INSIGHT.LAYOUT as \"layout\", "
//				+ "INSIGHT.CREATEDON as \"created_on\", "
//				+ "INSIGHT.LASTMODIFIEDON as \"last_modified_on\", "
//				+ "INSIGHT.CACHEABLE as \"cacheable\", "
//				+ "INSIGHT.GLOBAL as \"insight_global\", "
//				+ "LOWER(INSIGHT.INSIGHTNAME) AS \"low_name\" "
//				+ "FROM INSIGHT "
//				+ "INNER JOIN ENGINE ON ENGINE.ENGINEID=INSIGHT.ENGINEID "
//				+ "LEFT JOIN ENGINEPERMISSION ON ENGINE.ENGINEID=ENGINEPERMISSION.ENGINEID "
//				+ "LEFT JOIN USER ON ENGINEPERMISSION.USERID=USER.ID "
//				+ "LEFT JOIN USERINSIGHTPERMISSION ON USER.ID=USERINSIGHTPERMISSION.USERID "
//				+ "WHERE "
//				// engine is visible to me
//				+ "( ENGINE.GLOBAL=TRUE "
//				+ "OR ENGINEPERMISSION.USERID IN " + userFilters + " ) "
//				+ "AND ENGINE.ENGINEID NOT IN (SELECT ENGINEID FROM ENGINEPERMISSION WHERE VISIBILITY=FALSE AND USERID IN " + userFilters + " "
//				// have access to insight
//				+ "AND (USERINSIGHTPERMISSION.USERID IN " + userFilters + " OR INSIGHT.GLOBAL=TRUE OR "
//						// if i own this, i dont care what permissions you want to give me + i want to see this engine
//						+ "(ENGINEPERMISSION.PERMISSION=1 AND ENGINEPERMISSION.USERID IN " + userFilters + " AND ENGINEPERMISSION.VISIBILITY=TRUE) )) "
//				// and match what i search
//				+ ( (searchTerm != null && !searchTerm.trim().isEmpty()) ? "AND REGEXP_LIKE(INSIGHT.INSIGHTNAME, '"+ RdbmsQueryBuilder.escapeForSQLStatement(RdbmsQueryBuilder.escapeRegexCharacters(searchTerm)) + "', 'i') " : "")
//				+ "ORDER BY LOWER(INSIGHT.INSIGHTNAME), \"last_modified_on\" DESC "
//				+ ( (limit != null && !limit.trim().isEmpty()) ? "LIMIT " + limit + " " : "")
//				+ ( (offset != null && !offset.trim().isEmpty()) ? "OFFSET " + offset + " ": "")
//				;
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		boolean hasProjectFilters = projectFilter != null && !projectFilter.isEmpty();
		
		Collection<String> userIds = getUserFiltersQs(user);
		SelectQueryStruct qs1 = new SelectQueryStruct();
		// selectors
		qs1.addSelector(new QueryColumnSelector("INSIGHT__PROJECTID", "app_id"));
		qs1.addSelector(new QueryColumnSelector("PROJECT__PROJECTNAME", "app_name"));
		qs1.addSelector(new QueryColumnSelector("INSIGHT__INSIGHTID", "app_insight_id"));
		qs1.addSelector(new QueryColumnSelector("INSIGHT__INSIGHTNAME", "name"));
		qs1.addSelector(new QueryColumnSelector("INSIGHT__EXECUTIONCOUNT", "view_count"));
		qs1.addSelector(new QueryColumnSelector("INSIGHT__LAYOUT", "layout"));
		qs1.addSelector(new QueryColumnSelector("INSIGHT__CREATEDON", "created_on"));
		qs1.addSelector(new QueryColumnSelector("INSIGHT__LASTMODIFIEDON", "last_modified_on"));
		qs1.addSelector(new QueryColumnSelector("INSIGHT__CACHEABLE", "cacheable"));
		qs1.addSelector(new QueryColumnSelector("INSIGHT__GLOBAL", "insight_global"));
		QueryFunctionSelector fun = new QueryFunctionSelector();
		fun.setFunction(QueryFunctionHelper.LOWER);
		fun.addInnerSelector(new QueryColumnSelector("INSIGHT__INSIGHTNAME"));
		fun.setAlias("low_name");
		qs1.addSelector(fun);

		// filters
		// if we have an engine filter
		// i'm assuming you want these even if visibility is false
		if(hasProjectFilters) {
			// will filter to the list of engines
			qs1.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHT__PROJECTID", "==", projectFilter));
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
			qs1.addExplicitFilter(orFilter);
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
			qs1.addExplicitFilter(firstOrFilter);

			// subquery time
			// remove those engines you have visibility as false
			{
				SelectQueryStruct subQs = new SelectQueryStruct();
				// store first and fill in sub query after
				qs1.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery("PROJECT__PROJECTID", "!=", subQs));
				
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
			qs1.addExplicitFilter(secondOrFilter);
		}
		// add the search term filter
		if(searchTerm != null && !searchTerm.trim().isEmpty()) {
			securityDb.getQueryUtil().appendSearchRegexFilter(qs1, "INSIGHT__INSIGHTNAME", searchTerm);
		}
		// if we have tag filters
		boolean tagFiltering = tags != null && !tags.isEmpty();
		if(tagFiltering) {
			qs1.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHTMETA__METAKEY", "==", "tag"));
			qs1.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHTMETA__METAVALUE", "==", tags));
		}
		
		// joins
		qs1.addRelation("PROJECT", "INSIGHT", "inner.join");
		if(tagFiltering) {
			qs1.addRelation("INSIGHT__INSIGHTID", "INSIGHTMETA__INSIGHTID", "inner.join");
			qs1.addRelation("INSIGHT__PROJECTID", "INSIGHTMETA__PROJECTID", "inner.join");
		}
		qs1.addRelation("PROJECT", "PROJECTPERMISSION", "left.outer.join");
		qs1.addRelation("INSIGHT", "USERINSIGHTPERMISSION", "left.outer.join");
		
		// get the favorites for this user
		SelectQueryStruct qs2 = new SelectQueryStruct();
//		qs2.addSelector(new QueryColumnSelector("USERINSIGHTPERMISSION__ENGINEID", "ENGINEID"));
		qs2.addSelector(new QueryColumnSelector("USERINSIGHTPERMISSION__INSIGHTID", "INSIGHTID"));
		qs2.addSelector(new QueryColumnSelector("USERINSIGHTPERMISSION__FAVORITE", "insight_favorite"));
		qs2.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__USERID", "==", userIds));
		qs2.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__FAVORITE", "==", true, PixelDataType.BOOLEAN));
		
		List<String> queries = new ArrayList<>();
		IQueryInterpreter interpreter = securityDb.getQueryInterpreter();
		interpreter.setQueryStruct(qs1);
		queries.add(interpreter.composeQuery());
		interpreter = securityDb.getQueryInterpreter();
		interpreter.setQueryStruct(qs2);
		queries.add(interpreter.composeQuery());

		List<Join> joins = new ArrayList<>();
		if(favoritesOnly) {
//			joins.add(new Join("app_id", "inner.join", "ENGINEID"));
			joins.add(new Join("app_insight_id", "inner.join", "INSIGHTID"));
		} else {
//			joins.add(new Join("app_id", "left.outer.join", "ENGINEID"));
			joins.add(new Join("app_insight_id", "left.outer.join", "INSIGHTID"));
		}
		
		GenExpression retExpression = SQLQueryUtils.joinSQL(queries, joins);
		StringBuffer finalQuery = GenExpression.printQS(retExpression, 
				new StringBuffer(queries.get(0).length() + queries.get(1).length() + 300));
		
		// TODO: NEED BETTER WAY TO DO THIS
		// sort
		if(sortBy == null) {
			finalQuery.append(" ORDER BY \"low_name\" ");
		} else {
			String sort = sortBy.getQueryStructName();
			String dir = sortBy.getSortDir().toString();
			finalQuery.append(" ORDER BY \"").append(sort).append("\" ").append(dir).append(" ");;
		}
		
		Long long_limit = -1L;
		Long long_offset = -1L;
		if(limit != null && !limit.trim().isEmpty()) {
			long_limit = Long.parseLong(limit);
		}
		if(offset != null && !offset.trim().isEmpty()) {
			long_offset = Long.parseLong(offset);
		}
		finalQuery = securityDb.getQueryUtil().addLimitOffsetToQuery(finalQuery, long_limit, long_offset);
		HardSelectQueryStruct finalQs = new HardSelectQueryStruct();
		finalQs.setQuery(finalQuery.toString());
		
		return QueryExecutionUtility.flushRsToMap(securityDb, finalQs);
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
	public static List<Map<String, Object>> searchInsights(List<String> projectFilter, String searchTerm, List<String> tags, 
			QueryColumnOrderBySelector sortBy, String limit, String offset) {
//		String query = "SELECT DISTINCT "
//				+ "INSIGHT.ENGINEID AS \"app_id\", "
//				+ "ENGINE.ENGINENAME AS \"app_name\", "
//				+ "INSIGHT.INSIGHTID as \"app_insight_id\", "
//				+ "INSIGHT.INSIGHTNAME as \"name\", "
//				+ "INSIGHT.LAYOUT as \"layout\", "
//				+ "INSIGHT.CREATEDON as \"created_on\", "
//				+ "INSIGHT.LASTMODIFIEDON as \"last_modified_on\", "
//				+ "INSIGHT.CACHEABLE as \"cacheable\", "
//				+ "INSIGHT.GLOBAL as \"insight_global\", "
//				+ "LOWER(INSIGHT.INSIGHTNAME) as \"low_name\" "
//				+ "FROM INSIGHT "
//				+ "INNER JOIN ENGINE ON ENGINE.ENGINEID=INSIGHT.ENGINEID "
//				+ "WHERE "
//				+ "INSIGHT.ENGINEID " + createFilter(engineFilter) + " "
//				+ ( (searchTerm != null && !searchTerm.trim().isEmpty()) ? "AND REGEXP_LIKE(INSIGHT.INSIGHTNAME, '"+ RdbmsQueryBuilder.escapeForSQLStatement(RdbmsQueryBuilder.escapeRegexCharacters(searchTerm)) + "', 'i')" : "")
//				+ "ORDER BY LOWER(INSIGHT.INSIGHTNAME), \"last_modified_on\" DESC "
//				+ ( (limit != null && !limit.trim().isEmpty()) ? "LIMIT " + limit + " " : "")
//				+ ( (offset != null && !offset.trim().isEmpty()) ? "OFFSET " + offset + " ": "")
//				;
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
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
		// if we have tag filters
		boolean tagFiltering = tags != null && !tags.isEmpty();
		if(tagFiltering) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHTMETA__METAKEY", "==", "tag"));
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHTMETA__METAVALUE", "==", tags));
		}
		// joins
		qs.addRelation("PROJECT", "INSIGHT", "inner.join");
		if(tagFiltering) {
			qs.addRelation("INSIGHT__INSIGHTID", "INSIGHTMETA__INSIGHTID", "inner.join");
			qs.addRelation("INSIGHT__PROJECTID", "INSIGHTMETA__PROJECTID", "inner.join");
		}
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
				String metaKey = data[2].toString();
				String metaValue = AbstractSqlQueryUtil.flushClobToString((java.sql.Clob) data[3]);

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

}
