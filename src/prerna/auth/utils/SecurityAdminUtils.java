package prerna.auth.utils;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.Vector;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessPermissionEnum;
import prerna.auth.AuthProvider;
import prerna.auth.PasswordRequirements;
import prerna.auth.User;
import prerna.date.SemossDate;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.InsightAdministrator;
import prerna.project.api.IProject;
import prerna.query.querystruct.AbstractQueryStruct.QUERY_STRUCT_TYPE;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.joins.IRelation;
import prerna.query.querystruct.joins.SubqueryRelationship;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryConstantSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.query.querystruct.selectors.QueryIfSelector;
import prerna.query.querystruct.update.UpdateQueryStruct;
import prerna.query.querystruct.update.UpdateSqlInterpreter;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.PixelDataType;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.QueryExecutionUtility;
import prerna.util.Utility;

public class SecurityAdminUtils extends AbstractSecurityUtils {

	private static SecurityAdminUtils instance = new SecurityAdminUtils();
	private static final Logger classLogger = LogManager.getLogger(SecurityAdminUtils.class);

	private SecurityAdminUtils() {
		
	}
	
	public static SecurityAdminUtils getInstance(User user) {
		if(user == null) {
			return null;
		}
		if(userIsAdmin(user)) {
			return instance;
		}
		return null;
	}

	/**
	 * Check if the user is an admin
	 * @param userId	String representing the id of the user to check
	 */
	public static Boolean userIsAdmin(User user) {
//		String userFilters = getUserFilters(user);
//		String query = "SELECT * FROM SMSS_USER WHERE ADMIN=TRUE AND ID IN " + userFilters + " LIMIT 1;";
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__ID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__ID", "==", getUserFiltersQs(user)));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__ADMIN", "==", true, PixelDataType.BOOLEAN));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			return wrapper.hasNext();
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
	 * See if the user is an admin
	 * @param userId
	 * @param type
	 * @return
	 */
	public boolean userIsAdmin(String userId, String type) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__ID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__ID", "==", userId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__TYPE", "==", type));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__ADMIN", "==", true, PixelDataType.BOOLEAN));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			return wrapper.hasNext();
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
	
	public boolean otherAdminsExist(String userId, String type) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__ID"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__TYPE"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__ADMIN", "==", true, PixelDataType.BOOLEAN));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			while(wrapper.hasNext()) {
				Object[] row = wrapper.next().getValues();
				if( (row[0] + "").equals(userId)
						&& (row[1] + "").equals(type) ) {
					continue;
				} else {
					return true;
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
	
	/*
	 * all other methods should be on the instance
	 * so that we cannot bypass security easily
	 */
	
	/**
	 * Get all users
	 * @param offset 
	 * @param limit 
	 * @return
	 * @throws IllegalArgumentException
	 */
	public List<Map<String, Object>> getAllUsers(long limit, long offset) throws IllegalArgumentException{
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
		qs.addOrderBy(new QueryColumnOrderBySelector("SMSS_USER__NAME"));
		qs.addOrderBy(new QueryColumnOrderBySelector("SMSS_USER__TYPE"));
		if(limit > 0) {
			qs.setLimit(limit);
		}
		if(offset > 0) {
			qs.setOffSet(offset);
		}
		return getSimpleQuery(qs);
	}
	
	/**
	 * Get all user engines
	 * @param userId
	 * @return
	 * @throws IllegalArgumentException
	 */
	public List<Map<String, Object>> getAllUserEngines(String userId, List<String> engineTypes) throws IllegalArgumentException{
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__USERID", "user_id"));
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__PERMISSION", "app_permission"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID", "app_id"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINENAME", "app_name"));
		qs.addSelector(new QueryColumnSelector("PERMISSION__NAME", "app_permission"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", userId));
		if(engineTypes != null && !engineTypes.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINETYPE", "==", engineTypes));
		}
		qs.addRelation("ENGINEPERMISSION", "ENGINE", "inner.join");
		qs.addRelation("ENGINEPERMISSION", "PERMISSION", "inner.join");

		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}
	
	
	/**
	 * Get all user projects
	 * @param userId
	 * @return
	 * @throws IllegalArgumentException
	 */
	public List<Map<String, Object>> getAllUserProjects(String userId) throws IllegalArgumentException{
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__USERID", "user_id"));
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__PERMISSION", "project_permission"));
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTID", "project_id"));
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTNAME", "project_name"));
		qs.addSelector(new QueryColumnSelector("PERMISSION__NAME", "project_permission"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", userId));
		qs.addRelation("PROJECTPERMISSION", "PROJECT", "inner.join");
		qs.addRelation("PROJECTPERMISSION", "PERMISSION", "inner.join");

		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}
	
	/**
	 * Get all user insights
	 * @param user
	 * @param searchTerm
	 * @param limit
	 * @param offset
	 * @return
	 * @throws IllegalArgumentException
	 */
	public List<Map<String, Object>> getAllUserInsights(User user, List<String> projectFilter, String searchTerm, long limit, long offset) throws IllegalArgumentException{
		boolean hasSearchTerm = searchTerm != null && !(searchTerm=searchTerm.trim()).isEmpty();
		boolean hasProjectFilters = projectFilter != null && !projectFilter.isEmpty();
		Collection<String> userIds = getUserFiltersQs(user);
		
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
			subQuery = new SubqueryRelationship(qs2, "INSIGHT_USER_PERMISSIONS", "left.outer.join", new String[] {"INSIGHT_USER_PERMISSIONS__INSIGHTID", insightPrefix + "INSIGHTID", "="});
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
		// optional word filter on the engine name
		if(hasSearchTerm) {
			securityDb.getQueryUtil().appendSearchRegexFilter(qs, "INSIGHT__INSIGHTNAME", searchTerm);
		}
		qs.addOrderBy("low_name");;
		if(limit > 0) {
			qs.setLimit(limit);
		}
		if(offset > 0) {
			qs.setOffSet(offset);
		}
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}
	
	//TODO >>> Kunal: update below method	
	/**
	 * Get all user databases
	 * @param userId
	 * @return
	 * @throws IllegalArgumentException
	 */
	public List<Map<String, Object>> getAllUserInsightAccess(String projectId, String userId) throws IllegalArgumentException{
		String query = "SELECT DISTINCT " 
				+ "INSIGHT.INSIGHTID AS \"insight_id\", "
				+ "INSIGHT.INSIGHTNAME AS \"insight_name\", " 
				+ "INSIGHT.GLOBAL AS \"insight_public\", " 
				+ "INSIGHT.PROJECTID AS \"project_id\", " 
				+ "SUB_Q.NAME AS \"insight_permission\", " 
				+ "SUB_Q.USERID AS \"user_id\" " 
				+ "FROM INSIGHT LEFT OUTER JOIN ( "
					+ "SELECT USERINSIGHTPERMISSION.INSIGHTID, "
						+ "PERMISSION.NAME, "
						+ "USERINSIGHTPERMISSION.USERID "
						+ "FROM USERINSIGHTPERMISSION "
						+ "INNER JOIN PERMISSION on USERINSIGHTPERMISSION.PERMISSION=PERMISSION.ID "
						+ "WHERE USERINSIGHTPERMISSION.PROJECTID = '" + projectId + "' AND USERINSIGHTPERMISSION.USERID = '" + userId + "'" 
					+ ") AS SUB_Q ON SUB_Q.INSIGHTID = INSIGHT.INSIGHTID "
				+ "WHERE INSIGHT.PROJECTID = '" + projectId + "' ORDER BY INSIGHT.INSIGHTNAME";
		
		HardSelectQueryStruct qs = new HardSelectQueryStruct();
		qs.setQuery(query);
		qs.setQsType(QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY);
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}
	
	/**
	 * Update user information.
	 * @param adminId
	 * @param userInfo
	 * @return
	 * @throws IllegalArgumentException
	 */
	public boolean editUser(Map<String, Object> userInfo) {
		// input fields
		String userId = userInfo.get("id") != null ? userInfo.get("id").toString() : "";
		if(userId == null || userId.isEmpty()) {
			throw new NullPointerException("Must provide a unique and non-empty user id");
		}
		String password = userInfo.get("password") != null ? userInfo.get("password").toString() : "";
		String name = userInfo.get("name") != null ? userInfo.get("name").toString() : "";
		String type = userInfo.get("type") != null ? userInfo.get("type").toString() : "";
		String phone = userInfo.get("phone") != null ?  userInfo.get("phone").toString() : ""; 
		String phoneExtension = userInfo.get("phoneextension") != null ?  userInfo.get("phoneextension").toString() : ""; 
		String countryCode = userInfo.get("countrycode") != null ?  userInfo.get("countrycode").toString() : ""; 
		// modified fields
		String newUserId = (String) userInfo.get("newId");
		if(newUserId != null && newUserId.trim().isEmpty()) {
			newUserId = null;
		}
		String newUsername = (String) userInfo.get("newUsername");
		if(newUsername != null && newUsername.trim().isEmpty()) {
			newUsername = null;
		}
		String newEmail = (String) userInfo.get("newEmail");
		// always lower case emails
		if(newEmail != null) {
			newEmail = newEmail.toLowerCase();
		}
		Boolean adminChange = null;
		if(userInfo.containsKey("admin")) {
			if(userInfo.get("admin") instanceof Number) {
				adminChange = ((Number) userInfo.get("admin")).intValue() == 1;
			} else {
				adminChange = Boolean.parseBoolean( userInfo.get("admin") + "");
			}
		}

		Boolean publisherChange = null;
		if(userInfo.containsKey("publisher")) {
			if(userInfo.get("publisher") instanceof Number) {
				publisherChange = ((Number) userInfo.get("publisher")).intValue() == 1;
			} else {
				publisherChange = Boolean.parseBoolean( userInfo.get("publisher") + "");
			}
		}
		
		Boolean exporterChange = Boolean.TRUE;
		if(userInfo.containsKey("exporter")) {
			if(userInfo.get("exporter") instanceof Number) {
				exporterChange = ((Number) userInfo.get("exporter")).intValue() == 1;
			} else {
				exporterChange = Boolean.parseBoolean( userInfo.get("exporter") + "");
			}
		}

		String newSalt = null;
		String newHashPass = null;
		
		// validate new inputs and insert into selectors and values to use for update
		List<IQuerySelector> selectors = new Vector<>();
		List<Object> values = new Vector<>();
		
		// cannot edit a user to match another user when native... would cause some serious issues :/
		// so we will check if you are switching to a native
		boolean isNative = false;
		if(type != null && !type.isEmpty()) {
			isNative = type.equalsIgnoreCase("NATIVE");
		} else {
			isNative = SecurityQueryUtils.isUserType(userId, AuthProvider.NATIVE);
		}
		if(isNative) {
			// username and id must match for native
			// so they should be updated together and have the same value
			if( !( (newUsername != null && newUserId != null) || (newUsername == null && newUserId == null) ) ){
				throw new IllegalArgumentException("For native users, the id and the username must be updated together and have the same value");
			}
			if(newUserId != null && newUserId.isEmpty()) {
				throw new IllegalArgumentException("For native users, the id and the username must be updated together and have the same value");
			}
			if(newUserId != null && !newUserId.equalsIgnoreCase(newUsername)) {
				throw new IllegalArgumentException("For native users, the id and the username must be updated together and have the same value");
			}
		}
		// if we are updating the user id
		// make sure the new id does not exist
		if(newUserId != null) {
			if(SecurityQueryUtils.checkUserExist(newUserId)){
				throw new IllegalArgumentException("The new user id already exists. Please enter a unique user id.");
			}
		}
		
		// check new userID
		if(newUserId != null) {
			selectors.add(new QueryColumnSelector("SMSS_USER__ID"));
			values.add(newUserId);
		}
	
		String error = "";
		if(newEmail != null && !newEmail.isEmpty()){
			try {
				validEmail(newEmail, true);
			} catch(Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
				error += e.getMessage();
			}
			selectors.add(new QueryColumnSelector("SMSS_USER__EMAIL"));
			values.add(newEmail);
		}
		if(newUsername != null && !newUsername.isEmpty()) {
			boolean usernameExists = SecurityQueryUtils.checkUsernameExist(newUsername);
			if(usernameExists) {
				throw new IllegalArgumentException("The username already exists");
			}
			selectors.add(new QueryColumnSelector("SMSS_USER__USERNAME"));
			values.add(newUsername);
		}
		if(password != null && !password.isEmpty()){
            try {
				validPassword(userId, AuthProvider.NATIVE, password);
			} catch (Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
				error += e.getMessage();
			}
            if(error.isEmpty()){
                newSalt = SecurityQueryUtils.generateSalt();
    			selectors.add(new QueryColumnSelector("SMSS_USER__PASSWORD"));
    			newHashPass = SecurityQueryUtils.hash(password, newSalt); 
    			values.add(newHashPass);
    			selectors.add(new QueryColumnSelector("SMSS_USER__SALT"));
    			values.add(newSalt);
            }
        }
		if(name != null && !name.isEmpty()) {
			selectors.add(new QueryColumnSelector("SMSS_USER__NAME"));
			values.add(name);
		}
		if(type != null && !type.isEmpty()) {
			selectors.add(new QueryColumnSelector("SMSS_USER__TYPE"));
			values.add(type);
		}
		if(adminChange != null) {
			selectors.add(new QueryColumnSelector("SMSS_USER__ADMIN"));
			values.add(adminChange);
		}
		if(publisherChange != null) {
			selectors.add(new QueryColumnSelector("SMSS_USER__PUBLISHER"));
			values.add(publisherChange);
		}
		if(exporterChange != null) {
			selectors.add(new QueryColumnSelector("SMSS_USER__EXPORTER"));
			values.add(exporterChange);
		}
		if(phone != null && !phone.isEmpty()) {
			try {
				phone = formatPhone(phone);
			} catch(Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
				error += e.getMessage();
			}
			selectors.add(new QueryColumnSelector("SMSS_USER__PHONE"));
			values.add(phone);
		}
		if(phoneExtension != null && !phoneExtension.isEmpty()) {
			selectors.add(new QueryColumnSelector("SMSS_USER__PHONEEXTENSION"));
			values.add(phoneExtension);
		}
		if(countryCode != null && !countryCode.isEmpty()) {
			selectors.add(new QueryColumnSelector("SMSS_USER__COUNTRYCODE"));
			values.add(countryCode);
		}

		if(error != null && !error.isEmpty()) {
			throw new IllegalArgumentException(error);
		}

		UpdateQueryStruct qs = new UpdateQueryStruct();
		qs.setEngine(securityDb);
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__ID", "==", userId));
		qs.setSelectors(selectors);
		qs.setValues(values);
		
		UpdateSqlInterpreter updateInterp = new UpdateSqlInterpreter(qs);
		String updateQ = updateInterp.composeQuery();
		try {
			securityDb.insertData(updateQ);
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
			return false;
		}
		if(isNative) {
			if(newUserId != null && !userId.equals(newUserId)) {
				// need to update the password history
				String updateQuery = "UPDATE PASSWORD_HISTORY SET USERID=? WHERE USERID=? and TYPE=?";
				PreparedStatement ps = null;
				try {
					ps = securityDb.getPreparedStatement(updateQuery);
					int parameterIndex = 1;
					ps.setString(parameterIndex++, newUserId);
					ps.setString(parameterIndex++, userId);
					ps.setString(parameterIndex++, type);
					ps.execute();
					if(!ps.getConnection().getAutoCommit()) {
						ps.getConnection().commit();
					}
				} catch (SQLException e) {
					classLogger.error(Constants.STACKTRACE, e);
				} finally {
					ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
				}
			}
			if(newHashPass != null && newSalt != null) {
				Calendar cal = Calendar.getInstance(TimeZone.getTimeZone(Utility.getApplicationTimeZoneId()));
				java.sql.Timestamp timestamp = java.sql.Timestamp.valueOf(LocalDateTime.now());
				try {
					SecurityNativeUserUtils.storeUserPassword(userId, type, newHashPass, newSalt, timestamp, cal);
				} catch (Exception e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		return true;
    }
	
	/**
	 * Delete a user and all its relationships.
	 * @param userIdToDelete
	 * @param userTypeToDelete
	 * @return
	 */
	public boolean deleteUser(String userIdToDelete, String userTypeToDelete) {
		//TODO: need to start binding on userId + type
		//TODO: need to start binding on userId + type
		//TODO: need to start binding on userId + type
		//TODO: need to start binding on userId + type
		//TODO: need to start binding on userId + type
		//TODO: need to start binding on userId + type
		//TODO: need to start binding on userId + type
		{
			String[] deleteQueries = new String[] {
					"DELETE FROM ENGINEPERMISSION WHERE USERID=?",
					"DELETE FROM USERINSIGHTPERMISSION WHERE USERID=?",
					"DELETE FROM SMSS_USER WHERE ID=?",
					"DELETE FROM SMSS_USER_ACCESS_KEYS WHERE ID=?"
			};
			for(String query : deleteQueries) {
				PreparedStatement ps = null;
				try {
					ps = securityDb.getPreparedStatement(query);
					int parameterIndex = 1;
					ps.setString(parameterIndex++, userIdToDelete);
					ps.execute();
					if(!ps.getConnection().getAutoCommit()) {
						ps.getConnection().commit();
					}
				} catch (SQLException e) {
					classLogger.error(Constants.STACKTRACE, e);
				} finally {
					ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
				}
			}
		}
//		{
//			String[] deleteQueries = new String[] {
//					"DELETE FROM SMSS_USER WHERE ID=? AND TYPE=?",
//					"DELETE FROM SMSS_USER_ACCESS_KEYS WHERE ID=? AND TYPE=?"
//			};
//			for(String query : deleteQueries) {
//				PreparedStatement ps = null;
//				try {
//					ps = securityDb.getPreparedStatement(query);
//					int parameterIndex = 1;
//					ps.setString(parameterIndex++, userToDelete);
//					ps.setString(parameterIndex++, type);
//					ps.execute();
//					if(!ps.getConnection().getAutoCommit()) {
//						ps.getConnection().commit();
//					}
//				} catch (SQLException e) {
//					logger.error(Constants.STACKTRACE, e);
//				} finally {
//					ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
//				}
//			}
//		}
		return true;
	}
	

	/**
	 * Set the user's publishing rights
	 * @param userId
	 * @param isPublisher
	 */
	public void setUserPublisher(String userId, boolean isPublisher) {
		String query = "UPDATE SMSS_USER SET PUBLISHER=? WHERE ID=?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			int parameterIndex = 1;
			ps.setBoolean(parameterIndex++, isPublisher);
			ps.setString(parameterIndex++, userId);
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred setting this user as a publisher");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}
	
	/**
	 * Set the user's exporting rights
	 * @param userId
	 * @param isExporter
	 */
	public void setUserExporter(String userId, boolean isExporter) {
		String query = "UPDATE SMSS_USER SET EXPORTER=? WHERE ID=?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			int parameterIndex = 1;
			ps.setBoolean(parameterIndex++, isExporter);
			ps.setString(parameterIndex++, userId);
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred setting this user as an exporter");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);

		}
	}
	
	/**
	 * Set the user locked/unlocked
	 * @param userId
	 * @param isExporter
	 */
	public void setUserLock(String userId, String type, boolean isLocked) {
		String query = null;
		if(isLocked) {
			query = "UPDATE SMSS_USER SET LOCKED=? WHERE ID=? AND TYPE=?";
		} else {
			query = "UPDATE SMSS_USER SET LOCKED=?, LASTLOGIN=? WHERE ID=? AND TYPE=?";
		}
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			int parameterIndex = 1;
			ps.setBoolean(parameterIndex++, isLocked);
			if(!isLocked) {
				// we reset the counter so lastlogin will be today
				Calendar cal = Calendar.getInstance(TimeZone.getTimeZone(Utility.getApplicationTimeZoneId()));
				java.sql.Timestamp timestamp = java.sql.Timestamp.valueOf(LocalDateTime.now());
				ps.setTimestamp(parameterIndex++, timestamp, cal);
			}
			ps.setString(parameterIndex++, userId);
			ps.setString(parameterIndex++, type);
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred setting this user as locked/unlocked");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}
	
	///////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////////////

	/*
	 * METHODS FOR DATABASE AUTHORIZATION THAT ARE AT THE ADMIN LEVEL
	 */
	
	/**
	 * 
	 * @param engineFilter
	 * @param engineTypes
	 * @param engineMetadataFilter
	 * @param searchTerm
	 * @param limit
	 * @param offset
	 * @return
	 */
	public List<Map<String, Object>> getAllEngineSettings(
			List<String> engineFilter, 
			List<String> engineTypes,
			Map<String, Object> engineMetadataFilter, 
			String searchTerm, 
			String limit, 
			String offset) {
		
		boolean hasSearchTerm = searchTerm != null && !(searchTerm=searchTerm.trim()).isEmpty();

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID", "engine_id"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINENAME", "engine_name"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINETYPE", "engine_type"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINESUBTYPE", "engine_subtype"));
		qs.addSelector(new QueryColumnSelector("ENGINE__COST", "engine_cost"));
		qs.addSelector(new QueryColumnSelector("ENGINE__DISCOVERABLE", "engine_discoverable"));
		qs.addSelector(new QueryColumnSelector("ENGINE__GLOBAL", "engine_global"));
		qs.addSelector(new QueryColumnSelector("ENGINE__CREATEDBY", "engine_created_by"));
		qs.addSelector(new QueryColumnSelector("ENGINE__CREATEDBYTYPE", "engine_created_by_type"));
		qs.addSelector(new QueryColumnSelector("ENGINE__DATECREATED", "engine_date_created"));
		qs.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.LOWER, "ENGINE__ENGINENAME", "low_engine_name"));
		
		// legacy alias names
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID", "database_id"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINENAME", "database_name"));
		qs.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.LOWER, "ENGINE__ENGINENAME", "low_database_name"));
		qs.addSelector(new QueryColumnSelector("ENGINE__GLOBAL", "database_global"));
		// legacy alias names
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID", "app_id"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINENAME", "app_name"));
		qs.addSelector(new QueryColumnSelector("ENGINE__GLOBAL", "app_global"));
		
		if(engineFilter != null && !engineFilter.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINEID", "==", engineFilter));
		}
		if(engineTypes != null && !engineTypes.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINETYPE", "==", engineTypes));
		}
		if(hasSearchTerm) {
			securityDb.getQueryUtil().appendSearchRegexFilter(qs, "ENGINE__ENGINENAME", searchTerm);
		}
		// filtering by enginemeta key-value pairs (i.e. <tag>:value): for each pair, add in-filter against engineids from subquery
		if (engineMetadataFilter!=null && !engineMetadataFilter.isEmpty()) {
			for (String k : engineMetadataFilter.keySet()) {
				SelectQueryStruct subQs = new SelectQueryStruct();
				subQs.addSelector(new QueryColumnSelector("ENGINEMETA__ENGINEID"));
				subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEMETA__METAKEY", "==", k));
				subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEMETA__METAVALUE", "==", engineMetadataFilter.get(k)));
				qs.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery("ENGINE__ENGINEID", "==", subQs));
			}
		}
		
		// add the sort
		qs.addOrderBy(new QueryColumnOrderBySelector("low_engine_name"));
		
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
	 * 
	 * @param projectFilter
	 * @param projectMetadataFilter
	 * @param searchTerm
	 * @param limit
	 * @param offset
	 * @return
	 */
	public List<Map<String, Object>> getAllProjectSettings(
			List<String> projectFilter, 
			Map<String, Object> projectMetadataFilter, 
			String searchTerm, 
			String limit, 
			String offset) {
		
		boolean hasSearchTerm = searchTerm != null && !(searchTerm=searchTerm.trim()).isEmpty();

		String projectPrefix = "PROJECT__";
		SelectQueryStruct qs = new SelectQueryStruct();
		// selectors
		qs.addSelector(new QueryColumnSelector(projectPrefix+"PROJECTID", "project_id"));
		qs.addSelector(new QueryColumnSelector(projectPrefix+"PROJECTNAME", "project_name"));
		qs.addSelector(new QueryColumnSelector(projectPrefix+"TYPE", "project_type"));
		qs.addSelector(new QueryColumnSelector(projectPrefix+"COST", "project_cost"));
		qs.addSelector(new QueryColumnSelector(projectPrefix+"GLOBAL", "project_global"));
		qs.addSelector(new QueryColumnSelector(projectPrefix+"DISCOVERABLE", "project_discoverable"));
		qs.addSelector(new QueryColumnSelector(projectPrefix+"CATALOGNAME", "project_catalog_name"));
		qs.addSelector(new QueryColumnSelector(projectPrefix+"CREATEDBY", "project_created_by"));
		qs.addSelector(new QueryColumnSelector(projectPrefix+"CREATEDBYTYPE", "project_created_by_type"));
		qs.addSelector(new QueryColumnSelector(projectPrefix+"DATECREATED", "project_date_created"));
		// dont forget reactors/portal information
		qs.addSelector(new QueryColumnSelector(projectPrefix+"HASPORTAL", "project_has_portal"));
		qs.addSelector(new QueryColumnSelector(projectPrefix+"PORTALNAME", "project_portal_name"));
		qs.addSelector(new QueryColumnSelector(projectPrefix+"PORTALPUBLISHED", "project_portal_published_date"));
		qs.addSelector(new QueryColumnSelector(projectPrefix+"PORTALPUBLISHEDUSER", "project_published_user"));
		qs.addSelector(new QueryColumnSelector(projectPrefix+"PORTALPUBLISHEDTYPE", "project_published_user_type"));
		qs.addSelector(new QueryColumnSelector(projectPrefix+"REACTORSCOMPILED", "project_reactors_compiled_date"));
		qs.addSelector(new QueryColumnSelector(projectPrefix+"REACTORSCOMPILEDUSER", "project_reactors_compiled_user"));
		qs.addSelector(new QueryColumnSelector(projectPrefix+"REACTORSCOMPILEDTYPE", "project_reactors_compiled_user_type"));
		// for sort
		qs.addSelector(QueryFunctionSelector.makeFunctionSelector(QueryFunctionHelper.LOWER, "PROJECT__PROJECTNAME", "low_project_name"));
		
		if(projectFilter != null && !projectFilter.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__PROJECTID", "==", projectFilter));
		}
		if(hasSearchTerm) {
			securityDb.getQueryUtil().appendSearchRegexFilter(qs, projectPrefix+"PROJECTNAME", searchTerm);
		}
		// filtering by projectmeta key-value pairs (i.e. <tag>:value): for each pair, add in-filter against projectids from subquery
		if (projectMetadataFilter!=null && !projectMetadataFilter.isEmpty()) {
			for (String k : projectMetadataFilter.keySet()) {
				SelectQueryStruct subQs = new SelectQueryStruct();
				subQs.addSelector(new QueryColumnSelector("PROJECTMETA__PROJECTID"));
				subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTMETA__METAKEY", "==", k));
				subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTMETA__METAVALUE", "==", projectMetadataFilter.get(k)));
				qs.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery("PROJECT__PROJECTID", "==", subQs));
			}
		}
		
		qs.addOrderBy(new QueryColumnOrderBySelector("low_project_name"));
		
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
	 * Set if engine should be public or not
	 * @param engineId
	 * @param global
	 */
	public boolean setEngineGlobal(String engineId, boolean global) {
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement("UPDATE ENGINE SET GLOBAL=? WHERE ENGINEID=?");
			ps.setBoolean(1, global);
			ps.setString(2, engineId);
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred setting the engine public");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
		return true;
	}
	
	/**
	 * Set if the engine is discoverable to all users on this instance
	 * @param user
	 * @param engineId
	 * @param discoverable
	 * @return
	 * @throws IllegalAccessException
	 */
	public boolean setEngineDiscoverable(String engineId, boolean discoverable) {
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement("UPDATE ENGINE SET DISCOVERABLE=? WHERE ENGINEID=?");
			ps.setBoolean(1, discoverable);
			ps.setString(2, engineId);
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred setting the engine discoverable flag");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
		return true;
	}
	
	/**
	 * Set if project should be public or not
	 * @param projectId
	 * @param isPublic
	 */
	public boolean setProjectGlobal(String projectId, boolean global) {
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement("UPDATE PROJECT SET GLOBAL=? WHERE PROJECTID=?");
			ps.setBoolean(1, global);
			ps.setString(2, projectId);
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred setting the project public");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
		return true;
	}
	
	/**
	 * Change if this project has a portal or not
	 * @param user
	 * @param projectId
	 * @param visibility
	 * @throws SQLException 
	 * @throws IllegalAccessException 
	 */
	public void setProjectPortal(User user, String projectId, boolean hasPortal, String portalName) {
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement("UPDATE PROJECT SET HASPORTAL=?, PORTALNAME=? WHERE PROJECTID=?");
			int parameterIndex = 1;
			ps.setBoolean(parameterIndex++, hasPortal);
			if(portalName != null) {
				ps.setString(parameterIndex++, portalName);
			} else {
				ps.setNull(parameterIndex++, java.sql.Types.VARCHAR);
			}
			ps.setString(parameterIndex++, projectId);
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred setting the project portal active");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}
	
	/**
	 * Set if the project is discoverable to all users on this instance
	 * @param user
	 * @param projectId
	 * @param discoverable
	 * @return
	 * @throws IllegalAccessException
	 */
	public boolean setProjectDiscoverable(String projectId, boolean discoverable) {
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement("UPDATE PROJECT SET DISCOVERABLE=? WHERE PROJECTID=?");
			ps.setBoolean(1, discoverable);
			ps.setString(2, projectId);
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred setting the project discoverable flag");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
		return true;
	}
	
	/**
	 * DEPRECIATED DO NOT USE
	 * Get all the users for a databases
	 * @param databaseId
	 * @return
	 */
	@Deprecated
	public List<Map<String, Object>> getAppUsers(String databaseId) {
		return SecurityEngineUtils.getFullDatabaseOwnersAndEditors(databaseId);
	}

	/**
	 * Get all the users for a databases
	 * @param engineId
	 * @param userId
	 * @param permission
	 * @param limit
	 * @param offset
	 * @return
	 */
	public List<Map<String, Object>> getEngineUsers(String engineId, String userId, String permission, long limit, long offset) {
		return SecurityEngineUtils.getFullEngineOwnersAndEditors(engineId, userId, permission, limit, offset);
	}
	
	/**
	 * 
	 * @param engineId
	 * @param userId
	 * @param permission
	 * @return
	 */
	public static long getEngineUsersCount(String engineId, String userId, String permission) {
		boolean hasUserId = userId != null && !(userId=userId.trim()).isEmpty();
		boolean hasPermission = permission != null && !(permission=permission.trim()).isEmpty();
		SelectQueryStruct qs = new SelectQueryStruct();
		QueryFunctionSelector fSelector = new QueryFunctionSelector();
        fSelector.setAlias("count");
        fSelector.setFunction(QueryFunctionHelper.COUNT);
        fSelector.addInnerSelector(new QueryColumnSelector("SMSS_USER__ID"));
        qs.addSelector(fSelector);
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__ENGINEID", "==", engineId));
		if (hasUserId) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "?like", userId));
		}
		if (hasPermission) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__PERMISSION", "==", AccessPermissionEnum.getIdByPermission(permission)));
		}
		qs.addRelation("SMSS_USER", "ENGINEPERMISSION", "inner.join");
		qs.addRelation("ENGINEPERMISSION", "PERMISSION", "inner.join");
		return QueryExecutionUtility.flushToLong(securityDb, qs);
	}
	
	
	/**
	 * Get all the users for a project
	 * @param projectId
	 * @return
	 */
	public List<Map<String, Object>> getProjectUsers(String projectId, String userId, String permission, long limit, long offset) {
		return SecurityProjectUtils.getFullProjectOwnersAndEditors(projectId, userId, permission, limit, offset);
	}
	
	public static long getProjectUsersCount(String projectId, String userId, String permission) {
		boolean hasUserId = userId != null && !(userId=userId.trim()).isEmpty();
		boolean hasPermission = permission != null && !(permission=permission.trim()).isEmpty();
		SelectQueryStruct qs = new SelectQueryStruct();
		QueryFunctionSelector fSelector = new QueryFunctionSelector();
        fSelector.setAlias("count");
        fSelector.setFunction(QueryFunctionHelper.COUNT);
        fSelector.addInnerSelector(new QueryColumnSelector("SMSS_USER__ID"));
        qs.addSelector(fSelector);
        qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__PROJECTID", "==", projectId));
		if (hasUserId) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "?like", userId));
		}
		if (hasPermission) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__PERMISSION", "==", AccessPermissionEnum.getIdByPermission(permission)));
		}
		qs.addRelation("SMSS_USER", "PROJECTPERMISSION", "inner.join");
		qs.addRelation("PROJECTPERMISSION", "PERMISSION", "inner.join");
		return QueryExecutionUtility.flushToLong(securityDb, qs);
	}
	
	/**
	 * 
	 * @param newUserId
	 * @param engineId
	 * @param permission
	 * @return
	 */
	public void addEngineUser(String newUserId, String engineId, String permission) {
		// make sure user doesn't already exist for this database
		if(SecurityUserEngineUtils.getUserEnginePermission(newUserId, engineId) != null) {
			// that means there is already a value
			throw new IllegalArgumentException("This user already has access to this engine. Please edit the existing permission level.");
		}
		
		// insert new user permissions in bulk
		String insertQ = "INSERT INTO ENGINEPERMISSION (USERID, ENGINEID, PERMISSION, VISIBILITY) VALUES(?,?,?,?)";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(insertQ);
			int parameterIndex = 1;
			ps.setString(parameterIndex++, newUserId);
			ps.setString(parameterIndex++, engineId);
			ps.setInt(parameterIndex++, AccessPermissionEnum.getIdByPermission(permission));
			ps.setBoolean(parameterIndex++, true);
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred adding user permissions for this database");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}
	
	/**
	 * 
	 * @param newUserId
	 * @param databaseId
	 * @param permission
	 * @return
	 */
	public void addEngineUserPermissions(String databaseId, List<Map<String,String>> permission) {
		// first, check to make sure these users do not already have permissions to database
		// get list of userids from permission list map
		List<String> userIds = permission.stream().map(map -> map.get("userid")).collect(Collectors.toList());
		// this returns a list of existing permissions
		Map<String, Integer> existingUserPermission = SecurityUserEngineUtils.getUserEnginePermissions(userIds, databaseId);
		if (!existingUserPermission.isEmpty()) {
			throw new IllegalArgumentException("The following users already have access to this database. Please edit the existing permission level: "+String.join(",", existingUserPermission.keySet()));
		}
		
		// insert new user permissions in bulk
		String insertQ = "INSERT INTO ENGINEPERMISSION (USERID, ENGINEID, PERMISSION, VISIBILITY) VALUES(?,?,?,?)";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(insertQ);
			for(int i=0; i<permission.size(); i++) {
				int parameterIndex = 1;
				ps.setString(parameterIndex++, permission.get(i).get("userid"));
				ps.setString(parameterIndex++, databaseId);
				ps.setInt(parameterIndex++, AccessPermissionEnum.getIdByPermission(permission.get(i).get("permission")));
				ps.setBoolean(parameterIndex++, true);
				ps.addBatch();
			}
			ps.executeBatch();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred setting the permissions for this engine");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}
	
	/**
	 * 
	 * @param newUserId
	 * @param projectId
	 * @param permission
	 * @return
	 */
	public void addProjectUserPermissions(String projectId, List<Map<String,String>> permission) {
		// first, check to make sure these users do not already have permissions to project
		// get list of userids from permission list map
		List<String> userIds = permission.stream().map(map -> map.get("userid")).collect(Collectors.toList());
		// this returns a list of existing permissions
		Map<String, Integer> existingUserPermission = SecurityProjectUtils.getUserProjectPermissions(userIds, projectId);
		if (!existingUserPermission.isEmpty()) {
			throw new IllegalArgumentException("The following users already have access to this project. Please edit the existing permission level: "+String.join(",", existingUserPermission.keySet()));
		}
		
		// insert new user permissions in bulk
		String insertQ = "INSERT INTO PROJECTPERMISSION (USERID, PROJECTID, PERMISSION, VISIBILITY) VALUES(?,?,?,?)";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(insertQ);
			for(int i=0; i<permission.size(); i++) {
				int parameterIndex = 1;
				ps.setString(parameterIndex++, permission.get(i).get("userid"));
				ps.setString(parameterIndex++, projectId);
				ps.setInt(parameterIndex++, AccessPermissionEnum.getIdByPermission(permission.get(i).get("permission")));
				ps.setBoolean(parameterIndex++, true);
				ps.addBatch();
			}
			ps.executeBatch();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred adding user permissions for this project");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}
	
	/**
	 * 
	 * @param newUserId
	 * @param insightId
	 * @param permission
	 * @return
	 */
	public void addInsightUserPermissions(String projectId, String insightId, List<Map<String,String>> permission) {
		// first, check to make sure these users do not already have permissions to insight
		// get list of userids from permission list map
		List<String> userIds = permission.stream().map(map -> map.get("userid")).collect(Collectors.toList());
		// this returns a list of existing permissions
		Map<String, Integer> existingUserPermission = SecurityInsightUtils.getUserInsightPermissions(userIds, projectId, insightId);
		if (!existingUserPermission.isEmpty()) {
			throw new IllegalArgumentException("The following users already have access to this insight. Please edit the existing permission level: "+String.join(",", existingUserPermission.keySet()));
		}
		// insert new user permissions in bulk
		String insertQ = "INSERT INTO USERINSIGHTPERMISSION (USERID, PROJECTID, INSIGHTID, PERMISSION) VALUES(?,?,?,?)";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(insertQ);
			for(int i=0; i<permission.size(); i++) {
				int parameterIndex = 1;
				ps.setString(parameterIndex++, permission.get(i).get("userid"));
				ps.setString(parameterIndex++, projectId);
				ps.setString(parameterIndex++, insightId);
				ps.setInt(parameterIndex++, AccessPermissionEnum.getIdByPermission(permission.get(i).get("permission")));
				ps.addBatch();
			}
			ps.executeBatch();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred adding user permissions for this insight");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}
	
	/**
	 * 
	 * @param newUserId
	 * @param projectId
	 * @param permission
	 * @return
	 */
	public void addProjectUser(String newUserId, String projectId, String permission) {
		// make sure user doesn't already exist for this project
		if(SecurityUserProjectUtils.getUserProjectPermission(newUserId, projectId) != null) {
			// that means there is already a value
			throw new IllegalArgumentException("This user already has access to this project. Please edit the existing permission level.");
		}
		
		// insert new user permissions in bulk
		String insertQ = "INSERT INTO PROJECTPERMISSION (USERID, PROJECTID, PERMISSION, VISIBILITY) VALUES(?,?,?,?)";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(insertQ);
			int parameterIndex = 1;
			ps.setString(parameterIndex++, newUserId);
			ps.setString(parameterIndex++, projectId);
			ps.setInt(parameterIndex++, AccessPermissionEnum.getIdByPermission(permission));
			ps.setBoolean(parameterIndex++, true);
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred adding user permissions for this project");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}
	
	/**
	 * Return the databases the user has explicit access to
	 * @param singleUserId
	 * @return
	 */
	public List<String> getProjectsUserHasExplicitAccess(String singleUserId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__PROJECTID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", singleUserId));
		return QueryExecutionUtility.flushToListString(securityDb, qs);
	}
	
	/**
	 * Return the databases the user has explicit access to
	 * @param singleUserId
	 * @return
	 */
	public Map<String, Boolean> getProjectsAndVisibilityUserHasExplicitAccess(String singleUserId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__PROJECTID"));
		qs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__VISIBILITY"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", singleUserId));
		Map<String, Boolean> values = new HashMap<>();
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			while(wrapper.hasNext()) {
				Object[] row = wrapper.next().getValues();
				values.put((String) row[0], (Boolean) row[1]);
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
		
		return values;
	}
	
	/** 
	 * Give user permission for all the projects
	 * @param userId		String - 	The user id we are providing permissions to
	 * @param permission	String - 	The permission level for the access
	 * @param isAddNew 		boolean - 	If false, modifying existing project permissions to the new permission level
	 * 									If true, adding new projects with the permission level specified
	 */
	public void grantAllProjects(String userId, String permission, boolean isAddNew) {
		if(isAddNew) {
			List<String> currentProjectAccess = getProjectsUserHasExplicitAccess(userId);
			List<String> projectIds = SecurityProjectUtils.getAllProjectIds();
			String insertQuery = "INSERT INTO PROJECTPERMISSION (USERID, PROJECTID, VISIBILITY, PERMISSION) VALUES(?,?,?,?)";
			int permissionLevel = AccessPermissionEnum.getIdByPermission(permission);
			boolean visible = true;
			PreparedStatement ps = null;

			try {
				ps = securityDb.getPreparedStatement(insertQuery);
				// add new permission for projects
				for (String projectId : projectIds) {
					if(currentProjectAccess.contains(projectId)) {
						// only add for new projects, not existing projects
						continue;
					}
					int parameterIndex = 1;
					ps.setString(parameterIndex++, userId);
					ps.setString(parameterIndex++, projectId);
					ps.setBoolean(parameterIndex++, visible);
					ps.setInt(parameterIndex++, permissionLevel);
					ps.addBatch();
				}
				ps.executeBatch();
				if(!ps.getConnection().getAutoCommit()) {
					ps.getConnection().commit();
				}
			} catch (SQLException e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("An error occurred granting the user permission for all the projects");
			} finally {
				ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
			}
		} else {
			// first grab the projects and visibility
			Map<String, Boolean> currentProjectToVisibilityMap = getProjectsAndVisibilityUserHasExplicitAccess(userId);
			
			// we will remove all the current permissions
			// and then re-add the ones they used to have but with the new level
			
			// delete first
			{
				String deleteQuery = "DELETE FROM PROJECTPERMISSION WHERE USERID=?";
				PreparedStatement ps = null;
				try {
					ps = securityDb.getPreparedStatement(deleteQuery);
					ps.setString(1, userId);
					ps.execute();
					if(!ps.getConnection().getAutoCommit()) {
						ps.getConnection().commit();
					}
				} catch (SQLException e) {
					classLogger.error(Constants.STACKTRACE, e);
					throw new IllegalArgumentException("An error occurred granting the user permission for all the projects");
				} finally {
					ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
				}
			}
			// now add
			{
				// now we insert the values
				String insertQuery = "INSERT INTO PROJECTPERMISSION (USERID, PROJECTID, VISIBILITY, PERMISSION) VALUES(?,?,?,?)";
				int permissionLevel = AccessPermissionEnum.getIdByPermission(permission);
				PreparedStatement ps = null;

				try {
					ps = securityDb.getPreparedStatement(insertQuery);
					// add new permission for all projects
					for (String projectId : currentProjectToVisibilityMap.keySet()) {
						boolean visible = currentProjectToVisibilityMap.get(projectId);
						int parameterIndex = 1;
						ps.setString(parameterIndex++, userId);
						ps.setString(parameterIndex++, projectId);
						ps.setBoolean(parameterIndex++, visible);
						ps.setInt(parameterIndex++, permissionLevel);
						ps.addBatch();
					}
					ps.executeBatch();
					if(!ps.getConnection().getAutoCommit()) {
						ps.getConnection().commit();
					}
				} catch (SQLException e) {
					classLogger.error(Constants.STACKTRACE, e);
					throw new IllegalArgumentException("An error occurred granting the user permission for all the projects");
				} finally {
					ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
				}
			}
		}
	}
	
	/**
	 * Return the databases the user has explicit access to
	 * @param singleUserId
	 * @return
	 */
	public static List<String> getEnginesUserHasExplicitAccess(String singleUserId, List<String> engineTypes) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__ENGINEID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", singleUserId));
		if(engineTypes != null && !engineTypes.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINETYPE", "==", engineTypes));
			qs.addRelation("ENGINEPERMISSION__ENGINEID", "ENGINE__ENGINEID", "inner.join");
		}
		return QueryExecutionUtility.flushToListString(securityDb, qs);
	}
	
	/**
	 * Return the databases the user has explicit access to
	 * @param singleUserId
	 * @return
	 */
	public Map<String, Boolean> getEnginesAndVisibilityUserHasExplicitAccess(String singleUserId, List<String> engineTypes) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__ENGINEID"));
		qs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__VISIBILITY"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", singleUserId));
		if(engineTypes != null && !engineTypes.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINETYPE", "==", engineTypes));
			qs.addRelation("ENGINEPERMISSION__ENGINEID", "ENGINE__ENGINEID", "inner.join");
		}
		Map<String, Boolean> values = new HashMap<>();
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			while(wrapper.hasNext()) {
				Object[] row = wrapper.next().getValues();
				values.put((String) row[0], (Boolean) row[1]);
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
		
		return values;
	}
	
	/**
	 * Give user permission for all the engines
	 * @param userId		String - 	The user id we are providing permissions to
	 * @param permission	String - 	The permission level for the access
	 * @param isAddNew 		boolean - 	If false, modifying existing project permissions to the new permission level
	 * 									If true, adding new projects with the permission level specified
	 * @param engineTypes
	 */
	public void grantAllEngines(String userId, String permission, boolean isAddNew, List<String> engineTypes) {
		String logETypes = (engineTypes == null || engineTypes.isEmpty()) ? "[ALL]" : ("[" + String.join(", ", engineTypes) + "]");

		if(isAddNew) {
			List<String> currentEngineAccess = getEnginesUserHasExplicitAccess(userId, engineTypes);
			List<String> engineIds = SecurityEngineUtils.getAllEngineIds();
			String insertQuery = "INSERT INTO ENGINEPERMISSION (USERID, ENGINEID, VISIBILITY, PERMISSION) VALUES(?,?,?,?)";
			int permissionLevel = AccessPermissionEnum.getIdByPermission(permission);
			boolean visible = true;
			PreparedStatement ps = null;

			try {
				ps = securityDb.getPreparedStatement(insertQuery);
				// add new permission for databases
				for (String databaseId : engineIds) {
					if(currentEngineAccess.contains(databaseId)) {
						// only add for new databases, not existing databases
						continue;
					}
					int parameterIndex = 1;
					ps.setString(parameterIndex++, userId);
					ps.setString(parameterIndex++, databaseId);
					ps.setBoolean(parameterIndex++, visible);
					ps.setInt(parameterIndex++, permissionLevel);
					ps.addBatch();
				}
				ps.executeBatch();
				if(!ps.getConnection().getAutoCommit()) {
					ps.getConnection().commit();
				}
			} catch (SQLException e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("An error occurred granting the user permission for all the engines of type "+logETypes);
			} finally {
				ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
			}
		} else {
			// first grab the databases and visibility
			Map<String, Boolean> currentEngineToVisibilityMap = getEnginesAndVisibilityUserHasExplicitAccess(userId, engineTypes);
			
			// we will remove all the current permissions
			// and then re-add the ones they used to have but with the new level
			
			// delete first
			{
				String deleteQuery = "DELETE FROM ENGINEPERMISSION WHERE USERID=?";
				PreparedStatement ps = null;
				try {
					ps = securityDb.getPreparedStatement(deleteQuery);
					ps.setString(1, userId);
					ps.execute();
					if(!ps.getConnection().getAutoCommit()) {
						ps.getConnection().commit();
					}
				} catch (SQLException e) {
					classLogger.error(Constants.STACKTRACE, e);
					throw new IllegalArgumentException("An error occurred granting the user permission for all the engines of type "+logETypes);
				} finally {
					ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
				}
			}
			// now add
			{
				// now we insert the values
				String insertQuery = "INSERT INTO ENGINEPERMISSION (USERID, ENGINEID, VISIBILITY, PERMISSION) VALUES(?,?,?,?)";
				int permissionLevel = AccessPermissionEnum.getIdByPermission(permission);
				PreparedStatement ps = null;

				try {
					ps = securityDb.getPreparedStatement(insertQuery);
					// add new permission for all projects
					for (String databaseId : currentEngineToVisibilityMap.keySet()) {
						boolean visible = currentEngineToVisibilityMap.get(databaseId);
						int parameterIndex = 1;
						ps.setString(parameterIndex++, userId);
						ps.setString(parameterIndex++, databaseId);
						ps.setBoolean(parameterIndex++, visible);
						ps.setInt(parameterIndex++, permissionLevel);
						ps.addBatch();
					}
					ps.executeBatch();
					if(!ps.getConnection().getAutoCommit()) {
						ps.getConnection().commit();
					}
				} catch (SQLException e) {
					classLogger.error(Constants.STACKTRACE, e);
					throw new IllegalArgumentException("An error occurred granting the user permission for all the engines of type "+logETypes);
				} finally {
					ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
				}
			}
		}
	}
	
	
	/** 
	 * give new users access to a database
	 * @param engineId
	 * @param permission
	 */
	public void grantNewUsersEngineAccess(String engineId, String permission) {
		String query = "INSERT INTO ENGINEPERMISSION (USERID, ENGINEID, PERMISSION, VISIBILITY) VALUES(?,?,?,?)";
		int permissionLevel = AccessPermissionEnum.getIdByPermission(permission);
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			// get users with no access to app
			List<Map<String, Object>> users = getEngineUsersNoCredentials(engineId);
			for (Map<String, Object> userMap : users) {
				String userId = (String) userMap.get("id");
				int parameterIndex = 1;
				ps.setString(parameterIndex++, userId);
				ps.setString(parameterIndex++, engineId);
				ps.setInt(parameterIndex++, permissionLevel);
				ps.setBoolean(parameterIndex++, true);
				ps.addBatch();
			}
			ps.executeBatch();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred adding user permissions for engine "+engineId + " with permission " + permission);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}
	
	public void grantNewUsersProjectAccess(String projectId, String permission) {
		String query = "INSERT INTO PROJECTPERMISSION (USERID, PROJECTID, PERMISSION, VISIBILITY) VALUES(?,?,?,?)";
		int permissionLevel = AccessPermissionEnum.getIdByPermission(permission);
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			// get users with no access to project
			List<Map<String, Object>> users = getProjectUsersNoCredentials(projectId);
			for (Map<String, Object> userMap : users) {
				String userId = (String) userMap.get("id");
				int parameterIndex = 1;
				ps.setString(parameterIndex++, userId);
				ps.setString(parameterIndex++, projectId);
				ps.setInt(parameterIndex++, permissionLevel);
				ps.setBoolean(parameterIndex++, true);
				ps.addBatch();
			}
			ps.executeBatch();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred adding user permissions for this project");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}		
	}
	
	/**
	 * Give the user permission for all the insights in a project
	 * @param projectId
	 * @param userId
	 * @param permission
	 */
	public void grantAllProjectInsights(String projectId, String userId, String permission) {
		int permissionLevel = AccessPermissionEnum.getIdByPermission(permission);
		
		// delete all previous permissions for the user
		String deleteQuery = "DELETE FROM USERINSIGHTPERMISSION WHERE USERID=? AND PROJECTID=?";
		String insertQuery = "INSERT INTO USERINSIGHTPERMISSION (USERID, PROJECTID, INSIGHTID, PERMISSION) VALUES(?,?,?,?)";

		PreparedStatement deletePs = null;
		PreparedStatement insertPs = null;
		try {
			deletePs = securityDb.getPreparedStatement(deleteQuery);
			int parameterIndex = 1;
			deletePs.setString(parameterIndex++, userId);
			deletePs.setString(parameterIndex++, projectId);
			deletePs.execute();
			
			insertPs = securityDb.getPreparedStatement(insertQuery);
			// add new permission for all insights
			List<String> insightIds = getAllProjectInsights(projectId);
			for (String insightId : insightIds) {
				parameterIndex = 1;
				insertPs.setString(parameterIndex++, userId);
				insertPs.setString(parameterIndex++, projectId);
				insertPs.setString(parameterIndex++, insightId);
				insertPs.setInt(parameterIndex++, permissionLevel);
				insertPs.addBatch();
			}
			insertPs.executeBatch();
			
			// do commits
			if(!deletePs.getConnection().getAutoCommit()) {
				deletePs.getConnection().commit();
			}
			if(!insertPs.getConnection().getAutoCommit()) {
				insertPs.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred granting the user permission for all the projects");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, deletePs);
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, insertPs);
		}
	}
	/**
	 * 
	 * @param existingUserId
	 * @param engineId
	 * @param newPermission
	 * @return
	 */
	public void editEngineUserPermission(String existingUserId, String engineId, String newPermission) {
		// make sure we are trying to edit a permission that exists
		Integer existingUserPermission = SecurityUserEngineUtils.getUserEnginePermission(existingUserId, engineId);
		if(existingUserPermission == null) {
			throw new IllegalArgumentException("Attempting to modify user permission for a user who does not currently have access to the engine");
		}
		
		String updateQ = "UPDATE ENGINEPERMISSION SET PERMISSION = ? WHERE USERID = ? AND ENGINEID = ?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(updateQ);
			int parameterIndex = 1;
			//SET
			ps.setInt(parameterIndex++, AccessPermissionEnum.getIdByPermission(newPermission));
			//WHERE
			ps.setString(parameterIndex++, existingUserId);
			ps.setString(parameterIndex++, engineId);
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred editing user permissions for this engine");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}	
	}
	
	/**
	 * 
	 * @param user
	 * @param existingUserId
	 * @param engineId
	 * @param newPermission
	 * @return
	 * @throws IllegalAccessException 
	 */
	public static void editEngineUserPermissions(String engineId, List<Map<String, String>> requests) throws IllegalAccessException {
		// get userid of all requests
		List<String> existingUserIds = new ArrayList<String>();
	    for(Map<String,String> i:requests){
	    	existingUserIds.add(i.get("userid"));
	    }
			    
		// get user permissions to edit
		Map<String, Integer> existingUserPermission = SecurityUserEngineUtils.getUserEnginePermissions(existingUserIds, engineId);
		
		// make sure all users to edit currently has access to database
		Set<String> toRemoveUserIds = new HashSet<String>(existingUserIds);
		toRemoveUserIds.removeAll(existingUserPermission.keySet());
		if (!toRemoveUserIds.isEmpty()) {
			throw new IllegalArgumentException("Attempting to modify user permission for the following users who do not currently have access to the engine: "+String.join(",", toRemoveUserIds));
		}
		
		// update user permissions in bulk
		String updateQ = "UPDATE ENGINEPERMISSION SET PERMISSION = ? WHERE USERID = ? AND ENGINEID = ?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(updateQ);
			for(int i=0; i<requests.size(); i++) {
				int parameterIndex = 1;
				//SET
				ps.setInt(parameterIndex++, AccessPermissionEnum.getIdByPermission(requests.get(i).get("permission")));
				//WHERE
				ps.setString(parameterIndex++, requests.get(i).get("userid"));
				ps.setString(parameterIndex++, engineId);
				ps.addBatch();
			}
			ps.executeBatch();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred editing user permissions for this engine");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}
	
	
	/**
	 * 
	 * @param existingUserId
	 * @param projectId
	 * @param newPermission
	 * @return
	 */
	public void editProjectUserPermission(String existingUserId, String projectId, String newPermission) {
		// make sure we are trying to edit a permission that exists
		Integer existingUserPermission = SecurityUserProjectUtils.getUserProjectPermission(existingUserId, projectId);
		if(existingUserPermission == null) {
			throw new IllegalArgumentException("Attempting to modify user permission for a user who does not currently have access to the project");
		}
		
		// update user permissions in bulk
		String updateQ = "UPDATE PROJECTPERMISSION SET PERMISSION = ? WHERE USERID = ? AND PROJECTID = ?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(updateQ);
			int parameterIndex = 1;
			//SET
			ps.setInt(parameterIndex++, AccessPermissionEnum.getIdByPermission(newPermission));
			//WHERE
			ps.setString(parameterIndex++, existingUserId);
			ps.setString(parameterIndex++, projectId);
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred editing user permissions for this project");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}
	
	/**
	 * 
	 * @param user
	 * @param existingUserId
	 * @param projectId
	 * @param newPermission
	 * @return
	 * @throws IllegalAccessException 
	 */
	public static void editProjectUserPermissions(String projectId, List<Map<String, String>> requests) throws IllegalAccessException {
		// get userid of all requests
		List<String> existingUserIds = new ArrayList<String>();
	    for(Map<String,String> i:requests){
	    	existingUserIds.add(i.get("userid"));
	    }
			    
		// get user permissions to edit
		Map<String, Integer> existingUserPermission = SecurityProjectUtils.getUserProjectPermissions(existingUserIds, projectId);
		
		// make sure all users to edit currently has access to database
		Set<String> toRemoveUserIds = new HashSet<String>(existingUserIds);
		toRemoveUserIds.removeAll(existingUserPermission.keySet());
		if (!toRemoveUserIds.isEmpty()) {
			throw new IllegalArgumentException("Attempting to modify user permission for the following users who do not currently have access to the project: "+String.join(",", toRemoveUserIds));
		}
		
		// update user permissions in bulk
		String updateQ = "UPDATE PROJECTPERMISSION SET PERMISSION = ? WHERE USERID = ? AND PROJECTID = ?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(updateQ);
			for(int i=0; i<requests.size(); i++) {
				int parameterIndex = 1;
				//SET
				ps.setInt(parameterIndex++, AccessPermissionEnum.getIdByPermission(requests.get(i).get("permission")));
				//WHERE
				ps.setString(parameterIndex++, requests.get(i).get("userid"));
				ps.setString(parameterIndex++, projectId);
				ps.addBatch();
			}
			ps.executeBatch();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred editing user permissions for this project");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}	
	}
	
	/**
	 * Edit insight user permissions in bulk
	 * @param projectId
	 * @param insightId
	 * @param requests
	 * @throws IllegalAccessException
	 */
	public static void editInsightUserPermissions(String projectId, String insightId, List<Map<String, String>> requests) throws IllegalAccessException {
		// get userid of all requests
		List<String> existingUserIds = new ArrayList<String>();
	    for(Map<String,String> i:requests){
	    	existingUserIds.add(i.get("userid"));
	    }
			    
		// get user permissions to edit
		Map<String, Integer> existingUserPermission = SecurityInsightUtils.getUserInsightPermissions(existingUserIds, projectId, insightId);
		
		// make sure all users to edit currently has access to database
		Set<String> toRemoveUserIds = new HashSet<String>(existingUserIds);
		toRemoveUserIds.removeAll(existingUserPermission.keySet());
		if (!toRemoveUserIds.isEmpty()) {
			throw new IllegalArgumentException("Attempting to modify user permission for the following users who do not currently have access to the insight: "+String.join(",", toRemoveUserIds));
		}
		
		// update user permissions in bulk
		String updateQ = "UPDATE USERINSIGHTPERMISSION SET PERMISSION = ? WHERE USERID = ? AND PROJECTID = ? AND INSIGHTID = ?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(updateQ);
			for(int i=0; i<requests.size(); i++) {
				int parameterIndex = 1;
				//SET
				ps.setInt(parameterIndex++, AccessPermissionEnum.getIdByPermission(requests.get(i).get("permission")));
				//WHERE
				ps.setString(parameterIndex++, requests.get(i).get("userid"));
				ps.setString(parameterIndex++, projectId);
				ps.setString(parameterIndex++, insightId);
				ps.addBatch();
			}
			ps.executeBatch();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred editing user permissions for this insight");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}	
	}
	/**
	 * 
	 * @param editedUserId
	 * @param engineId
	 * @return
	 */
	public void removeEngineUser(String existingUserId, String engineId) {
		// make sure we are trying to edit a permission that exists
		Integer existingUserPermission = SecurityUserEngineUtils.getUserEnginePermission(existingUserId, engineId);
		if(existingUserPermission == null) {
			throw new IllegalArgumentException("Attempting to modify user permission for a user who does not currently have access to the engine");
		}
		
		String deleteQ = "DELETE FROM ENGINEPERMISSION WHERE USERID=? AND ENGINEID=?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(deleteQ);
			int parameterIndex = 1;
			ps.setString(parameterIndex++, existingUserId);
			ps.setString(parameterIndex++, engineId);
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred removing the users access to this engine");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}
	
	/**
	 * 
	 * @param editedUserId
	 * @param engineId
	 * @return
	 */
	public void removeEngineUsers(List<String> existingUserIds, String engineId) {
		Map<String, Integer> existingUserPermission = SecurityUserEngineUtils.getUserEnginePermissions(existingUserIds, engineId);
		
		// make sure these users all exist and have access
		Set<String> toRemoveUserIds = new HashSet<String>(existingUserIds);
		toRemoveUserIds.removeAll(existingUserPermission.keySet());
		if (!toRemoveUserIds.isEmpty()) {
			throw new IllegalArgumentException("Attempting to modify user permission for the following users who do not currently have access to the engine: "+String.join(",", toRemoveUserIds));
		}

		String deleteQ = "DELETE FROM ENGINEPERMISSION WHERE USERID=? AND ENGINEID=?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(deleteQ);
			for(int i=0; i<existingUserIds.size(); i++) {
				int parameterIndex = 1;
				ps.setString(parameterIndex++, existingUserIds.get(i));
				ps.setString(parameterIndex++, engineId);
				ps.addBatch();
			}
			ps.executeBatch();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred removing user permissions from this engine");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}
	
	/**
	 * 
	 * @param editedUserId
	 * @param projectId
	 * @return
	 */
	public void removeProjectUsers(List<String> existingUserIds, String projectId) {
		Map<String, Integer> existingUserPermission = SecurityProjectUtils.getUserProjectPermissions(existingUserIds, projectId);
		
		// make sure these users all exist and have access
		Set<String> toRemoveUserIds = new HashSet<String>(existingUserIds);
		toRemoveUserIds.removeAll(existingUserPermission.keySet());
		if (!toRemoveUserIds.isEmpty()) {
			throw new IllegalArgumentException("Attempting to modify user permission for the following users who do not currently have access to the project: "+String.join(",", toRemoveUserIds));
		}

		String deleteQ = "DELETE FROM PROJECTPERMISSION WHERE USERID=? AND PROJECTID=?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(deleteQ);
			for(int i=0; i<existingUserIds.size(); i++) {
				int parameterIndex = 1;
				ps.setString(parameterIndex++, existingUserIds.get(i));
				ps.setString(parameterIndex++, projectId);
				ps.addBatch();
			}
			ps.executeBatch();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred removing user permissions from this project");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}
	
	/**
	 * 
	 * @param editedUserId
	 * @param projectId
	 * @return
	 */
	public void removeInsightUsers(List<String> existingUserIds, String projectId, String insightId) {
		Map<String, Integer> existingUserPermission = SecurityInsightUtils.getUserInsightPermissions(existingUserIds, projectId, insightId);
		// make sure these users all exist and have access
		Set<String> toRemoveUserIds = new HashSet<String>(existingUserIds);
		toRemoveUserIds.removeAll(existingUserPermission.keySet());
		if (!toRemoveUserIds.isEmpty()) {
			throw new IllegalArgumentException("Attempting to modify user permission for the following users who do not currently have access to the insight: "+String.join(",", toRemoveUserIds));
		}
		String deleteQ = "DELETE FROM USERINSIGHTPERMISSION WHERE USERID=? AND PROJECTID=? AND INSIGHTID=?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(deleteQ);
			for(int i=0; i<existingUserIds.size(); i++) {
				int parameterIndex = 1;
				ps.setString(parameterIndex++, existingUserIds.get(i));
				ps.setString(parameterIndex++, projectId);
				ps.setString(parameterIndex++, insightId);
				ps.addBatch();
			}
			ps.executeBatch();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred removing insight permissions for this project");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}
	
	
	/**
	 * 
	 * @param editedUserId
	 * @param projectId
	 * @return
	 */
	public void removeProjectUser(String existingUserId, String projectId) {
		// make sure we are trying to edit a permission that exists
		Integer existingUserPermission = SecurityUserProjectUtils.getUserProjectPermission(existingUserId, projectId);
		if(existingUserPermission == null) {
			throw new IllegalArgumentException("Attempting to modify user permission for a user who does not currently have access to the project");
		}
		
		String deleteQ = "DELETE FROM PROJECTPERMISSION WHERE USERID=? AND PROJECTID=?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(deleteQ);
			int parameterIndex = 1;
			ps.setString(parameterIndex++, existingUserId);
			ps.setString(parameterIndex++, projectId);
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred removing user permissions for this project");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}
	
	///////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////////////

	/*
	 * METHODS FOR INSIGHT AUTHORIZATION THAT ARE AT THE ADMIN LEVEL
	 */
	
	/**
	 * 
	 * @param projectId
	 * @return
	 */
	public List<Map<String, Object>> getProjectInsights(String projectId) {

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("INSIGHT__PROJECTID", "project_id"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__INSIGHTID", "project_insight_id"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__INSIGHTNAME", "name"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__GLOBAL", "insight_global"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__EXECUTIONCOUNT", "exec_count"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__CREATEDON", "created_on"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__LASTMODIFIEDON", "last_modified_on"));
		qs.addSelector(new QueryColumnSelector("INSIGHT__CACHEABLE", "cacheable"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHT__PROJECTID", "==", projectId));
		qs.addOrderBy(new QueryColumnOrderBySelector("INSIGHT__INSIGHTNAME"));
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}
	
	/**
	 * @param projectId
	 * @param insightIds
	 * @throws Exception 
	 */
	public void deleteProjectInsights(String projectId, List<String> insightIds) throws Exception {
		IProject project =  Utility.getProject(projectId);
		InsightAdministrator admin = new InsightAdministrator(project.getInsightDatabase());
	
		// delete from insights database
		admin.dropInsight(insightIds);

		// delete from the security database
		String insightFilters = createFilter(insightIds);
		//TODO:
		//TODO:
		//TODO:
		//TODO:
		String query = "DELETE FROM INSIGHT WHERE INSIGHTID " + insightFilters + " AND PROJECTID='" + projectId + "';";
		query += "DELETE FROM USERINSIGHTPERMISSION  WHERE INSIGHTID " + insightFilters + " AND PROJECTID='" + projectId + "'";
		securityDb.insertData(query);
		securityDb.commit();
	}
	
	/**
	 * Retrieve the list of users for a given insight
	 * @param projectId
	 * @param insightId
	 * @return
	 * @throws IllegalAccessException
	 */
	public List<Map<String, Object>> getInsightUsers(String projectId, String insightId, String userId, String permission, long limit, long offset) {
		boolean hasUserId = userId != null && !(userId=userId.trim()).isEmpty();
		boolean hasPermission = permission != null && !(permission=permission.trim()).isEmpty();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__ID", "id"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__NAME", "name"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__EMAIL", "email"));
		qs.addSelector(new QueryColumnSelector("PERMISSION__NAME", "permission"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__INSIGHTID", "==", insightId));
		if (hasUserId) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__USERID", "?like", userId));
		}
		if (hasPermission) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__PERMISSION", "==", AccessPermissionEnum.getIdByPermission(permission)));
		}
		qs.addRelation("SMSS_USER", "USERINSIGHTPERMISSION", "inner.join");
		qs.addRelation("USERINSIGHTPERMISSION", "PERMISSION", "inner.join");
		qs.addOrderBy(new QueryColumnOrderBySelector("PERMISSION__ID"));
		qs.addOrderBy(new QueryColumnOrderBySelector("SMSS_USER__ID"));
		if(limit > 0) {
			qs.setLimit(limit);
		}
		if(offset > 0) {
			qs.setOffSet(offset);
		}
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}
	
	/**
	 * 
	 * @param projectId
	 * @param insightId
	 * @param userId
	 * @param permission
	 * @return
	 */
	public static long getInsightUsersCount(String projectId, String insightId, String userId, String permission) {
		boolean hasUserId = userId != null && !(userId=userId.trim()).isEmpty();
		boolean hasPermission = permission != null && !(permission=permission.trim()).isEmpty();
		SelectQueryStruct qs = new SelectQueryStruct();
		QueryFunctionSelector fSelector = new QueryFunctionSelector();
        fSelector.setAlias("count");
        fSelector.setFunction(QueryFunctionHelper.COUNT);
        fSelector.addInnerSelector(new QueryColumnSelector("SMSS_USER__ID"));
        qs.addSelector(fSelector);
        qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__INSIGHTID", "==", insightId));
		if (hasUserId) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__USERID", "?like", userId));
		}
		if (hasPermission) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__PERMISSION", "==", AccessPermissionEnum.getIdByPermission(permission)));
		}
		qs.addRelation("SMSS_USER", "USERINSIGHTPERMISSION", "inner.join");
		qs.addRelation("USERINSIGHTPERMISSION", "PERMISSION", "inner.join");
		return QueryExecutionUtility.flushToLong(securityDb, qs);
	}
	
	/**
	 * 
	 * @param newUserId
	 * @param projectId
	 * @param insightId
	 * @param permission
	 * @return
	 */
	public void addInsightUser(String newUserId, String projectId, String insightId, String permission) {
		// make sure user doesn't already exist for this insight
		if(SecurityInsightUtils.getUserInsightPermission(newUserId, projectId, insightId) != null) {
			// that means there is already a value
			throw new IllegalArgumentException("This user already has access to this insight. Please edit the existing permission level.");
		}
		
		String insertQ = "INSERT INTO USERINSIGHTPERMISSION (USERID, PROJECTID, INSIGHTID, PERMISSION) VALUES(?,?,?,?)";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(insertQ);
			int parameterIndex = 1;
			ps.setString(parameterIndex++, newUserId);
			ps.setString(parameterIndex++, projectId);
			ps.setString(parameterIndex++, insightId);
			ps.setInt(parameterIndex++, AccessPermissionEnum.getIdByPermission(permission));
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred adding user permissions for this insight");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}
	
	/**
	 * Add all users to an insight with permission level
	 * @param projectId
	 * @param insightId
	 * @param permission
	 * @return
	 */
	public void addAllInsightUsers(String projectId, String insightId, String permission) {
		int permissionLevel = AccessPermissionEnum.getIdByPermission(permission);
		String inertQ = "INSERT INTO USERINSIGHTPERMISSION (USERID, PROJECTID, INSIGHTID, PERMISSION) VALUES(?,?,?,?)";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(inertQ);
			if (projectId != null && permission != null) {
				List<Map<String, Object>> users = getInsightUsersNoCredentials(projectId, insightId);
				for (Map<String, Object> userMap : users) {
					String userId = (String) userMap.get("id");
					int parameterIndex = 1;
					ps.setString(parameterIndex++, userId);
					ps.setString(parameterIndex++, projectId);
					ps.setString(parameterIndex++, insightId);
					ps.setInt(parameterIndex++, permissionLevel);
					ps.addBatch();
				}
				ps.executeBatch();
				if(!ps.getConnection().getAutoCommit()) {
					ps.getConnection().commit();
				}
				// update existing permissions for users
				updateInsightUserPermissions(projectId, insightId, permission);
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred adding all users for this insight");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}
	
	/**
	 * 
	 * @param existingUserId
	 * @param projectId
	 * @param insightId
	 * @param newPermission
	 * @return
	 */
	public void editInsightUserPermission(String existingUserId, String projectId, String insightId, String newPermission) {
		// make sure we are trying to edit a permission that exists
		Integer existingUserPermission = SecurityInsightUtils.getUserInsightPermission(existingUserId, projectId, insightId);
		if(existingUserPermission == null) {
			throw new IllegalArgumentException("Attempting to modify user permission for a user who does not currently have access to the insight");
		}
		
		// update user permissions in bulk
		String updateQ = "UPDATE USERINSIGHTPERMISSION SET PERMISSION = ? WHERE USERID = ? AND PROJECTID = ? AND INSIGHTID = ?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(updateQ);
			int parameterIndex = 1;
			//SET
			ps.setInt(parameterIndex++, AccessPermissionEnum.getIdByPermission(newPermission));
			//WHERE
			ps.setString(parameterIndex++, existingUserId);
			ps.setString(parameterIndex++, projectId);
			ps.setString(parameterIndex++, insightId);
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred adding user permissions for this insight");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}
	
	/**
	 * 
	 * @param editedUserId
	 * @param projectId
	 * @param insightId
	 * @return
	 */
	public void removeInsightUser(String existingUserId, String projectId, String insightId) {
		// make sure we are trying to edit a permission that exists
		Integer existingUserPermission = SecurityInsightUtils.getUserInsightPermission(existingUserId, projectId, insightId);
		if(existingUserPermission == null) {
			throw new IllegalArgumentException("Attempting to modify user permission for a user who does not currently have access to the insight");
		}
		
		// update user permissions in bulk
		String deleteQ = "DELETE FROM USERINSIGHTPERMISSION WHERE USERID = ? AND PROJECTID = ? AND INSIGHTID = ?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(deleteQ);
			int parameterIndex = 1;
			//WHERE
			ps.setString(parameterIndex++, existingUserId);
			ps.setString(parameterIndex++, projectId);
			ps.setString(parameterIndex++, insightId);
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred deleting user permissions for this insight");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}
	
	/**
	 * 	
	 * @param projectId
	 * @param isPublic
	 */
	public void setInsightGlobalWithinProject(String projectId, String insightId, boolean isPublic) {
		// update user permissions in bulk
		String updateQ = "UPDATE INSIGHT SET GLOBAL=? WHERE PROJECTID=? AND INSIGHTID=?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(updateQ);
			int parameterIndex = 1;
			// SET
			ps.setBoolean(parameterIndex++, isPublic);
			// WHERE
			ps.setString(parameterIndex++, projectId);
			ps.setString(parameterIndex++, insightId);
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred setting this insight global");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}
	
	/**
	 * Returns List of users that have no access credentials to a given engine
	 * @param engineId
	 * @return
	 */
	public List<Map<String, Object>> getEngineUsersNoCredentials(String engineId) {
		/*
		 * String Query = 
		 * "SELECT USER.ID, USER.USERNAME, USER.NAME, USER.EMAIL  FROM USER WHERE ID NOT IN 
		 * (SELECT e.USERID FROM ENGINEPERMISSION e WHERE e.ENGINEID = '"+ databaseId + "' e.PERMISSION IS NOT NULL);"
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
			subQs.addSelector(new QueryColumnSelector("ENGINEPERMISSION__USERID"));
			subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__ENGINEID","==",engineId));
			subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__PERMISSION", "!=", null, PixelDataType.NULL_VALUE));
		}
		
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}
	
	
	/**
	 * Returns List of users that have no access credentials to a given project
	 * @param projectId
	 * @return 
	 */
	public List<Map<String, Object>> getProjectUsersNoCredentials(String projectId) {
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
			subQs.addSelector(new QueryColumnSelector("PROJECTPERMISSION__USERID"));
			subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__PROJECTID","==",projectId));
			subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__PERMISSION", "!=", null, PixelDataType.NULL_VALUE));
		}
		
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}
	
	
	/**
	 * Returns List of users that have no access credentials to a given insight 
	 * @param insightID
	 * @param projectId
	 * @return 
	 */
	public List<Map<String, Object>> getInsightUsersNoCredentials(String projectId, String insightId) {
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
			subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("USERINSIGHTPERMISSION__PERMISSION","!=", null, PixelDataType.NULL_VALUE));
		}
		
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}

	/**
	 * 
	 * @param engineId
	 * @param newPermission
	 */
	public void updateEngineUserPermissions(String engineId, String newPermission) {
		String query = "UPDATE ENGINEPERMISSION SET PERMISSION=? WHERE ENGINEID=?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			int parameterIndex = 1;
			// SET
			ps.setInt(parameterIndex++, AccessPermissionEnum.getIdByPermission(newPermission));
			// WHERE
			ps.setString(parameterIndex++, engineId);
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred editing user permissions for this engine");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}
	
	/**
	 * 
	 * @param projectId
	 * @param newPermission
	 */
	public void updateProjectUserPermissions(String projectId, String newPermission) {
		String query = "UPDATE PROJECTPERMISSION SET PERMISSION=? WHERE PROJECTID=?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			int parameterIndex = 1;
			// SET
			ps.setInt(parameterIndex++, AccessPermissionEnum.getIdByPermission(newPermission));
			// WHERE
			ps.setString(parameterIndex++, projectId);
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred editing user permissions for this project");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * Add all users to an database with the same permission
	 * @param engineId
	 * @param permission
	 */
	public void addAllEngineUsers(String engineId, String permission) {
		String query = "INSERT INTO ENGINEPERMISSION (USERID, ENGINEID, PERMISSION, VISIBILITY) VALUES(?,?,?,?)";
		int permissionLevel = AccessPermissionEnum.getIdByPermission(permission);
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			if (engineId != null && permission != null) {
				List<Map<String, Object>> users = getEngineUsersNoCredentials(engineId);
				for (Map<String, Object> userMap : users) {
					String userId = (String) userMap.get("id");
					int parameterIndex = 1;
					ps.setString(parameterIndex++, userId);
					ps.setString(parameterIndex++, engineId);
					ps.setInt(parameterIndex++, permissionLevel);
					ps.setBoolean(parameterIndex++, true);
					ps.addBatch();
				}
				ps.executeBatch();
				if(!ps.getConnection().getAutoCommit()) {
					ps.getConnection().commit();
				}
				// update existing user permissions
				updateEngineUserPermissions(engineId, permission);
			}
		} catch (SQLException e1) {
			classLogger.error(Constants.STACKTRACE, e1);
			throw new IllegalArgumentException("An error occurred adding all users to this engine");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}
	
	
	/**
	 * Add all users to an project with the same permission
	 * @param projectId
	 * @param permission
	 */
	public void addAllProjectUsers(String projectId, String permission) {
		String query = "INSERT INTO PROJECTPERMISSION (USERID, PROJECTID, PERMISSION, VISIBILITY) VALUES(?,?,?,?)";
		int permissionLevel = AccessPermissionEnum.getIdByPermission(permission);
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			if (projectId != null && permission != null) {
				List<Map<String, Object>> users = getProjectUsersNoCredentials(projectId);
				for (Map<String, Object> userMap : users) {
					String userId = (String) userMap.get("id");
					int parameterIndex = 1;
					ps.setString(parameterIndex++, userId);
					ps.setString(parameterIndex++, projectId);
					ps.setInt(parameterIndex++, permissionLevel);
					ps.setBoolean(parameterIndex++, true);
					ps.addBatch();
				}
				ps.executeBatch();
				if(!ps.getConnection().getAutoCommit()) {
					ps.getConnection().commit();
				}
				// update existing user permissions
				updateProjectUserPermissions(projectId, permission);
			}
		} catch (SQLException e1) {
			classLogger.error(Constants.STACKTRACE, e1);
			throw new IllegalArgumentException("An error occurred adding user permissions for this project");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}
	
	/**
	 * 
	 * @param projectId
	 * @param insightId
	 * @param newPermission
	 */
	public void updateInsightUserPermissions(String projectId, String insightId, String newPermission) {
		String updateQ = "UPDATE USERINSIGHTPERMISSION SET PERMISSION=? WHERE PROJECTID=? AND INSIGHTID=?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(updateQ);
			int parameterIndex = 1;
			// SET
			ps.setInt(parameterIndex++, AccessPermissionEnum.getIdByPermission(newPermission));
			// WHERE
			ps.setString(parameterIndex++, projectId);
			ps.setString(parameterIndex++, insightId);
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred updating the permissions for this insight");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}
	
	/**
	 * 
	 * @param projectId
	 * @return
	 */
	private List<String> getAllProjectInsights(String projectId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("INSIGHT__INSIGHTID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHT__PROJECTID", "==", projectId));
		return QueryExecutionUtility.flushToListString(securityDb, qs);
	}

	/**
	 * 
	 * @param projectId
	 * @param insightId
	 * @param permission
	 */
	public void grantNewUsersInsightAccess(String projectId, String insightId, String permission) {
		List<Map<String, Object>> users = getInsightUsersNoCredentials(projectId, insightId);
		String insertQuery = "INSERT INTO USERINSIGHTPERMISSION (USERID, PROJECTID, INSIGHTID, PERMISSION) VALUES(?,?,?,?)";
		int permissionLevel = AccessPermissionEnum.getIdByPermission(permission);
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(insertQuery);
			for (Map<String, Object> userMap : users) {
				String userId = (String) userMap.get("id");
				int parameterIndex = 1;
				ps.setString(parameterIndex++, userId);
				ps.setString(parameterIndex++, projectId);
				ps.setString(parameterIndex++, insightId);
				ps.setInt(parameterIndex++, permissionLevel);
				ps.addBatch();
			}
			ps.executeBatch();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred granting the user permission for all the projects");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}
	
	/**
	 * Lock accounts
	 * @param numDaysSinceLastLogin
	 */
	public int lockAccounts(int numDaysSinceLastLogin) {
		int numUpdated = 0;
		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone(Utility.getApplicationTimeZoneId()));
		
		LocalDateTime dateToFilter = LocalDateTime.now();
		dateToFilter = dateToFilter.minusDays(numDaysSinceLastLogin);
		
		String query = "UPDATE SMSS_USER SET LOCKED=? WHERE LASTLOGIN<=?";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			int parameterIndex = 1;
			ps.setBoolean(parameterIndex++, true);
			ps.setTimestamp(parameterIndex++, java.sql.Timestamp.valueOf(dateToFilter), cal);
			numUpdated = ps.executeUpdate();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred granting the user permission for all the projects");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
		
		classLogger.info("Number of accounts locked = " + numUpdated);
		return numUpdated;
	}
	
	/**
	 * Lock accounts
	 * @param numDaysSinceLastLogin
	 */
	public List<Object[]> getUserEmailsGettingLocked() {
		// if we never lock - nothing to worry about
		int daysToLock = -1;
		int daysToLockEmail = 14;
		try {
			PasswordRequirements passReqInstance = PasswordRequirements.getInstance();
			daysToLock = passReqInstance.getDaysToLock();
			daysToLockEmail = passReqInstance.getDaysToLockEmail();
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		if(daysToLock < 0) {
			return new ArrayList<>();
		}
		int daysSinceLastLoginToSendEmail = (daysToLock - daysToLockEmail);
		if(daysSinceLastLoginToSendEmail < 0) {
			classLogger.warn("Days to Lock is less than the Days To Lock Email Warning. Would result in constant emails. Returning empty set until configured properly");
			return new ArrayList<>();
		}
		
		LocalDateTime now = LocalDateTime.now();
		
		List<Object[]> emailsToSend = new ArrayList<>();
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__EMAIL"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__LASTLOGIN"));
		List<Boolean> values = new ArrayList<>();
		values.add(null);
		values.add(false);
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__LOCKED", "==", values, PixelDataType.BOOLEAN));
		
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			while(wrapper.hasNext()) {
				Object[] row = wrapper.next().getValues();
				String email = (String) row[0];
				if(email != null) {
					SemossDate lastLogin = null;
					if(row[1] != null) {
						Object potentialDateValue = row[1];
						if(potentialDateValue instanceof SemossDate) {
							lastLogin = (SemossDate) potentialDateValue;
						} else if(potentialDateValue instanceof String) {
							lastLogin = SemossDate.genTimeStampDateObj(potentialDateValue + "");
						}
					}
					
					long daysSinceLastLogin = Duration.between(lastLogin.getLocalDateTime(), now).toDays();
					if(daysSinceLastLogin >= daysSinceLastLoginToSendEmail) {
						emailsToSend.add(new Object[] {email, daysSinceLastLogin});
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
		
		/*
		 * Sadly, the below does work with sqlite since it is a dumb db 
		 * and doesn't store dates properly as one would expect
		 */
		
//		AbstractSqlQueryUtil queryUtil = securityDb.getQueryUtil();
//		String dateDiff = queryUtil.getDateDiffFunctionSyntax("day", "SMSS_USER.LASTLOGIN", queryUtil.getCurrentTimestamp());
//
//		String query = "SELECT DISTINCT SMSS_USER.EMAIL, (" + dateDiff + ") as DAYS_SINCE_LASTLOGIN FROM SMSS_USER WHERE "
//				+ "(LOCKED IS NULL OR LOCKED='false') AND (" + dateDiff + ") > " + (daysToLock - daysToLockEmail); 
//		
//		IRawSelectWrapper wrapper = null;
//		try {
//			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
//			while(wrapper.hasNext()) {
//				Object[] row = wrapper.next().getValues();
//				if(row[0] != null) {
//					emailsToSend.add(row);
//				}
//			}
//		} catch (Exception e) {
//			logger.error(Constants.STACKTRACE, e);
//		} finally {
//			if(wrapper != null) {
//				wrapper.cleanUp();
//			}
//		}
		
		return emailsToSend;
	}
	
	/**
	 * Lock accounts
	 * @param numDaysSinceLastLogin
	 */
	public int setLockAccountsAndRecalculate(int numDaysSinceLastLogin) {
		int numUpdated = 0;
		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone(Utility.getApplicationTimeZoneId()));
		
		LocalDateTime dateToFilter = LocalDateTime.now();
		dateToFilter = dateToFilter.minusDays(numDaysSinceLastLogin);
		java.sql.Timestamp sqlTimestamp = java.sql.Timestamp.valueOf(dateToFilter);
		
		String[] queries = new String[] {
				"UPDATE SMSS_USER SET LOCKED=? WHERE LASTLOGIN<=?",
				"UPDATE SMSS_USER SET LOCKED=? WHERE LASTLOGIN>?"
		};
		boolean [] queryUpdateBool = new boolean[] {true, false};
		
		for(int i = 0; i < queries.length; i++) {
			String query = queries[i];
			boolean updateBool = queryUpdateBool[i];
			PreparedStatement ps = null;
			try {
				ps = securityDb.getPreparedStatement(query);
				int parameterIndex = 1;
				ps.setBoolean(parameterIndex++, updateBool);
				ps.setTimestamp(parameterIndex++, sqlTimestamp, cal);
				numUpdated = ps.executeUpdate();
				if(!ps.getConnection().getAutoCommit()) {
					ps.getConnection().commit();
				}
			} catch (SQLException e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("An error occurred granting the user permission for all the projects");
			} finally {
				ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
			}
		}
		
		classLogger.info("Number of accounts locked = " + numUpdated);
		return numUpdated;
	}
	
	/**
	 * Return the number of admins
	 * @return
	 */
	public int getNumAdmins() {
		SelectQueryStruct qs = new SelectQueryStruct();
		QueryFunctionSelector fun = new QueryFunctionSelector();
		fun.setFunction(QueryFunctionHelper.COUNT);
		fun.addInnerSelector(new QueryColumnSelector("SMSS_USER__ID"));
		qs.addSelector(fun);
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__ADMIN", "==", true, PixelDataType.BOOLEAN));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if(wrapper.hasNext()) {
				return ((Number) wrapper.next().getValues()[0]).intValue();
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
		
		return 0;
	}
	
	/**
	 * Approving user access requests and giving user access in permissions
	 * @param userId
	 * @param userType
	 * @param engineId
	 * @param requests
	 */
	public static void approveEngineUserAccessRequests(String userId, String userType, String engineId, List<Map<String, Object>> requests) {
		// bulk delete
		String deleteQ = "DELETE FROM ENGINEPERMISSION WHERE USERID=? AND ENGINEID=?";
		PreparedStatement deletePs = null;
		try {
			deletePs = securityDb.getPreparedStatement(deleteQ);
			for(int i=0; i<requests.size(); i++) {
				int parameterIndex = 1;
				deletePs.setString(parameterIndex++, (String) requests.get(i).get("userid"));
				deletePs.setString(parameterIndex++, engineId);
				deletePs.addBatch();
			}
			deletePs.executeBatch();
			if(!deletePs.getConnection().getAutoCommit()) {
				deletePs.getConnection().commit();
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred while deleting enginepermission with detailed message = " + e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, deletePs);
		}
		// insert new user permissions in bulk
		String insertQ = "INSERT INTO ENGINEPERMISSION (USERID, ENGINEID, PERMISSION, VISIBILITY) VALUES(?,?,?,?)";
		PreparedStatement insertPs = null;
		try {
			insertPs = securityDb.getPreparedStatement(insertQ);
			for(int i=0; i<requests.size(); i++) {
				int parameterIndex = 1;
				insertPs.setString(parameterIndex++, (String) requests.get(i).get("userid"));
				insertPs.setString(parameterIndex++, engineId);
				insertPs.setInt(parameterIndex++, AccessPermissionEnum.getIdByPermission((String) requests.get(i).get("permission")));
				insertPs.setBoolean(parameterIndex++, true);
				insertPs.addBatch();
			}
			insertPs.executeBatch();
			if(!insertPs.getConnection().getAutoCommit()) {
				insertPs.getConnection().commit();
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred editing user permissions for this engine");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, insertPs);
		}

		// now we do the new bulk update to engineaccessrequest table
		String updateQ = "UPDATE ENGINEACCESSREQUEST SET PERMISSION = ?, APPROVER_USERID = ?, APPROVER_TYPE = ?, APPROVER_DECISION = ?, APPROVER_TIMESTAMP = ? WHERE ID = ? AND ENGINEID = ?";
		PreparedStatement updatePs = null;
		try {
			Calendar cal = Calendar.getInstance(TimeZone.getTimeZone(Utility.getApplicationTimeZoneId()));
			java.sql.Timestamp timestamp = java.sql.Timestamp.valueOf(LocalDateTime.now());
			updatePs = securityDb.getPreparedStatement(updateQ);
			for(int i=0; i<requests.size(); i++) {
				int index = 1;
				//set
				updatePs.setInt(index++, AccessPermissionEnum.getIdByPermission((String) requests.get(i).get("permission")));
				updatePs.setString(index++, userId);
				updatePs.setString(index++, userType);
				updatePs.setString(index++, "APPROVED");
				updatePs.setTimestamp(index++, timestamp, cal);
				//where
				updatePs.setString(index++, (String) requests.get(i).get("requestid"));
				updatePs.setString(index++, engineId);
				updatePs.addBatch();
			}
			updatePs.executeBatch();
			if(!updatePs.getConnection().getAutoCommit()) {
				updatePs.getConnection().commit();
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred while updating user access request detailed message = " + e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, updatePs);
		}
	}
	
	/**
	 * Denying user access requests to engine
	 * @param userId
	 * @param userType
	 * @param engineId
	 * @param requests
	 */
	public static void denyEngineUserAccessRequests(String userId, String userType, String engineId, List<String> requestIds) {
		// bulk update to databaseaccessrequest table
		String updateQ = "UPDATE ENGINEACCESSREQUEST SET APPROVER_USERID = ?, APPROVER_TYPE = ?, APPROVER_DECISION = ?, APPROVER_TIMESTAMP = ? WHERE ID = ? AND ENGINEID = ?";
		PreparedStatement ps = null;
		try {
			Calendar cal = Calendar.getInstance(TimeZone.getTimeZone(Utility.getApplicationTimeZoneId()));
			java.sql.Timestamp timestamp = java.sql.Timestamp.valueOf(LocalDateTime.now());
			ps = securityDb.getPreparedStatement(updateQ);
			for(int i = 0; i < requestIds.size(); i++) {
				int index = 1;
				//set
				ps.setString(index++, userId);
				ps.setString(index++, userType);
				ps.setString(index++, "DENIED");
				ps.setTimestamp(index++, timestamp, cal);
				//where
				ps.setString(index++, requestIds.get(i));
				ps.setString(index++, engineId);
				ps.addBatch();
			}
			ps.executeBatch();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred while updating user access request detailed message = " + e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}
	
	/**
	 * Approving user access requests and giving user access in permissions
	 * @param userId
	 * @param userType
	 * @param projectId
	 * @param requests
	 */
	public static void approveProjectUserAccessRequests(String userId, String userType, String projectId, List<Map<String, Object>> requests) {
		// bulk delete
		String deleteQ = "DELETE FROM PROJECTPERMISSION WHERE USERID=? AND PROJECTID=?";
		PreparedStatement deletePs = null;
		try {
			deletePs = securityDb.getPreparedStatement(deleteQ);
			for(int i=0; i<requests.size(); i++) {
				int parameterIndex = 1;
				deletePs.setString(parameterIndex++, (String) requests.get(i).get("userid"));
				deletePs.setString(parameterIndex++, projectId);
				deletePs.addBatch();
			}
			deletePs.executeBatch();
			if(!deletePs.getConnection().getAutoCommit()) {
				deletePs.getConnection().commit();
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred while deleting projectpermission with detailed message = " + e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, deletePs);
		}
		// insert new user permissions in bulk
		String insertQ = "INSERT INTO PROJECTPERMISSION (USERID, PROJECTID, PERMISSION, VISIBILITY) VALUES(?,?,?,?)";
		PreparedStatement insertPs = null;
		try {
			insertPs = securityDb.getPreparedStatement(insertQ);
			for(int i=0; i<requests.size(); i++) {
				int parameterIndex = 1;
				insertPs.setString(parameterIndex++, (String) requests.get(i).get("userid"));
				insertPs.setString(parameterIndex++, projectId);
				insertPs.setInt(parameterIndex++, AccessPermissionEnum.getIdByPermission((String) requests.get(i).get("permission")));
				insertPs.setBoolean(parameterIndex++, true);
				insertPs.addBatch();
			}
			insertPs.executeBatch();
			if(!insertPs.getConnection().getAutoCommit()) {
				insertPs.getConnection().commit();
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred inserting user project permissions on request approval");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, insertPs);
		}

		// now we do the new bulk update to projectaccessrequest table
		String updateQ = "UPDATE PROJECTACCESSREQUEST SET PERMISSION = ?, APPROVER_USERID = ?, APPROVER_TYPE = ?, APPROVER_DECISION = ?, APPROVER_TIMESTAMP = ? WHERE ID = ?";
		PreparedStatement updatePs = null;
		try {
			Calendar cal = Calendar.getInstance(TimeZone.getTimeZone(Utility.getApplicationTimeZoneId()));
			java.sql.Timestamp timestamp = java.sql.Timestamp.valueOf(LocalDateTime.now());
			updatePs = securityDb.getPreparedStatement(updateQ);
			for(int i=0; i<requests.size(); i++) {
				int index = 1;
				//set
				updatePs.setInt(index++, AccessPermissionEnum.getIdByPermission((String) requests.get(i).get("permission")));
				updatePs.setString(index++, userId);
				updatePs.setString(index++, userType);
				updatePs.setString(index++, "APPROVED");
				updatePs.setTimestamp(index++, timestamp, cal);
				//where
				updatePs.setString(index++, (String) requests.get(i).get("requestid"));
				updatePs.addBatch();
			}
			updatePs.executeBatch();
			if(!updatePs.getConnection().getAutoCommit()) {
				updatePs.getConnection().commit();
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred while updating user access request detailed message = " + e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, updatePs);
		}
	}
	
	/**
	 * Denying user access requests to project
	 * @param userId
	 * @param userType
	 * @param projectId
	 * @param requests
	 */
	public static void denyProjectUserAccessRequests(String userId, String userType, String projectId, List<String> RequestIdList) {
		// bulk update to projectaccessrequest table
		String updateQ = "UPDATE PROJECTACCESSREQUEST SET APPROVER_USERID = ?, APPROVER_TYPE = ?, APPROVER_DECISION = ?, APPROVER_TIMESTAMP = ? WHERE ID = ?";
		PreparedStatement ps = null;
		try {
			Calendar cal = Calendar.getInstance(TimeZone.getTimeZone(Utility.getApplicationTimeZoneId()));
			java.sql.Timestamp timestamp = java.sql.Timestamp.valueOf(LocalDateTime.now());
			ps = securityDb.getPreparedStatement(updateQ);
			for(int i=0; i<RequestIdList.size(); i++) {
				int index = 1;
				//set
				ps.setString(index++, userId);
				ps.setString(index++, userType);
				ps.setString(index++, "DENIED");
				ps.setTimestamp(index++, timestamp, cal);
				//where
				ps.setString(index++, RequestIdList.get(i));
				ps.addBatch();
			}
			ps.executeBatch();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred while updating user access request detailed message = " + e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}
	
	/**
	 * Approving user access requests and giving user access in permissions
	 * @param userId
	 * @param userType
	 * @param projectId
	 * @param insightId
	 * @param requests
	 */
	public static void approveInsightUserAccessRequests(String userId, String userType, String projectId, String insightId, List<Map<String, Object>> requests) {
		// bulk delete
		String deleteQ = "DELETE FROM USERINSIGHTPERMISSION WHERE USERID=? AND PROJECTID=? AND INSIGHTID=?";
		PreparedStatement deletePs = null;
		try {
			deletePs = securityDb.getPreparedStatement(deleteQ);
			for(int i=0; i<requests.size(); i++) {
				int parameterIndex = 1;
				deletePs.setString(parameterIndex++, (String) requests.get(i).get("userid"));
				deletePs.setString(parameterIndex++, projectId);
				deletePs.setString(parameterIndex++, insightId);
				deletePs.addBatch();
			}
			deletePs.executeBatch();
			if(!deletePs.getConnection().getAutoCommit()) {
				deletePs.getConnection().commit();
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred while deleting projectpermission with detailed message = " + e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, deletePs);
		}
		// insert new user permissions in bulk
		String insertQ = "INSERT INTO USERINSIGHTPERMISSION (USERID, PROJECTID, INSIGHTID, PERMISSION) VALUES(?,?,?,?)";
		PreparedStatement insertPs = null;
		try {
			insertPs = securityDb.getPreparedStatement(insertQ);
			for(int i=0; i<requests.size(); i++) {
				int parameterIndex = 1;
				insertPs.setString(parameterIndex++, (String) requests.get(i).get("userid"));
				insertPs.setString(parameterIndex++, projectId);
				insertPs.setString(parameterIndex++, insightId);
				insertPs.setInt(parameterIndex++, AccessPermissionEnum.getIdByPermission((String) requests.get(i).get("permission")));
				insertPs.addBatch();
			}
			insertPs.executeBatch();
			if(!insertPs.getConnection().getAutoCommit()) {
				insertPs.getConnection().commit();
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred inserting user insight permissions on request approval");
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, insertPs);
		}

		// now we do the new bulk update to accessrequest table
		String updateQ = "UPDATE INSIGHTACCESSREQUEST SET PERMISSION = ?, APPROVER_USERID = ?, APPROVER_TYPE = ?, APPROVER_DECISION = ?, APPROVER_TIMESTAMP = ? WHERE ID = ?";
		PreparedStatement updatePs = null;
		try {
			Calendar cal = Calendar.getInstance(TimeZone.getTimeZone(Utility.getApplicationTimeZoneId()));
			java.sql.Timestamp timestamp = java.sql.Timestamp.valueOf(LocalDateTime.now());
			updatePs = securityDb.getPreparedStatement(updateQ);
			for(int i=0; i<requests.size(); i++) {
				int index = 1;
				//set
				updatePs.setInt(index++, AccessPermissionEnum.getIdByPermission((String) requests.get(i).get("permission")));
				updatePs.setString(index++, userId);
				updatePs.setString(index++, userType);
				updatePs.setString(index++, "APPROVED");
				updatePs.setTimestamp(index++, timestamp, cal);
				//where
				updatePs.setString(index++, (String) requests.get(i).get("requestid"));
				updatePs.addBatch();
			}
			updatePs.executeBatch();
			if(!updatePs.getConnection().getAutoCommit()) {
				updatePs.getConnection().commit();
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred while updating user access request detailed message = " + e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, updatePs);
		}
	}
	
	/**
	 * Denying user access requests
	 * @param userId
	 * @param userType
	 * @param projectId
	 * @param insightId
	 * @param requests
	 */
	public static void denyInsightUserAccessRequests(String userId, String userType, String projectId, String insightId, List<String> RequestIdList) {
		// bulk update to accessrequest table
		String updateQ = "UPDATE INSIGHTACCESSREQUEST SET APPROVER_USERID = ?, APPROVER_TYPE = ?, APPROVER_DECISION = ?, APPROVER_TIMESTAMP = ? WHERE ID = ?";
		PreparedStatement ps = null;
		try {
			Calendar cal = Calendar.getInstance(TimeZone.getTimeZone(Utility.getApplicationTimeZoneId()));
			java.sql.Timestamp timestamp = java.sql.Timestamp.valueOf(LocalDateTime.now());
			ps = securityDb.getPreparedStatement(updateQ);
			for(int i=0; i<RequestIdList.size(); i++) {
				int index = 1;
				//set
				ps.setString(index++, userId);
				ps.setString(index++, userType);
				ps.setString(index++, "DENIED");
				ps.setTimestamp(index++, timestamp, cal);
				//where
				ps.setString(index++, RequestIdList.get(i));
				ps.addBatch();
			}
			ps.executeBatch();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("An error occurred while updating user access request detailed message = " + e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

}
