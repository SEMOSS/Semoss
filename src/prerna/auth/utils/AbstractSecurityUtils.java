package prerna.auth.utils;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import jodd.util.BCrypt;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.ds.util.RdbmsQueryBuilder;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.OwlSeparatePixelFromConceptual;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.PixelDataType;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

public abstract class AbstractSecurityUtils {

	static RDBMSNativeEngine securityDb;
	static boolean securityEnabled = false;
	static boolean adminSetPublisher = false;
	static String ADMIN_ADDED_USER = "ADMIN_ADDED_USER";
	static boolean anonymousUsersEnabled = false;
	static boolean anonymousUsersUploadData = false;

	static Gson securityGson = new GsonBuilder().disableHtmlEscaping().create();
	
	/**
	 * Only used for static references
	 */
	AbstractSecurityUtils() {
		
	}
	
	public static void loadSecurityDatabase() throws Exception {
		securityDb = (RDBMSNativeEngine) Utility.getEngine(Constants.SECURITY_DB);
		SecurityOwlCreator owlCreator = new SecurityOwlCreator(securityDb);
		if(owlCreator.needsRemake()) {
			owlCreator.remakeOwl();
		}
		// Update OWL
		OwlSeparatePixelFromConceptual.fixOwl(securityDb.getProp());
		
		initialize();
		
		Object security = DIHelper.getInstance().getLocalProp(Constants.SECURITY_ENABLED);
		if(security == null) {
			securityEnabled = false;
		} else {
			securityEnabled = (security instanceof Boolean && ((boolean) security) ) || (Boolean.parseBoolean(security.toString()));
		}
		
		Object anonymousUsers = DIHelper.getInstance().getLocalProp(Constants.ANONYMOUS_USER_ALLOWED);
		if(anonymousUsers == null) {
			anonymousUsersEnabled = false;
		} else {
			anonymousUsersEnabled = (anonymousUsers instanceof Boolean && ((boolean) anonymousUsers) ) || (Boolean.parseBoolean(anonymousUsers.toString()));
		}
		
		Object anonymousUsersData = DIHelper.getInstance().getLocalProp(Constants.ANONYMOUS_USER_UPLOAD_DATA);
		if(anonymousUsersData == null) {
			anonymousUsersUploadData = false;
		} else {
			anonymousUsersUploadData = (anonymousUsersData instanceof Boolean && ((boolean) anonymousUsersData) ) || (Boolean.parseBoolean(anonymousUsersData.toString()));
		}
		
		Object adminSetsPublisher = DIHelper.getInstance().getLocalProp(Constants.ADMIN_SET_PUBLISHER);
		if(adminSetsPublisher == null) {
			adminSetPublisher = false;
		} else {
			adminSetPublisher = (adminSetsPublisher instanceof Boolean && ((boolean) adminSetsPublisher) ) || (Boolean.parseBoolean(adminSetsPublisher.toString()));
		}
	}

	public static boolean securityEnabled() {
		return securityEnabled;
	}
	
	public static boolean anonymousUsersEnabled() {
		return securityEnabled && anonymousUsersEnabled;
	}
	
	public static boolean anonymousUserUploadData() {
		return anonymousUsersEnabled() && anonymousUsersUploadData;
	}
	
	public static boolean adminSetPublisher() {
		return securityEnabled && adminSetPublisher;
	}
	
	public static void initialize() throws Exception {
		String schema = securityDb.getSchema();
		Connection conn = securityDb.getConnection();
		String[] colNames = null;
		String[] types = null;
		Object[] defaultValues = null;
		/*
		 * Currently used
		 */
		
		AbstractSqlQueryUtil queryUtil = securityDb.getQueryUtil();
		boolean allowIfExistsTable = queryUtil.allowsIfExistsTableSyntax();
		boolean allowIfExistsIndexs = queryUtil.allowIfExistsIndexSyntax();
		final String CLOB_DATATYPE_NAME = queryUtil.getClobDataTypeName();
		final String BOOLEAN_DATATYPE_NAME = queryUtil.getBooleanDataTypeName();
		
		// 2021-08-06
		// on h2 when you renmae a column it doens't update/change anything on the index name
		// also had some invalid indexes on certain tables
		if(allowIfExistsIndexs) {
			securityDb.removeData(queryUtil.dropIndexIfExists("INSIGHT_ENGINEID_INDEX", "INSIGHT"));
			securityDb.removeData(queryUtil.dropIndexIfExists("INSIGHTMETA_ENGINEID_INDEX", "INSIGHT"));
			securityDb.removeData(queryUtil.dropIndexIfExists("INSIGHTMETA_ENGINEID_INDEX", "INSIGHTMETA"));
			securityDb.removeData(queryUtil.dropIndexIfExists("USERINSIGHTPERMISSION_ENGINEID_INDEX", "USERINSIGHTPERMISSION"));

			// these are right name - but were added to wrong table
			// so will do an exists check anyway
			if(queryUtil.indexExists(securityDb, "INSIGHTMETA_PROJECTID_INDEX", "INSIGHT", schema)) {
				securityDb.removeData(queryUtil.dropIndex("INSIGHTMETA_PROJECTID_INDEX", "INSIGHT"));
			}
			if(queryUtil.indexExists(securityDb, "INSIGHTMETA_INSIGHTID_INDEX", "INSIGHT", schema)) {
				securityDb.removeData(queryUtil.dropIndex("INSIGHTMETA_INSIGHTID_INDEX", "INSIGHT"));
			}
		} else {
			// see if index exists
			if(queryUtil.indexExists(securityDb, "INSIGHT_ENGINEID_INDEX", "INSIGHT", schema)) {
				securityDb.removeData(queryUtil.dropIndex("INSIGHT_ENGINEID_INDEX", "INSIGHT"));
			}
			if(queryUtil.indexExists(securityDb, "INSIGHTMETA_ENGINEID_INDEX", "INSIGHT", schema)) {
				securityDb.removeData(queryUtil.dropIndex("INSIGHTMETA_ENGINEID_INDEX", "INSIGHT"));
			}
			if(queryUtil.indexExists(securityDb, "INSIGHTMETA_ENGINEID_INDEX", "INSIGHTMETA", schema)) {
				securityDb.removeData(queryUtil.dropIndex("INSIGHTMETA_ENGINEID_INDEX", "INSIGHTMETA"));
			}
			if(queryUtil.indexExists(securityDb, "USERINSIGHTPERMISSION_ENGINEID_INDEX", "USERINSIGHTPERMISSION", schema)) {
				securityDb.removeData(queryUtil.dropIndex("USERINSIGHTPERMISSION_ENGINEID_INDEX", "USERINSIGHTPERMISSION"));
			}
			if(queryUtil.indexExists(securityDb, "INSIGHTMETA_PROJECTID_INDEX", "INSIGHT", schema)) {
				securityDb.removeData(queryUtil.dropIndex("INSIGHTMETA_PROJECTID_INDEX", "INSIGHT"));
			}
			if(queryUtil.indexExists(securityDb, "INSIGHTMETA_INSIGHTID_INDEX", "INSIGHT", schema)) {
				securityDb.removeData(queryUtil.dropIndex("INSIGHTMETA_INSIGHTID_INDEX", "INSIGHT"));
			}
		}
		
		// ENGINE
		colNames = new String[] { "ENGINENAME", "ENGINEID", "GLOBAL", "TYPE", "COST" };
		types = new String[] { "VARCHAR(255)", "VARCHAR(255)", BOOLEAN_DATATYPE_NAME, "VARCHAR(255)", "VARCHAR(255)" };
		if(allowIfExistsTable) {
			securityDb.insertData(queryUtil.createTableIfNotExists("ENGINE", colNames, types));
		} else {
			// see if table exists
			if(!queryUtil.tableExists(conn, "ENGINE", schema)) {
				// make the table
				securityDb.insertData(queryUtil.createTable("ENGINE", colNames, types));
			}
		}
		if(allowIfExistsIndexs) {
			securityDb.insertData(queryUtil.createIndexIfNotExists("ENGINE_GLOBAL_INDEX", "ENGINE", "GLOBAL"));
			securityDb.insertData(queryUtil.createIndexIfNotExists("ENGINE_ENGINENAME_INDEX", "ENGINE", "ENGINENAME"));
			securityDb.insertData(queryUtil.createIndexIfNotExists("ENGINE_ENGINEID_INDEX", "ENGINE", "ENGINEID"));
		} else {
			// see if index exists
			if(!queryUtil.indexExists(securityDb, "ENGINE_GLOBAL_INDEX", "ENGINE", schema)) {
				securityDb.insertData(queryUtil.createIndex("ENGINE_GLOBAL_INDEX", "ENGINE", "GLOBAL"));
			}
			if(!queryUtil.indexExists(securityDb, "ENGINE_ENGINENAME_INDEX", "ENGINE", schema)) {
				securityDb.insertData(queryUtil.createIndex("ENGINE_ENGINENAME_INDEX", "ENGINE", "ENGINENAME"));
			}
			if(!queryUtil.indexExists(securityDb, "ENGINE_ENGINEID_INDEX", "ENGINE", schema)) {
				securityDb.insertData(queryUtil.createIndex("ENGINE_ENGINEID_INDEX", "ENGINE", "ENGINEID"));
			}
		}
		
		// ENGINEMETA
		// check if column exists
		// TEMPORARY CHECK! - not sure when added but todays date is 12/16 
		{
			List<String> allCols = queryUtil.getTableColumns(conn, "ENGINEMETA", schema);
			// this should return in all upper case
			// ... but sometimes it is not -_- i.e. postgres always lowercases
			if(!allCols.contains("METAORDER") && !allCols.contains("metaorder")) {
				if(allowIfExistsTable) {
					securityDb.removeData(queryUtil.dropTableIfExists("ENGINEMETA"));
				} else if(queryUtil.tableExists(conn, "ENGINEMETA", schema)) {
					securityDb.removeData(queryUtil.dropTable("ENGINEMETA"));
				}
			}
		}
		colNames = new String[] { "ENGINEID", "METAKEY", "METAVALUE", "METAORDER" };
		types = new String[] { "VARCHAR(255)", "VARCHAR(255)", CLOB_DATATYPE_NAME, "INT" };
		if(allowIfExistsTable) {
			securityDb.insertData(queryUtil.createTableIfNotExists("ENGINEMETA", colNames, types));
		} else {
			// see if table exists
			if(!queryUtil.tableExists(conn, "ENGINEMETA", schema)) {
				// make the table
				securityDb.insertData(queryUtil.createTable("ENGINEMETA", colNames, types));
			}
		}
		if(allowIfExistsIndexs) {
			securityDb.insertData(queryUtil.createIndexIfNotExists("ENGINEMETA_ENGINEID_INDEX", "ENGINEMETA", "ENGINEID"));
		} else {
			// see if index exists
			if(!queryUtil.indexExists(securityDb, "ENGINEMETA_ENGINEID_INDEX", "ENGINEMETA", schema)) {
				securityDb.insertData(queryUtil.createIndex("ENGINEMETA_ENGINEID_INDEX", "ENGINEMETA", "ENGINEID"));
			}
		}
		
		// ENGINEPERMISSION
		colNames = new String[] { "USERID", "PERMISSION", "ENGINEID", "VISIBILITY", "FAVORITE" };
		types = new String[] { "VARCHAR(255)", "INT", "VARCHAR(255)", BOOLEAN_DATATYPE_NAME, BOOLEAN_DATATYPE_NAME };
		defaultValues = new Object[]{null, null, null, true, false};
		if(allowIfExistsTable) {
			securityDb.insertData(queryUtil.createTableIfNotExistsWithDefaults("ENGINEPERMISSION", colNames, types, defaultValues));
		} else {
			// see if table exists
			if(!queryUtil.tableExists(conn, "ENGINEPERMISSION", schema)) {
				// make the table
				securityDb.insertData(queryUtil.createTable("ENGINEPERMISSION", colNames, types));
			}
		}
		// TEMPORARY CHECK! - ADDED 03/17/2021
		{
			List<String> allCols = queryUtil.getTableColumns(conn, "ENGINEPERMISSION", schema);
			// this should return in all upper case
			// ... but sometimes it is not -_- i.e. postgres always lowercases
			if(!allCols.contains("FAVORITE") && !allCols.contains("favorite")) {
				if(queryUtil.allowIfExistsModifyColumnSyntax()) {
					securityDb.insertData(queryUtil.alterTableAddColumnIfNotExists("ENGINEPERMISSION", "FAVORITE", BOOLEAN_DATATYPE_NAME));
				} else {
					securityDb.insertData(queryUtil.alterTableAddColumn("ENGINEPERMISSION", "FAVORITE", BOOLEAN_DATATYPE_NAME));
				}
			}
		}
		if(allowIfExistsIndexs) {
			securityDb.insertData(queryUtil.createIndexIfNotExists("ENGINEPERMISSION_PERMISSION_INDEX", "ENGINEPERMISSION", "PERMISSION"));
			securityDb.insertData(queryUtil.createIndexIfNotExists("ENGINEPERMISSION_VISIBILITY_INDEX", "ENGINEPERMISSION", "VISIBILITY"));
			securityDb.insertData(queryUtil.createIndexIfNotExists("ENGINEPERMISSION_ENGINEID_INDEX", "ENGINEPERMISSION", "ENGINEID"));
			securityDb.insertData(queryUtil.createIndexIfNotExists("ENGINEPERMISSION_FAVORITE_INDEX", "ENGINEPERMISSION", "FAVORITE"));
			securityDb.insertData(queryUtil.createIndexIfNotExists("ENGINEPERMISSION_USERID_INDEX", "ENGINEPERMISSION", "USERID"));
		} else {
			// see if index exists
			if(!queryUtil.indexExists(securityDb, "ENGINEPERMISSION_PERMISSION_INDEX", "ENGINEPERMISSION", schema)) {
				securityDb.insertData(queryUtil.createIndex("ENGINEPERMISSION_PERMISSION_INDEX", "ENGINEPERMISSION", "PERMISSION"));
			}
			if(!queryUtil.indexExists(securityDb, "ENGINEPERMISSION_VISIBILITY_INDEX", "ENGINEPERMISSION", schema)) {
				securityDb.insertData(queryUtil.createIndex("ENGINEPERMISSION_VISIBILITY_INDEX", "ENGINEPERMISSION", "VISIBILITY"));
			}
			if(!queryUtil.indexExists(securityDb, "ENGINEPERMISSION_ENGINEID_INDEX", "ENGINEPERMISSION", schema)) {
				securityDb.insertData(queryUtil.createIndex("ENGINEPERMISSION_ENGINEID_INDEX", "ENGINEPERMISSION", "ENGINEID"));
			}
			if(!queryUtil.indexExists(securityDb, "ENGINEPERMISSION_FAVORITE_INDEX", "ENGINEPERMISSION", schema)) {
				securityDb.insertData(queryUtil.createIndex("ENGINEPERMISSION_FAVORITE_INDEX", "ENGINEPERMISSION", "FAVORITE"));
			}
			if(!queryUtil.indexExists(securityDb, "ENGINEPERMISSION_USERID_INDEX", "ENGINEPERMISSION", schema)) {
				securityDb.insertData(queryUtil.createIndex("ENGINEPERMISSION_USERID_INDEX", "ENGINEPERMISSION", "USERID"));
			}
		}

		
		/*
		 * 
		 * 
		 * ADDING IN INITIAL PROJECT TABLES
		 * 
		 */
		
		// PROJECT
		// Type and cost are the main questions - 
		boolean projectExists = queryUtil.tableExists(conn, "PROJECT", schema);
		colNames = new String[] { "PROJECTNAME", "PROJECTID", "GLOBAL", "TYPE", "COST" };
		types = new String[] { "VARCHAR(255)", "VARCHAR(255)", BOOLEAN_DATATYPE_NAME, "VARCHAR(255)", "VARCHAR(255)" };
		if(allowIfExistsTable) {
			securityDb.insertData(queryUtil.createTableIfNotExists("PROJECT", colNames, types));
		} else {
			// see if table exists
			if(!queryUtil.tableExists(conn, "PROJECT", schema)) {
				// make the table
				securityDb.insertData(queryUtil.createTable("PROJECT", colNames, types));
			}
		}
		if(allowIfExistsIndexs) {
			securityDb.insertData(queryUtil.createIndexIfNotExists("PROJECT_GLOBAL_INDEX", "PROJECT", "GLOBAL"));
			securityDb.insertData(queryUtil.createIndexIfNotExists("PROJECT_PROJECTENAME_INDEX", "PROJECT", "PROJECTNAME"));
			securityDb.insertData(queryUtil.createIndexIfNotExists("PROJECT_PROJECTID_INDEX", "PROJECT", "PROJECTID"));
		} else {
			// see if index exists
			if(!queryUtil.indexExists(securityDb, "PROJECT_GLOBAL_INDEX", "PROJECT", schema)) {
				securityDb.insertData(queryUtil.createIndex("PROJECT_GLOBAL_INDEX", "PROJECT", "GLOBAL"));
			}
			if(!queryUtil.indexExists(securityDb, "PROJECT_PROJECTENAME_INDEX", "PROJECT", schema)) {
				securityDb.insertData(queryUtil.createIndex("PROJECT_PROJECTENAME_INDEX", "PROJECT", "PROJECTNAME"));
			}
			if(!queryUtil.indexExists(securityDb, "PROJECT_PROJECTID_INDEX", "PROJECT", schema)) {
				securityDb.insertData(queryUtil.createIndex("PROJECT_PROJECTID_INDEX", "PROJECT", "PROJECTID"));
			}
		}
		
		List<String> newProjectsAutoAdded = new ArrayList<>();
		if(!projectExists) {
			IRawSelectWrapper wrapper2 = null;
			try {
				wrapper2 = WrapperManager.getInstance().getRawWrapper(securityDb, "select engineid, enginename, global from engine");
				while(wrapper2.hasNext()) {
					Object[] values = wrapper2.next().getValues();
					// insert into project table
					securityDb.insertData(queryUtil.insertIntoTable("PROJECT", colNames, types, new Object[]{values[1], values[0], values[2], null, null}));
					
					// store this so we also move over permissions
					// this is the engine id which is the same as the project id
					newProjectsAutoAdded.add(values[0] + "");
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				if(wrapper2 != null) {
					wrapper2.cleanUp();
				}
			}
		}
		
		// PROJECTMETA
		// check if column exists
		colNames = new String[] { "PROJECTID", "METAKEY", "METAVALUE", "METAORDER" };
		types = new String[] { "VARCHAR(255)", "VARCHAR(255)", CLOB_DATATYPE_NAME, "INT" };
		if(allowIfExistsTable) {
			securityDb.insertData(queryUtil.createTableIfNotExists("PROJECTMETA", colNames, types));
		} else {
			// see if table exists
			if(!queryUtil.tableExists(conn, "PROJECTMETA", schema)) {
				// make the table
				securityDb.insertData(queryUtil.createTable("PROJECTMETA", colNames, types));
			}
		}
		if(allowIfExistsIndexs) {
			securityDb.insertData(queryUtil.createIndexIfNotExists("PROJECTMETA_PROJECTID_INDEX", "PROJECTMETA", "PROJECTID"));
		} else {
			// see if index exists
			if(!queryUtil.indexExists(securityDb, "PROJECTMETA_PROJECTID_INDEX", "PROJECTMETA", schema)) {
				securityDb.insertData(queryUtil.createIndex("PROJECTMETA_PROJECTID_INDEX", "PROJECTMETA", "PROJECTID"));
			}
		}
		
		// PROJECTPERMISSION
		boolean projectPermissionExists = queryUtil.tableExists(conn, "PROJECTPERMISSION", schema);
		colNames = new String[] { "USERID", "PERMISSION", "PROJECTID", "VISIBILITY", "FAVORITE" };
		types = new String[] { "VARCHAR(255)", "INT", "VARCHAR(255)", BOOLEAN_DATATYPE_NAME, BOOLEAN_DATATYPE_NAME };
		defaultValues = new Object[]{null, null, null, true, false};
		if(allowIfExistsTable) {
			securityDb.insertData(queryUtil.createTableIfNotExistsWithDefaults("PROJECTPERMISSION", colNames, types, defaultValues));
		} else {
			// see if table exists
			if(!queryUtil.tableExists(conn, "PROJECTPERMISSION", schema)) {
				// make the table
				securityDb.insertData(queryUtil.createTable("PROJECTPERMISSION", colNames, types));
			}
		}
		
		if(!projectPermissionExists) {
			IRawSelectWrapper wrapper2 = null;
			try {
				wrapper2 = WrapperManager.getInstance().getRawWrapper(securityDb, "select userid, permission, engineid, visibility, favorite from enginepermission");
				while(wrapper2.hasNext()) {
					Object[] values = wrapper2.next().getValues();
					// if the project exists - we will insert it
					if(newProjectsAutoAdded.contains(values[2])) {
						// insert into project permission table
						securityDb.insertData(queryUtil.insertIntoTable("PROJECTPERMISSION", colNames, types, values));
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				if(wrapper2 != null) {
					wrapper2.cleanUp();
				}
			}
		}

		if(allowIfExistsIndexs) {
			securityDb.insertData(queryUtil.createIndexIfNotExists("PROJECTPERMISSION_PERMISSION_INDEX", "PROJECTPERMISSION", "PERMISSION"));
			securityDb.insertData(queryUtil.createIndexIfNotExists("PROJECTPERMISSION_VISIBILITY_INDEX", "PROJECTPERMISSION", "VISIBILITY"));
			securityDb.insertData(queryUtil.createIndexIfNotExists("PROJECTPERMISSION_PROJECTID_INDEX", "PROJECTPERMISSION", "PROJECTID"));
			securityDb.insertData(queryUtil.createIndexIfNotExists("PROJECTPERMISSION_FAVORITE_INDEX", "PROJECTPERMISSION", "FAVORITE"));
			securityDb.insertData(queryUtil.createIndexIfNotExists("PROJECTPERMISSION_USERID_INDEX", "PROJECTPERMISSION", "USERID"));
		} else {
			// see if index exists
			if(!queryUtil.indexExists(securityDb, "PROJECTPERMISSION_PERMISSION_INDEX", "PROJECTPERMISSION", schema)) {
				securityDb.insertData(queryUtil.createIndex("PROJECTPERMISSION_PERMISSION_INDEX", "PROJECTPERMISSION", "PERMISSION"));
			}
			if(!queryUtil.indexExists(securityDb, "PROJECTPERMISSION_VISIBILITY_INDEX", "PROJECTPERMISSION", schema)) {
				securityDb.insertData(queryUtil.createIndex("PROJECTPERMISSION_VISIBILITY_INDEX", "PROJECTPERMISSION", "VISIBILITY"));
			}
			if(!queryUtil.indexExists(securityDb, "PROJECTPERMISSION_PROJECTID_INDEX", "PROJECTPERMISSION", schema)) {
				securityDb.insertData(queryUtil.createIndex("PROJECTPERMISSION_PROJECTID_INDEX", "PROJECTPERMISSION", "PROJECTID"));
			}
			if(!queryUtil.indexExists(securityDb, "PROJECTPERMISSION_FAVORITE_INDEX", "PROJECTPERMISSION", schema)) {
				securityDb.insertData(queryUtil.createIndex("PROJECTPERMISSION_FAVORITE_INDEX", "PROJECTPERMISSION", "FAVORITE"));
			}
			if(!queryUtil.indexExists(securityDb, "PROJECTPERMISSION_USERID_INDEX", "PROJECTPERMISSION", schema)) {
				securityDb.insertData(queryUtil.createIndex("PROJECTPERMISSION_USERID_INDEX", "PROJECTPERMISSION", "USERID"));
			}
		}
		
		/**
		 * 
		 * END PROJECT TABLES
		 * 
		 */

		// WORKSPACEENGINE
		colNames = new String[] {"TYPE", "USERID", "PROJECTID"};
		types = new String[] {"VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)"};
		if(allowIfExistsTable) {
			securityDb.insertData(queryUtil.createTableIfNotExists("WORKSPACEENGINE", colNames, types));
		} else {
			// see if table exists
			if(!queryUtil.tableExists(conn, "WORKSPACEENGINE", schema)) {
				// make the table
				securityDb.insertData(queryUtil.createTable("WORKSPACEENGINE", colNames, types));
			}
		}
		if(allowIfExistsIndexs) {
			securityDb.insertData(queryUtil.createIndexIfNotExists("WORKSPACEENGINE_TYPE_INDEX", "WORKSPACEENGINE", "TYPE"));
			securityDb.insertData(queryUtil.createIndexIfNotExists("WORKSPACEENGINE_USERID_INDEX", "WORKSPACEENGINE", "USERID"));
		} else {
			// see if index exists
			if(!queryUtil.indexExists(securityDb, "WORKSPACEENGINE_TYPE_INDEX", "WORKSPACEENGINE", schema)) {
				securityDb.insertData(queryUtil.createIndex("WORKSPACEENGINE_TYPE_INDEX", "WORKSPACEENGINE", "TYPE"));
			}
			if(!queryUtil.indexExists(securityDb, "WORKSPACEENGINE_USERID_INDEX", "WORKSPACEENGINE", schema)) {
				securityDb.insertData(queryUtil.createIndex("WORKSPACEENGINE_USERID_INDEX", "WORKSPACEENGINE", "USERID"));
			}			
		}
		//MAKING MODIFICATION FROM ENGINEID TO PROJECTID
		{
			List<String> allCols = queryUtil.getTableColumns(conn, "WORKSPACEENGINE", schema);
			// this should return in all upper case
			// ... but sometimes it is not -_- i.e. postgres always lowercases
			if((!allCols.contains("PROJECTID") && !allCols.contains("projectid")) && (allCols.contains("ENGINEID") || allCols.contains("engineid") )) {
				String updateColName = queryUtil.modColumnName("WORKSPACEENGINE", "ENGINEID", "PROJECTID");
				securityDb.insertData(updateColName);
			}
		}
		
		// ASSETENGINE
		colNames = new String[] {"TYPE", "USERID", "PROJECTID"};
		types = new String[] {"VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)"};
		if(allowIfExistsTable) {
			securityDb.insertData(queryUtil.createTableIfNotExists("ASSETENGINE", colNames, types));
		} else {
			// see if table exists
			if(!queryUtil.tableExists(conn, "ASSETENGINE", schema)) {
				// make the table
				securityDb.insertData(queryUtil.createTable("ASSETENGINE", colNames, types));
			}
		}
		if(allowIfExistsIndexs) {
			securityDb.insertData(queryUtil.createIndexIfNotExists("ASSETENGINE_TYPE_INDEX", "ASSETENGINE", "TYPE"));
			securityDb.insertData(queryUtil.createIndexIfNotExists("ASSETENGINE_USERID_INDEX", "ASSETENGINE", "USERID"));
		} else {
			// see if index exists
			if(!queryUtil.indexExists(securityDb, "ASSETENGINE_TYPE_INDEX", "ASSETENGINE", schema)) {
				securityDb.insertData(queryUtil.createIndex("ASSETENGINE_TYPE_INDEX", "ASSETENGINE", "TYPE"));
			}
			if(!queryUtil.indexExists(securityDb, "ASSETENGINE_USERID_INDEX", "ASSETENGINE", schema)) {
				securityDb.insertData(queryUtil.createIndex("ASSETENGINE_USERID_INDEX", "ASSETENGINE", "USERID"));
			}
		}
		//MAKING MODIFICATION FROM ENGINEID TO PROJECTID
		{
			List<String> allCols = queryUtil.getTableColumns(conn, "ASSETENGINE", schema);
			// this should return in all upper case
			// ... but sometimes it is not -_- i.e. postgres always lowercases
			if((!allCols.contains("PROJECTID") && !allCols.contains("projectid")) && (allCols.contains("ENGINEID") || allCols.contains("engineid") )) {
				String updateColName = queryUtil.modColumnName("ASSETENGINE", "ENGINEID", "PROJECTID");
				securityDb.insertData(updateColName);
			}
		}
		
		
		// INSIGHT
		colNames = new String[] { "PROJECTID", "INSIGHTID", "INSIGHTNAME", "GLOBAL", "EXECUTIONCOUNT", "CREATEDON", "LASTMODIFIEDON", "LAYOUT", "CACHEABLE", "RECIPE" };
		types = new String[] { "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)", BOOLEAN_DATATYPE_NAME, "BIGINT", "TIMESTAMP", "TIMESTAMP", "VARCHAR(255)", BOOLEAN_DATATYPE_NAME, CLOB_DATATYPE_NAME };
		if(allowIfExistsTable) {
			securityDb.insertData(queryUtil.createTableIfNotExists("INSIGHT", colNames, types));
		} else {
			// see if table exists
			if(!queryUtil.tableExists(conn, "INSIGHT", schema)) {
				// make the table
				securityDb.insertData(queryUtil.createTable("INSIGHT", colNames, types));
			}
		}
		// INSIGHT RECIPE
		// check if column exists
		// TEMPORARY CHECK! - not sure when added but todays date is 12/16 
		{
			List<String> allCols = queryUtil.getTableColumns(conn, "INSIGHT", schema);
			// this should return in all upper case
			// ... but sometimes it is not -_- i.e. postgres always lowercases
			if(!allCols.contains("RECIPE") && !allCols.contains("recipe")) {
				String addRecipeColumnSql = queryUtil.alterTableAddColumn("INSIGHT", "RECIPE", CLOB_DATATYPE_NAME);
				securityDb.insertData(addRecipeColumnSql);
			}
		}
		
		//MAKING MODIFICATION FROM ENGINEID TO PROJECTID
		{
			List<String> allCols = queryUtil.getTableColumns(conn, "INSIGHT", schema);
			// this should return in all upper case
			// ... but sometimes it is not -_- i.e. postgres always lowercases
			if((!allCols.contains("PROJECTID") && !allCols.contains("projectid")) && (allCols.contains("ENGINEID") || allCols.contains("engineid") )) {
				String updateColName = queryUtil.modColumnName("INSIGHT", "ENGINEID", "PROJECTID");
				securityDb.insertData(updateColName);
			}
		}
		if(allowIfExistsIndexs) {
			securityDb.insertData(queryUtil.createIndexIfNotExists("INSIGHT_LASTMODIFIEDON_INDEX", "INSIGHT", "LASTMODIFIEDON"));
			securityDb.insertData(queryUtil.createIndexIfNotExists("INSIGHT_GLOBAL_INDEX", "INSIGHT", "GLOBAL"));
			securityDb.insertData(queryUtil.createIndexIfNotExists("INSIGHT_PROJECTID_INDEX", "INSIGHT", "PROJECTID"));
			securityDb.insertData(queryUtil.createIndexIfNotExists("INSIGHT_INSIGHTID_INDEX", "INSIGHT", "INSIGHTID"));
		} else {
			// see if index exists
			if(!queryUtil.indexExists(securityDb, "INSIGHT_LASTMODIFIEDON_INDEX", "INSIGHT", schema)) {
				securityDb.insertData(queryUtil.createIndex("INSIGHT_LASTMODIFIEDON_INDEX", "INSIGHT", "LASTMODIFIEDON"));
			}
			if(!queryUtil.indexExists(securityDb, "INSIGHT_GLOBAL_INDEX", "INSIGHT", schema)) {
				securityDb.insertData(queryUtil.createIndex("INSIGHT_GLOBAL_INDEX", "INSIGHT", "GLOBAL"));
			}
			if(!queryUtil.indexExists(securityDb, "INSIGHT_PROJECTID_INDEX", "INSIGHT", schema)) {
				securityDb.insertData(queryUtil.createIndex("INSIGHT_PROJECTID_INDEX", "INSIGHT", "PROJECTID"));
			}
			if(!queryUtil.indexExists(securityDb, "INSIGHT_INSIGHTID_INDEX", "INSIGHT", schema)) {
				securityDb.insertData(queryUtil.createIndex("INSIGHT_INSIGHTID_INDEX", "INSIGHT", "INSIGHTID"));
			}
		}

		// USERINSIGHTPERMISSION
		colNames = new String[] { "USERID", "PROJECTID", "INSIGHTID", "PERMISSION", "FAVORITE" };
		types = new String[] { "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)", "INT", BOOLEAN_DATATYPE_NAME };
		if(allowIfExistsTable) {
			securityDb.insertData(queryUtil.createTableIfNotExists("USERINSIGHTPERMISSION", colNames, types));
		} else {
			// see if table exists
			if(!queryUtil.tableExists(conn, "USERINSIGHTPERMISSION", schema)) {
				// make the table
				securityDb.insertData(queryUtil.createTable("USERINSIGHTPERMISSION", colNames, types));
			}
		}
		// TEMPORARY CHECK! - ADDED 03/17/2021
		{
			List<String> allCols = queryUtil.getTableColumns(conn, "USERINSIGHTPERMISSION", schema);
			// this should return in all upper case
			// ... but sometimes it is not -_- i.e. postgres always lowercases
			if(!allCols.contains("FAVORITE") && !allCols.contains("favorite")) {
				if(queryUtil.allowIfExistsModifyColumnSyntax()) {
					securityDb.insertData(queryUtil.alterTableAddColumnIfNotExists("USERINSIGHTPERMISSION", "FAVORITE", BOOLEAN_DATATYPE_NAME));
				} else {
					securityDb.insertData(queryUtil.alterTableAddColumn("USERINSIGHTPERMISSION", "FAVORITE", BOOLEAN_DATATYPE_NAME));
				}
			}
		}
		
		//MAKING MODIFICATION FROM ENGINEID TO PROJECTID - 04/22/2021
		{
			List<String> allCols = queryUtil.getTableColumns(conn, "USERINSIGHTPERMISSION", schema);
			// this should return in all upper case
			// ... but sometimes it is not -_- i.e. postgres always lowercases
			if((!allCols.contains("PROJECTID") && !allCols.contains("projectid")) && (allCols.contains("ENGINEID") || allCols.contains("engineid") )) {
				String updateColName = queryUtil.modColumnName("USERINSIGHTPERMISSION", "ENGINEID", "PROJECTID");
				securityDb.insertData(updateColName);
			}
		}
		if(allowIfExistsIndexs) {
			securityDb.insertData(queryUtil.createIndexIfNotExists("USERINSIGHTPERMISSION_PERMISSION_INDEX", "USERINSIGHTPERMISSION", "PERMISSION"));
			securityDb.insertData(queryUtil.createIndexIfNotExists("USERINSIGHTPERMISSION_PROJECTID_INDEX", "USERINSIGHTPERMISSION", "PROJECTID"));
			securityDb.insertData(queryUtil.createIndexIfNotExists("USERINSIGHTPERMISSION_USERID_INDEX", "USERINSIGHTPERMISSION", "USERID"));
			securityDb.insertData(queryUtil.createIndexIfNotExists("USERINSIGHTPERMISSION_FAVORITE_INDEX", "USERINSIGHTPERMISSION", "FAVORITE"));
		} else {
			// see if index exists
			if(!queryUtil.indexExists(securityDb, "USERINSIGHTPERMISSION_PERMISSION_INDEX", "USERINSIGHTPERMISSION", schema)) {
				securityDb.insertData(queryUtil.createIndex("USERINSIGHTPERMISSION_PERMISSION_INDEX", "USERINSIGHTPERMISSION", "PERMISSION"));
			}
			if(!queryUtil.indexExists(securityDb, "USERINSIGHTPERMISSION_PROJECTID_INDEX", "USERINSIGHTPERMISSION", schema)) {
				securityDb.insertData(queryUtil.createIndex("USERINSIGHTPERMISSION_PROJECTID_INDEX", "USERINSIGHTPERMISSION", "PROJECTID"));
			}
			if(!queryUtil.indexExists(securityDb, "USERINSIGHTPERMISSION_USERID_INDEX", "USERINSIGHTPERMISSION", schema)) {
				securityDb.insertData(queryUtil.createIndex("USERINSIGHTPERMISSION_USERID_INDEX", "USERINSIGHTPERMISSION", "USERID"));
			}
			if(!queryUtil.indexExists(securityDb, "USERINSIGHTPERMISSION_FAVORITE_INDEX", "USERINSIGHTPERMISSION", schema)) {
				securityDb.insertData(queryUtil.createIndex("USERINSIGHTPERMISSION_FAVORITE_INDEX", "USERINSIGHTPERMISSION", "FAVORITE"));
			}
		}
		
		// INSIGHTMETA
		colNames = new String[] { "PROJECTID", "INSIGHTID", "METAKEY", "METAVALUE", "METAORDER" };
		types = new String[] { "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)", CLOB_DATATYPE_NAME, "INT" };
		if(allowIfExistsTable) {
			securityDb.insertData(queryUtil.createTableIfNotExists("INSIGHTMETA", colNames, types));
		} else {
			// see if table exists
			if(!queryUtil.tableExists(conn, "INSIGHTMETA", schema)) {
				// make the table
				securityDb.insertData(queryUtil.createTable("INSIGHTMETA", colNames, types));
			}
		}
		if(allowIfExistsIndexs) {
			securityDb.insertData(queryUtil.createIndexIfNotExists("INSIGHTMETA_PROJECTID_INDEX", "INSIGHTMETA", "PROJECTID"));
			securityDb.insertData(queryUtil.createIndexIfNotExists("INSIGHTMETA_INSIGHTID_INDEX", "INSIGHTMETA", "INSIGHTID"));
		} else {
			// see if index exists
			if(!queryUtil.indexExists(securityDb, "INSIGHTMETA_PROJECTID_INDEX", "INSIGHTMETA", schema)) {
				securityDb.insertData(queryUtil.createIndex("INSIGHTMETA_PROJECTID_INDEX", "INSIGHTMETA", "PROJECTID"));
			}
			if(!queryUtil.indexExists(securityDb, "INSIGHTMETA_INSIGHTID_INDEX", "INSIGHTMETA", schema)) {
				securityDb.insertData(queryUtil.createIndex("INSIGHTMETA_INSIGHTID_INDEX", "INSIGHTMETA", "INSIGHTID"));
			}
		}
		
		//MAKING MODIFICATION FROM ENGINEID TO PROJECTID - 04/22/2021
		{
			List<String> allCols = queryUtil.getTableColumns(conn, "INSIGHTMETA", schema);
			// this should return in all upper case
			// ... but sometimes it is not -_- i.e. postgres always lowercases
			if((!allCols.contains("PROJECTID") && !allCols.contains("projectid")) && (allCols.contains("ENGINEID") || allCols.contains("engineid") )) {
				String updateColName = queryUtil.modColumnName("INSIGHTMETA", "ENGINEID", "PROJECTID");
				securityDb.insertData(updateColName);
			}
		}
		
		if(allowIfExistsIndexs) {
			securityDb.insertData(queryUtil.createIndexIfNotExists("INSIGHTMETA_PROJECTID_INDEX", "INSIGHTMETA", "PROJECTID"));
			securityDb.insertData(queryUtil.createIndexIfNotExists("INSIGHTMETA_INSIGHTID_INDEX", "INSIGHTMETA", "INSIGHTID"));
		} else {
			// see if index exists
			if(!queryUtil.indexExists(securityDb, "INSIGHTMETA_PROJECTID_INDEX", "INSIGHTMETA", schema)) {
				securityDb.insertData(queryUtil.createIndex("INSIGHTMETA_PROJECTID_INDEX", "INSIGHTMETA", "PROJECTID"));
			}
			if(!queryUtil.indexExists(securityDb, "INSIGHTMETA_INSIGHTID_INDEX", "INSIGHTMETA", schema)) {
				securityDb.insertData(queryUtil.createIndex("INSIGHTMETA_INSIGHTID_INDEX", "INSIGHTMETA", "INSIGHTID"));
			}
		}

		// SMSS_USER
		colNames = new String[] { "NAME", "EMAIL", "TYPE", "ID", "PASSWORD", "SALT", "USERNAME", "ADMIN", "PUBLISHER" };
		types = new String[] { "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)", BOOLEAN_DATATYPE_NAME, BOOLEAN_DATATYPE_NAME };
		// TEMPORARY CHECK! - 2021-01-17 this table used to be USER
		// but some rdbms types (postgres) does not allow it
		// so i am going ahead and moving over user to smss_user
		if(queryUtil.tableExists(conn, "USER", schema)) {
			performSmssUserTemporaryUpdate(securityDb, queryUtil, colNames, types, conn, schema, allowIfExistsTable);
		} else {
			if(allowIfExistsTable) {
				securityDb.insertData(queryUtil.createTableIfNotExists("SMSS_USER", colNames, types));
			} else {
				// see if table exists
				if(!queryUtil.tableExists(conn, "SMSS_USER", schema)) {
					// make the table
					securityDb.insertData(queryUtil.createTable("SMSS_USER", colNames, types));
				}
			}
		}
		if(allowIfExistsIndexs) {
			securityDb.insertData(queryUtil.createIndexIfNotExists("SMSS_USER_ID_INDEX", "SMSS_USER", "ID"));
		} else {
			// see if index exists
			if(!queryUtil.indexExists(securityDb, "SMSS_USER_ID_INDEX", "SMSS_USER", schema)) {
				securityDb.insertData(queryUtil.createIndex("SMSS_USER_ID_INDEX", "SMSS_USER", "ID"));
			}
		}
		
		// PERMISSION
		colNames = new String[] { "ID", "NAME" };
		types = new String[] { "INT", "VARCHAR(255)" };
		if(allowIfExistsTable) {
			securityDb.insertData(queryUtil.createTableIfNotExists("PERMISSION", colNames, types));
		} else {
			// see if table exists
			if(!queryUtil.tableExists(conn, "PERMISSION", schema)) {
				// make the table
				securityDb.insertData(queryUtil.createTable("PERMISSION", colNames, types));
			}
		}
		if(allowIfExistsIndexs) {
			List<String> iCols = new Vector<String>();
			iCols.add("ID");
			iCols.add("NAME");
			securityDb.insertData(queryUtil.createIndexIfNotExists("PERMISSION_ID_NAME_INDEX", "PERMISSION", iCols));
		} else {
			// see if index exists
			if(!queryUtil.indexExists(securityDb, "PERMISSION_ID_NAME_INDEX", "PERMISSION", schema)) {
				List<String> iCols = new Vector<String>();
				iCols.add("ID");
				iCols.add("NAME");
				securityDb.insertData(queryUtil.createIndex("PERMISSION_ID_NAME_INDEX", "PERMISSION", iCols));
			}
		}
		
		{
			IRawSelectWrapper wrapper = null;
			try {
				wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, "select count(*) from permission");
				if(wrapper.hasNext()) {
					int numrows = ((Number) wrapper.next().getValues()[0]).intValue();
					if(numrows > 3) {
						securityDb.removeData("DELETE FROM PERMISSION WHERE 1=1;");
						securityDb.insertData(queryUtil.insertIntoTable("PERMISSION", colNames, types, new Object[]{1, "OWNER"}));
						securityDb.insertData(queryUtil.insertIntoTable("PERMISSION", colNames, types, new Object[]{2, "EDIT"}));
						securityDb.insertData(queryUtil.insertIntoTable("PERMISSION", colNames, types, new Object[]{3, "READ_ONLY"}));
					} else if(numrows == 0) {
						securityDb.insertData(queryUtil.insertIntoTable("PERMISSION", colNames, types, new Object[]{1, "OWNER"}));
						securityDb.insertData(queryUtil.insertIntoTable("PERMISSION", colNames, types, new Object[]{2, "EDIT"}));
						securityDb.insertData(queryUtil.insertIntoTable("PERMISSION", colNames, types, new Object[]{3, "READ_ONLY"}));
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				if(wrapper != null) {
					wrapper.cleanUp();
				}
			}
		}
		
		// ACCESSREQUEST
		colNames = new String[] { "ID", "SUBMITTEDBY", "ENGINE", "PERMISSION" };
		types = new String[] { "VARCHAR(255)", "VARCHAR(255)", "VARCHAR(255)", "INT" };
		if(allowIfExistsTable) {
			securityDb.insertData(queryUtil.createTableIfNotExists("ACCESSREQUEST", colNames, types));
		} else {
			// see if table exists
			if(!queryUtil.tableExists(conn, "ACCESSREQUEST", schema)) {
				// make the table
				securityDb.insertData(queryUtil.createTable("ACCESSREQUEST", colNames, types));
			}
		}
		
		// clean up the connection used for this method
		if(securityDb.isConnectionPooling()) {
			conn.close();
		}
		
		////////////////////////////////////////////////////////////////////
		////////////////////////////////////////////////////////////////////
		////////////////////////////////////////////////////////////////////
		////////////////////////////////////////////////////////////////////
		
		/*
		 * Tables accounted for that we are not using yet...
		 */
		
//		// USERGROUP
//		colNames = new String[] { "groupid", "name", "owner" };
//		types = new String[] { "int identity", "varchar(255)", "varchar(255)" };
//		securityDb.insertData(RdbmsQueryBuilder.makeOptionalCreate("USERGROUP", colNames, types));
//		
//		// GROUPMEMBERS
//		colNames = new String[] {"groupmembersid", "groupid", "userid"};
//		types = new String[] {"int identity", "integer", "varchar(255)"};
//		securityDb.insertData(RdbmsQueryBuilder.makeOptionalCreate("GROUPMEMBERS", colNames, types));
//		
//		// ENGINEGROUPMEMBERVISIBILITY
//		colNames = new String[] { "id", "groupenginepermissionid", "groupmembersid", "visibility" };
//		types = new String[] { "int identity", "integer", "integer", "boolean" };
//		defaultValues = new Object[]{null, null, null, true};
//		securityDb.insertData(RdbmsQueryBuilder.makeOptionalCreateWithDefault("ENGINEGROUPMEMBERVISIBILITY", colNames, types, defaultValues));
//
//		// GROUPENGINEPERMISSION
//		colNames = new String[] {"groupenginepermissionid", "groupid", "permission", "engine"};
//		types = new String[] {"int identity", "integer", "integer", "varchar(255)"};
//		securityDb.insertData(RdbmsQueryBuilder.makeOptionalCreate("GROUPENGINEPERMISSION", colNames, types));
//		
//		// FOREIGN KEYS FOR CASCASDE DELETE
//		wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, "select count(*) from INFORMATION_SCHEMA.CONSTRAINTS where constraint_name='FK_GROUPENGINEPERMISSION'");
//		if(wrapper.hasNext()) {
//			if( ((Number) wrapper.next().getValues()[0]).intValue() == 0) {
//				securityDb.insertData("ALTER TABLE ENGINEGROUPMEMBERVISIBILITY ADD CONSTRAINT FK_GROUPENGINEPERMISSION FOREIGN KEY (GROUPENGINEPERMISSIONID) REFERENCES GROUPENGINEPERMISSION(GROUPENGINEPERMISSIONID) ON DELETE CASCADE;");
//			}
//		}
//		wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, "select count(*) from INFORMATION_SCHEMA.CONSTRAINTS where constraint_name='FK_GROUPMEMBERSID'");
//		if(wrapper.hasNext()) {
//			if( ((Number) wrapper.next().getValues()[0]).intValue() == 0) {
//				securityDb.insertData("ALTER TABLE ENGINEGROUPMEMBERVISIBILITY ADD CONSTRAINT FK_GROUPMEMBERSID FOREIGN KEY (GROUPMEMBERSID) REFERENCES GROUPMEMBERS (GROUPMEMBERSID) ON DELETE CASCADE;");
//			}
//		}
//				
//		// GROUPINSIGHTPERMISSION
//		colNames = new String[] { "groupid", "engineid", "insightid" };
//		types = new String[] { "integer", "integer", "varchar(255)" };
//		securityDb.insertData(RdbmsQueryBuilder.makeOptionalCreate("GROUPINSIGHTPERMISSION", colNames, types));

//		// INSIGHTEXECUTION
//		colNames = new String[] { "user", "database", "insight", "count", "lastexecuted", "session" };
//		types = new String[] { "varchar(255)", "varchar(255)", "varchar(255)", "integer", "date", "varchar(255)" };
//		securityDb.insertData(RdbmsQueryBuilder.makeOptionalCreate("INSIGHTEXECUTION", colNames, types));

//		// SEED
//		colNames = new String[] { "id", "name", "databaseid", "tablename", "columnname", "rlsvalue", "rlsjavacode", "owner" };
//		types = new String[] { "integer", "varchar(255)", "integer", "varchar(255)", "varchar(255)", "varchar(255)", CLOB_DATATYPE_NAME, "varchar(255)" };
//		securityDb.insertData(RdbmsQueryBuilder.makeOptionalCreate("SEED", colNames, types));

//		// USERSEEDPERMISSION
//		colNames = new String[] { "userid", "seedid" };
//		types = new String[] { "varchar(255)", "integer" };
//		securityDb.insertData(RdbmsQueryBuilder.makeOptionalCreate("USERSEEDPERMISSION", colNames, types));
		
//		// GROUPSEEDPERMISSION
//		colNames = new String[] { "groupid", "seedid" };
//		types = new String[] { "integer", "integer" };
//		securityDb.insertData(RdbmsQueryBuilder.makeOptionalCreate("GROUPSEEDPERMISSION", colNames, types));
	}
	
	@Deprecated
	private static void performSmssUserTemporaryUpdate(RDBMSNativeEngine securityDb,
			AbstractSqlQueryUtil queryUtil,
			String[] colNames,
			String[] types,
			Connection conn, 
			String schema,
			boolean allowIfExistsTable
			) throws Exception {
		// we will move over all the data and create SMSS_USER
		if(allowIfExistsTable) {
			securityDb.insertData(queryUtil.createTableIfNotExists("SMSS_USER", colNames, types));
		} else {
			// see if table exists
			if(!queryUtil.tableExists(conn, "SMSS_USER", schema)) {
				// make the table
				securityDb.insertData(queryUtil.createTable("SMSS_USER", colNames, types));
			}
		}
		StringBuilder query = new StringBuilder("SELECT ");
		Object[] input = new Object[colNames.length+1];
		input[0] = "SMSS_USER";
		for(int i = 0; i < colNames.length; i++) {
			input[i+1] = colNames[i];
			if(i > 0) {
				query.append(", ");
			}
			query.append(colNames[i]);
		}
		query.append(" FROM USER");
		PreparedStatement insertPs = securityDb.bulkInsertPreparedStatement(input);
		IRawSelectWrapper iterator = WrapperManager.getInstance().getRawWrapper(securityDb, query.toString());
		try {
			while(iterator.hasNext()) {
				Object[] values = iterator.next().getValues();
				int index = 0;
				String name = (String) values[index++];
				String email = (String) values[index++];
				String type = (String) values[index++];
				String id = (String) values[index++];
				String password = (String) values[index++];
				String salt = (String) values[index++];
				String username = (String) values[index++];
				Boolean admin = Boolean.parseBoolean( values[index++] + "" );
				Boolean publisher = Boolean.parseBoolean( values[index++] + "" );

				index = 1;
				if(name == null) {
					insertPs.setNull(index++, java.sql.Types.VARCHAR);
				} else {
					insertPs.setString(index++, name);
				}
				if(email == null) {
					insertPs.setNull(index++, java.sql.Types.VARCHAR);
				} else {
					insertPs.setString(index++, email);
				}
				if(type == null) {
					insertPs.setNull(index++, java.sql.Types.VARCHAR);
				} else {
					insertPs.setString(index++, type);
				}
				if(id == null) {
					insertPs.setNull(index++, java.sql.Types.VARCHAR);
				} else {
					insertPs.setString(index++, id);
				}
				if(password == null) {
					insertPs.setNull(index++, java.sql.Types.VARCHAR);
				} else {
					insertPs.setString(index++, password);
				}
				if(salt == null) {
					insertPs.setNull(index++, java.sql.Types.VARCHAR);
				} else {
					insertPs.setString(index++, salt);
				}
				if(username == null) {
					insertPs.setNull(index++, java.sql.Types.VARCHAR);
				} else {
					insertPs.setString(index++, username);
				}
				insertPs.setBoolean(index++, admin);
				insertPs.setBoolean(index++, publisher);
				insertPs.addBatch();
			}
		} finally {
			if(iterator != null) {
				iterator.cleanUp();
			}
		}
		insertPs.executeBatch();
		if(securityDb.isConnectionPooling()) {
			insertPs.getConnection().close();
		}
		// now delete the user table
		securityDb.insertData(queryUtil.alterTableName("USER", "OLD_USER_TABLE"));
	}

	/**
	 * Does this database name already exist
	 * @param user
	 * @param databaseName
	 * @return
	 */
	public static boolean userContainsDatabaseName(User user, String databaseName) {
		if(ignoreDatabase(databaseName)) {
			// dont add local master or security db to security db
			return true;
		}
//		String userFilters = getUserFilters(user);
//		String query = "SELECT * "
//				+ "FROM ENGINE "
//				+ "INNER JOIN ENGINEPERMISSION ON ENGINE.ENGINEID=ENGINEPERMISSION.ENGINEID "
//				+ "WHERE ENGINENAME='" + appName + "' AND PERMISSION IN (1,2) AND ENGINEPERMISSION.USERID IN " + userFilters;
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID"));
		qs.addRelation("ENGINE", "ENGINEPERMISSION", "inner.join");
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINENAME", "==", databaseName));
		List<Integer> permissionValues = new Vector<Integer>(2);
		permissionValues.add(new Integer(1));
		permissionValues.add(new Integer(2));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__PERMISSION", "==", permissionValues, PixelDataType.CONST_INT));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINEPERMISSION__USERID", "==", getUserFiltersQs(user)));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if(wrapper.hasNext()) {
				return true;
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
		}
		
		return false;
	}
	
	public static boolean containsDatabaseName(String databaseName) {
		if(ignoreDatabase(databaseName)) {
			// dont add local master or security db to security db
			return true;
		}
//		String query = "SELECT ENGINEID FROM ENGINE WHERE ENGINENAME='" + appName + "'";
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINENAME", "==", databaseName));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if(wrapper.hasNext()) {
				return true;
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
		}
		
		return false;
	}
	
	public static boolean userContainsProjectName(User user, String projectName) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTID"));
		qs.addRelation("PROJECT", "PROJECTPERMISSION", "inner.join");
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__PROJECTNAME", "==", projectName));
		List<Integer> permissionValues = new Vector<Integer>(2);
		permissionValues.add(new Integer(1));
		permissionValues.add(new Integer(2));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__PERMISSION", "==", permissionValues, PixelDataType.CONST_INT));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECTPERMISSION__USERID", "==", getUserFiltersQs(user)));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if(wrapper.hasNext()) {
				return true;
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
		}
		
		return false;
	}
	
	public static boolean containsProjectName(String projectName) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__PROJECTNAME", "==", projectName));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if(wrapper.hasNext()) {
				return true;
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
		}
		
		return false;
	}
	
	public static boolean containsDatabaseId(String databaseId) {
		if(ignoreDatabase(databaseId)) {
			// dont add local master or security db to security db
			return true;
		}
//		String query = "SELECT ENGINEID FROM ENGINE WHERE ENGINEID='" + appId + "'";
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINEID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINEID", "==", databaseId));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if(wrapper.hasNext()) {
				return true;
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
		}
		
		return false;
	}
	
	
	public static boolean containsProjectId(String projectId) {
		if(ignoreDatabase(projectId)) {
			// dont add local master or security db to security db
			return true;
		}
//		String query = "SELECT ENGINEID FROM ENGINE WHERE ENGINEID='" + appId + "'";
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__PROJECTID", "==", projectId));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if(wrapper.hasNext()) {
				return true;
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
		}
		
		return false;
	}
	
	
	public static boolean ignoreDatabase(String databaseId) {
		if(databaseId.equals(Constants.LOCAL_MASTER_DB_NAME) || databaseId.equals(Constants.SECURITY_DB) || databaseId.equals(Constants.SCHEDULER_DB) ) {
			// dont add local master or security db to security db
			return true;
		}
		// engine is an asset
		if(WorkspaceAssetUtils.isAssetProject(databaseId)) {
			return true;
		}
		// so that way all those Asset apps do not appear a bunch of times
		String smssFile = DIHelper.getInstance().getDbProperty(databaseId + "_" + Constants.STORE) + "";
		File smssF = new File(smssFile);
		if(smssFile != null && smssF.exists() && smssF.isFile()) {
			Properties prop = Utility.loadProperties(smssFile);
			return Boolean.parseBoolean(prop.get(Constants.IS_ASSET_APP) + "");
		}
		
		return false;
	}
	
	/**
	 * Get default image for insight
	 * @param databaseId
	 * @param insightId
	 * @return
	 */
	public static File getStockImage(String databaseId, String insightId) {
		String imageDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "/images/stock/";
		String layout = null;

//		String query = "SELECT LAYOUT FROM INSIGHT WHERE INSIGHT.ENGINEID='" + appId + "' AND INSIGHT.INSIGHTID='" + insightId + "'";
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("INSIGHT__LAYOUT"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHT__PROJECTID", "==", databaseId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHT__INSIGHTID", "==", insightId));
		
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if(wrapper.hasNext()) {
				layout = wrapper.next().getValues()[0].toString();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
		}
		
		// if no layout defined, also return the default
		if(layout == null) {
			return new File(imageDir + "color-logo.png");
		}
		
		if(layout.equalsIgnoreCase("area")) {
			return new File(imageDir + "area.png");
		} else if(layout.equalsIgnoreCase("column")) {
			return new File(imageDir + "bar.png");
		} else if(layout.equalsIgnoreCase("boxwhisker")) {
			return new File(imageDir + "boxwhisker.png");
		} else if(layout.equalsIgnoreCase("bubble")) {
			return new File(imageDir + "bubble.png");
		} else if(layout.equalsIgnoreCase("choropleth")) {
			return new File(imageDir + "choropleth.png");
		} else if(layout.equalsIgnoreCase("cloud")) {
			return new File(imageDir + "cloud.png");
		} else if(layout.equalsIgnoreCase("cluster")) {
			return new File(imageDir + "cluster.png");
		} else if(layout.equalsIgnoreCase("dendrogram")) {
			return new File(imageDir + "dendrogram-echarts.png");
		} else if(layout.equalsIgnoreCase("funnel")) {
			return new File(imageDir + "funnel.png");
		} else if(layout.equalsIgnoreCase("gauge")) {
			return new File(imageDir + "gauge.png");
		} else if(layout.equalsIgnoreCase("graph")) {
			return new File(imageDir + "graph.png");
		} else if(layout.equalsIgnoreCase("grid")) {
			return new File(imageDir + "grid.png");
		} else if(layout.equalsIgnoreCase("heatmap")) {
			return new File(imageDir + "heatmap.png");
		} else if(layout.equalsIgnoreCase("infographic")) {
			return new File(imageDir + "infographic.png");
		} else if(layout.equalsIgnoreCase("line")) {
			return new File(imageDir + "line.png");
		} else if(layout.equalsIgnoreCase("map")) {
			return new File(imageDir + "map.png");
		} else if(layout.equalsIgnoreCase("pack")) {
			return new File(imageDir + "pack.png");
		} else if(layout.equalsIgnoreCase("parallelcoordinates")) {
			return new File(imageDir + "parallel-coordinates.png");
		} else if(layout.equalsIgnoreCase("pie")) {
			return new File(imageDir + "pie.png");
		} else if(layout.equalsIgnoreCase("polar")) {
			return new File(imageDir + "polar-bar.png");
		} else if(layout.equalsIgnoreCase("radar")) {
			return new File(imageDir + "radar.png");
		} else if(layout.equalsIgnoreCase("sankey")) {
			return new File(imageDir + "sankey.png");
		} else if(layout.equalsIgnoreCase("scatter")) {
			return new File(imageDir + "scatter.png");
		} else if(layout.equalsIgnoreCase("scatterplotmatrix")) {
			return new File(imageDir + "scatter-matrix.png");
		} else if(layout.equalsIgnoreCase("singleaxiscluster")) {
			return new File(imageDir + "single-axis.png");
		} else if(layout.equalsIgnoreCase("sunburst")) {
			return new File(imageDir + "sunburst.png");
		} else if(layout.equalsIgnoreCase("text-widget")) {
			return new File(imageDir + "text-widget.png");
		} else if(layout.equalsIgnoreCase("treemap")) {
			return new File(imageDir + "treemap.png");
		} else {
			return new File(imageDir + "color-logo.png");
		}
	}
	
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	
	
	/*
	 * Utility methods
	 */
	
	static String createFilter(String... filterValues) {
		StringBuilder b = new StringBuilder();
		boolean hasData = false;
		if(filterValues.length > 0) {
			hasData = true;
			b.append(" IN (");
			b.append("'").append(filterValues[0]).append("'");
			for(int i = 1; i < filterValues.length; i++) {
				b.append(", '").append(filterValues[i]).append("'");
			}
		}
		if(hasData) {
			b.append(")");
		}
		return b.toString();
	}
	
	static String createFilter(Collection<String> filterValues) {
		if(filterValues.isEmpty()) {
			return " IN () ";
		}
		StringBuilder b = new StringBuilder();
		boolean hasData = false;
		if(filterValues.size() > 0) {
			hasData = true;
			b.append(" IN (");
			Iterator<String> iterator = filterValues.iterator();
			b.append("'").append(RdbmsQueryBuilder.escapeForSQLStatement(iterator.next())).append("'");
			while(iterator.hasNext()) {
				b.append(", '").append(RdbmsQueryBuilder.escapeForSQLStatement(iterator.next())).append("'");
			}
		}
		if(hasData) {
			b.append(")");
		}
		return b.toString();
	}
	
	/**
	 * Get all ids from user object
	 * @param user
	 * @return
	 */
	static String getUserFilters(User user) {
		StringBuilder b = new StringBuilder();
		b.append("(");
		if(user != null) {
			List<AuthProvider> logins = user.getLogins();
			if(!logins.isEmpty()) {
				int numLogins = logins.size();
				b.append("'").append(RdbmsQueryBuilder.escapeForSQLStatement(user.getAccessToken(logins.get(0)).getId())).append("'");
				for(int i = 1; i < numLogins; i++) {
					b.append(", '").append(RdbmsQueryBuilder.escapeForSQLStatement(user.getAccessToken(logins.get(i)).getId())).append("'");
				}
			}
		}
		b.append(")");
		return b.toString();
	}
	
	/**
	 * Get a vector of the user ids
	 * @param user
	 * @return
	 */
	static Collection<String> getUserFiltersQs(User user) {
		List<String> filters = new Vector<String>();
		if(user != null) {
			List<AuthProvider> logins = user.getLogins();
			for(AuthProvider thisLogin : logins) {
				filters.add(user.getAccessToken(thisLogin).getId());
			}
		}
		
		return filters;
	}
	
	////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////
	
	/**
	 * Returns a list of values given a query with one column/variable.
	 * @param query		Query to be executed
	 * @return			
	 */
	static List<Map<String, Object>> getSimpleQuery(String query) {
		List<Map<String, Object>> ret = new Vector<Map<String, Object>>();
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
			while(wrapper.hasNext()) {
				IHeadersDataRow row = wrapper.next();
				String[] headers = row.getHeaders();
				Object[] values = row.getValues();
				Map<String, Object> rowData = new HashMap<String, Object>();
				for(int idx = 0; idx < headers.length; idx++){
					if(values[idx] == null) {
						rowData.put(headers[idx].toLowerCase(), "null");
					} else {
						rowData.put(headers[idx].toLowerCase(), values[idx]);
					}
				}
				ret.add(rowData);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
		}

		return ret;
	}
	
	/**
	 * Returns a list of values given a query with one column/variable.
	 * @param qs		Query Struct to be executed
	 * @return			
	 */
	static List<Map<String, Object>> getSimpleQuery(SelectQueryStruct qs) {
		List<Map<String, Object>> ret = new Vector<Map<String, Object>>();
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			while(wrapper.hasNext()) {
				IHeadersDataRow row = wrapper.next();
				String[] headers = row.getHeaders();
				Object[] values = row.getValues();
				Map<String, Object> rowData = new HashMap<String, Object>();
				for(int idx = 0; idx < headers.length; idx++){
					if(values[idx] == null) {
						rowData.put(headers[idx].toLowerCase(), "null");
					} else {
						rowData.put(headers[idx].toLowerCase(), values[idx]);
					}
				}
				ret.add(rowData);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
		}

		return ret;
	}
	
	
	////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////

	public static String validEmail(String email){
		if(email == null || !email.matches("^[^\\s@]+@[^\\s@]+\\.[^\\s@]{2,}$")){
			return  email + " is not a valid email address. ";
		}
		return "";
	}
	
	public static String validPassword(String password){
		if(password == null || password.isEmpty()) {
			return "Password cannot be empty.";
		}
		Pattern pattern = Pattern.compile("^(?=.*[a-z])(?=.*[A-Z])(?=.*[0-9])(?=.*[!@#$%^&*])(?=.{8,})");
        Matcher matcher = pattern.matcher(password);
		
		if(!matcher.lookingAt()){
			return "Password doesn't comply with the security policies.";
		}
		return "";
	}
	
	/**
	 * Current salt generation by BCrypt
	 * @return salt
	 */
	public static String generateSalt(){
		return BCrypt.gensalt();
	}

	/**
	 * Create the password hash based on the password and salt provided.
	 * @param password
	 * @param salt
	 * @return hash
	 */
	public static String hash(String password, String salt) {
		return BCrypt.hashpw(password, salt);
	}

}
