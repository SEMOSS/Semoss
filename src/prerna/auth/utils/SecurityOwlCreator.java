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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.javatuples.Pair;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.impl.owl.AbstractOwlCreator;
import prerna.engine.impl.owl.WriteOWLEngine;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

public class SecurityOwlCreator extends AbstractOwlCreator {

	private static List<String[]> relationshipsRequired = new ArrayList<String[]>();
	static {
		relationshipsRequired.add(
				new String[] { "GITHUB_APP", "GITHUB_PROJECT_LINK", "GITHUB_APP.APP_ID.GITHUB_PROJECT_LINK.APP_ID" });
	}

	public SecurityOwlCreator(AbstractSqlQueryUtil queryUtil) {
		createColumnsAndTypes(queryUtil);
	}

	public void createColumnsAndTypes(AbstractSqlQueryUtil queryUtil) {
		final String CLOB_DATATYPE_NAME = queryUtil.getClobDataTypeName();
		final String BOOLEAN_DATATYPE_NAME = queryUtil.getBooleanDataTypeName();
		final String TIMESTAMP_DATATYPE_NAME = queryUtil.getDateWithTimeDataType();
		final String INTEGER_DATATYPE_NAME = queryUtil.getIntegerDataTypeName();
		final String DOUBLE_DATATYPE_NAME = queryUtil.getDoubleDataTypeName();
		final String VARCHAR_255 = "VARCHAR(255)";
		final String VARCHAR_500 = "VARCHAR(500)";

		this.allSchemas = new ArrayList<>();

		// @formatter:off
		addTable("ENGINE", Arrays.asList(
				Pair.with("ENGINEID", VARCHAR_255),
				Pair.with("ENGINENAME", VARCHAR_255),
				Pair.with("ENGINEDISPLAYNAME", VARCHAR_255),
				Pair.with("GLOBAL", BOOLEAN_DATATYPE_NAME),
				Pair.with("DISCOVERABLE", BOOLEAN_DATATYPE_NAME),
				Pair.with("ENGINETYPE", VARCHAR_255),
				Pair.with("ENGINESUBTYPE", VARCHAR_255),
				Pair.with("COST", VARCHAR_255),
				Pair.with("CREATEDBY", VARCHAR_255),
				Pair.with("CREATEDBYTYPE", VARCHAR_255),
				Pair.with("DATECREATED", TIMESTAMP_DATATYPE_NAME),
				Pair.with("TOOL_APP", VARCHAR_255)));

		addTable("ENGINEMETA", Arrays.asList(
				Pair.with("ENGINEID", VARCHAR_255),
				Pair.with("METAKEY", VARCHAR_255),
				Pair.with("METAVALUE", CLOB_DATATYPE_NAME),
				Pair.with("METAORDER", INTEGER_DATATYPE_NAME)));

		addTable("ENGINEPERMISSION", Arrays.asList(
				Pair.with("ENGINEID", VARCHAR_255),
				Pair.with("USERID", VARCHAR_255),
				Pair.with("PERMISSION", INTEGER_DATATYPE_NAME),
				Pair.with("VISIBILITY", BOOLEAN_DATATYPE_NAME),
				Pair.with("FAVORITE", BOOLEAN_DATATYPE_NAME),
				Pair.with("PERMISSIONGRANTEDBY", VARCHAR_255),
				Pair.with("PERMISSIONGRANTEDBYTYPE", VARCHAR_255),
				Pair.with("DATEADDED", TIMESTAMP_DATATYPE_NAME),
				Pair.with("ENDDATE", TIMESTAMP_DATATYPE_NAME),
				Pair.with("USAGERESTRICTION", VARCHAR_255),
				Pair.with("USAGEFREQUENCY", VARCHAR_255),
				Pair.with("MAXTOKENS", INTEGER_DATATYPE_NAME),
				Pair.with("MAXRESPONSETIME", DOUBLE_DATATYPE_NAME)));

		addTable("PROJECT", Arrays.asList(
				Pair.with("PROJECTID", VARCHAR_255),
				Pair.with("PROJECTNAME", VARCHAR_255),
				Pair.with("PROJECTDISPLAYNAME", VARCHAR_255),
				Pair.with("GLOBAL", BOOLEAN_DATATYPE_NAME),
				Pair.with("DISCOVERABLE", BOOLEAN_DATATYPE_NAME),
				Pair.with("TYPE", VARCHAR_255),
				Pair.with("COST", VARCHAR_255),
				Pair.with("CATALOGNAME", VARCHAR_255),
				Pair.with("PORTALPUBLISHED", TIMESTAMP_DATATYPE_NAME),
				Pair.with("PORTALPUBLISHEDUSER", VARCHAR_255),
				Pair.with("PORTALPUBLISHEDTYPE", VARCHAR_255),
				Pair.with("REACTORSCOMPILED", TIMESTAMP_DATATYPE_NAME),
				Pair.with("REACTORSCOMPILEDUSER", VARCHAR_255),
				Pair.with("REACTORSCOMPILEDTYPE", VARCHAR_255),
				Pair.with("CREATEDBY", VARCHAR_255),
				Pair.with("CREATEDBYTYPE", VARCHAR_255),
				Pair.with("DATECREATED", TIMESTAMP_DATATYPE_NAME),
				Pair.with("DATELASTEDITED", TIMESTAMP_DATATYPE_NAME)));

		addTable("PROJECTPERMISSION", Arrays.asList(
				Pair.with("PROJECTID", VARCHAR_255),
				Pair.with("USERID", VARCHAR_255),
				Pair.with("PERMISSION", INTEGER_DATATYPE_NAME),
				Pair.with("VISIBILITY", BOOLEAN_DATATYPE_NAME),
				Pair.with("FAVORITE", BOOLEAN_DATATYPE_NAME),
				Pair.with("PERMISSIONGRANTEDBY", VARCHAR_255),
				Pair.with("PERMISSIONGRANTEDBYTYPE", VARCHAR_255),
				Pair.with("DATEADDED", TIMESTAMP_DATATYPE_NAME),
				Pair.with("ENDDATE", TIMESTAMP_DATATYPE_NAME)));

		addTable("PROJECTMETA", Arrays.asList(
				Pair.with("PROJECTID", VARCHAR_255),
				Pair.with("METAKEY", VARCHAR_255),
				Pair.with("METAVALUE", CLOB_DATATYPE_NAME),
				Pair.with("METAORDER", INTEGER_DATATYPE_NAME)));

		addTable("PROJECTDEPENDENCIES", Arrays.asList(
				Pair.with("PROJECTID", VARCHAR_255),
				Pair.with("ENGINEID", VARCHAR_255),
				Pair.with("ENGINETYPE", VARCHAR_255),
				Pair.with("USERID", VARCHAR_255),
				Pair.with("TYPE", VARCHAR_255),
				Pair.with("DATEADDED", TIMESTAMP_DATATYPE_NAME)));

		addTable("ASSETENGINE", Arrays.asList(
				Pair.with("PROJECTID", VARCHAR_255),
				Pair.with("USERID", VARCHAR_255),
				Pair.with("TYPE", VARCHAR_255)));

		addTable("INSIGHT", Arrays.asList(
				Pair.with("INSIGHTID", VARCHAR_255),
				Pair.with("PROJECTID", VARCHAR_255),
				Pair.with("INSIGHTNAME", VARCHAR_255),
				Pair.with("GLOBAL", BOOLEAN_DATATYPE_NAME),
				Pair.with("EXECUTIONCOUNT", "BIGINT"),
				Pair.with("CREATEDON", TIMESTAMP_DATATYPE_NAME),
				Pair.with("LASTMODIFIEDON", TIMESTAMP_DATATYPE_NAME),
				Pair.with("LAYOUT", VARCHAR_255),
				Pair.with("CACHEABLE", BOOLEAN_DATATYPE_NAME),
				Pair.with("CACHEMINUTES", INTEGER_DATATYPE_NAME),
				Pair.with("CACHECRON", "VARCHAR(25)"),
				Pair.with("CACHEDON", TIMESTAMP_DATATYPE_NAME),
				Pair.with("CACHEENCRYPT", BOOLEAN_DATATYPE_NAME),
				Pair.with("RECIPE", CLOB_DATATYPE_NAME),
				Pair.with("SCHEMANAME", VARCHAR_255)));

		addTable("USERINSIGHTPERMISSION", Arrays.asList(
				Pair.with("INSIGHTID", VARCHAR_255),
				Pair.with("USERID", VARCHAR_255),
				Pair.with("PROJECTID", VARCHAR_255),
				Pair.with("PERMISSION", INTEGER_DATATYPE_NAME),
				Pair.with("FAVORITE", BOOLEAN_DATATYPE_NAME),
				Pair.with("PERMISSIONGRANTEDBY", VARCHAR_255),
				Pair.with("PERMISSIONGRANTEDBYTYPE", VARCHAR_255),
				Pair.with("DATEADDED", TIMESTAMP_DATATYPE_NAME),
				Pair.with("ENDDATE", TIMESTAMP_DATATYPE_NAME)));

		addTable("INSIGHTMETA", Arrays.asList(
				Pair.with("INSIGHTID", VARCHAR_255),
				Pair.with("PROJECTID", VARCHAR_255),
				Pair.with("METAKEY", VARCHAR_255),
				Pair.with("METAVALUE", CLOB_DATATYPE_NAME),
				Pair.with("METAORDER", INTEGER_DATATYPE_NAME)));

		addTable("INSIGHTFRAMES", Arrays.asList(
				Pair.with("INSIGHTID", VARCHAR_255),
				Pair.with("PROJECTID", VARCHAR_255),
				Pair.with("TABLENAME", VARCHAR_255),
				Pair.with("TABLETYPE", VARCHAR_255),
				Pair.with("COLUMNNAME", VARCHAR_255),
				Pair.with("COLUMNTYPE", VARCHAR_255),
				Pair.with("ADDITIONALTYPE", VARCHAR_255)));

		addTable("SMSS_USER", Arrays.asList(
				Pair.with("ID", VARCHAR_255),
				Pair.with("NAME", VARCHAR_255),
				Pair.with("EMAIL", VARCHAR_255),
				Pair.with("TYPE", VARCHAR_255),
				Pair.with("PASSWORD", VARCHAR_255),
				Pair.with("SALT", VARCHAR_255),
				Pair.with("USERNAME", VARCHAR_255),
				Pair.with("ADMIN", BOOLEAN_DATATYPE_NAME),
				Pair.with("PUBLISHER", BOOLEAN_DATATYPE_NAME),
				Pair.with("EXPORTER", BOOLEAN_DATATYPE_NAME),
				Pair.with("DATECREATED", TIMESTAMP_DATATYPE_NAME),
				Pair.with("LASTLOGIN", TIMESTAMP_DATATYPE_NAME),
				Pair.with("LASTPASSWORDRESET", TIMESTAMP_DATATYPE_NAME),
				Pair.with("LOCKED", BOOLEAN_DATATYPE_NAME),
				Pair.with("PHONE", VARCHAR_255),
				Pair.with("PHONEEXTENSION", VARCHAR_255),
				Pair.with("COUNTRYCODE", VARCHAR_255),
				Pair.with("MODELUSAGERESTRICTION", VARCHAR_255),
				Pair.with("MODELUSAGEFREQUENCY", VARCHAR_255),
				Pair.with("MODELMAXTOKENS", INTEGER_DATATYPE_NAME),
				Pair.with("MODELMAXRESPONSETIME", DOUBLE_DATATYPE_NAME)));

		addTable("SMSS_USER_ACCESS_KEYS", Arrays.asList(
				// TODO: DELETE ID AFTER SOME TIME, REPLACED WITH USERID ... 2023-09-19
				Pair.with("ID", VARCHAR_255),
				Pair.with("USERID", VARCHAR_255),
				Pair.with("TYPE", VARCHAR_255),
				Pair.with("ACCESSKEY", VARCHAR_255),
				Pair.with("SECRETKEY", VARCHAR_255),
				Pair.with("SECRETSALT", VARCHAR_255),
				Pair.with("DATECREATED", TIMESTAMP_DATATYPE_NAME),
				Pair.with("LASTUSED", TIMESTAMP_DATATYPE_NAME),
				Pair.with("TOKENNAME", VARCHAR_255),
				Pair.with("TOKENDESCRIPTION", VARCHAR_500)));

		addTable("TOKEN", Arrays.asList(
				Pair.with("IPADDR", VARCHAR_255),
				Pair.with("VAL", VARCHAR_255),
				Pair.with("DATEADDED", TIMESTAMP_DATATYPE_NAME),
				Pair.with("CLIENTID", VARCHAR_255)));

		addTable("PERMISSION", Arrays.asList(
				Pair.with("ID", INTEGER_DATATYPE_NAME),
				Pair.with("NAME", VARCHAR_255)));

		addTable("PASSWORD_RULES", Arrays.asList(
				Pair.with("PASS_LENGTH", INTEGER_DATATYPE_NAME),
				Pair.with("REQUIRE_UPPER", BOOLEAN_DATATYPE_NAME),
				Pair.with("REQUIRE_LOWER", BOOLEAN_DATATYPE_NAME),
				Pair.with("REQUIRE_NUMERIC", BOOLEAN_DATATYPE_NAME),
				Pair.with("REQUIRE_SPECIAL", BOOLEAN_DATATYPE_NAME),
				Pair.with("EXPIRATION_DAYS", INTEGER_DATATYPE_NAME),
				Pair.with("ADMIN_RESET_EXPIRATION", BOOLEAN_DATATYPE_NAME),
				Pair.with("ALLOW_USER_PASS_CHANGE", BOOLEAN_DATATYPE_NAME),
				Pair.with("PASS_REUSE_COUNT", INTEGER_DATATYPE_NAME),
				Pair.with("DAYS_TO_LOCK", INTEGER_DATATYPE_NAME),
				Pair.with("DAYS_TO_LOCK_WARNING", INTEGER_DATATYPE_NAME)));

		addTable("PASSWORD_HISTORY", Arrays.asList(
				Pair.with("ID", VARCHAR_255),
				Pair.with("USERID", VARCHAR_255),
				Pair.with("TYPE", VARCHAR_255),
				Pair.with("PASSWORD", VARCHAR_255),
				Pair.with("SALT", VARCHAR_255),
				Pair.with("DATE_ADDED", TIMESTAMP_DATATYPE_NAME)));

		addTable("PASSWORD_RESET", Arrays.asList(
				Pair.with("EMAIL", VARCHAR_255),
				Pair.with("TYPE", VARCHAR_255),
				Pair.with("TOKEN", VARCHAR_255),
				Pair.with("DATE_ADDED", TIMESTAMP_DATATYPE_NAME)));

		addTable("SESSION_SHARE", Arrays.asList(
				Pair.with("SHARE_VAL", VARCHAR_255),
				Pair.with("SESSION_VAL", VARCHAR_255),
				Pair.with("ROUTE_VAL", VARCHAR_255),
				Pair.with("DATE_ADDED", TIMESTAMP_DATATYPE_NAME),
				Pair.with("IS_SESSION_SHARE", BOOLEAN_DATATYPE_NAME),
				Pair.with("IS_AUTH_SHARE", BOOLEAN_DATATYPE_NAME),
				Pair.with("DATE_USED", TIMESTAMP_DATATYPE_NAME),
				Pair.with("USE_VALID", BOOLEAN_DATATYPE_NAME),
				Pair.with("USERID", VARCHAR_255),
				Pair.with("TYPE", VARCHAR_255)));

		addTable("ENGINEACCESSREQUEST", Arrays.asList(
				Pair.with("ID", VARCHAR_255),
				Pair.with("REQUEST_USERID", VARCHAR_255),
				Pair.with("REQUEST_TYPE", VARCHAR_255),
				Pair.with("REQUEST_TIMESTAMP", TIMESTAMP_DATATYPE_NAME),
				Pair.with("ENGINEID", VARCHAR_255),
				Pair.with("PERMISSION", INTEGER_DATATYPE_NAME),
				Pair.with("REQUEST_REASON", CLOB_DATATYPE_NAME),
				Pair.with("APPROVER_USERID", VARCHAR_255),
				Pair.with("APPROVER_TYPE", VARCHAR_255),
				Pair.with("APPROVER_DECISION", VARCHAR_255),
				Pair.with("APPROVER_TIMESTAMP", TIMESTAMP_DATATYPE_NAME),
				Pair.with("SUBMITTED_BY_USERID", VARCHAR_255),
				Pair.with("SUBMITTED_BY_TYPE", VARCHAR_255)));

		addTable("PROJECTACCESSREQUEST", Arrays.asList(
				Pair.with("ID", VARCHAR_255),
				Pair.with("REQUEST_USERID", VARCHAR_255),
				Pair.with("REQUEST_TYPE", VARCHAR_255),
				Pair.with("REQUEST_TIMESTAMP", TIMESTAMP_DATATYPE_NAME),
				Pair.with("PROJECTID", VARCHAR_255),
				Pair.with("PERMISSION", INTEGER_DATATYPE_NAME),
				Pair.with("REQUEST_REASON", CLOB_DATATYPE_NAME),
				Pair.with("APPROVER_USERID", VARCHAR_255),
				Pair.with("APPROVER_TYPE", VARCHAR_255),
				Pair.with("APPROVER_DECISION", VARCHAR_255),
				Pair.with("APPROVER_TIMESTAMP", TIMESTAMP_DATATYPE_NAME),
				Pair.with("SUBMITTED_BY_USERID", VARCHAR_255),
				Pair.with("SUBMITTED_BY_TYPE", VARCHAR_255)));

		addTable("INSIGHTACCESSREQUEST", Arrays.asList(
				Pair.with("ID", VARCHAR_255),
				Pair.with("REQUEST_USERID", VARCHAR_255),
				Pair.with("REQUEST_TYPE", VARCHAR_255),
				Pair.with("REQUEST_TIMESTAMP", TIMESTAMP_DATATYPE_NAME),
				Pair.with("PROJECTID", VARCHAR_255),
				Pair.with("INSIGHTID", VARCHAR_255),
				Pair.with("PERMISSION", INTEGER_DATATYPE_NAME),
				Pair.with("REQUEST_REASON", CLOB_DATATYPE_NAME),
				Pair.with("APPROVER_USERID", VARCHAR_255),
				Pair.with("APPROVER_TYPE", VARCHAR_255),
				Pair.with("APPROVER_DECISION", VARCHAR_255),
				Pair.with("APPROVER_TIMESTAMP", TIMESTAMP_DATATYPE_NAME),
				Pair.with("SUBMITTED_BY_USERID", VARCHAR_255),
				Pair.with("SUBMITTED_BY_TYPE", VARCHAR_255)));

		addTable("USERMETA", Arrays.asList(
				Pair.with("USERID", VARCHAR_255),
				Pair.with("TYPE", VARCHAR_255),
				Pair.with("METAKEY", VARCHAR_255),
				Pair.with("METAVALUE", CLOB_DATATYPE_NAME),
				Pair.with("METAORDER", INTEGER_DATATYPE_NAME)));

		addTable("SMSS_GROUP", Arrays.asList(
				Pair.with("ID", VARCHAR_255),
				Pair.with("TYPE", VARCHAR_255),
				Pair.with("DESCRIPTION", CLOB_DATATYPE_NAME),
				Pair.with("DATEADDED", TIMESTAMP_DATATYPE_NAME),
				Pair.with("USERID", VARCHAR_255),
				Pair.with("USERIDTYPE", VARCHAR_255)));

		addTable("CUSTOMGROUPASSIGNMENT", Arrays.asList(
				Pair.with("GROUPID", VARCHAR_255),
				Pair.with("USERID", VARCHAR_255),
				Pair.with("TYPE", VARCHAR_255),
				Pair.with("DATEADDED", TIMESTAMP_DATATYPE_NAME),
				Pair.with("ENDDATE", TIMESTAMP_DATATYPE_NAME),
				Pair.with("PERMISSIONGRANTEDBY", VARCHAR_255),
				Pair.with("PERMISSIONGRANTEDBYTYPE", VARCHAR_255)));

		addTable("GROUPENGINEPERMISSION", Arrays.asList(
				Pair.with("ID", VARCHAR_255),
				Pair.with("TYPE", VARCHAR_255),
				Pair.with("ENGINEID", VARCHAR_255),
				Pair.with("PERMISSION", INTEGER_DATATYPE_NAME),
				Pair.with("DATEADDED", TIMESTAMP_DATATYPE_NAME),
				Pair.with("ENDDATE", TIMESTAMP_DATATYPE_NAME),
				Pair.with("PERMISSIONGRANTEDBY", VARCHAR_255),
				Pair.with("PERMISSIONGRANTEDBYTYPE", VARCHAR_255)));

		addTable("GROUPPROJECTPERMISSION", Arrays.asList(
				Pair.with("ID", VARCHAR_255),
				Pair.with("TYPE", VARCHAR_255),
				Pair.with("PROJECTID", VARCHAR_255),
				Pair.with("PERMISSION", INTEGER_DATATYPE_NAME),
				Pair.with("DATEADDED", TIMESTAMP_DATATYPE_NAME),
				Pair.with("ENDDATE", TIMESTAMP_DATATYPE_NAME),
				Pair.with("PERMISSIONGRANTEDBY", VARCHAR_255),
				Pair.with("PERMISSIONGRANTEDBYTYPE", VARCHAR_255)));

		addTable("GROUPINSIGHTPERMISSION", Arrays.asList(
				Pair.with("ID", VARCHAR_255),
				Pair.with("TYPE", VARCHAR_255),
				Pair.with("PROJECTID", VARCHAR_255),
				Pair.with("INSIGHTID", VARCHAR_255),
				Pair.with("PERMISSION", INTEGER_DATATYPE_NAME),
				Pair.with("DATEADDED", TIMESTAMP_DATATYPE_NAME),
				Pair.with("ENDDATE", TIMESTAMP_DATATYPE_NAME),
				Pair.with("PERMISSIONGRANTEDBY", VARCHAR_255),
				Pair.with("PERMISSIONGRANTEDBYTYPE", VARCHAR_255)));

		addTable("JIRA_CONNECTIONS", Arrays.asList(
				Pair.with("ID", VARCHAR_255),
				Pair.with("ALIAS", VARCHAR_255),
				Pair.with("CLIENTID", VARCHAR_255),
				Pair.with("CLIENTSECRET", VARCHAR_255),
				Pair.with("SCOPE", "VARCHAR(1000)"),
				Pair.with("USERPROFILEURL", VARCHAR_255)));

		addTable("SALESFORCE_CONNECTIONS", Arrays.asList(
				Pair.with("ID", VARCHAR_255),
				Pair.with("ALIAS", VARCHAR_255),
				Pair.with("CLIENTID", VARCHAR_255),
				Pair.with("CLIENTSECRET", VARCHAR_255)));

		addTable("SERVICENOW_CONNECTIONS", Arrays.asList(
				Pair.with("ID", VARCHAR_255),
				Pair.with("INSTANCEURL", VARCHAR_255),
				Pair.with("ALIAS", VARCHAR_255),
				Pair.with("CLIENTID", VARCHAR_255),
				Pair.with("CLIENTSECRET", VARCHAR_255),
				Pair.with("USERPROFILEURL", VARCHAR_255)));

		addTable("GITHUB_APP", Arrays.asList(
				Pair.with("APP_ID", "BIGINT"),
				Pair.with("SLUG", VARCHAR_255),
				Pair.with("APP_NAME", VARCHAR_255),
				Pair.with("OWNER_LOGIN", VARCHAR_255),
				Pair.with("HTML_URL", VARCHAR_500),
				Pair.with("WEBHOOK_URL", VARCHAR_500),
				Pair.with("CLIENT_ID", VARCHAR_255),
				Pair.with("CLIENT_SECRET", CLOB_DATATYPE_NAME),
				Pair.with("WEBHOOK_SECRET", CLOB_DATATYPE_NAME),
				Pair.with("PRIVATE_KEY", CLOB_DATATYPE_NAME),
				Pair.with("CREATED_ON", TIMESTAMP_DATATYPE_NAME),
				Pair.with("UPDATED_ON", TIMESTAMP_DATATYPE_NAME)));

		addTable("GITHUB_PROJECT_LINK", Arrays.asList(
				Pair.with("PROJECT_ID", VARCHAR_255),
				Pair.with("APP_ID", "BIGINT"),
				Pair.with("INSTALLATION_ID", "BIGINT"),
				Pair.with("REPO_ID", "BIGINT"),
				Pair.with("REPO_FULL_NAME", "VARCHAR(511)"),
				Pair.with("BRANCH", VARCHAR_255),
				Pair.with("SUBDIR", "VARCHAR(1024)"),
				Pair.with("CREATED_ON", TIMESTAMP_DATATYPE_NAME),
				Pair.with("UPDATED_ON", TIMESTAMP_DATATYPE_NAME)));

		// "ENGINEMETAKEYS", "PROJECTMETAKEYS", "INSIGHTMETAKEYS", "USERMETAKEYS"
		// all have the same columns and default values
		List<String> metaKeyTableNames = Arrays.asList(Constants.ENGINE_METAKEYS, Constants.PROJECT_METAKEYS,
				Constants.INSIGHT_METAKEYS, Constants.USER_METAKEYS);
		for (String tableName : metaKeyTableNames) { 
			addTable(tableName, Arrays.asList(
					Pair.with("METAKEY", VARCHAR_255),
					Pair.with("SINGLEMULTI", VARCHAR_255),
					Pair.with("DISPLAYORDER", INTEGER_DATATYPE_NAME),
					Pair.with("DISPLAYOPTIONS", VARCHAR_255),
					Pair.with("DEFAULTVALUES", VARCHAR_500)));
		}
		// @formatter:on
	}

	@Override
	protected void writeRelations(WriteOWLEngine owler) throws Exception {
		// joins
		owler.addRelation("ENGINE", "ENGINEMETA", "ENGINE.ENGINEID.ENGINEMETA.ENGINEID");
		owler.addRelation("ENGINE", "ENGINEPERMISSION", "ENGINE.ENGINEID.ENGINEPERMISSION.ENGINEID");
		owler.addRelation("ENGINE", "WORKSPACEENGINE", "ENGINE.ENGINEID.WORKSPACEENGINE.ENGINEID");

		owler.addRelation("PROJECT", "ASSETENGINE", "PROJECT.PROJECTID.ASSETENGINE.PROJECTID");
		owler.addRelation("PROJECT", "PROJECTMETA", "PROJECT.PROJECTID.PROJECTMETA.PROJECTID");
		owler.addRelation("PROJECT", "INSIGHT", "PROJECT.PROJECTID.INSIGHT.PROJECTID");
		owler.addRelation("PROJECT", "USERINSIGHTPERMISSION", "PROJECT.PROJECTID.USERINSIGHTPERMISSION.PROJECTID");
		owler.addRelation("PROJECT", "PROJECTPERMISSION", "PROJECT.PROJECTID.PROJECTPERMISSION.PROJECTID");

		owler.addRelation("INSIGHT", "USERINSIGHTPERMISSION", "INSIGHT.INSIGHTID.USERINSIGHTPERMISSION.INSIGHTID");
		owler.addRelation("INSIGHT", "USERINSIGHTPERMISSION", "INSIGHT.PROJECTID.USERINSIGHTPERMISSION.PROJECTID");

		owler.addRelation("SMSS_USER", "USERINSIGHTPERMISSION", "SMSS_USER.ID.USERINSIGHTPERMISSION.USERID");
		owler.addRelation("SMSS_USER", "ENGINEPERMISSION", "SMSS_USER.ID.ENGINEPERMISSION.USERID");
		owler.addRelation("SMSS_USER", "PROJECTPERMISSION", "SMSS_USER.ID.PROJECTPERMISSION.USERID");
		owler.addRelation("SMSS_USER", "USERMETA", "SMSS_USER.ID.USERMETA.USERID");

		owler.addRelation("ENGINEPERMISSION", "PERMISSION", "ENGINEPERMISSION.PERMISSION.PERMISSION.ID");
		owler.addRelation("USERINSIGHTPERMISSION", "PERMISSION", "USERINSIGHTPERMISSION.PERMISSION.PERMISSION.ID");
		owler.addRelation("PROJECTPERMISSION", "PERMISSION", "PROJECTPERMISSION.PERMISSION.PERMISSION.ID");

		owler.addRelation("INSIGHT", "INSIGHTMETA", "INSIGHT.INSIGHTID.INSIGHTMETA.INSIGHTID");
		owler.addRelation("INSIGHT", "INSIGHTMETA", "INSIGHT.PROJECTID.INSIGHTMETA.PROJECTID");

		owler.addRelation("INSIGHT", "INSIGHTFRAMES", "INSIGHT.INSIGHTID.INSIGHTFRAMES.INSIGHTID");
		owler.addRelation("INSIGHT", "INSIGHTFRAMES", "INSIGHT.PROJECTID.INSIGHTFRAMES.PROJECTID");

		owler.addRelation("SMSS_USER", "CUSTOMGROUPASSIGNMENT", "SMSS_USER.ID.CUSTOMGROUPASSIGNMENT.USERID");
		owler.addRelation("SMSS_GROUP", "CUSTOMGROUPASSIGNMENT", "SMSS_GROUP.ID.CUSTOMGROUPASSIGNMENT.GROUPID");

		owler.addRelation("SMSS_GROUP", "GROUPENGINEPERMISSION", "SMSS_GROUP.ID.GROUPENGINEPERMISSION.ID");
		owler.addRelation("SMSS_GROUP", "GROUPENGINEPERMISSION", "SMSS_GROUP.TYPE.GROUPENGINEPERMISSION.TYPE");
		owler.addRelation("ENGINE", "GROUPENGINEPERMISSION", "ENGINE.ENGINEID.GROUPENGINEPERMISSION.ENGINEID");

		owler.addRelation("SMSS_GROUP", "GROUPPROJECTPERMISSION", "SMSS_GROUP.ID.GROUPPROJECTPERMISSION.ID");
		owler.addRelation("SMSS_GROUP", "GROUPPROJECTPERMISSION", "SMSS_GROUP.TYPE.GROUPPROJECTPERMISSION.TYPE");
		owler.addRelation("PROJECT", "GROUPPROJECTPERMISSION", "PROJECT.PROJECTID.GROUPPROJECTPERMISSION.PROJECTID");

		owler.addRelation("SMSS_GROUP", "GROUPINSIGHTPERMISSION", "SMSS_GROUP.ID.GROUPINSIGHTPERMISSION.ID");
		owler.addRelation("SMSS_GROUP", "GROUPINSIGHTPERMISSION", "SMSS_GROUP.TYPE.GROUPINSIGHTPERMISSION.TYPE");
		owler.addRelation("INSIGHT", "GROUPINSIGHTPERMISSION", "INSIGHT.PROJECTID.GROUPINSIGHTPERMISSION.PROJECTID");
		owler.addRelation("INSIGHT", "GROUPINSIGHTPERMISSION", "INSIGHT.INSIGHTID.GROUPINSIGHTPERMISSION.INSIGHTID");

		// github app integration joins
		owler.addRelation("GITHUB_APP", "GITHUB_PROJECT_LINK", "GITHUB_APP.APP_ID.GITHUB_PROJECT_LINK.APP_ID");
		owler.addRelation("PROJECT", "GITHUB_PROJECT_LINK", "PROJECT.PROJECTID.GITHUB_PROJECT_LINK.PROJECT_ID");
	}

	@Override
	protected boolean additionalRemakeChecks(IDatabaseEngine engine) {
		List<String[]> allRelationships = engine.getPhysicalRelationships();
		HAS_REQUIRED_REL_LOOP: for (String[] requiredRel : relationshipsRequired) {
			for (String[] existingRel : allRelationships) {
				String c1 = Utility.getInstanceName(existingRel[0]);
				String c2 = Utility.getInstanceName(existingRel[1]);
				String relName = Utility.getInstanceName(existingRel[2]);

				if (c1.equals(requiredRel[0]) && c2.equals(requiredRel[1]) && relName.equals(requiredRel[2])) {
					continue HAS_REQUIRED_REL_LOOP;
				}
			}

			// if we got here, the above didn't continue the loop so we dont have this rel
			// need to remake
			return true;
		}

		return false;
	}

}
