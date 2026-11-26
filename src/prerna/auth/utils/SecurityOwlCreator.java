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
import prerna.util.sql.AbstractSqlQueryUtil;

public class SecurityOwlCreator extends AbstractOwlCreator {

	/**
	 * Columns the security database no longer has, as table then column. An OWL
	 * that still describes one is made again, so nothing reads it from there.
	 */
	private static final List<Pair<String, String>> REMOVED_COLUMNS = List.of(Pair.with("ENGINE", "TOOL_APP"));

	private static List<String[]> relationshipsRequired = new ArrayList<String[]>();
	static {
		relationshipsRequired.add(
				new String[] { "GITHUB_APP", "GITHUB_PROJECT_LINK", "GITHUB_APP.APP_ID.GITHUB_PROJECT_LINK.APP_ID" });
		relationshipsRequired.add(
				new String[] { "SMSS_USER", "MS_GRAPH_SUBSCRIPTION", "SMSS_USER.ID.MS_GRAPH_SUBSCRIPTION.USER_ID" });
	}

	public SecurityOwlCreator(AbstractSqlQueryUtil queryUtil) {
		super(queryUtil);
	}

	@Override
	public void createColumnsAndTypes(AbstractSqlQueryUtil queryUtil) {
		final String CLOB_DATATYPE_NAME = queryUtil.getClobDataTypeName();
		final String BOOLEAN_DATATYPE_NAME = queryUtil.getBooleanDataTypeName();
		final String TIMESTAMP_DATATYPE_NAME = queryUtil.getDateWithTimeDataType();
		final String INTEGER_DATATYPE_NAME = queryUtil.getIntegerDataTypeName();
		final String VARCHAR_20 = "VARCHAR(20)";
		final String VARCHAR_255 = "VARCHAR(255)";
		final String VARCHAR_500 = "VARCHAR(500)";

		this.allSchemas = new ArrayList<>();

		// @formatter:off
		// engines: databases, models, storage, vector databases, functions, guardrails and virtual
		// environments, by catalog type (IEngine.CATALOG_TYPE). ENGINE is the catalog and the next
		// tables keep each engine's metadata, a model's details, people's grants and their own
		// settings for it. An engine's team grants (GROUPENGINEPERMISSION), the permissions it
		// gives apps and other engines (ENGINE2ENGINEPERMISSION), its access requests
		// (ENGINEACCESSREQUEST) and a model's usage limits (MODELUSAGELIMIT) follow further down

		// one row per engine: its id, name and display name, catalog type (ENGINETYPE) and subtype,
		// whether it is public (GLOBAL, which gives everyone Read-Only access) and discoverable
		// (listed to people without access, who can then request it), and who created it and when.
		// Creating or registering an engine writes the row, and engine lists, searches and
		// permission checks read it
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
				Pair.with("DATECREATED", TIMESTAMP_DATATYPE_NAME)));

		// the metadata values set on an engine, such as its description, markdown, tags and domain:
		// one row per value, METAORDER keeping the order of a key with several values. The metadata
		// reactors accept only the keys in ENGINEMETAKEYS, and engine lists filter on these values
		addTable("ENGINEMETA", Arrays.asList(
				Pair.with("ENGINEID", VARCHAR_255),
				Pair.with("METAKEY", VARCHAR_255),
				Pair.with("METAVALUE", CLOB_DATATYPE_NAME),
				Pair.with("METAORDER", INTEGER_DATATYPE_NAME)));

		// one row per model engine describing the model: the static model catalog entry it matches
		// (CATALOGMODELKEY), its provider, capability and family, input and output modalities,
		// context window and output token limit, built-in tools, which features it supports, its
		// benchmarks, and its PRICING, which cost limits count its calls at (ModelUsageCost).
		// Creating a model writes it, the static model catalog fills and syncs it, and editors
		// update it; only owners change its prices (SecurityModelMetadataUtils)
		addTable("MODELMETADATA", Arrays.asList(
				Pair.with("ENGINEID", VARCHAR_255),
				Pair.with("MODELID", VARCHAR_255),
				Pair.with("CATALOGMODELKEY", VARCHAR_255),
				Pair.with("MODELPROVIDER", VARCHAR_255),
				Pair.with("SERVINGPROVIDER", VARCHAR_255),
				Pair.with("CAPABILITY", VARCHAR_255),
				Pair.with("FAMILY", VARCHAR_255),
				Pair.with("INPUTMODALITIES", CLOB_DATATYPE_NAME),
				Pair.with("OUTPUTMODALITIES", CLOB_DATATYPE_NAME),
				Pair.with("CONTEXTWINDOW", "BIGINT"),
				Pair.with("MAXOUTPUTTOKENS", "BIGINT"),
				Pair.with("BUILTINTOOLS", CLOB_DATATYPE_NAME),
				Pair.with("ATTACHMENT", BOOLEAN_DATATYPE_NAME),
				Pair.with("REASONING", BOOLEAN_DATATYPE_NAME),
				Pair.with("TOOLCALL", BOOLEAN_DATATYPE_NAME),
				Pair.with("STRUCTUREDOUTPUT", BOOLEAN_DATATYPE_NAME),
				Pair.with("TEMPERATURE", BOOLEAN_DATATYPE_NAME),
				Pair.with("KNOWLEDGECUTOFF", VARCHAR_255),
				Pair.with("RELEASEDATE", VARCHAR_255),
				Pair.with("SUPPORTEDPARAMETERS", CLOB_DATATYPE_NAME),
				Pair.with("REASONINGCONFIG", CLOB_DATATYPE_NAME),
				Pair.with("BENCHMARKS", CLOB_DATATYPE_NAME),
				Pair.with("PRICING", CLOB_DATATYPE_NAME)));

		// a person's grant on an engine: the user id, whose grant holds on each of its logins, the
		// level (PERMISSION, an id in the PERMISSION table), who gave it and when, and when it ends
		// (ENDDATE, null for never; ExpiredAccessRemoval removes the row once that passes). On a
		// model, the grant's usage limit is in MODELUSAGELIMIT (LIMITTYPE USER). Permission checks
		// read it with the team grants, and every change to it is saved as a permission batch
		// (PermissionBatchUtils)
		addTable("ENGINEPERMISSION", Arrays.asList(
				Pair.with("ENGINEID", VARCHAR_255),
				Pair.with("USERID", VARCHAR_255),
				Pair.with("PERMISSION", INTEGER_DATATYPE_NAME),
				Pair.with("PERMISSIONGRANTEDBY", VARCHAR_255),
				Pair.with("PERMISSIONGRANTEDBYTYPE", VARCHAR_255),
				Pair.with("DATEADDED", TIMESTAMP_DATATYPE_NAME),
				Pair.with("ENDDATE", TIMESTAMP_DATATYPE_NAME)));

		// a person's own settings for an engine, apart from their access: one row per login (USERID
		// and TYPE) and engine, with whether they favorited it (FAVORITE) and whether it shows in
		// their lists (VISIBILITY). Setting either one writes it for each of the user's logins
		addTable("USERENGINEPREFERENCE", Arrays.asList(
				Pair.with("ENGINEID", VARCHAR_255),
				Pair.with("USERID", VARCHAR_255),
				Pair.with("TYPE", VARCHAR_255),
				Pair.with("FAVORITE", BOOLEAN_DATATYPE_NAME),
				Pair.with("VISIBILITY", BOOLEAN_DATATYPE_NAME),
				Pair.with("USERDISCOVERABLE", BOOLEAN_DATATYPE_NAME)));

		// model usage limits: the tokens, cost and compute time calls to a model may use.
		// ModelUsageGuard checks them on each call, every read and write goes through
		// SecurityModelUsageLimitUtils, and a limit on a grant is saved in the same permission
		// batch as the grant

		// a model usage limit: one row per key (ModelUsageLimitKey), such as a
		// person's grant on a model, a team's, an app's or engine's, a model's public
		// use or total, or a limit across all models (ENGINEID null). Its rules are
		// in MODELUSAGELIMITRULE. LIMITTYPE is USER, TEAM, ENGINE, PUBLIC, PLATFORM or
		// MODEL; SUBJECTTYPE is the login type, the team type, or the catalog type of
		// the app or engine
		addTable("MODELUSAGELIMIT", Arrays.asList(
				Pair.with("ID", VARCHAR_255),
				Pair.with("ENGINEID", VARCHAR_255),
				Pair.with("LIMITTYPE", VARCHAR_20),
				Pair.with("SUBJECTID", VARCHAR_255),
				Pair.with("SUBJECTTYPE", VARCHAR_255),
				Pair.with("UNLIMITED", BOOLEAN_DATATYPE_NAME),
				Pair.with("DATEADDED", TIMESTAMP_DATATYPE_NAME),
				Pair.with("ADDEDBYID", VARCHAR_255),
				Pair.with("ADDEDBYTYPE", VARCHAR_255),
				Pair.with("DATEMODIFIED", TIMESTAMP_DATATYPE_NAME),
				Pair.with("MODIFIEDBYID", VARCHAR_255),
				Pair.with("MODIFIEDBYTYPE", VARCHAR_255)));

		// the rules of a limit, one row per rule, LIMITID being the limit's MODELUSAGELIMIT.ID; a
		// call must fit in every rule of its limit. METRIC is TOKENS, COST or COMPUTE_TIME, and
		// AMOUNT is tokens, millionths of a dollar or milliseconds; LIMITWINDOW avoids the reserved
		// word WINDOW; WINDOWLENGTH is null for 1 and only set on minute and hour windows; COUNTS
		// is EACH_PERSON, an amount for each person, or SHARED, one total for everyone the limit
		// covers
		addTable("MODELUSAGELIMITRULE", Arrays.asList(
				Pair.with("LIMITID", VARCHAR_255),
				Pair.with("METRIC", VARCHAR_20),
				Pair.with("AMOUNT", "BIGINT"),
				Pair.with("LIMITWINDOW", VARCHAR_20),
				Pair.with("WINDOWLENGTH", INTEGER_DATATYPE_NAME),
				Pair.with("COUNTS", VARCHAR_20)));

		// projects: apps, agent workspaces, skills, notebooks and automations, by TYPE
		// (IProject.PROJECT_TYPE). PROJECT is the catalog and the next tables keep people's grants
		// on a project, their own settings for it, its metadata, the engines it depends on and each
		// login's asset project. A project's team grants (GROUPPROJECTPERMISSION), access requests
		// (PROJECTACCESSREQUEST) and GitHub link (GITHUB_PROJECT_LINK) follow further down

		// one row per project: its id, name and display name, TYPE, whether it is public (GLOBAL),
		// discoverable, a template others can copy (IS_TEMPLATE), or has its own login page
		// (HAS_CUSTOM_LOGIN, so its portal is served to signed out visitors), when and by whom its
		// portal was last published and its custom reactors last compiled, and who created it,
		// when, and when it was last edited. An app given a permission by an engine
		// (ENGINE2ENGINEPERMISSION, GRANTEEENGINETYPE PROJECT) is a project here
		addTable("PROJECT", Arrays.asList(
				Pair.with("PROJECTID", VARCHAR_255),
				Pair.with("PROJECTNAME", VARCHAR_255),
				Pair.with("PROJECTDISPLAYNAME", VARCHAR_255),
				Pair.with("GLOBAL", BOOLEAN_DATATYPE_NAME),
				Pair.with("DISCOVERABLE", BOOLEAN_DATATYPE_NAME),
				Pair.with("IS_TEMPLATE", BOOLEAN_DATATYPE_NAME),
				Pair.with("HAS_CUSTOM_LOGIN", BOOLEAN_DATATYPE_NAME),
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

		// a person's grant on a project, as ENGINEPERMISSION is on an engine: the user id, the
		// level, who gave it and when, and when it ends
		addTable("PROJECTPERMISSION", Arrays.asList(
				Pair.with("PROJECTID", VARCHAR_255),
				Pair.with("USERID", VARCHAR_255),
				Pair.with("PERMISSION", INTEGER_DATATYPE_NAME),
				Pair.with("PERMISSIONGRANTEDBY", VARCHAR_255),
				Pair.with("PERMISSIONGRANTEDBYTYPE", VARCHAR_255),
				Pair.with("DATEADDED", TIMESTAMP_DATATYPE_NAME),
				Pair.with("ENDDATE", TIMESTAMP_DATATYPE_NAME)));

		// a person's favorite and visibility settings for a project, one row per login and project,
		// as USERENGINEPREFERENCE keeps them for an engine
		addTable("USERPROJECTPREFERENCE", Arrays.asList(
				Pair.with("PROJECTID", VARCHAR_255),
				Pair.with("USERID", VARCHAR_255),
				Pair.with("TYPE", VARCHAR_255),
				Pair.with("FAVORITE", BOOLEAN_DATATYPE_NAME),
				Pair.with("VISIBILITY", BOOLEAN_DATATYPE_NAME),
				Pair.with("USERDISCOVERABLE", BOOLEAN_DATATYPE_NAME)));

		// the metadata values set on a project, such as its description, markdown and tags, one row
		// per value; the metadata reactors accept only the keys in PROJECTMETAKEYS. The server-only
		// key digital_twin (SecurityProjectUtils.DIGITAL_TWIN_METAKEY) marks a Digital Twin
		addTable("PROJECTMETA", Arrays.asList(
				Pair.with("PROJECTID", VARCHAR_255),
				Pair.with("METAKEY", VARCHAR_255),
				Pair.with("METAVALUE", CLOB_DATATYPE_NAME),
				Pair.with("METAORDER", INTEGER_DATATYPE_NAME)));

		// the engines and other projects a project uses: one row per dependency, with its catalog
		// type (ENGINETYPE, PROJECT for a project), who set it (USERID and TYPE) and when.
		// SetProjectDependencies and the agent workspace reactors replace the list. Passing a
		// person's project access on to the dependencies gives them the level on each, or files an
		// access request for its owners where the person giving access is Read-Only or has no
		// access there
		addTable("PROJECTDEPENDENCIES", Arrays.asList(
				Pair.with("PROJECTID", VARCHAR_255),
				Pair.with("ENGINEID", VARCHAR_255),
				Pair.with("ENGINETYPE", VARCHAR_255),
				Pair.with("USERID", VARCHAR_255),
				Pair.with("TYPE", VARCHAR_255),
				Pair.with("DATEADDED", TIMESTAMP_DATATYPE_NAME)));

		// each login's asset project, the project that holds that user's own files: one row per
		// login (USERID, and TYPE the login provider) with its PROJECTID. The project is not in
		// PROJECT, so this row is how it is found and known to be an asset project
		// (UserAssetUtils); User.getAssetProjectId makes it the first time it is needed
		addTable("ASSETENGINE", Arrays.asList(
				Pair.with("PROJECTID", VARCHAR_255),
				Pair.with("USERID", VARCHAR_255),
				Pair.with("TYPE", VARCHAR_255)));

		// insights: the saved analyses of a project, each known by PROJECTID and INSIGHTID
		// together, with people's grants and own settings, metadata and frame columns. An owner of
		// the project is an owner of its insights. Team grants (GROUPINSIGHTPERMISSION) and access
		// requests (INSIGHTACCESSREQUEST) follow further down

		// one row per saved insight: its name, whether it is public (GLOBAL), how many times it was
		// opened (EXECUTIONCOUNT), its layout, its cache settings, the pixel recipe it is rebuilt
		// from (RECIPE), a schema name unique within the project, and when it was created and last
		// changed. Saving an insight writes the row, and OpenInsight builds the insight from its
		// recipe
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

		// a person's grant on an insight (PROJECTID and INSIGHTID), as ENGINEPERMISSION is on an
		// engine: the user id, the level, who gave it and when, and when it ends
		addTable("USERINSIGHTPERMISSION", Arrays.asList(
				Pair.with("INSIGHTID", VARCHAR_255),
				Pair.with("USERID", VARCHAR_255),
				Pair.with("PROJECTID", VARCHAR_255),
				Pair.with("PERMISSION", INTEGER_DATATYPE_NAME),
				Pair.with("PERMISSIONGRANTEDBY", VARCHAR_255),
				Pair.with("PERMISSIONGRANTEDBYTYPE", VARCHAR_255),
				Pair.with("DATEADDED", TIMESTAMP_DATATYPE_NAME),
				Pair.with("ENDDATE", TIMESTAMP_DATATYPE_NAME)));

		// a person's favorite and visibility settings for an insight, one row per login and
		// insight, as USERENGINEPREFERENCE keeps them for an engine
		addTable("USERINSIGHTPREFERENCE", Arrays.asList(
				Pair.with("INSIGHTID", VARCHAR_255),
				Pair.with("USERID", VARCHAR_255),
				Pair.with("TYPE", VARCHAR_255),
				Pair.with("PROJECTID", VARCHAR_255),
				Pair.with("FAVORITE", BOOLEAN_DATATYPE_NAME),
				Pair.with("VISIBILITY", BOOLEAN_DATATYPE_NAME)));

		// the metadata values set on an insight, such as its description and tags, one row per
		// value; the metadata reactors accept only the keys in INSIGHTMETAKEYS
		addTable("INSIGHTMETA", Arrays.asList(
				Pair.with("INSIGHTID", VARCHAR_255),
				Pair.with("PROJECTID", VARCHAR_255),
				Pair.with("METAKEY", VARCHAR_255),
				Pair.with("METAVALUE", CLOB_DATATYPE_NAME),
				Pair.with("METAORDER", INTEGER_DATATYPE_NAME)));

		// the columns of the frames a saved insight builds: one row per column, with the frame's
		// name and type, the column's name and type, and its additional type. Saving an insight
		// replaces its rows, and GetInsightFrames, GetInsightFrameStructure and GetInsightMetamodel
		// read them
		addTable("INSIGHTFRAMES", Arrays.asList(
				Pair.with("INSIGHTID", VARCHAR_255),
				Pair.with("PROJECTID", VARCHAR_255),
				Pair.with("TABLENAME", VARCHAR_255),
				Pair.with("TABLETYPE", VARCHAR_255),
				Pair.with("COLUMNNAME", VARCHAR_255),
				Pair.with("COLUMNTYPE", VARCHAR_255),
				Pair.with("ADDITIONALTYPE", VARCHAR_255)));

		// people and signing in: accounts, access keys, the access levels, password rules and
		// history, password reset links and shared session links. A person is a user id and a login
		// type together (SMSS_USER.ID and TYPE)

		// one row per account, named by ID and TYPE together (the login type, such as NATIVE or
		// MICROSOFT): the person's name, email and username, a native login's password hash and
		// salt, whether they are an admin, a publisher (who can create engines and projects where
		// only publishers can) and an exporter (who can export where only exporters can), when the
		// account was made and last signed in, when its password was last reset, whether it is
		// locked, and their phone. Each sign in checks the lock and records LASTLOGIN, and an
		// account whose last sign in is older than PASSWORD_RULES.DAYS_TO_LOCK is locked
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
				Pair.with("COUNTRYCODE", VARCHAR_255)));

		// one row per access key a person creates so programs can sign in as them: the owner's id
		// and login type (USERID and TYPE), the access key, the secret key's hash and salt, the
		// key's name and description, and when it was made and last used. A request that signs in
		// with an access key and its secret key is matched to its owner here
		// (SecurityUserAccessKeyUtils)
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

		// the access levels, seeded at startup: 1 OWNER (Author in the UI), 2 EDIT and 3 READ_ONLY
		// (AccessPermissionEnum). The PERMISSION column of each grant and access request table
		// holds one of these ids, and reads join here for the level's name
		addTable("PERMISSION", Arrays.asList(
				Pair.with("ID", INTEGER_DATATYPE_NAME),
				Pair.with("NAME", VARCHAR_255)));

		// the one row of password rules, seeded with defaults when the table is empty: the minimum
		// length, the kinds of characters required, how many days a password lasts and whether an
		// expired one needs an admin to reset it, whether people may change their own, how many
		// earlier passwords cannot be used again, and after how many days without a sign in an
		// account is locked, with how many days of warning. PasswordRequirements reads it, and
		// AdminResetPasswordRules reads it again after a change
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

		// a native login's earlier passwords: one row per password set, with the user id and login
		// type, the hash and salt, and when it was set. Setting a password adds a row and keeps
		// only the newest PASSWORD_RULES.PASS_REUSE_COUNT, and a new password that matches one of
		// them is refused
		addTable("PASSWORD_HISTORY", Arrays.asList(
				Pair.with("ID", VARCHAR_255),
				Pair.with("USERID", VARCHAR_255),
				Pair.with("TYPE", VARCHAR_255),
				Pair.with("PASSWORD", VARCHAR_255),
				Pair.with("SALT", VARCHAR_255),
				Pair.with("DATE_ADDED", TIMESTAMP_DATATYPE_NAME)));

		// one row per password reset link: the account's email and login type, the token the
		// emailed link carries, and when it was made. The link works for 15 minutes, for native,
		// LDAP and LinOTP logins (SecurityPasswordResetUtils)
		addTable("PASSWORD_RESET", Arrays.asList(
				Pair.with("EMAIL", VARCHAR_255),
				Pair.with("TYPE", VARCHAR_255),
				Pair.with("TOKEN", VARCHAR_255),
				Pair.with("DATE_ADDED", TIMESTAMP_DATATYPE_NAME)));

		// one row per share token made by GetShareSessionToken or GetShareAuthToken: the token, the
		// session and route it shares, whether it shares the session itself or only the sign in,
		// the user's id and login type, when it was made, and when it was used and whether that use
		// was accepted. A token is used once, within 5 minutes (SecurityShareSessionUtils)
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

		// access requests: a person asking for a level on an engine, a project or an insight.
		// Approving one saves the access in a permission batch and denying one changes no one's
		// access; both go through PermissionBatchUtils and record the decision on the request

		// one row per request for access to an engine: who it is for (REQUEST_USERID and
		// REQUEST_TYPE), when, the level asked (PERMISSION) and why, who submitted it
		// (SUBMITTED_BY_USERID and SUBMITTED_BY_TYPE: the person, or someone passing on their
		// project access to the project's dependencies), and who decided it and when.
		// APPROVER_DECISION is NEW_REQUEST while it waits, then APPROVED, DENIED, WITHDRAWN when
		// the person withdraws it, or OLD once they ask again
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

		// one row per request for access to a project (PROJECTID), with the same columns and
		// decisions as ENGINEACCESSREQUEST
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

		// one row per request for access to an insight (PROJECTID and INSIGHTID), with the same
		// columns as ENGINEACCESSREQUEST, approved or denied the same way
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

		// the metadata values set on a person: one row per value for a login (USERID and TYPE),
		// such as their default models; SetUserMetadata and AdminSetUserMetadata accept only the
		// keys in USERMETAKEYS. A sign in saves the values its login brings, or else loads the
		// stored ones into the user
		addTable("USERMETA", Arrays.asList(
				Pair.with("USERID", VARCHAR_255),
				Pair.with("TYPE", VARCHAR_255),
				Pair.with("METAKEY", VARCHAR_255),
				Pair.with("METAVALUE", CLOB_DATATYPE_NAME),
				Pair.with("METAORDER", INTEGER_DATATYPE_NAME)));

		// teams: a team is an id and a type together. A login provider's groups (TYPE the
		// provider's group type) come with each sign in, and a custom team (TYPE CUSTOM) keeps its
		// members and managers in the tables below. Teams are given access through
		// GROUPENGINEPERMISSION, GROUPPROJECTPERMISSION and GROUPINSIGHTPERMISSION

		// one row per team an admin registers: its ID and TYPE, a description, and who created it
		// (USERID and USERIDTYPE) and when. The admin who creates a custom team becomes its first
		// manager (AdminSecurityGroupUtils.addGroup)
		addTable("SMSS_GROUP", Arrays.asList(
				Pair.with("ID", VARCHAR_255),
				Pair.with("TYPE", VARCHAR_255),
				Pair.with("DESCRIPTION", CLOB_DATATYPE_NAME),
				Pair.with("DATEADDED", TIMESTAMP_DATATYPE_NAME),
				Pair.with("USERID", VARCHAR_255),
				Pair.with("USERIDTYPE", VARCHAR_255)));

		// the members of custom teams: one row per member, the team by its id alone (GROUPID) and
		// the person by USERID and TYPE, with when they were added, by whom, and when their
		// membership ends (ENDDATE; ExpiredAccessRemoval removes the row once that passes).
		// Permission checks and model usage limits find a person's custom teams here
		addTable("CUSTOMGROUPASSIGNMENT", Arrays.asList(
				Pair.with("GROUPID", VARCHAR_255),
				Pair.with("USERID", VARCHAR_255),
				Pair.with("TYPE", VARCHAR_255),
				Pair.with("DATEADDED", TIMESTAMP_DATATYPE_NAME),
				Pair.with("ENDDATE", TIMESTAMP_DATATYPE_NAME),
				Pair.with("PERMISSIONGRANTEDBY", VARCHAR_255),
				Pair.with("PERMISSIONGRANTEDBYTYPE", VARCHAR_255)));

		// the managers of custom teams: one row per manager, the team by GROUPID and the person by
		// USERID and TYPE, with who made them a manager and when. Outside admin mode, only a team's
		// managers change its members and managers
		addTable("GROUPMANAGERS", Arrays.asList(
				Pair.with("GROUPID", VARCHAR_255),
				Pair.with("USERID", VARCHAR_255),
				Pair.with("TYPE", VARCHAR_255),
				Pair.with("DATEADDED", TIMESTAMP_DATATYPE_NAME),
				Pair.with("PERMISSIONGRANTEDBY", VARCHAR_255),
				Pair.with("PERMISSIONGRANTEDBYTYPE", VARCHAR_255)));

		// a team's grant on an engine: the team (ID and TYPE), the level, who gave it and when, and
		// when it ends. On a model, its Team Limit is in MODELUSAGELIMIT (LIMITTYPE TEAM), and
		// DATEADDED, which a change of level keeps, sets the order the teams are billed in
		addTable("GROUPENGINEPERMISSION", Arrays.asList(
				Pair.with("ID", VARCHAR_255),
				Pair.with("TYPE", VARCHAR_255),
				Pair.with("ENGINEID", VARCHAR_255),
				Pair.with("PERMISSION", INTEGER_DATATYPE_NAME),
				Pair.with("DATEADDED", TIMESTAMP_DATATYPE_NAME),
				Pair.with("ENDDATE", TIMESTAMP_DATATYPE_NAME),
				Pair.with("PERMISSIONGRANTEDBY", VARCHAR_255),
				Pair.with("PERMISSIONGRANTEDBYTYPE", VARCHAR_255)));

		// a permission an engine gives an app or another engine, one row per engine and grantee,
		// kept unique by an index. ENGINEID and ENGINETYPE are the engine giving it and its catalog
		// type; GRANTEEENGINEID and GRANTEEENGINETYPE are the grantee, PROJECT for an app or else
		// the other engine's catalog type. The level is READ_ONLY or EDIT, with who gave it, when
		// and when it ends. The app's custom reactors, or the other engine's own code, use it in
		// place of the signed in user's permission, read through
		// SecurityEngineAccessUtils.getGranteePermission. On a model it can carry a usage limit in
		// MODELUSAGELIMIT (LIMITTYPE ENGINE). Only the engine's owners, and admin mode, change it
		addTable("ENGINE2ENGINEPERMISSION", Arrays.asList(
				Pair.with("ENGINEID", VARCHAR_255),
				Pair.with("ENGINETYPE", VARCHAR_255),
				Pair.with("GRANTEEENGINEID", VARCHAR_255),
				Pair.with("GRANTEEENGINETYPE", VARCHAR_255),
				Pair.with("PERMISSION", INTEGER_DATATYPE_NAME),
				Pair.with("DATEADDED", TIMESTAMP_DATATYPE_NAME),
				Pair.with("ENDDATE", TIMESTAMP_DATATYPE_NAME),
				Pair.with("PERMISSIONGRANTEDBY", VARCHAR_255),
				Pair.with("PERMISSIONGRANTEDBYTYPE", VARCHAR_255)));

		// a team's grant on a project, as GROUPENGINEPERMISSION is on an engine: the team (ID and
		// TYPE), the level, who gave it and when, and when it ends
		addTable("GROUPPROJECTPERMISSION", Arrays.asList(
				Pair.with("ID", VARCHAR_255),
				Pair.with("TYPE", VARCHAR_255),
				Pair.with("PROJECTID", VARCHAR_255),
				Pair.with("PERMISSION", INTEGER_DATATYPE_NAME),
				Pair.with("DATEADDED", TIMESTAMP_DATATYPE_NAME),
				Pair.with("ENDDATE", TIMESTAMP_DATATYPE_NAME),
				Pair.with("PERMISSIONGRANTEDBY", VARCHAR_255),
				Pair.with("PERMISSIONGRANTEDBYTYPE", VARCHAR_255)));

		// a team's grant on an insight (PROJECTID and INSIGHTID), as GROUPENGINEPERMISSION is on an
		// engine: the team (ID and TYPE), the level, who gave it and when, and when it ends
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

		// external connectors: the OAuth clients admins add so people can sign in to Jira,
		// Salesforce and ServiceNow, the GitHub App and the projects linked to repositories, and
		// the Microsoft sign ins and change notification subscriptions that work without a session.
		// They are read and written only through SecurityExternalConnectorsUtils, with the admin
		// writes in SecurityAdminExternalConnectorsUtils

		// one row per Jira OAuth client an admin adds (AdminJiraInsertConnections): its id, an
		// alias unique among them, the client id and secret, the scopes it asks for, and the URL
		// that reads the signed in person's profile. GetJiraConnections lists the ids and aliases,
		// and signing in to Jira uses the client of the connection picked
		addTable("JIRA_CONNECTIONS", Arrays.asList(
				Pair.with("ID", VARCHAR_255),
				Pair.with("ALIAS", VARCHAR_255),
				Pair.with("CLIENTID", VARCHAR_255),
				Pair.with("CLIENTSECRET", VARCHAR_255),
				Pair.with("SCOPE", "VARCHAR(1000)"),
				Pair.with("USERPROFILEURL", VARCHAR_255)));

		// one row per Salesforce OAuth client an admin adds (AdminSalesforceInsertConnections): its
		// id, an alias unique among them, and the client id and secret. GetSalesforceConnections
		// lists the ids and aliases, and signing in to Salesforce uses the client of the connection
		// picked
		addTable("SALESFORCE_CONNECTIONS", Arrays.asList(
				Pair.with("ID", VARCHAR_255),
				Pair.with("ALIAS", VARCHAR_255),
				Pair.with("CLIENTID", VARCHAR_255),
				Pair.with("CLIENTSECRET", VARCHAR_255)));

		// one row per ServiceNow OAuth client an admin adds (AdminServiceNowInsertConnections): its
		// id, the instance URL, an alias unique among them, the client id and secret, and the URL
		// that reads the signed in person's profile. GetServiceNowConnections lists them without
		// their secrets, and signing in to ServiceNow uses the client of the connection picked
		addTable("SERVICENOW_CONNECTIONS", Arrays.asList(
				Pair.with("ID", VARCHAR_255),
				Pair.with("INSTANCEURL", VARCHAR_255),
				Pair.with("ALIAS", VARCHAR_255),
				Pair.with("CLIENTID", VARCHAR_255),
				Pair.with("CLIENTSECRET", VARCHAR_255),
				Pair.with("USERPROFILEURL", VARCHAR_255)));

		// the GitHub App an admin creates through GitHub's app manifest flow, one row per app keyed
		// by APP_ID: its slug, name, owning account, links and webhook URL, and its client secret,
		// webhook secret and private key. The GitHub service uses the configured app to install on
		// repositories, check that webhook deliveries are signed with its secret, and sign its own
		// requests (GitHubAppClient)
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

		// the GitHub repository a project is linked to, one row per project (PROJECT_ID): the app
		// (APP_ID) and installation that give access, the repository's id and full name, the branch
		// it tracks and an optional subdirectory. A push to that repository and branch syncs the
		// project (GitHubProjectSync), and GitHubPullProject and GitHubPushProject use the link
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

		// the Microsoft sign in a person saves so Collaboration can read their mailbox while they
		// are signed out: one row per Microsoft user id (USER_ID), with their email, the access and
		// refresh tokens and when the access token ends. Through MicrosoftGraphCredentialStore,
		// saving background access writes it and routines, syncs and the new mail webhook read and
		// renew it; forgetting background access removes it
		addTable("MS_GRAPH_CREDENTIAL", Arrays.asList(
				Pair.with("USER_ID", VARCHAR_255),
				Pair.with("USER_EMAIL", VARCHAR_255),
				Pair.with("ACCESS_TOKEN", CLOB_DATATYPE_NAME),
				Pair.with("REFRESH_TOKEN", CLOB_DATATYPE_NAME),
				Pair.with("TOKEN_EXPIRATION", TIMESTAMP_DATATYPE_NAME),
				Pair.with("CREATED_ON", TIMESTAMP_DATATYPE_NAME),
				Pair.with("UPDATED_ON", TIMESTAMP_DATATYPE_NAME)));

		// one row per Microsoft Graph change notification subscription: the id Microsoft gave it,
		// the person it was made for (USER_ID, USER_PROVIDER and USER_EMAIL), the CLIENT_STATE
		// secret each notification echoes back, what it watches (RESOURCE and CHANGE_TYPE), where
		// notifications are posted, when it expires, and that person's Microsoft tokens.
		// MicrosoftGraphSubscriptionRegistry keeps it, so any server that receives a notification
		// finds the subscription here and acts as that person
		addTable("MS_GRAPH_SUBSCRIPTION", Arrays.asList(
				Pair.with("SUBSCRIPTION_ID", VARCHAR_255),
				Pair.with("USER_ID", VARCHAR_255),
				Pair.with("USER_PROVIDER", VARCHAR_255),
				Pair.with("USER_EMAIL", VARCHAR_255),
				Pair.with("CLIENT_STATE", VARCHAR_255),
				Pair.with("RESOURCE", VARCHAR_500),
				Pair.with("CHANGE_TYPE", VARCHAR_255),
				Pair.with("NOTIFICATION_URL", VARCHAR_500),
				Pair.with("EXPIRATION", TIMESTAMP_DATATYPE_NAME),
				Pair.with("ACCESS_TOKEN", CLOB_DATATYPE_NAME),
				Pair.with("REFRESH_TOKEN", CLOB_DATATYPE_NAME),
				Pair.with("TOKEN_EXPIRATION", TIMESTAMP_DATATYPE_NAME),
				Pair.with("CREATED_ON", TIMESTAMP_DATATYPE_NAME),
				Pair.with("UPDATED_ON", TIMESTAMP_DATATYPE_NAME)));

		// ENGINEMETAKEYS, PROJECTMETAKEYS, INSIGHTMETAKEYS and USERMETAKEYS, with the same columns:
		// the metadata keys ENGINEMETA, PROJECTMETA, INSIGHTMETA and USERMETA may hold, one row per
		// key, with whether it takes one value or several (SINGLEMULTI), its place in the list, how
		// the UI shows it (DISPLAYOPTIONS, such as textarea or select-box) and the choices of a
		// select box (DEFAULTVALUES). Startup seeds the engine, project and insight tables with
		// description, markdown, tag, domain, data classification and data restrictions, and
		// USERMETAKEYS with the platform's default model and playground keys; admins replace the
		// options with the AdminSet*MetakeyOptions reactors
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
		owler.addRelation("ENGINE", "MODELMETADATA", "ENGINE.ENGINEID.MODELMETADATA.ENGINEID");
		owler.addRelation("ENGINE", "ENGINEPERMISSION", "ENGINE.ENGINEID.ENGINEPERMISSION.ENGINEID");
		owler.addRelation("ENGINE", "USERENGINEPREFERENCE", "ENGINE.ENGINEID.USERENGINEPREFERENCE.ENGINEID");
		owler.addRelation("ENGINE", "MODELUSAGELIMIT", "ENGINE.ENGINEID.MODELUSAGELIMIT.ENGINEID");
		owler.addRelation("MODELUSAGELIMIT", "MODELUSAGELIMITRULE", "MODELUSAGELIMIT.ID.MODELUSAGELIMITRULE.LIMITID");
		owler.addRelation("ENGINE", "WORKSPACEENGINE", "ENGINE.ENGINEID.WORKSPACEENGINE.ENGINEID");

		owler.addRelation("PROJECT", "ASSETENGINE", "PROJECT.PROJECTID.ASSETENGINE.PROJECTID");
		owler.addRelation("PROJECT", "PROJECTMETA", "PROJECT.PROJECTID.PROJECTMETA.PROJECTID");
		owler.addRelation("PROJECT", "INSIGHT", "PROJECT.PROJECTID.INSIGHT.PROJECTID");
		owler.addRelation("PROJECT", "USERINSIGHTPERMISSION", "PROJECT.PROJECTID.USERINSIGHTPERMISSION.PROJECTID");
		owler.addRelation("PROJECT", "PROJECTPERMISSION", "PROJECT.PROJECTID.PROJECTPERMISSION.PROJECTID");
		owler.addRelation("PROJECT", "USERPROJECTPREFERENCE", "PROJECT.PROJECTID.USERPROJECTPREFERENCE.PROJECTID");

		owler.addRelation("INSIGHT", "USERINSIGHTPERMISSION", "INSIGHT.INSIGHTID.USERINSIGHTPERMISSION.INSIGHTID");
		owler.addRelation("INSIGHT", "USERINSIGHTPERMISSION", "INSIGHT.PROJECTID.USERINSIGHTPERMISSION.PROJECTID");
		owler.addRelation("INSIGHT", "USERINSIGHTPREFERENCE", "INSIGHT.INSIGHTID.USERINSIGHTPREFERENCE.INSIGHTID");
		owler.addRelation("INSIGHT", "USERINSIGHTPREFERENCE", "INSIGHT.PROJECTID.USERINSIGHTPREFERENCE.PROJECTID");

		owler.addRelation("SMSS_USER", "USERINSIGHTPERMISSION", "SMSS_USER.ID.USERINSIGHTPERMISSION.USERID");
		owler.addRelation("SMSS_USER", "ENGINEPERMISSION", "SMSS_USER.ID.ENGINEPERMISSION.USERID");
		owler.addRelation("SMSS_USER", "PROJECTPERMISSION", "SMSS_USER.ID.PROJECTPERMISSION.USERID");
		owler.addRelation("SMSS_USER", "USERENGINEPREFERENCE", "SMSS_USER.ID.USERENGINEPREFERENCE.USERID");
		owler.addRelation("SMSS_USER", "USERPROJECTPREFERENCE", "SMSS_USER.ID.USERPROJECTPREFERENCE.USERID");
		owler.addRelation("SMSS_USER", "USERINSIGHTPREFERENCE", "SMSS_USER.ID.USERINSIGHTPREFERENCE.USERID");
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

		owler.addRelation("SMSS_USER", "GROUPMANAGERS", "SMSS_USER.ID.GROUPMANAGERS.USERID");
		owler.addRelation("SMSS_GROUP", "GROUPMANAGERS", "SMSS_GROUP.ID.GROUPMANAGERS.GROUPID");

		owler.addRelation("SMSS_GROUP", "GROUPENGINEPERMISSION", "SMSS_GROUP.ID.GROUPENGINEPERMISSION.ID");
		owler.addRelation("SMSS_GROUP", "GROUPENGINEPERMISSION", "SMSS_GROUP.TYPE.GROUPENGINEPERMISSION.TYPE");
		owler.addRelation("ENGINE", "GROUPENGINEPERMISSION", "ENGINE.ENGINEID.GROUPENGINEPERMISSION.ENGINEID");

		owler.addRelation("ENGINE", "ENGINE2ENGINEPERMISSION", "ENGINE.ENGINEID.ENGINE2ENGINEPERMISSION.ENGINEID");

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

		// microsoft graph subscription joins
		owler.addRelation("SMSS_USER", "MS_GRAPH_SUBSCRIPTION", "SMSS_USER.ID.MS_GRAPH_SUBSCRIPTION.USER_ID");
	}

	@Override
	protected boolean additionalRemakeChecks(IDatabaseEngine engine) {
		for (Pair<String, String> removed : REMOVED_COLUMNS) {
			if (!columnMissing(engine, removed.getValue0(), removed.getValue1())) {
				return true;
			}
		}
		for (String[] requiredRel : relationshipsRequired) {
			if (relationMissing(engine, requiredRel[0], requiredRel[1], requiredRel[2])) {
				return true;
			}
		}
		return false;
	}

}
