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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;

import prerna.engine.api.IRDBMSEngine;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.util.ConnectionUtils;
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;

public class SecurityExternalConnectorsUtils extends AbstractSecurityUtils {

	private static final Logger classLogger = LogManager.getLogger(SecurityExternalConnectorsUtils.class);

	private static final String GITHUB_APP_TABLE = "GITHUB_APP";
	private static final String GITHUB_PROJECT_LINK_TABLE = "GITHUB_PROJECT_LINK";
	private static final String MS_GRAPH_SUBSCRIPTION_TABLE = "MS_GRAPH_SUBSCRIPTION";

	/**
	 * Retrieves the ID and alias for each configured Salesforce connection.
	 *
	 * <p>
	 * Queries the {@code SALESFORCE_CONNECTIONS} table in the security database,
	 * returning all available connections with their identifiers and human-readable
	 * aliases.
	 *
	 * @return a list of maps, each containing:
	 *         <ul>
	 *         <li>{@code id} - the unique identifier of the connection</li>
	 *         <li>{@code alias} - the human-readable name of the connection</li>
	 *         </ul>
	 *         Returns an empty list if no connections are configured.
	 */
	public static List<Map<String, Object>> getSalesforceConnections() {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SALESFORCE_CONNECTIONS__ID", "id"));
		qs.addSelector(new QueryColumnSelector("SALESFORCE_CONNECTIONS__ALIAS", "alias"));
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}

	/**
	 * Retrieves the client id and client secret for a specific configured
	 * Salesforce connection
	 * <p>
	 * Queries the {@code SALESFORCE_CONNECTIONS} table in the security database,
	 * returning the client id adn client secret for a specific connection id that
	 * is generated upon insertion into the system.
	 * 
	 * @param connectionId the unique connection id that was added
	 * @return a pair of client id and client secret id
	 */
	public static Pair<String, String> getSalesforceConnectionDetails(String connectionId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SALESFORCE_CONNECTIONS__CLIENTID", "clientid"));
		qs.addSelector(new QueryColumnSelector("SALESFORCE_CONNECTIONS__CLIENTSECRET", "clientsecret"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SALESFORCE_CONNECTIONS__ID", "==", connectionId));
		List<Map<String, Object>> resultList = QueryExecutionUtility.flushRsToMap(securityDb, qs);
		if (resultList.isEmpty()) {
			throw new IllegalArgumentException("Connection id " + connectionId + " is not valid");
		}
		Map<String, Object> result = resultList.get(0);
		return new Pair<String, String>((String) result.get("clientid"), (String) result.get("clientsecret"));
	}

	/**
	 * Inserts or updates the single GitHub App record produced by the GitHub
	 * app-manifest conversion flow.
	 * <p>
	 * The {@code GITHUB_APP} table is keyed on {@code APP_ID}. If a row already
	 * exists for the supplied {@code appId} it is updated in place (preserving
	 * {@code CREATED_ON}); otherwise a new row is inserted. The secret columns
	 * ({@code CLIENT_SECRET}, {@code WEBHOOK_SECRET}, {@code PRIVATE_KEY}) are
	 * stored as CLOBs.
	 *
	 * @param appId         GitHub numeric app id (JWT issuer / natural key)
	 * @param slug          app slug used to build the install URL
	 * @param appName       app display name
	 * @param ownerLogin    account/org login that owns the app
	 * @param htmlUrl       link to the app on GitHub
	 * @param webhookUrl    configured webhook url
	 * @param clientId      OAuth client id
	 * @param clientSecret  OAuth client secret
	 * @param webhookSecret secret used to verify webhook signatures
	 * @param privateKey    PEM private key used to sign the App JWT
	 */
	public static void upsertGitHubApp(long appId, String slug, String appName, String ownerLogin, String htmlUrl,
			String webhookUrl, String clientId, String clientSecret, String webhookSecret, String privateKey) {
		if (slug == null || (slug = slug.trim()).isEmpty()) {
			throw new IllegalArgumentException("Slug must not be empty.");
		}

		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		boolean exists = getGitHubApp(appId) != null;

		Connection conn = null;
		try {
			conn = securityDb.getConnection();
			Timestamp now = Utility.getCurrentSqlTimestampUTC();
			if (exists) {
				String sql = "UPDATE " + GITHUB_APP_TABLE + " SET SLUG = ?, APP_NAME = ?, OWNER_LOGIN = ?, "
						+ "HTML_URL = ?, WEBHOOK_URL = ?, CLIENT_ID = ?, CLIENT_SECRET = ?, WEBHOOK_SECRET = ?, "
						+ "PRIVATE_KEY = ?, UPDATED_ON = ? WHERE APP_ID = ?";
				try (PreparedStatement ps = conn.prepareStatement(sql)) {
					int i = 1;
					ps.setString(i++, slug);
					ps.setString(i++, appName);
					ps.setString(i++, ownerLogin);
					ps.setString(i++, htmlUrl);
					ps.setString(i++, webhookUrl);
					ps.setString(i++, clientId);
					securityDb.getQueryUtil().handleInsertionOfClob(conn, ps, clientSecret, i++, securityGson);
					securityDb.getQueryUtil().handleInsertionOfClob(conn, ps, webhookSecret, i++, securityGson);
					securityDb.getQueryUtil().handleInsertionOfClob(conn, ps, privateKey, i++, securityGson);
					ps.setTimestamp(i++, now);
					ps.setLong(i++, appId);
					ps.executeUpdate();
					if (!conn.getAutoCommit()) {
						conn.commit();
					}
				}
			} else {
				String sql = "INSERT INTO " + GITHUB_APP_TABLE + " (APP_ID, SLUG, APP_NAME, OWNER_LOGIN, HTML_URL, "
						+ "WEBHOOK_URL, CLIENT_ID, CLIENT_SECRET, WEBHOOK_SECRET, PRIVATE_KEY, CREATED_ON, UPDATED_ON) "
						+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
				try (PreparedStatement ps = conn.prepareStatement(sql)) {
					int i = 1;
					ps.setLong(i++, appId);
					ps.setString(i++, slug);
					ps.setString(i++, appName);
					ps.setString(i++, ownerLogin);
					ps.setString(i++, htmlUrl);
					ps.setString(i++, webhookUrl);
					ps.setString(i++, clientId);
					securityDb.getQueryUtil().handleInsertionOfClob(conn, ps, clientSecret, i++, securityGson);
					securityDb.getQueryUtil().handleInsertionOfClob(conn, ps, webhookSecret, i++, securityGson);
					securityDb.getQueryUtil().handleInsertionOfClob(conn, ps, privateKey, i++, securityGson);
					ps.setTimestamp(i++, now);
					ps.setTimestamp(i++, now);
					ps.execute();
					if (!conn.getAutoCommit()) {
						conn.commit();
					}
				}
			}
		} catch (Exception e) {
			classLogger.error("Failed to save GitHub app.", e);
			throw new SemossPixelException("Unable to save GitHub app: " + e.getMessage(), e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, conn);
		}
	}

	/**
	 * Retrieves the single configured GitHub App.
	 * <p>
	 * Queries the {@code GITHUB_APP} table in the security database and returns the
	 * first (and, today, only) configured app.
	 *
	 * @return a map of the GitHub App fields, or {@code null} if no app is
	 *         configured
	 */
	public static Map<String, Object> getGitHubApp() {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = buildGitHubAppSelect();
		qs.setLimit(1);
		List<Map<String, Object>> resultList = QueryExecutionUtility.flushRsToMap(securityDb, qs);
		return resultList.isEmpty() ? null : resultList.get(0);
	}

	/**
	 * Retrieves a configured GitHub App by its numeric app id.
	 *
	 * @param appId GitHub numeric app id
	 * @return a map of the GitHub App fields, or {@code null} if no app exists for
	 *         the supplied id
	 */
	public static Map<String, Object> getGitHubApp(long appId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = buildGitHubAppSelect();
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GITHUB_APP__APP_ID", "==", appId));
		List<Map<String, Object>> resultList = QueryExecutionUtility.flushRsToMap(securityDb, qs);
		return resultList.isEmpty() ? null : resultList.get(0);
	}

	/**
	 * Builds the common selector list for {@code GITHUB_APP} reads.
	 *
	 * @return a {@link SelectQueryStruct} selecting all GitHub App columns
	 */
	private static SelectQueryStruct buildGitHubAppSelect() {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("GITHUB_APP__APP_ID", "appId"));
		qs.addSelector(new QueryColumnSelector("GITHUB_APP__SLUG", "slug"));
		qs.addSelector(new QueryColumnSelector("GITHUB_APP__APP_NAME", "appName"));
		qs.addSelector(new QueryColumnSelector("GITHUB_APP__OWNER_LOGIN", "ownerLogin"));
		qs.addSelector(new QueryColumnSelector("GITHUB_APP__HTML_URL", "htmlUrl"));
		qs.addSelector(new QueryColumnSelector("GITHUB_APP__WEBHOOK_URL", "webhookUrl"));
		qs.addSelector(new QueryColumnSelector("GITHUB_APP__CLIENT_ID", "clientId"));
		qs.addSelector(new QueryColumnSelector("GITHUB_APP__CLIENT_SECRET", "clientSecret"));
		qs.addSelector(new QueryColumnSelector("GITHUB_APP__WEBHOOK_SECRET", "webhookSecret"));
		qs.addSelector(new QueryColumnSelector("GITHUB_APP__PRIVATE_KEY", "privateKey"));
		qs.addSelector(new QueryColumnSelector("GITHUB_APP__CREATED_ON", "createdOn"));
		qs.addSelector(new QueryColumnSelector("GITHUB_APP__UPDATED_ON", "updatedOn"));
		qs.addOrderBy("GITHUB_APP__APP_ID");
		return qs;
	}

	/**
	 * Inserts or updates the GitHub link for a Semoss project.
	 * <p>
	 * The {@code GITHUB_PROJECT_LINK} table is keyed on {@code PROJECT_ID} (one
	 * repo per project). If a link already exists for the supplied
	 * {@code projectId} it is updated in place (preserving {@code CREATED_ON});
	 * otherwise a new row is inserted.
	 *
	 * @param projectId      Semoss project id
	 * @param appId          GitHub app id the installation belongs to
	 * @param installationId installation id granting repo access
	 * @param repoId         GitHub numeric repo id (stable across renames)
	 * @param repoFullName   owner/repo full name (human-readable)
	 */
	public static void upsertGitHubProjectLink(String projectId, long appId, long installationId, long repoId,
			String repoFullName, String branch, String subdir) {
		if (projectId == null || (projectId = projectId.trim()).isEmpty()) {
			throw new IllegalArgumentException("Project id must not be empty.");
		}
		if (branch == null || (branch = branch.trim()).isEmpty()) {
			throw new IllegalArgumentException("Branch must not be empty.");
		}
		// normalize subdir: blank/null becomes null (full-repo sync)
		String normalizedSubdir = (subdir != null && !subdir.trim().isEmpty()) ? subdir.trim() : null;
		normalizedSubdir = Utility.normalizePath(normalizedSubdir);

		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		boolean exists = getGitHubProjectLink(projectId) != null;

		Connection conn = null;
		try {
			conn = securityDb.getConnection();
			Timestamp now = Utility.getCurrentSqlTimestampUTC();
			if (exists) {
				String sql = "UPDATE " + GITHUB_PROJECT_LINK_TABLE + " SET APP_ID = ?, INSTALLATION_ID = ?, "
						+ "REPO_ID = ?, REPO_FULL_NAME = ?, BRANCH = ?, SUBDIR = ?, UPDATED_ON = ? WHERE PROJECT_ID = ?";
				try (PreparedStatement ps = conn.prepareStatement(sql)) {
					int i = 1;
					ps.setLong(i++, appId);
					ps.setLong(i++, installationId);
					ps.setLong(i++, repoId);
					ps.setString(i++, repoFullName);
					ps.setString(i++, branch);
					if (normalizedSubdir == null || normalizedSubdir.isEmpty()) {
						ps.setNull(i++, java.sql.Types.VARCHAR);
					} else {
						ps.setString(i++, normalizedSubdir);
					}
					ps.setTimestamp(i++, now);
					ps.setString(i++, projectId);
					ps.executeUpdate();
					if (!conn.getAutoCommit()) {
						conn.commit();
					}
				}
			} else {
				String sql = "INSERT INTO " + GITHUB_PROJECT_LINK_TABLE + " (PROJECT_ID, APP_ID, INSTALLATION_ID, "
						+ "REPO_ID, REPO_FULL_NAME, BRANCH, SUBDIR, CREATED_ON, UPDATED_ON) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
				try (PreparedStatement ps = conn.prepareStatement(sql)) {
					int i = 1;
					ps.setString(i++, projectId);
					ps.setLong(i++, appId);
					ps.setLong(i++, installationId);
					ps.setLong(i++, repoId);
					ps.setString(i++, repoFullName);
					ps.setString(i++, branch);
					if (normalizedSubdir == null || normalizedSubdir.isEmpty()) {
						ps.setNull(i++, java.sql.Types.VARCHAR);
					} else {
						ps.setString(i++, normalizedSubdir);
					}
					ps.setTimestamp(i++, now);
					ps.setTimestamp(i++, now);
					ps.execute();
					if (!conn.getAutoCommit()) {
						conn.commit();
					}
				}
			}
		} catch (Exception e) {
			classLogger.error("Failed to save GitHub project link.", e);
			throw new SemossPixelException("Unable to save GitHub project link: " + e.getMessage(), e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, conn);
		}
	}

	/**
	 * Updates the tracked branch for a project's GitHub link. This is the branch
	 * the push webhook syncs the project's local repo to.
	 *
	 * @param projectId the project whose link to update
	 * @param branch    the branch to track (e.g. "main")
	 */
	public static void updateGitHubProjectLinkBranch(String projectId, String branch) {
		if (projectId == null || (projectId = projectId.trim()).isEmpty()) {
			throw new IllegalArgumentException("Project id must not be empty.");
		}
		if (branch == null || (branch = branch.trim()).isEmpty()) {
			throw new IllegalArgumentException("Branch must not be empty.");
		}

		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		Connection conn = null;
		try {
			conn = securityDb.getConnection();
			String sql = "UPDATE " + GITHUB_PROJECT_LINK_TABLE + " SET BRANCH = ?, UPDATED_ON = ? WHERE PROJECT_ID = ?";
			try (PreparedStatement ps = conn.prepareStatement(sql)) {
				ps.setString(1, branch);
				ps.setTimestamp(2, Utility.getCurrentSqlTimestampUTC());
				ps.setString(3, projectId);
				ps.executeUpdate();
				if (!conn.getAutoCommit()) {
					conn.commit();
				}
			}
		} catch (Exception e) {
			classLogger.error("Failed to update GitHub project link branch.", e);
			throw new SemossPixelException("Unable to update GitHub project link branch: " + e.getMessage(), e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, conn);
		}
	}

	/**
	 * Retrieves the GitHub link for a specific Semoss project.
	 *
	 * @param projectId Semoss project id
	 * @return a map of the link fields, or {@code null} if the project has no link
	 */
	public static Map<String, Object> getGitHubProjectLink(String projectId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = buildGitHubProjectLinkSelect();
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GITHUB_PROJECT_LINK__PROJECT_ID", "==", projectId));
		List<Map<String, Object>> resultList = QueryExecutionUtility.flushRsToMap(securityDb, qs);
		return resultList.isEmpty() ? null : resultList.get(0);
	}

	/**
	 * Retrieves the GitHub link for a specific repo, used to route incoming
	 * webhooks to the owning project. Matching is done on the stable
	 * {@code REPO_ID} (which survives repo renames).
	 *
	 * @param repoId GitHub numeric repo id
	 * @return a map of the link fields, or {@code null} if no project is linked to
	 *         the repo
	 */
	public static Map<String, Object> getGitHubProjectLinkByRepoId(long repoId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = buildGitHubProjectLinkSelect();
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GITHUB_PROJECT_LINK__REPO_ID", "==", repoId));
		List<Map<String, Object>> resultList = QueryExecutionUtility.flushRsToMap(securityDb, qs);
		return resultList.isEmpty() ? null : resultList.get(0);
	}

	/**
	 * Retrieves every project link for a repository. A repository can feed more
	 * than one project (each tracking the same or a different branch), so push
	 * webhooks must fan out to all of them - prefer this over
	 * {@link #getGitHubProjectLinkByRepoId(long)} when routing repo events.
	 *
	 * @param repoId GitHub numeric repo id
	 * @return a list of link maps; empty if no project is linked to the repo
	 */
	public static List<Map<String, Object>> getGitHubProjectLinksByRepoId(long repoId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = buildGitHubProjectLinkSelect();
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GITHUB_PROJECT_LINK__REPO_ID", "==", repoId));
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}

	/**
	 * Retrieves all GitHub project links associated with an installation, used to
	 * route installation-level webhook events (e.g. installation removed).
	 *
	 * @param installationId GitHub installation id
	 * @return a list of link maps; empty if none are associated with the
	 *         installation
	 */
	public static List<Map<String, Object>> getGitHubProjectLinksByInstallationId(long installationId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = buildGitHubProjectLinkSelect();
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("GITHUB_PROJECT_LINK__INSTALLATION_ID", "==", installationId));
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}

	/**
	 * Retrieves every project to GitHub repository link configured on this
	 * instance, used by the admin settings UI to show which projects are connected
	 * to GitHub.
	 *
	 * @return a list of link maps; empty if no projects are linked
	 */
	public static List<Map<String, Object>> getAllGitHubProjectLinks() {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = buildGitHubProjectLinkSelect();
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}

	public static void deleteGitHubApp(long appId) throws SQLException {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		Connection conn = null;
		try {
			conn = securityDb.getConnection();
			try (PreparedStatement ps = conn
					.prepareStatement("DELETE FROM " + GITHUB_PROJECT_LINK_TABLE + " WHERE APP_ID = ?")) {
				ps.setLong(1, appId);
				ps.executeUpdate();
			}
			try (PreparedStatement ps = conn
					.prepareStatement("DELETE FROM " + GITHUB_APP_TABLE + " WHERE APP_ID = ?")) {
				ps.setLong(1, appId);
				ps.executeUpdate();
			}
			if (!conn.getAutoCommit()) {
				conn.commit();
			}
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, conn);
		}

	}

	/**
	 * Removes the GitHub link for a specific Semoss project.
	 *
	 * @param projectId Semoss project id
	 */
	public static void deleteGitHubProjectLink(String projectId) {
		if (projectId == null || (projectId = projectId.trim()).isEmpty()) {
			throw new IllegalArgumentException("Project id must not be empty.");
		}

		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		Connection conn = null;
		try {
			conn = securityDb.getConnection();
			String sql = "DELETE FROM " + GITHUB_PROJECT_LINK_TABLE + " WHERE PROJECT_ID = ?";
			try (PreparedStatement ps = conn.prepareStatement(sql)) {
				ps.setString(1, projectId);
				ps.executeUpdate();
				if (!conn.getAutoCommit()) {
					conn.commit();
				}
			}
		} catch (Exception e) {
			classLogger.error("Failed to delete GitHub project link.", e);
			throw new SemossPixelException("Unable to delete GitHub project link: " + e.getMessage(), e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, conn);
		}
	}

	/**
	 * Builds the common selector list for {@code GITHUB_PROJECT_LINK} reads.
	 *
	 * @return a {@link SelectQueryStruct} selecting all project link columns
	 */
	private static SelectQueryStruct buildGitHubProjectLinkSelect() {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("GITHUB_PROJECT_LINK__PROJECT_ID", "projectId"));
		qs.addSelector(new QueryColumnSelector("GITHUB_PROJECT_LINK__APP_ID", "appId"));
		qs.addSelector(new QueryColumnSelector("GITHUB_PROJECT_LINK__INSTALLATION_ID", "installationId"));
		qs.addSelector(new QueryColumnSelector("GITHUB_PROJECT_LINK__REPO_ID", "repoId"));
		qs.addSelector(new QueryColumnSelector("GITHUB_PROJECT_LINK__REPO_FULL_NAME", "repoFullName"));
		qs.addSelector(new QueryColumnSelector("GITHUB_PROJECT_LINK__BRANCH", "branch"));
		qs.addSelector(new QueryColumnSelector("GITHUB_PROJECT_LINK__SUBDIR", "subdir"));
		qs.addSelector(new QueryColumnSelector("GITHUB_PROJECT_LINK__CREATED_ON", "createdOn"));
		qs.addSelector(new QueryColumnSelector("GITHUB_PROJECT_LINK__UPDATED_ON", "updatedOn"));
		return qs;
	}

	/**
	 * Retrieves the ID, alias, instance url and user profile url for each
	 * configured ServiceNow connection.
	 *
	 * <p>
	 * Queries the {@code SERVICENOW_CONNECTIONS} table in the security database,
	 * returning all available connections with their identifiers, human-readable
	 * aliases, instance url and user profile url
	 *
	 * @return a list of maps, each containing:
	 *         <ul>
	 *         <li>{@code id} - the unique identifier of the connection</li>
	 *         <li>{@code alias} - the human-readable name of the connection</li>
	 *         <li>{@code instanceUrl} - the instance url of the connection</li>
	 *         <li>{@code userProfileUrl} - the user profile url of the
	 *         connection</li>
	 *         </ul>
	 *         Returns an empty list if no connections are configured.
	 */
	public static List<Map<String, Object>> getServiceNowConnections() {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SERVICENOW_CONNECTIONS__ID", "id"));
		qs.addSelector(new QueryColumnSelector("SERVICENOW_CONNECTIONS__ALIAS", "alias"));
		qs.addSelector(new QueryColumnSelector("SERVICENOW_CONNECTIONS__INSTANCEURL", "instanceUrl"));
		qs.addSelector(new QueryColumnSelector("SERVICENOW_CONNECTIONS__USERPROFILEURL", "userProfileUrl"));
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}

	/**
	 * Retrieves the instance url, client id, client secret and user profile url for
	 * a specific configured ServiceNow connection
	 * <p>
	 * Queries the {@code SERVICENOW_CONNECTIONS} table in the security database,
	 * returning the instance url, client id, client secret and user profile url for
	 * a specific connection id that is generated upon insertion into the system.
	 * 
	 * @param connectionId the unique connection id that was added
	 * @return a map of instance url, client id, client secret and user profile url
	 */
	public static Map<String, String> getServiceNowConnectionDetails(String connectionId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SERVICENOW_CONNECTIONS__INSTANCEURL", "instanceUrl"));
		qs.addSelector(new QueryColumnSelector("SERVICENOW_CONNECTIONS__CLIENTID", "clientid"));
		qs.addSelector(new QueryColumnSelector("SERVICENOW_CONNECTIONS__CLIENTSECRET", "clientsecret"));
		qs.addSelector(new QueryColumnSelector("SERVICENOW_CONNECTIONS__USERPROFILEURL", "userProfileUrl"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SERVICENOW_CONNECTIONS__ID", "==", connectionId));
		List<Map<String, Object>> resultList = QueryExecutionUtility.flushRsToMap(securityDb, qs);
		if (resultList.isEmpty()) {
			throw new IllegalArgumentException("Connection id " + connectionId + " is not valid");
		}
		Map<String, Object> result = resultList.get(0);

		Map<String, String> response = new HashMap<>();
		response.put("instanceUrl", (String) result.get("instanceUrl"));
		response.put("clientId", (String) result.get("clientid"));
		response.put("clientSecret", (String) result.get("clientsecret"));
		response.put("userProfileUrl", (String) result.get("userProfileUrl"));
		return response;
	}

	/**
	 * Retrieves the ID and client id for each configured Jira connection.
	 *
	 * <p>
	 * Queries the {@code JIRA_CONNECTIONS} table in the security database,
	 * returning all available connections with their identifiers.
	 *
	 * @return a list of maps, each containing:
	 *         <ul>
	 *         <li>{@code id} - the unique identifier of the connection</li>
	 *         <li>{@code alias} - the human-readable name of the connection</li>
	 *         </ul>
	 *         Returns an empty list if no connections are configured.
	 */
	public static List<Map<String, Object>> getJiraConnections() {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("JIRA_CONNECTIONS__ID", "id"));
		qs.addSelector(new QueryColumnSelector("JIRA_CONNECTIONS__ALIAS", "alias"));
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}

	/**
	 * Retrieves the full details for a specific configured Jira connection.
	 * <p>
	 * Queries the {@code JIRA_CONNECTIONS} table in the security database,
	 * returning the client id, client secret, scope, and user profile URL for a
	 * specific connection id that is generated upon insertion into the system.
	 *
	 * @param connectionId the unique connection id that was added
	 * @return a map containing alias, client id, client secret, scope, and user
	 *         profile URL for the specified connection id
	 * @throws IllegalArgumentException if the provided connection id does not exist
	 *                                  in the database
	 */
	public static Map<String, Object> getJiraConnectionDetails(String connectionId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("JIRA_CONNECTIONS__ALIAS", "alias"));
		qs.addSelector(new QueryColumnSelector("JIRA_CONNECTIONS__CLIENTID", "clientId"));
		qs.addSelector(new QueryColumnSelector("JIRA_CONNECTIONS__CLIENTSECRET", "clientSecret"));
		qs.addSelector(new QueryColumnSelector("JIRA_CONNECTIONS__SCOPE", "scope"));
		qs.addSelector(new QueryColumnSelector("JIRA_CONNECTIONS__USERPROFILEURL", "userInfoUrl"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("JIRA_CONNECTIONS__ID", "==", connectionId));
		List<Map<String, Object>> resultList = QueryExecutionUtility.flushRsToMap(securityDb, qs);
		if (resultList.isEmpty()) {
			throw new IllegalArgumentException("Connection id " + connectionId + " is not valid");
		}
		return resultList.get(0);
	}

	/**
	 * Records a Microsoft Graph change notification subscription, replacing
	 * whatever was held for the same subscription id.
	 *
	 * <p>
	 * The row is what lets any container answer a notification for this
	 * subscription: the client state recognizes the delivery, and the tokens let
	 * that container act as the user the subscription was made for without ever
	 * having seen their session. Both are credentials at rest, kept the same way
	 * the GitHub app's client secret and private key are kept in this database.
	 * </p>
	 *
	 * @param subscriptionId  the id Microsoft Graph gave the subscription
	 * @param userId          id of the user it was created for
	 * @param userProvider    the login provider that id belongs to
	 * @param userEmail       that user's address, for rebuilding their identity
	 * @param clientState     the secret every notification echoes back
	 * @param resource        what the subscription watches
	 * @param changeType      which changes it hears about
	 * @param notificationUrl where Graph posts them
	 * @param expiration      when Graph stops sending, or null when unknown
	 * @param accessToken     the user's Microsoft access token
	 * @param refreshToken    the user's Microsoft refresh token, which is what
	 *                        keeps the subscription usable once the access token
	 *                        runs out
	 * @param tokenExpiration when that access token runs out, or null when unknown
	 */
	public static void upsertMicrosoftGraphSubscription(String subscriptionId, String userId, String userProvider,
			String userEmail, String clientState, String resource, String changeType, String notificationUrl,
			Timestamp expiration, String accessToken, String refreshToken, Timestamp tokenExpiration) {
		if (subscriptionId == null || (subscriptionId = subscriptionId.trim()).isEmpty()) {
			throw new IllegalArgumentException("A subscription id must not be empty.");
		}
		if (userId == null || userId.trim().isEmpty()) {
			throw new IllegalArgumentException("A user id must not be empty.");
		}

		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		Connection conn = null;
		try {
			conn = securityDb.getConnection();
			Timestamp now = Utility.getCurrentSqlTimestampUTC();
			// replacing rather than updating in place: a subscription id is Graph's to
			// issue, so the same id arriving twice is the same subscription being
			// recreated and the old row has nothing worth keeping
			try (PreparedStatement ps = conn
					.prepareStatement("DELETE FROM " + MS_GRAPH_SUBSCRIPTION_TABLE + " WHERE SUBSCRIPTION_ID = ?")) {
				ps.setString(1, subscriptionId);
				ps.executeUpdate();
			}
			String sql = "INSERT INTO " + MS_GRAPH_SUBSCRIPTION_TABLE + " (SUBSCRIPTION_ID, USER_ID, USER_PROVIDER, "
					+ "USER_EMAIL, CLIENT_STATE, RESOURCE, CHANGE_TYPE, NOTIFICATION_URL, EXPIRATION, ACCESS_TOKEN, "
					+ "REFRESH_TOKEN, TOKEN_EXPIRATION, CREATED_ON, UPDATED_ON) "
					+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
			try (PreparedStatement ps = conn.prepareStatement(sql)) {
				int i = 1;
				ps.setString(i++, subscriptionId);
				ps.setString(i++, userId);
				ps.setString(i++, userProvider);
				ps.setString(i++, userEmail);
				ps.setString(i++, clientState);
				ps.setString(i++, resource);
				ps.setString(i++, changeType);
				ps.setString(i++, notificationUrl);
				ps.setTimestamp(i++, expiration);
				securityDb.getQueryUtil().handleInsertionOfClob(conn, ps, accessToken, i++, securityGson);
				securityDb.getQueryUtil().handleInsertionOfClob(conn, ps, refreshToken, i++, securityGson);
				ps.setTimestamp(i++, tokenExpiration);
				ps.setTimestamp(i++, now);
				ps.setTimestamp(i++, now);
				ps.execute();
			}
			if (!conn.getAutoCommit()) {
				conn.commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to save the Microsoft Graph subscription.", e);
			throw new SemossPixelException("Unable to save the Microsoft Graph subscription: " + e.getMessage(), e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, conn);
		}
	}

	/**
	 * Retrieves one Microsoft Graph subscription.
	 *
	 * @param subscriptionId the id a notification carried
	 * @return a map of the subscription fields, or {@code null} when this
	 *         deployment did not create it
	 */
	public static Map<String, Object> getMicrosoftGraphSubscription(String subscriptionId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = buildMicrosoftGraphSubscriptionSelect();
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("MS_GRAPH_SUBSCRIPTION__SUBSCRIPTION_ID", "==", subscriptionId));
		List<Map<String, Object>> resultList = QueryExecutionUtility.flushRsToMap(securityDb, qs);
		return resultList.isEmpty() ? null : resultList.get(0);
	}

	/**
	 * Retrieves the Microsoft Graph subscriptions held for one user.
	 *
	 * @param userId the user to look for
	 * @return their subscriptions, empty when they have none
	 */
	public static List<Map<String, Object>> getMicrosoftGraphSubscriptionsByUser(String userId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = buildMicrosoftGraphSubscriptionSelect();
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("MS_GRAPH_SUBSCRIPTION__USER_ID", "==", userId));
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}

	/**
	 * Builds the common selector list for {@code MS_GRAPH_SUBSCRIPTION} reads.
	 *
	 * @return a {@link SelectQueryStruct} selecting all subscription columns
	 */
	private static SelectQueryStruct buildMicrosoftGraphSubscriptionSelect() {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("MS_GRAPH_SUBSCRIPTION__SUBSCRIPTION_ID", "subscriptionId"));
		qs.addSelector(new QueryColumnSelector("MS_GRAPH_SUBSCRIPTION__USER_ID", "userId"));
		qs.addSelector(new QueryColumnSelector("MS_GRAPH_SUBSCRIPTION__USER_PROVIDER", "userProvider"));
		qs.addSelector(new QueryColumnSelector("MS_GRAPH_SUBSCRIPTION__USER_EMAIL", "userEmail"));
		qs.addSelector(new QueryColumnSelector("MS_GRAPH_SUBSCRIPTION__CLIENT_STATE", "clientState"));
		qs.addSelector(new QueryColumnSelector("MS_GRAPH_SUBSCRIPTION__RESOURCE", "resource"));
		qs.addSelector(new QueryColumnSelector("MS_GRAPH_SUBSCRIPTION__CHANGE_TYPE", "changeType"));
		qs.addSelector(new QueryColumnSelector("MS_GRAPH_SUBSCRIPTION__NOTIFICATION_URL", "notificationUrl"));
		qs.addSelector(new QueryColumnSelector("MS_GRAPH_SUBSCRIPTION__EXPIRATION", "expiration"));
		qs.addSelector(new QueryColumnSelector("MS_GRAPH_SUBSCRIPTION__ACCESS_TOKEN", "accessToken"));
		qs.addSelector(new QueryColumnSelector("MS_GRAPH_SUBSCRIPTION__REFRESH_TOKEN", "refreshToken"));
		qs.addSelector(new QueryColumnSelector("MS_GRAPH_SUBSCRIPTION__TOKEN_EXPIRATION", "tokenExpiration"));
		qs.addSelector(new QueryColumnSelector("MS_GRAPH_SUBSCRIPTION__CREATED_ON", "createdOn"));
		qs.addSelector(new QueryColumnSelector("MS_GRAPH_SUBSCRIPTION__UPDATED_ON", "updatedOn"));
		qs.addOrderBy("MS_GRAPH_SUBSCRIPTION__CREATED_ON");
		return qs;
	}

	/**
	 * Pushes a subscription's recorded expiry back out, after Graph has agreed to
	 * the same.
	 *
	 * @param subscriptionId the subscription that was renewed
	 * @param expiration     when it now expires
	 */
	public static void updateMicrosoftGraphSubscriptionExpiration(String subscriptionId, Timestamp expiration) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		Connection conn = null;
		try {
			conn = securityDb.getConnection();
			String sql = "UPDATE " + MS_GRAPH_SUBSCRIPTION_TABLE
					+ " SET EXPIRATION = ?, UPDATED_ON = ? WHERE SUBSCRIPTION_ID = ?";
			try (PreparedStatement ps = conn.prepareStatement(sql)) {
				ps.setTimestamp(1, expiration);
				ps.setTimestamp(2, Utility.getCurrentSqlTimestampUTC());
				ps.setString(3, subscriptionId);
				ps.executeUpdate();
			}
			if (!conn.getAutoCommit()) {
				conn.commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to record the renewal of Microsoft Graph subscription {}.", subscriptionId, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, conn);
		}
	}

	/**
	 * Writes back the token a container refreshed, so the next container to receive
	 * a notification starts from the fresh one.
	 *
	 * <p>
	 * A refresh token is single use in Microsoft Entra: the refresh that mints a
	 * new access token mints a new refresh token with it and retires the old one.
	 * Not writing the new pair back would leave every other container holding a
	 * refresh token that has already been spent.
	 * </p>
	 *
	 * @param subscriptionId  the subscription whose user was refreshed
	 * @param accessToken     the new access token
	 * @param refreshToken    the new refresh token
	 * @param tokenExpiration when the new access token runs out
	 */
	public static void updateMicrosoftGraphSubscriptionToken(String subscriptionId, String accessToken,
			String refreshToken, Timestamp tokenExpiration) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		Connection conn = null;
		try {
			conn = securityDb.getConnection();
			String sql = "UPDATE " + MS_GRAPH_SUBSCRIPTION_TABLE + " SET ACCESS_TOKEN = ?, REFRESH_TOKEN = ?, "
					+ "TOKEN_EXPIRATION = ?, UPDATED_ON = ? WHERE SUBSCRIPTION_ID = ?";
			try (PreparedStatement ps = conn.prepareStatement(sql)) {
				int i = 1;
				securityDb.getQueryUtil().handleInsertionOfClob(conn, ps, accessToken, i++, securityGson);
				securityDb.getQueryUtil().handleInsertionOfClob(conn, ps, refreshToken, i++, securityGson);
				ps.setTimestamp(i++, tokenExpiration);
				ps.setTimestamp(i++, Utility.getCurrentSqlTimestampUTC());
				ps.setString(i++, subscriptionId);
				ps.executeUpdate();
			}
			if (!conn.getAutoCommit()) {
				conn.commit();
			}
		} catch (Exception e) {
			classLogger.error("Failed to write back the refreshed token for Microsoft Graph subscription {}.",
					subscriptionId, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, conn);
		}
	}

	/**
	 * Forgets a Microsoft Graph subscription. Only removes this deployment's record
	 * of it; Graph keeps sending until the subscription itself is deleted.
	 *
	 * @param subscriptionId the subscription to forget
	 * @throws SQLException if the delete cannot be run
	 */
	public static void deleteMicrosoftGraphSubscription(String subscriptionId) throws SQLException {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		Connection conn = null;
		try {
			conn = securityDb.getConnection();
			try (PreparedStatement ps = conn
					.prepareStatement("DELETE FROM " + MS_GRAPH_SUBSCRIPTION_TABLE + " WHERE SUBSCRIPTION_ID = ?")) {
				ps.setString(1, subscriptionId);
				ps.executeUpdate();
			}
			if (!conn.getAutoCommit()) {
				conn.commit();
			}
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, conn);
		}
	}
}
