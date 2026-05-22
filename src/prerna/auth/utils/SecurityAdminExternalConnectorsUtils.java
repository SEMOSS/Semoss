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
import java.sql.Statement;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.util.ConnectionUtils;
import prerna.util.SystemEngineRegistry;

public class SecurityAdminExternalConnectorsUtils extends AbstractSecurityUtils {

	private static final Logger classLogger = LogManager.getLogger(SecurityAdminUtils.class);

	private static SecurityAdminExternalConnectorsUtils instance = new SecurityAdminExternalConnectorsUtils();

	private static final String SALESFORCE_CONNECTIONS_TABLE = "SALESFORCE_CONNECTIONS";
	private static final String INSERT_CONNECTION_SQL = "INSERT INTO " + SALESFORCE_CONNECTIONS_TABLE
			+ " (ID, ALIAS, CLIENTID, CLIENTSECRET) VALUES (?, ?, ?, ?)";
	private static final String SERVICENOW_CONNECTIONS_TABLE = "SERVICENOW_CONNECTIONS";
	private static final String INSERT_SERVICENOW_CONNECTION_SQL = "INSERT INTO " + SERVICENOW_CONNECTIONS_TABLE
			+ " (ID, INSTANCEURL, ALIAS, CLIENTID, CLIENTSECRET, USERPROFILEURL) VALUES (?, ?, ?, ?, ?, ?)";

	// Jira connections table and insert statement
	private static final String JIRA_CONNECTIONS_TABLE = "JIRA_CONNECTIONS";
	private static final String INSERT_JIRA_CONNECTION_SQL = "INSERT INTO " + JIRA_CONNECTIONS_TABLE
			+ " (ID, ALIAS, CLIENTID, CLIENTSECRET, SCOPE, USERPROFILEURL) VALUES (?, ?, ?, ?, ?, ?)";

	private SecurityAdminExternalConnectorsUtils() {

	}

	public static SecurityAdminExternalConnectorsUtils getInstance(User user) {
		if (user == null) {
			return null;
		}
		if (userIsAdmin(user)) {
			return instance;
		}
		return null;
	}

	/**
	 * Check if the user is an admin
	 * 
	 * @param userId String representing the id of the user to check
	 */
	public static Boolean userIsAdmin(User user) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__ID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__ID", "==", getUserFiltersQs(user)));
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter("SMSS_USER__ADMIN", "==", true, PixelDataType.BOOLEAN));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			return wrapper.hasNext();
		} catch (Exception e) {
			classLogger.error("Failed to verify whether the user has admin access", e);
		}

		return false;
	}

	/**
	 * Inserts a new Salesforce connection into the security database.
	 * <p>
	 * This method validates required inputs, checks for duplicate {@code CLIENTID}
	 * or {@code ALIAS}, then inserts a new row using prepared statements.
	 * </p>
	 *
	 * @param clientId     Salesforce connected-app client id
	 * @param clientSecret Salesforce connected-app client secret
	 * @param alias        unique alias for the saved connection
	 * @return generated Salesforce connection id
	 */
	public String insertSalesforceConnection(String clientId, String clientSecret, String alias) {
		if (clientId == null || (clientId = clientId.trim()).isEmpty()) {
			throw new IllegalArgumentException("Client id must not be empty.");
		}
		if (clientSecret == null || (clientSecret = clientSecret.trim()).isEmpty()) {
			throw new IllegalArgumentException("Client secret must not be empty.");
		}
		if (alias == null || (alias = alias.trim()).isEmpty()) {
			throw new IllegalArgumentException("Alias must not be empty.");
		}

		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String generatedId = UUID.randomUUID().toString();

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SALESFORCE_CONNECTIONS__ID", "id"));
		qs.addSelector(new QueryColumnSelector("SALESFORCE_CONNECTIONS__ALIAS", "alias"));
		OrQueryFilter or = new OrQueryFilter();
		or.addFilter(SimpleQueryFilter.makeColToValFilter("SALESFORCE_CONNECTIONS__ID", "==", clientId));
		or.addFilter(SimpleQueryFilter.makeColToValFilter("SALESFORCE_CONNECTIONS__ALIAS", "==", alias));
		qs.addExplicitFilter(or);

		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (wrapper.hasNext()) {
				throw new IllegalArgumentException(
						"A Salesforce connection with the same alias or clientId already exists.");
			}
		} catch (Exception e) {
			classLogger.error(
					"An error occurred attemping to determine if clientId or alias already exist for salesforce connection",
					e);
			throw new IllegalArgumentException(
					"An error occurred attemping to determine if clientId or alias already exist for salesforce connection",
					e);
		}

		Connection conn = null;
		Statement ps = null;
		try {
			conn = securityDb.getConnection();
			ps = conn.prepareStatement(INSERT_CONNECTION_SQL);
			try (PreparedStatement insertStmt = conn.prepareStatement(INSERT_CONNECTION_SQL)) {
				insertStmt.setString(1, generatedId);
				insertStmt.setString(2, alias);
				insertStmt.setString(3, clientId);
				insertStmt.setString(4, clientSecret);

				int rowsInserted = insertStmt.executeUpdate();
				if (rowsInserted != 1) {
					throw new SemossPixelException("Unable to insert Salesforce connection.");
				}

				if (!conn.getAutoCommit()) {
					conn.commit();
				}
				return generatedId;
			}
		} catch (SQLException e) {
			classLogger.error("Failed to insert Salesforce connection.", e);
			throw new SemossPixelException("Unable to insert Salesforce connection: " + e.getMessage(), e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, conn, ps);
		}
	}

	/**
	 * Inserts a new ServiceNow connection into the security database.
	 * <p>
	 * This method validates required inputs, checks for duplicate {@code CLIENTID}
	 * or {@code ALIAS}, then inserts a new row using prepared statements.
	 * </p>
	 *
	 * @param instanceUrl    ServiceNow instance URL
	 * @param alias          unique alias for the saved connection
	 * @param clientId       ServiceNow client id
	 * @param clientSecret   ServiceNow client secret
	 * @param userProfileUrl ServiceNow user profile URL
	 * @return generated ServiceNow connection id
	 */
	public String insertServiceNowConnection(String instanceUrl, String alias, String clientId, String clientSecret,
			String userProfileUrl) {
		if (instanceUrl == null || (instanceUrl = instanceUrl.trim()).isEmpty()) {
			throw new IllegalArgumentException("Instance URL must not be empty.");
		}
		if (alias == null || (alias = alias.trim()).isEmpty()) {
			throw new IllegalArgumentException("Alias must not be empty.");
		}
		if (clientId == null || (clientId = clientId.trim()).isEmpty()) {
			throw new IllegalArgumentException("Client id must not be empty.");
		}
		if (clientSecret == null || (clientSecret = clientSecret.trim()).isEmpty()) {
			throw new IllegalArgumentException("Client secret must not be empty.");
		}
		if (userProfileUrl == null || (userProfileUrl = userProfileUrl.trim()).isEmpty()) {
			throw new IllegalArgumentException("User profile URL must not be empty.");
		}

		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String generatedId = UUID.randomUUID().toString();

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SERVICENOW_CONNECTIONS__ID", "id"));
		qs.addSelector(new QueryColumnSelector("SERVICENOW_CONNECTIONS__ALIAS", "alias"));
		OrQueryFilter or = new OrQueryFilter();
		or.addFilter(SimpleQueryFilter.makeColToValFilter("SERVICENOW_CONNECTIONS__CLIENTID", "==", clientId));
		or.addFilter(SimpleQueryFilter.makeColToValFilter("SERVICENOW_CONNECTIONS__ALIAS", "==", alias));
		qs.addExplicitFilter(or);

		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (wrapper.hasNext()) {
				throw new IllegalArgumentException(
						"A ServiceNow connection with the same alias or clientId already exists.");
			}
		} catch (Exception e) {
			classLogger.error(
					"An error occurred attemping to determine if clientId or alias already exist for ServiceNow connection",
					e);
			throw new IllegalArgumentException(
					"An error occurred attemping to determine if clientId or alias already exist for ServiceNow connection",
					e);
		}

		Connection conn = null;
		Statement ps = null;
		try {
			conn = securityDb.getConnection();
			ps = conn.prepareStatement(INSERT_SERVICENOW_CONNECTION_SQL);
			try (PreparedStatement insertStmt = conn.prepareStatement(INSERT_SERVICENOW_CONNECTION_SQL)) {
				insertStmt.setString(1, generatedId);
				insertStmt.setString(2, instanceUrl);
				insertStmt.setString(3, alias);
				insertStmt.setString(4, clientId);
				insertStmt.setString(5, clientSecret);
				insertStmt.setString(6, userProfileUrl);

				int rowsInserted = insertStmt.executeUpdate();
				if (rowsInserted != 1) {
					throw new SemossPixelException("Unable to insert ServiceNow connection.");
				}

				if (!conn.getAutoCommit()) {
					conn.commit();
				}
				return generatedId;
			}
		} catch (SQLException e) {
			classLogger.error("Failed to insert ServiceNow connection.", e);
			throw new SemossPixelException("Unable to insert ServiceNow connection: " + e.getMessage(), e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, conn, ps);
		}
	}

	/**
	 * Inserts a new Jira connection into the security database.
	 * <p>
	 * This method validates required inputs, checks for a duplicate
	 * {@code CLIENTID} or {@code ALIAS}, then inserts a new row using prepared
	 * statements.
	 * </p>
	 *
	 * @param alias          unique alias for the saved connection
	 * @param clientId       Jira connected-app client id
	 * @param clientSecret   Jira connected-app client secret
	 * @param scope          OAuth scope for the Jira connection
	 * @param userProfileUrl URL used to retrieve the Jira user profile
	 * @return generated Jira connection id
	 */
	public String insertJiraConnection(String alias, String clientId, String clientSecret, String scope,
			String userProfileUrl) {
		if (alias == null || (alias = alias.trim()).isEmpty()) {
			throw new IllegalArgumentException("Alias must not be empty.");
		}
		if (clientId == null || (clientId = clientId.trim()).isEmpty()) {
			throw new IllegalArgumentException("Client id must not be empty.");
		}
		if (clientSecret == null || (clientSecret = clientSecret.trim()).isEmpty()) {
			throw new IllegalArgumentException("Client secret must not be empty.");
		}
		if (scope == null || (scope = scope.trim()).isEmpty()) {
			throw new IllegalArgumentException("Scope must not be empty.");
		}
		if (userProfileUrl == null || (userProfileUrl = userProfileUrl.trim()).isEmpty()) {
			throw new IllegalArgumentException("User profile URL must not be empty.");
		}

		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String generatedId = UUID.randomUUID().toString();

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("JIRA_CONNECTIONS__ID", "id"));
		qs.addSelector(new QueryColumnSelector("JIRA_CONNECTIONS__ALIAS", "alias"));
		OrQueryFilter or = new OrQueryFilter();
		or.addFilter(SimpleQueryFilter.makeColToValFilter("JIRA_CONNECTIONS__CLIENTID", "==", clientId));
		or.addFilter(SimpleQueryFilter.makeColToValFilter("JIRA_CONNECTIONS__ALIAS", "==", alias));
		qs.addExplicitFilter(or);

		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (wrapper.hasNext()) {
				throw new IllegalArgumentException("A Jira connection with the same clientId or alias already exists.");
			}
		} catch (Exception e) {
			classLogger.error(
					"An error occurred attempting to determine if clientId or alias already exists for Jira connection",
					e);
			throw new IllegalArgumentException(
					"An error occurred attempting to determine if clientId or alias already exists for Jira connection",
					e);
		}

		Connection conn = null;
		Statement ps = null;
		try {
			conn = securityDb.getConnection();
			ps = conn.prepareStatement(INSERT_JIRA_CONNECTION_SQL);
			try (PreparedStatement insertStmt = conn.prepareStatement(INSERT_JIRA_CONNECTION_SQL)) {
				insertStmt.setString(1, generatedId);
				insertStmt.setString(2, alias);
				insertStmt.setString(3, clientId);
				insertStmt.setString(4, clientSecret);
				insertStmt.setString(5, scope);
				insertStmt.setString(6, userProfileUrl);

				int rowsInserted = insertStmt.executeUpdate();
				if (rowsInserted != 1) {
					throw new SemossPixelException("Unable to insert Jira connection.");
				}

				if (!conn.getAutoCommit()) {
					conn.commit();
				}
				return generatedId;
			}
		} catch (SQLException e) {
			classLogger.error("Failed to insert Jira connection.", e);
			throw new SemossPixelException("Unable to insert Jira connection: " + e.getMessage(), e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, conn, ps);
		}
	}
}
