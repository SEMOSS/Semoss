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
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;

public class SecurityExternalConnectorsUtils extends AbstractSecurityUtils {

	private static final Logger classLogger = LogManager.getLogger(SecurityExternalConnectorsUtils.class);

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
}
