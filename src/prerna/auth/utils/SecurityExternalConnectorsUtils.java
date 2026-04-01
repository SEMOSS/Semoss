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

import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IRDBMSEngine;
import prerna.query.querystruct.SelectQueryStruct;
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

}
