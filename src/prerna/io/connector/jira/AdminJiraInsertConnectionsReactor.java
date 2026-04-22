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
package prerna.io.connector.jira;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminExternalConnectorsUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class AdminJiraInsertConnectionsReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(AdminJiraInsertConnectionsReactor.class);

	private static final String ALIAS = "alias";
	private static final String CLIENT_ID = "clientId";
	private static final String CLIENT_SECRET = "clientSecret";
	private static final String SCOPE = "scope";
	private static final String USER_PROFILE_URL = "userProfileUrl";

	public AdminJiraInsertConnectionsReactor() {
		this.keysToGet = new String[] { ALIAS, CLIENT_ID, CLIENT_SECRET, SCOPE, USER_PROFILE_URL };
		this.keyRequired = new int[] { 1, 1, 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		User user = this.insight.getUser();
		SecurityAdminExternalConnectorsUtils adminUtils = SecurityAdminExternalConnectorsUtils.getInstance(user);
		if (adminUtils == null) {
			throw new IllegalArgumentException("User must be an admin to perform this function");
		}
		this.organizeKeys();

		String alias = this.keyValue.get(this.keysToGet[0]);
		String clientId = this.keyValue.get(this.keysToGet[1]);
		String clientSecret = this.keyValue.get(this.keysToGet[2]);
		String scope = this.keyValue.get(this.keysToGet[3]);
		String userProfileUrl = this.keyValue.get(this.keysToGet[4]);

		Map<Object, Object> responseMap = new HashMap<>();
		try {
			String connectionId = adminUtils.insertJiraConnection(alias, clientId, clientSecret, scope, userProfileUrl);
			responseMap.put("id", connectionId);
			responseMap.put("success", connectionId != null && !connectionId.isEmpty());
			return new NounMetadata(responseMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (Exception e) {
			classLogger.error("Failed to insert Jira connection.", e);
			String error = "Error inserting Jira connection: " + e.getMessage();
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(error));
		}
	}

	@Override
	public String getReactorDescription() {
		return "Creates and stores a Jira OAuth connection configuration (client id, client secret, scope, and alias).";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (ALIAS.equals(key)) {
			return "Required unique alias used to identify this saved Jira connection.";
		} else if (CLIENT_ID.equals(key)) {
			return "Required Atlassian OAuth client id from the Jira app configuration.";
		} else if (CLIENT_SECRET.equals(key)) {
			return "Required Atlassian OAuth client secret from the Jira app configuration.";
		} else if (SCOPE.equals(key)) {
			return "Required space-separated Atlassian OAuth scopes (for example, read:jira-user read:jira-work write:jira-work).";
		} else if (USER_PROFILE_URL.equals(key)) {
			return "Required full HTTPS user profile endpoint URL for this Jira connector.";
		}
		return super.getDescriptionForKey(key);
	}
}
