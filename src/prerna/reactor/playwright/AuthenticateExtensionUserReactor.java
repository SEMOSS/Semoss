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
package prerna.reactor.playwright;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.auth.utils.SecurityUserAccessKeyUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Authenticates Chrome Extension users via API keys and returns accessible projects.
 * 
 * <p>Pixel Syntax:</p>
 * <pre>AuthenticateExtensionUser(clientKey=[string], secretKey=[string])</pre>
 * 
 * <p>Parameters:</p>
 * <ul>
 *   <li><b>clientKey</b> - The client/access key from Semoss user settings (required)</li>
 *   <li><b>secretKey</b> - The secret key from Semoss user settings (required)</li>
 * </ul>
 * 
 * <p>Returns:</p>
 * <pre>
 * {
 *   "success": true,
 *   "userId": "user123",
 *   "userName": "John Doe",
 *   "userEmail": "john@example.com",
 *   "projects": [
 *     {
 *       "id": "project-uuid",
 *       "name": "ProjectAlias",
 *       "displayName": "Project Display Name",
 *       "canEdit": true
 *     }
 *   ]
 * }
 * </pre>
 * 
 * <p>Note: Uses string literals "clientKey" and "secretKey" as ReactorKeysEnum 
 * does not contain CLIENT_KEY or SECRET_KEY constants.</p>
 */
public class AuthenticateExtensionUserReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(AuthenticateExtensionUserReactor.class);

	public AuthenticateExtensionUserReactor() {
		// Note: String literals used as CLIENT_KEY and SECRET_KEY don't exist in ReactorKeysEnum
		this.keysToGet = new String[] { 
			"clientKey", 
			"secretKey" 
		};
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String clientKey = this.keyValue.get("clientKey");
		String secretKey = this.keyValue.get("secretKey");

		classLogger.info("Authenticating extension user with client key");

		// Validate keys and get user
		User user = null;
		try {
			user = SecurityUserAccessKeyUtils.validateKeysAndReturnUser(clientKey, secretKey);
		} catch (IllegalAccessException e) {
			classLogger.error("Authentication failed for client key: {}", clientKey, e);
			throw new IllegalArgumentException("Invalid credentials: " + e.getMessage());
		}

		if (user == null) {
			throw new IllegalArgumentException("Invalid credentials");
		}

		// Get user's accessible projects
		List<String> projectIds = SecurityProjectUtils.getFullUserProjectIds(user);

		// Get user info from access token
		AccessToken token = user.getPrimaryLoginToken();
		String userId = token != null ? token.getId() : "unknown";
		String userName = token != null ? token.getName() : "Unknown User";
		String userEmail = token != null ? token.getEmail() : "";

		// Build response with user info and projects
		JSONObject response = new JSONObject();
		response.put("success", true);
		response.put("userId", userId);
		response.put("userName", userName);
		response.put("userEmail", userEmail);

		// Build projects array with details
		JSONArray projects = new JSONArray();
		for (String projectId : projectIds) {
			try {
				JSONObject project = new JSONObject();
				project.put("id", projectId);
				project.put("name", SecurityProjectUtils.getProjectAliasForId(projectId));
				
				String displayName = SecurityProjectUtils.getProjectDisplayNameForId(projectId);
				project.put("displayName", displayName != null ? displayName : SecurityProjectUtils.getProjectAliasForId(projectId));
				
				// Check if user has edit permission
				boolean canEdit = SecurityProjectUtils.userCanEditProject(user, projectId);
				project.put("canEdit", canEdit);
				
				projects.put(project);
			} catch (Exception e) {
				classLogger.warn("Could not load details for project: {}", projectId, e);
			}
		}
		response.put("projects", projects);

		classLogger.info("Successfully authenticated user: {} with {} accessible projects", user.getPrimaryLogin(), projectIds.size());

		return new NounMetadata(response, PixelDataType.JSON_OBJECT);
	}

	@Override
	public String getReactorDescription() {
		return "Authenticates Chrome Extension user using API keys and returns accessible projects";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals("clientKey")) {
			return "The client/access key from Semoss user settings";
		} else if (key.equals("secretKey")) {
			return "The secret key from Semoss user settings";
		}
		return super.getDescriptionForKey(key);
	}
}
