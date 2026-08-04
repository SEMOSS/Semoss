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
package prerna.reactor.agent.mcp;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

/**
 * Points a project's MCP at an external/remote MCP server instead of the tools
 * generated inside the project. The endpoint and optional credential are stored
 * on the project smss, so the change survives a restart and is pushed to the
 * cluster.
 */
public class SetRemoteMCPReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(SetRemoteMCPReactor.class);

	// only this reactor speaks these keys, so they are not in ReactorKeysEnum
	private static final String MCP_ENDPOINT_KEY = "mcpEndpoint";
	private static final String MCP_AUTH_SCHEME_KEY = "mcpAuthScheme";
	private static final String MCP_AUTH_TOKEN_KEY = "mcpAuthToken";

	public SetRemoteMCPReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), MCP_ENDPOINT_KEY, MCP_AUTH_SCHEME_KEY,
				MCP_AUTH_TOKEN_KEY };
		this.keyRequired = new int[] { 1, 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}

		String projectId = this.keyValue.get(ReactorKeysEnum.PROJECT.getKey());
		if (projectId == null || (projectId = projectId.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must input a project id");
		}
		projectId = SecurityProjectUtils.testUserProjectIdForAlias(user, projectId);
		if (!SecurityProjectUtils.userIsOwner(user, projectId)) {
			throw new IllegalArgumentException("Project " + projectId
					+ " does not exist or user does not have permissions to set the remote MCP. User must be the owner to perform this function.");
		}

		IProject project = Utility.getProject(projectId);
		if (project == null) {
			throw new IllegalArgumentException("Could not find project " + projectId);
		}

		String endpoint = trimToNull(this.keyValue.get(MCP_ENDPOINT_KEY));
		String authScheme = trimToNull(this.keyValue.get(MCP_AUTH_SCHEME_KEY));
		String authToken = trimToNull(this.keyValue.get(MCP_AUTH_TOKEN_KEY));

		Properties smssProp = project.getSmssProp();
		String existingToken = smssProp == null ? null : smssProp.getProperty(IProject.MCP_AUTH_TOKEN);

		// the FE renders the stored token as a mask so the secret is never shipped to
		// the client; getting it back means the user did not retype it
		if (Constants.SENSITIVE_INFO_MASK.equals(authToken)) {
			authToken = trimToNull(existingToken);
		}

		Map<String, String> smssUpdates = new HashMap<>();
		String successMessage;
		if (endpoint == null) {
			// blanking the endpoint disconnects and falls back to the project's own MCP
			smssUpdates.put(IProject.MCP_ENDPOINT, "");
			smssUpdates.put(IProject.MCP_AUTH_SCHEME, "");
			smssUpdates.put(IProject.MCP_AUTH_TOKEN, "");
			successMessage = "Successfully removed the remote MCP for the project";
		} else {
			validateEndpoint(endpoint);
			smssUpdates.put(IProject.MCP_ENDPOINT, endpoint);
			smssUpdates.put(IProject.MCP_AUTH_SCHEME, authScheme == null ? "" : authScheme);
			smssUpdates.put(IProject.MCP_AUTH_TOKEN, authToken == null ? "" : authToken);
			successMessage = "Successfully set the remote MCP for the project";
		}

		String smssFilePath = project.getSmssFilePath();
		try {
			Utility.changePropertiesFileValue(smssFilePath, smssUpdates, false);
		} catch (IOException e) {
			classLogger.error("Error occurred updating the project smss file for the remote MCP", e);
			throw new IllegalArgumentException(
					"Error occurred updating the project smss file for the remote MCP. Detailed error = "
							+ e.getMessage());
		}

		// keep the in-memory project in sync and drop the cached MCP handler so the
		// next MCP call is built against the new endpoint
		if (smssProp != null) {
			smssUpdates.forEach(smssProp::setProperty);
		}
		project.resetMCP();
		ClusterUtil.pushProjectSmss(projectId);

		Map<String, Object> retMap = new HashMap<>();
		retMap.put("project_id", projectId);
		retMap.put("project_remote_mcp", endpoint != null);
		retMap.put("project_remote_mcp_endpoint", endpoint == null ? "" : endpoint);
		retMap.put("project_remote_mcp_auth_scheme", authScheme == null ? "" : authScheme);
		retMap.put("project_remote_mcp_auth_token",
				(endpoint != null && authToken != null) ? Constants.SENSITIVE_INFO_MASK : "");

		NounMetadata noun = new NounMetadata(retMap, PixelDataType.MAP);
		noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage(successMessage));
		return noun;
	}

	/**
	 * Rejects anything that is not an absolute http/https url so a bad value cannot
	 * be written into the smss and break every later MCP call.
	 */
	private void validateEndpoint(String endpoint) {
		URI uri = null;
		try {
			uri = new URI(endpoint);
		} catch (URISyntaxException e) {
			throw new IllegalArgumentException("The remote MCP endpoint is not a valid url: " + endpoint);
		}
		String scheme = uri.getScheme();
		if (scheme == null || !(scheme.equalsIgnoreCase("http") || scheme.equalsIgnoreCase("https"))) {
			throw new IllegalArgumentException("The remote MCP endpoint must be an absolute http or https url");
		}
		if (uri.getHost() == null || uri.getHost().isBlank()) {
			throw new IllegalArgumentException("The remote MCP endpoint must include a host");
		}
	}

	private static String trimToNull(String value) {
		if (value == null) {
			return null;
		}
		String trimmed = value.trim();
		return trimmed.isEmpty() ? null : trimmed;
	}

	@Override
	public String getReactorDescription() {
		return """
				Points a project's MCP at an external/remote MCP server rather than the tools generated inside the project.

				Access: the caller must be the owner of the project.

				The endpoint and optional credential are written to the project smss (MCP_ENDPOINT, MCP_AUTH_SCHEME, \
				MCP_AUTH_TOKEN), the cached MCP handler is dropped so the next call uses the new endpoint, and the smss \
				is pushed to the cluster.

				Passing an empty mcpEndpoint clears all three properties, which disconnects the remote MCP and reverts \
				the project to serving its own generated tools.

				Because ProjectInfo returns the stored token as a mask rather than the secret, sending that same mask \
				value back as mcpAuthToken leaves the existing token untouched.

				Returns a map of project_id, project_remote_mcp, project_remote_mcp_endpoint, \
				project_remote_mcp_auth_scheme, and project_remote_mcp_auth_token (masked, never the secret).
				""";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (ReactorKeysEnum.PROJECT.getKey().equals(key)) {
			return "Id of the project to set the remote MCP on";
		} else if (MCP_ENDPOINT_KEY.equals(key)) {
			return "The http/https url of the remote MCP server; pass empty to disconnect the remote MCP";
		} else if (MCP_AUTH_SCHEME_KEY.equals(key)) {
			return "Optional authentication scheme for the remote MCP, such as Bearer or Basic; defaults to Bearer when a token is provided";
		} else if (MCP_AUTH_TOKEN_KEY.equals(key)) {
			return "Optional credential for the remote MCP; pass the masked value returned by ProjectInfo to leave the stored token unchanged";
		}
		return super.getDescriptionForKey(key);
	}

}
