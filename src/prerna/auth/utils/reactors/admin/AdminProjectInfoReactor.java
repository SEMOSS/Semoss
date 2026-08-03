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
package prerna.auth.utils.reactors.admin;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityExternalConnectorsUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class AdminProjectInfoReactor extends AbstractReactor {

	public AdminProjectInfoReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.META_KEYS.getKey() };
	}

	@Override
	public NounMetadata execute() {
		User user = this.insight.getUser();
		SecurityAdminUtils adminUtils = SecurityAdminUtils.getInstance(user);
		if (adminUtils == null) {
			throw new IllegalArgumentException("User must be an admin to perform this function");
		}
		organizeKeys();
		String projectId = this.keyValue.get(this.keysToGet[0]);

		if (projectId == null || (projectId = projectId.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must input an project id");
		}

		List<Map<String, Object>> baseInfo = adminUtils.getAllProjectSettings(Arrays.asList(projectId), null, null,
				null, null, null);
		if (baseInfo == null || baseInfo.isEmpty()) {
			throw new IllegalArgumentException("Could not find any project data");
		}

		// we filtered to a single project
		Map<String, Object> projectInfo = baseInfo.get(0);
		projectInfo.putAll(SecurityProjectUtils.getAggregateProjectMetadata(projectId, getMetaKeys(), true));

		String url = Utility.getApplicationUrl() + "/" + Utility.getPublicHomeFolder() + "/" + projectId + "/"
				+ Constants.PORTALS_FOLDER + "/";
		projectInfo.put("project_portal_url", url);

		// append any gh keys with gh_ into the project info map
		Map<String, Object> githubConnector = SecurityExternalConnectorsUtils.getGitHubProjectLink(projectId);
		if (githubConnector != null && !githubConnector.isEmpty()) {
			githubConnector.forEach((key, value) -> {
				if (value != null) {
					projectInfo.put("gh_" + key, value);
				}
			});
		}

		addRemoteMCPInfo(projectInfo, projectId);

		return new NounMetadata(projectInfo, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.PROJECT_INFO);
	}

	private List<String> getMetaKeys() {
		GenRowStruct grs = this.store.getGenRowStruct(ReactorKeysEnum.META_KEYS.getKey());
		if (grs != null && !grs.isEmpty()) {
			return grs.getAllStrValues();
		}

		return null;
	}

	private void addRemoteMCPInfo(Map<String, Object> projectInfo, String projectId) {
		IProject project = Utility.getProject(projectId);
		if (project == null) {
			return;
		}

		String endpoint = project.getRemoteMCPEndpoint();
		if (endpoint == null) {
			projectInfo.put("project_remote_mcp", false);
			return;
		}

		String authScheme = project.getRemoteMCPAuthScheme();

		projectInfo.put("project_remote_mcp", true);
		projectInfo.put("project_remote_mcp_endpoint", endpoint);
		projectInfo.put("project_remote_mcp_auth_scheme", authScheme == null ? "" : authScheme);
		projectInfo.put("project_remote_mcp_auth_token", Constants.SENSITIVE_INFO_MASK);
	}

	@Override
	public String getReactorDescription() {
		return """
				Admin-only: returns the full settings/metadata record for a single project regardless of the caller's \
				project permissions. The calling user must be an application admin or the reactor throws \
				"User must be an admin to perform this function".

				This is the admin counterpart to ProjectInfo, which instead enforces the caller's view/discoverable access.

				Inputs:
				  project  (required) - the project id.
				  metaKeys (optional) - restrict the returned metadata tags to this list; omit to return all metadata.

				Returns a single map (PROJECT_INFO/CUSTOM_DATA_STRUCTURE) containing:
				  Core project fields: project_id, project_name, project_display_name, project_type, project_cost,
				    project_global, project_discoverable, project_catalog_name, project_created_by, project_created_by_type,
				    project_date_created, project_date_last_edited, low_project_name.
				  Portal fields: project_has_portal, project_portal_name, project_portal_published_date, project_published_user,
				    project_published_user_type, project_reactors_compiled_date, project_reactors_compiled_user,
				    project_reactors_compiled_user_type.
				  Metadata: one entry per metadata tag (e.g. tag, domain, etc.); a tag with multiple values is returned as a list.
				  GitHub: if the project is linked to a GitHub repo, the link fields are returned prefixed with gh_ (e.g. gh_repo).
				""";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (ReactorKeysEnum.PROJECT.getKey().equals(key)) {
			return "Id of the project to look up";
		} else if (ReactorKeysEnum.META_KEYS.getKey().equals(key)) {
			return "Optional list of metadata tag names to return for the project; omit to return all metadata tags";
		}
		return super.getDescriptionForKey(key);
	}

}