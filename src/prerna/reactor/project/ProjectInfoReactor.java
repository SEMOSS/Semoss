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
package prerna.reactor.project;

import java.util.List;
import java.util.Map;

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

public class ProjectInfoReactor extends AbstractReactor {

	public ProjectInfoReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.META_KEYS.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = this.keyValue.get(this.keysToGet[0]);

		if (projectId == null || (projectId = projectId.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must input an project id");
		}

		boolean hasAccess = false;
		List<Map<String, Object>> baseInfo = null;
		// make sure valid id for user
		projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
		if (SecurityProjectUtils.userCanViewProject(this.insight.getUser(), projectId)) {
			hasAccess = true;
			// user has access!
			baseInfo = SecurityProjectUtils.getUserProjectList(this.insight.getUser(), projectId);
		} else if (SecurityProjectUtils.projectIsDiscoverable(projectId)) {
			baseInfo = SecurityProjectUtils.getDiscoverableProjectList(projectId, null);
		} else {
			// you dont have access
			throw new IllegalArgumentException("Project does not exist or user does not have access to the project");
		}

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

		if (hasAccess) {
			addRemoteMCPInfo(projectInfo, projectId);
		}

		return new NounMetadata(projectInfo, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.PROJECT_INFO);
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

	private List<String> getMetaKeys() {
		GenRowStruct grs = this.store.getGenRowStruct(ReactorKeysEnum.META_KEYS.getKey());
		if (grs != null && !grs.isEmpty()) {
			return grs.getAllStrValues();
		}

		return null;
	}

	@Override
	public String getReactorDescription() {
		return """
				Returns the full settings/metadata record for a single project.

				Access: the project must be one the current user can view, or the project must be marked discoverable. \
				If neither is true the reactor throws "Project does not exist or user does not have access to the project". \
				Use AdminProjectInfo if you need to read a project without regard to the caller's permissions.

				Inputs:
				  project  (required) - the project id.
				  metaKeys (optional) - restrict the returned metadata tags to this list; omit to return all metadata.

				Returns a single map (PROJECT_INFO/CUSTOM_DATA_STRUCTURE) containing:
				  Core project fields: project_id, project_name, project_display_name, project_type, project_cost,
				    project_global, project_discoverable, project_is_template, project_catalog_name, project_created_by, project_created_by_type,
				    project_date_created, project_date_last_edited, low_project_name, project_description.
				  Portal fields: project_has_portal, project_portal_name, project_portal_published_date, project_published_user,
				    project_published_user_type, project_reactors_compiled_date, project_reactors_compiled_user,
				    project_reactors_compiled_user_type. When project_has_portal is true, project_portal_url is also returned.
				  Permission fields (relative to the calling user): user_permission, group_permission, permission, project_favorite.
				  Metadata: one entry per metadata tag (e.g. tag, domain, etc.); a tag with multiple values is returned as a list.
				  GitHub: if the project is linked to a GitHub repo, the link fields are returned prefixed with gh_ (e.g. gh_repo).
				  Remote MCP (editors only): project_remote_mcp is true when the project points at an external MCP server, in \
				    which case project_remote_mcp_endpoint and project_remote_mcp_auth_scheme are also returned. \
				    project_remote_mcp_auth_token is a fixed mask when a credential is stored and empty when it is not - the \
				    stored credential is never returned. Use SetRemoteMCP to change these.
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
