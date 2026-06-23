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
import java.util.stream.Collectors;

import prerna.auth.utils.SecurityExternalConnectorsUtils;
import prerna.auth.utils.SecurityProjectUtils;
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

		List<Map<String, Object>> baseInfo = null;
		// make sure valid id for user
		projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
		if (SecurityProjectUtils.userCanViewProject(this.insight.getUser(), projectId)) {
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
		// also return the portal url if there is a portal
		if (Boolean.parseBoolean(projectInfo.get("project_has_portal") + "")) {
			String url = Utility.getApplicationUrl() + "/" + Utility.getPublicHomeFolder() + "/" + projectId + "/"
					+ Constants.PORTALS_FOLDER + "/";
			projectInfo.put("project_portal_url", url);
		}
		// append any gh keys with gh_ into the project info map
		Map<String, Object> githubConnector = SecurityExternalConnectorsUtils.getGitHubProjectLink(projectId);
		if (githubConnector != null && !githubConnector.isEmpty()) {
			projectInfo.putAll(githubConnector.entrySet().stream()
					.collect(Collectors.toMap(entry -> "gh_" + entry.getKey(), entry -> entry.getValue())));
		}

		return new NounMetadata(projectInfo, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.PROJECT_INFO);
	}

	private List<String> getMetaKeys() {
		GenRowStruct grs = this.store.getGenRowStruct(ReactorKeysEnum.META_KEYS.getKey());
		if (grs != null && !grs.isEmpty()) {
			return grs.getAllStrValues();
		}

		return null;
	}

}